import Denque from 'denque'
import logger from 'src/lib/logger'

function createRateLimiter(
  logger: pino.Logger<unknown>,
  opts = {
    concurrency: 1,
    ms: 0,
    retries: 3,
    backoffStart: 1000,
    backoffMultiplier: 2,
  }
) {
  const { concurrency, ms, retries, backoffStart, backoffMultiplier } = opts
  const qq = new Map<string | symbol, Denque<Task>[]>()
  const process = async (method: string | symbol, q: Denque<Task>) => {
    if (q.isEmpty()) return
    await q.peekBack()!()
    setTimeout(() => {
      q.pop()
      void process(method, q)
    }, ms)
  }
  const enqueue = async (method: string | symbol, f: () => Promise<unknown>) =>
    new Promise<unknown>((resolve, reject) => {
      if (!qq.has(method))
        qq.set(
          method,
          Array.from({
            length: concurrency,
          }).map(() => new Denque<Task>([]))
        )
      const qs = qq.get(method)!
      const lengths = qs.map((q) => q.length)
      const q = qs[lengths.indexOf(Math.min(...lengths))]
      q.unshift(async () => {
        for (let i = 0; i < retries; i++) {
          let waitBeforeRetry = backoffStart
          try {
            const result = await f()
            resolve(result)
            return
          } catch (err) {
            if (i === retries - 1) return reject(err)
            logger.error(err)
            logger.error(`Retrying ${String(method)} in ${waitBeforeRetry}ms`)
            await new Promise((resolve) => setTimeout(resolve, waitBeforeRetry))
            waitBeforeRetry *= backoffMultiplier
          }
        }
      }) === 1 && setTimeout(() => void process(method, q), 0)
    })
  return { process, enqueue }
}

function createRLClient() {
    const rl = createRateLimiter(logger, {
        concurrency,
        ms: delay || 0,
        retries: +process.env.MINIQUEUE_RETRIES || 3,
        backoffStart: +process.env.MINIQUEUE_BACKOFF_START || 1000,
        backoffMultiplier: +process.env.MINIQUEUE_BACKOFF_MULTIPLIER || 2,
    })
    const rlClient = new Proxy<InterfaceClient>(client as InterfaceClient, {
        get(client: InterfaceClient, method: string | symbol) {
            const value = Reflect.get(client, method, client)
            if (!method.endsWith('Async')) {
                return value
            }
            return async function () {
                return await rl.enqueue(method, async () => await value(...arguments))
            }
        },
    })
    return rlClient
}
