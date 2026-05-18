import fs from 'node:fs/promises'
import { existsSync } from 'node:fs'
import path from 'node:path'
import { Worker } from 'node:worker_threads'
import { fileURLToPath } from 'node:url'

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url))
const ROOT_DIR = path.resolve(SCRIPT_DIR, '..')
const TMP_DIR = path.join(ROOT_DIR, 'tmp')
const REPORT_PATH = path.join(TMP_DIR, 'trace_odelta_validation.md')
const JSON_REPORT_PATH = path.join(TMP_DIR, 'trace_odelta_validation.json')
const WORKER_URL = new URL('./cynos_worker_runtime.mjs', import.meta.url)

const SCENARIOS = {
  issue_stream_all: 'Issue Feed · 7-way left join · no order/limit',
  project_board_stream_all: 'Project Board · 6-way left join · no order/limit',
}

function readIntEnv(name, fallback) {
  const value = Number.parseInt(process.env[name] ?? '', 10)
  return Number.isFinite(value) && value > 0 ? value : fallback
}

function readFloatListEnv(name, fallback) {
  const raw = String(process.env[name] ?? '').trim()
  if (!raw) return fallback
  const values = raw
    .split(',')
    .map((value) => Number.parseFloat(value.trim()))
    .filter((value) => Number.isFinite(value) && value > 0)
  return values.length > 0 ? values : fallback
}

function median(values) {
  if (values.length === 0) return NaN
  const sorted = [...values].sort((left, right) => left - right)
  const middle = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 0
    ? (sorted[middle - 1] + sorted[middle]) / 2
    : sorted[middle]
}

function mean(values) {
  if (values.length === 0) return NaN
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

function formatMs(value) {
  if (!Number.isFinite(value)) return 'N/A'
  if (value < 1) return `${value.toFixed(3)} ms`
  return `${value.toFixed(2)} ms`
}

function formatCount(value) {
  if (!Number.isFinite(value)) return 'N/A'
  return Number(value).toLocaleString()
}

function factorText(base, next) {
  if (!Number.isFinite(base) || !Number.isFinite(next) || base === 0) {
    return 'N/A'
  }
  return `×${(next / base).toFixed(2)}`
}

function createDeferred() {
  let resolve
  let reject
  const promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

class WorkerClient {
  constructor(env, timeoutMs) {
    this.worker = new Worker(WORKER_URL, {
      type: 'module',
      env,
    })
    this.timeoutMs = timeoutMs
    this.inbox = []
    this.waiters = []

    this.worker.on('message', (message) => {
      if (message.type === 'error') {
        const error = new Error(`${message.context}: ${message.message}`)
        error.stack = message.stack
        const waiter = this.waiters.shift()
        if (waiter) {
          waiter.reject(error)
        } else {
          this.inbox.push({ type: '__error__', error })
        }
        return
      }

      const waiterIndex = this.waiters.findIndex((waiter) => waiter.match(message))
      if (waiterIndex >= 0) {
        const [waiter] = this.waiters.splice(waiterIndex, 1)
        waiter.resolve(message)
      } else {
        this.inbox.push(message)
      }
    })
  }

  post(message) {
    this.worker.postMessage(message)
  }

  waitFor(match, timeoutMs = this.timeoutMs) {
    const bufferedIndex = this.inbox.findIndex(
      (message) => message.type === '__error__' || match(message),
    )
    if (bufferedIndex >= 0) {
      const [message] = this.inbox.splice(bufferedIndex, 1)
      if (message.type === '__error__') {
        return Promise.reject(message.error)
      }
      return Promise.resolve(message)
    }

    const deferred = createDeferred()
    const timer = setTimeout(() => {
      const index = this.waiters.findIndex((entry) => entry === waiter)
      if (index >= 0) this.waiters.splice(index, 1)
      deferred.reject(new Error(`Timed out after ${timeoutMs}ms waiting for worker message.`))
    }, timeoutMs)

    const waiter = {
      match,
      resolve: (message) => {
        clearTimeout(timer)
        deferred.resolve(message)
      },
      reject: (error) => {
        clearTimeout(timer)
        deferred.reject(error)
      },
    }

    this.waiters.push(waiter)
    return deferred.promise
  }

  async terminate() {
    await this.worker.terminate()
  }
}

function summarizeRuns(runs) {
  const metric = (selector) => median(runs.map(selector))
  const avg = (selector) => mean(runs.map(selector))
  return {
    roundCount: runs.length,
    initMsMedian: metric((run) => run.initMs),
    initMsMean: avg((run) => run.initMs),
    initialRowCountMedian: metric((run) => run.initial.rowCount),
    socket: {
      workerMsMedian: metric((run) => run.socket.workerMs),
      workerMsMean: avg((run) => run.socket.workerMs),
      changeCountMedian: metric((run) => run.socket.changeCount),
      deltaAddedMedian: metric((run) => run.socket.deltaAddedCount),
      deltaRemovedMedian: metric((run) => run.socket.deltaRemovedCount),
      queryOnTableChangeMsMedian: metric((run) => run.socket.queryOnTableChangeMs),
      observableInternalMsMedian: metric((run) => run.socket.observableInternalMs),
      ivmBridgeMsMedian: metric((run) => run.socket.ivmBridgeMs),
      callbackCallMsMedian: metric((run) => run.socket.callbackCallMs),
      callbackTotalMsMedian: metric((run) => run.socket.callbackTotalMs),
      commitCallMsMedian: metric((run) => run.socket.commitCallMs),
    },
    api: {
      workerMsMedian: metric((run) => run.api.workerMs),
      workerMsMean: avg((run) => run.api.workerMs),
      changeCountMedian: metric((run) => run.api.changeCount),
      deltaAddedMedian: metric((run) => run.api.deltaAddedCount),
      deltaRemovedMedian: metric((run) => run.api.deltaRemovedCount),
      queryOnTableChangeMsMedian: metric((run) => run.api.queryOnTableChangeMs),
      observableInternalMsMedian: metric((run) => run.api.observableInternalMs),
      ivmBridgeMsMedian: metric((run) => run.api.ivmBridgeMs),
      callbackCallMsMedian: metric((run) => run.api.callbackCallMs),
      callbackTotalMsMedian: metric((run) => run.api.callbackTotalMs),
      commitCallMsMedian: metric((run) => run.api.commitCallMs),
    },
  }
}

async function ensureTmpDir() {
  if (!existsSync(TMP_DIR)) {
    await fs.mkdir(TMP_DIR, { recursive: true })
  }
}

async function runSingleProbe({ scale, scenarioId, patchCount, timeoutMs }) {
  const env = {
    ...process.env,
    LIVE_QUERY_BENCH_SCALE: String(scale),
    TANSTACK_BENCH_SCALE: String(scale),
    TANSTACK_BENCH_SOCKET_PATCHES: String(Math.max(1, patchCount)),
    TANSTACK_BENCH_API_REFRESHES: String(Math.max(1, patchCount)),
    TANSTACK_BENCH_TIMEOUT_MS: String(timeoutMs),
  }

  const client = new WorkerClient(env, timeoutMs)

  try {
    client.post({
      type: 'init',
      queryMode: 'trace',
      scenarioVariant: 'trace_capability_aligned',
      traceCallbackMode: 'delta-minimal',
    })

    const ready = await client.waitFor((message) => message.type === 'ready')

    client.post({
      type: 'subscribe',
      scenarioId,
      includeRows: false,
      traceCallbackMode: 'delta-minimal',
    })
    const initial = await client.waitFor(
      (message) =>
        message.type === 'snapshot' &&
        message.scenarioId === scenarioId &&
        message.phase === 'initial',
    )

    client.post({
      type: 'socket-patch',
      scenarioId,
      patchCount,
      mutationMode: 'projection-stable',
    })
    const socket = await client.waitFor(
      (message) =>
        message.type === 'snapshot' &&
        message.scenarioId === scenarioId &&
        message.phase === 'socket',
    )
    const socketProfile = await client.waitFor(
      (message) =>
        message.type === 'mutation-profile' &&
        message.scenarioId === scenarioId &&
        message.phase === 'socket',
    )

    client.post({
      type: 'api-refresh',
      scenarioId,
      patchCount,
      mutationMode: 'projection-stable',
    })
    const api = await client.waitFor(
      (message) =>
        message.type === 'snapshot' &&
        message.scenarioId === scenarioId &&
        message.phase === 'api',
    )
    const apiProfile = await client.waitFor(
      (message) =>
        message.type === 'mutation-profile' &&
        message.scenarioId === scenarioId &&
        message.phase === 'api',
    )

    client.post({ type: 'unsubscribe', scenarioId })
    await client.waitFor(
      (message) =>
        message.type === 'unsubscribed' && message.scenarioId === scenarioId,
    )

    client.post({ type: 'shutdown' })
    await client.waitFor((message) => message.type === 'shutdown-complete')

    return {
      initMs: ready.initMs,
      initial: {
        rowCount: initial.rowCount,
      },
      socket: {
        workerMs: socket.workerLatencyMs,
        changeCount: socket.changeCount,
        deltaAddedCount: socket.phaseProfile?.deltaAddedCount ?? 0,
        deltaRemovedCount: socket.phaseProfile?.deltaRemovedCount ?? 0,
        callbackTotalMs: socket.phaseProfile?.callbackTotalMs ?? null,
        queryOnTableChangeMs:
          socketProfile.mutationProfile?.deltaFlushProfile?.queryOnTableChangeMs ?? null,
        observableInternalMs:
          socketProfile.mutationProfile?.observableInternalMs ?? null,
        ivmBridgeMs:
          socketProfile.mutationProfile?.ivmBridgeProfile?.totalMs ?? null,
        callbackCallMs:
          socketProfile.mutationProfile?.ivmBridgeProfile?.callbackCallMs ?? null,
        commitCallMs: socketProfile.mutationProfile?.commitCallMs ?? null,
      },
      api: {
        workerMs: api.workerLatencyMs,
        changeCount: api.changeCount,
        deltaAddedCount: api.phaseProfile?.deltaAddedCount ?? 0,
        deltaRemovedCount: api.phaseProfile?.deltaRemovedCount ?? 0,
        callbackTotalMs: api.phaseProfile?.callbackTotalMs ?? null,
        queryOnTableChangeMs:
          apiProfile.mutationProfile?.deltaFlushProfile?.queryOnTableChangeMs ?? null,
        observableInternalMs:
          apiProfile.mutationProfile?.observableInternalMs ?? null,
        ivmBridgeMs:
          apiProfile.mutationProfile?.ivmBridgeProfile?.totalMs ?? null,
        callbackCallMs:
          apiProfile.mutationProfile?.ivmBridgeProfile?.callbackCallMs ?? null,
        commitCallMs: apiProfile.mutationProfile?.commitCallMs ?? null,
      },
    }
  } finally {
    await client.terminate()
  }
}

async function runProbeSeries({ scale, scenarioId, patchCount, rounds, timeoutMs }) {
  const runs = []
  for (let index = 0; index < rounds; index += 1) {
    runs.push(
      await runSingleProbe({
        scale,
        scenarioId,
        patchCount,
        timeoutMs,
      }),
    )
  }
  return {
    scale,
    scenarioId,
    patchCount,
    rounds,
    runs,
    summary: summarizeRuns(runs),
  }
}

function buildMarkdownReport({
  generatedAt,
  scales,
  scalePatchCount,
  deltaSweepScale,
  deltaPatchCounts,
  rounds,
  scaleSweep,
  deltaSweep,
}) {
  const lines = []
  lines.push('# Trace O(delta) Validation')
  lines.push('')
  lines.push(`Generated: ${generatedAt}`)
  lines.push('')
  lines.push('## Methodology')
  lines.push('')
  lines.push('- Engine under test: `Cynos trace` only.')
  lines.push('- Query mode: `trace()` with worker-side `delta-minimal` callback mode.')
  lines.push('- Mutations use `projection-stable` patches on `projects.name`, so membership stays stable and delta size stays bounded.')
  lines.push('- This separates native delta maintenance from the full-snapshot wrapper used by the 4-engine compare harness.')
  lines.push(`- Scale sweep: scales ${scales.join(', ')} with patchCount=${scalePatchCount}.`)
  lines.push(`- Delta sweep: scale=${deltaSweepScale} with patchCounts ${deltaPatchCounts.join(', ')}.`)
  lines.push(`- Measured rounds per point: ${rounds}.`)
  lines.push('')
  lines.push('## How To Read')
  lines.push('')
  lines.push('- `workerMs` is the end-to-end worker latency in `delta-minimal` mode.')
  lines.push('- `queryOnTableChangeMs` is the trace kernel + observable path inside the engine.')
  lines.push('- `ivmBridgeMs` is JS bridge delivery cost with a minimal subscriber callback.')
  lines.push('- If change counts stay roughly flat while `queryOnTableChangeMs` and `ivmBridgeMs` stay roughly flat across larger datasets, that is empirical evidence for `O(delta)` behavior.')
  lines.push('')

  for (const scenarioId of Object.keys(SCENARIOS)) {
    const scenarioLabel = SCENARIOS[scenarioId]
    const series = scaleSweep[scenarioId]
    lines.push(`## Scale Sweep · ${scenarioLabel}`)
    lines.push('')
    lines.push('| Scale | Init worker | Initial rows | Socket changes | Socket worker | Socket queryOnTableChange | Socket bridge | Socket callback | API changes | API worker | API queryOnTableChange | API bridge | API callback |')
    lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |')
    for (const point of series) {
      const summary = point.summary
      lines.push(
        `| ${point.scale}x | ${formatMs(summary.initMsMedian)} | ${formatCount(summary.initialRowCountMedian)} | ${formatCount(summary.socket.changeCountMedian)} | ${formatMs(summary.socket.workerMsMedian)} | ${formatMs(summary.socket.queryOnTableChangeMsMedian)} | ${formatMs(summary.socket.ivmBridgeMsMedian)} | ${formatMs(summary.socket.callbackCallMsMedian)} | ${formatCount(summary.api.changeCountMedian)} | ${formatMs(summary.api.workerMsMedian)} | ${formatMs(summary.api.queryOnTableChangeMsMedian)} | ${formatMs(summary.api.ivmBridgeMsMedian)} | ${formatMs(summary.api.callbackCallMsMedian)} |`,
      )
    }
    lines.push('')

    const base = series[0]?.summary
    const last = series[series.length - 1]?.summary
    if (base && last) {
      lines.push('- 1x -> max scale factors:')
      lines.push(
        `  - socket changeCount ${factorText(base.socket.changeCountMedian, last.socket.changeCountMedian)}, queryOnTableChange ${factorText(base.socket.queryOnTableChangeMsMedian, last.socket.queryOnTableChangeMsMedian)}, bridge ${factorText(base.socket.ivmBridgeMsMedian, last.socket.ivmBridgeMsMedian)}`,
      )
      lines.push(
        `  - api changeCount ${factorText(base.api.changeCountMedian, last.api.changeCountMedian)}, queryOnTableChange ${factorText(base.api.queryOnTableChangeMsMedian, last.api.queryOnTableChangeMsMedian)}, bridge ${factorText(base.api.ivmBridgeMsMedian, last.api.ivmBridgeMsMedian)}`,
      )
      lines.push('')
    }
  }

  for (const scenarioId of Object.keys(SCENARIOS)) {
    const scenarioLabel = SCENARIOS[scenarioId]
    const series = deltaSweep[scenarioId]
    lines.push(`## Delta Sweep · ${scenarioLabel}`)
    lines.push('')
    lines.push('| Patch count | Socket changes | Socket worker | Socket queryOnTableChange | Socket bridge | API changes | API worker | API queryOnTableChange | API bridge |')
    lines.push('| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |')
    for (const point of series) {
      const summary = point.summary
      lines.push(
        `| ${point.patchCount} | ${formatCount(summary.socket.changeCountMedian)} | ${formatMs(summary.socket.workerMsMedian)} | ${formatMs(summary.socket.queryOnTableChangeMsMedian)} | ${formatMs(summary.socket.ivmBridgeMsMedian)} | ${formatCount(summary.api.changeCountMedian)} | ${formatMs(summary.api.workerMsMedian)} | ${formatMs(summary.api.queryOnTableChangeMsMedian)} | ${formatMs(summary.api.ivmBridgeMsMedian)} |`,
      )
    }
    lines.push('')
  }

  lines.push('## Verdict')
  lines.push('')
  lines.push('- Read this as empirical validation, not a formal proof.')
  lines.push('- The decisive signal is whether native trace maintenance tracks changed rows, instead of total dataset size, once the full-snapshot wrapper is removed from the callback path.')
  lines.push('- Compare this report against `/Users/saviio/workspace/cynos/tmp/scale-matrix/...` if you want to contrast native delta behavior with full-payload benchmark behavior.')
  lines.push('')

  return `${lines.join('\n')}\n`
}

async function main() {
  await ensureTmpDir()

  const scales = readFloatListEnv('TRACE_COMPLEXITY_SCALES', [1, 4, 10, 20])
  const scalePatchCount = readIntEnv('TRACE_COMPLEXITY_SCALE_PATCH_COUNT', 1)
  const deltaSweepScale = readIntEnv('TRACE_COMPLEXITY_DELTA_SCALE', 10)
  const deltaPatchCounts = readFloatListEnv(
    'TRACE_COMPLEXITY_DELTA_PATCH_COUNTS',
    [1, 4, 8, 16],
  ).map((value) => Math.round(value))
  const rounds = readIntEnv('TRACE_COMPLEXITY_ROUNDS', 1)
  const timeoutMs = readIntEnv('TRACE_COMPLEXITY_TIMEOUT_MS', 3_600_000)

  const scaleSweep = {}
  for (const scenarioId of Object.keys(SCENARIOS)) {
    scaleSweep[scenarioId] = []
    for (const scale of scales) {
      process.stdout.write(
        `Running scale sweep: scenario=${scenarioId} scale=${scale}x patchCount=${scalePatchCount}\n`,
      )
      scaleSweep[scenarioId].push(
        await runProbeSeries({
          scale,
          scenarioId,
          patchCount: scalePatchCount,
          rounds,
          timeoutMs,
        }),
      )
    }
  }

  const deltaSweep = {}
  for (const scenarioId of Object.keys(SCENARIOS)) {
    deltaSweep[scenarioId] = []
    for (const patchCount of deltaPatchCounts) {
      process.stdout.write(
        `Running delta sweep: scenario=${scenarioId} scale=${deltaSweepScale}x patchCount=${patchCount}\n`,
      )
      deltaSweep[scenarioId].push(
        await runProbeSeries({
          scale: deltaSweepScale,
          scenarioId,
          patchCount,
          rounds,
          timeoutMs,
        }),
      )
    }
  }

  const generatedAt = new Date().toISOString()
  const report = buildMarkdownReport({
    generatedAt,
    scales,
    scalePatchCount,
    deltaSweepScale,
    deltaPatchCounts,
    rounds,
    scaleSweep,
    deltaSweep,
  })

  await fs.writeFile(REPORT_PATH, report, 'utf8')
  await fs.writeFile(
    JSON_REPORT_PATH,
    JSON.stringify(
      {
        generatedAt,
        methodology: {
          scales,
          scalePatchCount,
          deltaSweepScale,
          deltaPatchCounts,
          rounds,
          traceCallbackMode: 'delta-minimal',
          mutationMode: 'projection-stable',
        },
        scaleSweep,
        deltaSweep,
      },
      null,
      2,
    ),
    'utf8',
  )

  process.stdout.write(report)
  process.stdout.write(`Saved Markdown report to ${REPORT_PATH}\n`)
  process.stdout.write(`Saved JSON report to ${JSON_REPORT_PATH}\n`)
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
