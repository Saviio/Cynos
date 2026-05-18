import fs from 'node:fs/promises'
import { existsSync } from 'node:fs'
import path from 'node:path'
import { performance } from 'node:perf_hooks'
import { Worker } from 'node:worker_threads'
import { fileURLToPath } from 'node:url'
import {
  API_REFRESH_COUNT,
  DATASET_CONFIG,
  DATASET_SCALE,
  JSON_REPORT_PATH,
  MEASURED_ROUNDS,
  MESSAGE_TIMEOUT_MS,
  REPORT_PATH,
  SCENARIO_ORDER,
  SCENARIOS,
  SOCKET_PATCH_COUNT,
  WARMUP_ROUNDS,
  formatBytes,
  formatCount,
  formatMs,
  mean,
  median,
  nowIso,
} from './tanstack_db_benchmark_shared.mjs'

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url))
const ROOT_DIR = path.resolve(SCRIPT_DIR, '..')
const TMP_DIR = path.join(ROOT_DIR, 'tmp')
const WORKER_PATH = new URL('./tanstack_db_worker_runtime.mjs', import.meta.url)

function createDeferred() {
  let resolve
  let reject
  const promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

function payloadBytes(rows) {
  if (!rows) return 0
  return Buffer.byteLength(JSON.stringify(rows), 'utf8')
}

class BenchmarkWorkerClient {
  constructor() {
    this.worker = new Worker(WORKER_PATH, { type: 'module' })
    this.waiters = []

    this.worker.on('message', (message) => {
      if (message.type === 'error') {
        const error = new Error(`${message.context}: ${message.message}`)
        error.stack = message.stack
        const waiter = this.waiters.shift()
        if (waiter) {
          waiter.reject(error)
        }
        return
      }

      const matchedIndex = this.waiters.findIndex((waiter) => waiter.match(message))
      if (matchedIndex >= 0) {
        const [waiter] = this.waiters.splice(matchedIndex, 1)
        waiter.resolve(message)
      }
    })
  }

  post(message) {
    this.worker.postMessage(message)
  }

  waitFor(match, timeoutMs = MESSAGE_TIMEOUT_MS) {
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

async function ensureTmpDir() {
  if (!existsSync(TMP_DIR)) {
    await fs.mkdir(TMP_DIR, { recursive: true })
  }
}

async function runRound(client, scenarioId) {
  const initialSentAt = performance.now()
  client.post({ type: 'subscribe', scenarioId, includeRows: true })
  const initial = await client.waitFor(
    (message) =>
      message.type === 'snapshot' &&
      message.scenarioId === scenarioId &&
      message.phase === 'initial',
  )
  const initialHostMs = performance.now() - initialSentAt

  const socketSentAt = performance.now()
  client.post({
    type: 'socket-patch',
    scenarioId,
    patchCount: SOCKET_PATCH_COUNT,
  })
  const socket = await client.waitFor(
    (message) =>
      message.type === 'snapshot' &&
      message.scenarioId === scenarioId &&
      message.phase === 'socket',
  )
  const socketHostMs = performance.now() - socketSentAt

  const apiSentAt = performance.now()
  client.post({
    type: 'api-refresh',
    scenarioId,
    patchCount: API_REFRESH_COUNT,
  })
  const api = await client.waitFor(
    (message) =>
      message.type === 'snapshot' &&
      message.scenarioId === scenarioId &&
      message.phase === 'api',
  )
  const apiHostMs = performance.now() - apiSentAt

  client.post({ type: 'unsubscribe', scenarioId })
  await client.waitFor(
    (message) =>
      message.type === 'unsubscribed' && message.scenarioId === scenarioId,
  )

  return {
    initial: {
      workerMs: initial.workerLatencyMs,
      hostMs: initialHostMs,
      rowCount: initial.rowCount,
      changeCount: initial.changeCount,
      bytes: payloadBytes(initial.rows),
    },
    socket: {
      workerMs: socket.workerLatencyMs,
      hostMs: socketHostMs,
      rowCount: socket.rowCount,
      changeCount: socket.changeCount,
      bytes: payloadBytes(socket.rows),
    },
    api: {
      workerMs: api.workerLatencyMs,
      hostMs: apiHostMs,
      rowCount: api.rowCount,
      changeCount: api.changeCount,
      bytes: payloadBytes(api.rows),
    },
  }
}

function summarizePhase(rounds, phase) {
  const workerValues = rounds.map((round) => round[phase].workerMs)
  const hostValues = rounds.map((round) => round[phase].hostMs)
  const rowCounts = rounds.map((round) => round[phase].rowCount)
  const changeCounts = rounds.map((round) => round[phase].changeCount)
  const byteSizes = rounds.map((round) => round[phase].bytes)

  return {
    workerMsMedian: median(workerValues),
    workerMsMean: mean(workerValues),
    hostMsMedian: median(hostValues),
    hostMsMean: mean(hostValues),
    rowCountMedian: median(rowCounts),
    changeCountMedian: median(changeCounts),
    payloadBytesMedian: median(byteSizes),
  }
}

function summarizeScenario(rounds) {
  return {
    initial: summarizePhase(rounds, 'initial'),
    socket: summarizePhase(rounds, 'socket'),
    api: summarizePhase(rounds, 'api'),
  }
}

function buildMarkdownReport({ readyMessage, scenarioResults }) {
  const lines = []
  lines.push('# TanStack DB Worker Benchmark')
  lines.push('')
  lines.push(`Generated: ${nowIso()}`)
  lines.push('')
  lines.push('## Setup')
  lines.push('')
  lines.push(
    `- Dataset: ${formatCount(DATASET_CONFIG.issueCount)} issues, ${formatCount(DATASET_CONFIG.projectCount)} projects, ${formatCount(DATASET_CONFIG.userCount)} users, ${formatCount(DATASET_CONFIG.teamCount)} teams, ${formatCount(DATASET_CONFIG.organizationCount)} orgs, ${formatCount(DATASET_CONFIG.milestoneCount)} milestones`,
  )
  lines.push(`- Dataset scale: ${DATASET_SCALE}x`)
  lines.push(`- Warmup rounds: ${WARMUP_ROUNDS}`)
  lines.push(`- Measured rounds: ${MEASURED_ROUNDS}`)
  lines.push(`- Socket patch count per round: ${SOCKET_PATCH_COUNT}`)
  lines.push(`- API refresh count per round: ${API_REFRESH_COUNT}`)
  lines.push(`- Worker init time: ${formatMs(readyMessage.initMs)}`)
  lines.push('')
  lines.push('Base collection sizes:')
  lines.push('')
  for (const [tableName, count] of Object.entries(readyMessage.dataset)) {
    lines.push(`- ${tableName}: ${formatCount(count)}`)
  }
  lines.push('')
  lines.push('## Results')
  lines.push('')
  lines.push(
    '| Scenario | Initial worker | Initial host | Snapshot rows | Payload | Socket worker | Socket host | API worker | API host |',
  )
  lines.push(
    '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
  )

  for (const scenarioId of SCENARIO_ORDER) {
    const scenario = SCENARIOS[scenarioId]
    const summary = scenarioResults[scenarioId].summary
    lines.push(
      `| ${scenario.label} | ${formatMs(summary.initial.workerMsMedian)} | ${formatMs(summary.initial.hostMsMedian)} | ${formatCount(summary.initial.rowCountMedian)} | ${formatBytes(summary.initial.payloadBytesMedian)} | ${formatMs(summary.socket.workerMsMedian)} | ${formatMs(summary.socket.hostMsMedian)} | ${formatMs(summary.api.workerMsMedian)} | ${formatMs(summary.api.hostMsMedian)} |`,
    )
  }

  lines.push('')
  lines.push('## Notes')
  lines.push('')
  lines.push(
    '- Base collections are `queryCollectionOptions(...)` collections fed by mock API responses shaped as `{ items, revision, total }`.',
  )
  lines.push(
    '- Socket updates are simulated as `projects.utils.writeBatch(...writeUpdate(partial))` calls with only 2-3 top-level fields (`state`, `healthScore`, `updatedAt`).',
  )
  lines.push(
    '- API refreshes mutate the mock server copy, including nested project metadata (`metadata.risk.score`, `metadata.flags.strategic`), then call `projects.utils.refetch()`.',
  )
  lines.push(
    '- The host path measures worker compute + incremental maintenance + full snapshot structured-clone back to the parent thread.',
  )
  lines.push(
    '- Queries intentionally use 6-7 left joins and nested semi-structured fields to approximate Linear-style project/issue views rather than trivial single-table filters.',
  )
  lines.push('')
  lines.push('## Scenario details')
  lines.push('')
  for (const scenarioId of SCENARIO_ORDER) {
    const scenario = SCENARIOS[scenarioId]
    const summary = scenarioResults[scenarioId].summary
    lines.push(`### ${scenario.label}`)
    lines.push('')
    lines.push(`- ${scenario.description}`)
    lines.push(
      `- Initial rows: ${formatCount(summary.initial.rowCountMedian)}, median payload: ${formatBytes(summary.initial.payloadBytesMedian)}`,
    )
    lines.push(
      `- Socket change events observed: ${formatCount(summary.socket.changeCountMedian)}`,
    )
    lines.push(
      `- API change events observed: ${formatCount(summary.api.changeCountMedian)}`,
    )
    lines.push('')
  }

  return `${lines.join('\n')}\n`
}

async function main() {
  await ensureTmpDir()

  const client = new BenchmarkWorkerClient()

  try {
    client.post({ type: 'init' })
    const readyMessage = await client.waitFor((message) => message.type === 'ready')

    const scenarioResults = {}

    for (const scenarioId of SCENARIO_ORDER) {
      const warmupRounds = []
      for (let index = 0; index < WARMUP_ROUNDS; index += 1) {
        warmupRounds.push(await runRound(client, scenarioId))
      }

      const measuredRounds = []
      for (let index = 0; index < MEASURED_ROUNDS; index += 1) {
        measuredRounds.push(await runRound(client, scenarioId))
      }

      scenarioResults[scenarioId] = {
        warmupRounds,
        measuredRounds,
        summary: summarizeScenario(measuredRounds),
      }
    }

    const report = buildMarkdownReport({ readyMessage, scenarioResults })
    await fs.writeFile(REPORT_PATH, report, 'utf8')
    await fs.writeFile(
      JSON_REPORT_PATH,
      JSON.stringify(
        {
          generatedAt: nowIso(),
          datasetConfig: DATASET_CONFIG,
          datasetScale: DATASET_SCALE,
          readyMessage,
          scenarioResults,
        },
        null,
        2,
      ),
      'utf8',
    )

    process.stdout.write(report)
    process.stdout.write(`\nSaved Markdown report to ${REPORT_PATH}\n`)
    process.stdout.write(`Saved JSON report to ${JSON_REPORT_PATH}\n`)

    client.post({ type: 'shutdown' })
    await client.waitFor((message) => message.type === 'shutdown-complete')
  } finally {
    await client.terminate()
  }
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
