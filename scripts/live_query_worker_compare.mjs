import fs from 'node:fs/promises'
import { existsSync } from 'node:fs'
import path from 'node:path'
import { performance } from 'node:perf_hooks'
import { Worker } from 'node:worker_threads'
import { fileURLToPath } from 'node:url'
import { ResultSet } from '../js/packages/core/dist/index.js'
import {
  API_REFRESH_COUNT,
  DATASET_CONFIG,
  MEASURED_ROUNDS,
  MESSAGE_TIMEOUT_MS,
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
import { materializeResultSetForScenario } from './cynos_benchmark_row_shape.mjs'

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url))
const ROOT_DIR = path.resolve(SCRIPT_DIR, '..')
const TMP_DIR = path.join(ROOT_DIR, 'tmp')
const REPORT_PATH = path.join(TMP_DIR, 'live_query_worker_compare.md')
const JSON_REPORT_PATH = path.join(TMP_DIR, 'live_query_worker_compare.json')
const COMPARE_TIMEOUT_MS = Math.max(MESSAGE_TIMEOUT_MS, 120_000)
const INCLUDE_ROWS = process.env.LIVE_QUERY_INCLUDE_ROWS !== '0'
const SCENARIO_VARIANT = parseScenarioVariant()
const ACTIVE_SCENARIO_ORDER = scenarioOrderForVariant(SCENARIO_VARIANT)

const ENGINE_DEFS = {
  tanstack: {
    id: 'tanstack',
    label: 'TanStack DB',
    workerUrl: new URL('./tanstack_db_worker_runtime.mjs', import.meta.url),
    notes: [
      'Base collections are `queryCollectionOptions(...)` collections backed by mock API responses.',
      'JSON predicates are aligned to the Cynos capability surface: `customer.tier` and `risk.bucket` equality / OR checks.',
      'Socket patches use `projects.utils.writeBatch(...writeUpdate(partial))` with only top-level fields.',
      'API refresh mutates the mock API copy, including nested metadata, then triggers `projects.utils.refetch()`.',
    ],
  },
  cynos: {
    id: 'cynos',
    label: 'Cynos (GraphQL)',
    workerUrl: new URL('./cynos_graphql_worker_runtime.mjs', import.meta.url),
    notes: [
      'Worker initializes local Cynos WASM tables directly from the same synthetic server dataset and subscribes through generated GraphQL documents.',
      'GraphQL subscriptions can route through the engine’s delta-backed live planner when the query shape allows it, while still presenting full payload snapshots to the benchmark host.',
      'Issue-feed filters are expressed directly through GraphQL single-valued relation predicates on `project`, `counter`, and `snapshot`, so membership changes can flow through the planner/live runtime instead of a JS-side root-id workaround.',
    ],
  },
  'cynos-query': {
    id: 'cynos-query',
    label: 'Cynos (Query Builder)',
    workerUrl: new URL('./cynos_worker_runtime.mjs', import.meta.url),
    initMessage: {
      queryMode: 'changes',
    },
    expectsMutationProfiles: true,
    notes: [
      'Worker initializes local Cynos WASM tables directly from the same synthetic server dataset.',
      'Socket patches and API refreshes are batched in a single transaction commit to mirror burst delivery.',
      'This is the low-level query-builder path using direct `changes()` subscriptions over explicit joins.',
    ],
  },
  'cynos-query-binary': {
    id: 'cynos-query-binary',
    label: 'Cynos (Query Builder + binary)',
    workerUrl: new URL('./cynos_worker_runtime.mjs', import.meta.url),
    initMessage: {
      queryMode: 'changes',
      outputMode: 'binary',
    },
    expectsMutationProfiles: true,
    notes: [
      'Worker initializes local Cynos WASM tables directly from the same synthetic server dataset.',
      'Query-builder subscriptions use `changes().subscribeBinary()` and transfer a standalone `Uint8Array` back to the host.',
      'The benchmark host decodes `ResultSet` bytes and attempts to remap them into the same scenario row shapes as object mode, so host timings include real decode/materialization work.',
      'Current multi-join `changes()` binary layouts are still provisional: the decoded snapshots do not yet match object-mode rows exactly, so treat these timings as transport-focused / pre-fix numbers rather than final correctness-validated results.',
    ],
  },
  'cynos-trace': {
    id: 'cynos-trace',
    label: 'Cynos (trace)',
    workerUrl: new URL('./cynos_worker_runtime.mjs', import.meta.url),
    initMessage: {
      queryMode: 'trace',
    },
    expectsMutationProfiles: true,
    notes: [
      'Worker initializes local Cynos WASM tables directly from the same synthetic server dataset.',
      'The underlying query is compiled through query-builder `trace()` with no JS-side emulation of ORDER BY / LIMIT or top-N semantics.',
      'Use the dedicated no-order/no-limit control scenarios for apples-to-apples comparisons of pure incremental join/filter maintenance against TanStack.',
    ],
  },
}

function parseScenarioVariant() {
  const raw = String(process.env.LIVE_QUERY_BENCH_SCENARIO_VARIANT ?? '')
    .trim()
    .toLowerCase()

  if (!raw || raw === 'default') {
    return 'default'
  }

  if (raw === 'trace_aligned') {
    return 'trace_aligned'
  }

  if (raw === 'trace_capability_aligned') {
    return 'trace_capability_aligned'
  }

  throw new Error(
    `Unknown LIVE_QUERY_BENCH_SCENARIO_VARIANT "${raw}". Expected "default", "trace_aligned", or "trace_capability_aligned".`,
  )
}

function scenarioOrderForVariant(scenarioVariant) {
  if (scenarioVariant === 'trace_capability_aligned') {
    return ['issue_stream_all', 'project_board_stream_all']
  }

  return SCENARIO_ORDER
}

function parseEngines() {
  const requested = String(process.env.LIVE_QUERY_BENCH_ENGINES ?? '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)

  const engineIds = requested.length > 0 ? requested : ['tanstack', 'cynos']
  for (const engineId of engineIds) {
    if (!ENGINE_DEFS[engineId]) {
      throw new Error(`Unknown engine "${engineId}". Expected one of: ${Object.keys(ENGINE_DEFS).join(', ')}`)
    }
  }
  return engineIds
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

function payloadBytes(rows) {
  if (!rows) return 0
  return Buffer.byteLength(JSON.stringify(rows), 'utf8')
}

function materializeSnapshotRows(message) {
  if (message.payloadKind !== 'binary') {
    return message.rows
  }

  if (!message.binaryBytes || !message.layout) {
    return undefined
  }

  const resultSet = new ResultSet(message.binaryBytes, message.layout)
  const rows = materializeResultSetForScenario(
    message.scenarioId,
    resultSet,
  )
  resultSet.free()
  return rows
}

function snapshotPayloadBytes(message) {
  if (message.payloadKind === 'binary') {
    return message.binaryByteLength ?? 0
  }

  return payloadBytes(message.rows)
}

class BenchmarkWorkerClient {
  constructor(workerUrl) {
    this.worker = new Worker(workerUrl, { type: 'module' })
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

      const matchedIndex = this.waiters.findIndex((waiter) => waiter.match(message))
      if (matchedIndex >= 0) {
        const [waiter] = this.waiters.splice(matchedIndex, 1)
        waiter.resolve(message)
      } else {
        this.inbox.push(message)
      }
    })
  }

  post(message) {
    this.worker.postMessage(message)
  }

  waitFor(match, timeoutMs = COMPARE_TIMEOUT_MS) {
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

async function ensureTmpDir() {
  if (!existsSync(TMP_DIR)) {
    await fs.mkdir(TMP_DIR, { recursive: true })
  }
}

async function runRound(client, scenarioId, engineDef) {
  const initialSentAt = performance.now()
  client.post({ type: 'subscribe', scenarioId, includeRows: INCLUDE_ROWS })
  const initial = await client.waitFor(
    (message) =>
      message.type === 'snapshot' &&
      message.scenarioId === scenarioId &&
      message.phase === 'initial',
  )
  materializeSnapshotRows(initial)
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
  materializeSnapshotRows(socket)
  const socketHostMs = performance.now() - socketSentAt
  const socketMutationProfile = engineDef.expectsMutationProfiles
    ? await maybeWaitForMutationProfile(client, scenarioId, 'socket')
    : null

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
  materializeSnapshotRows(api)
  const apiHostMs = performance.now() - apiSentAt
  const apiMutationProfile = engineDef.expectsMutationProfiles
    ? await maybeWaitForMutationProfile(client, scenarioId, 'api')
    : null

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
      bytes: snapshotPayloadBytes(initial),
      phaseProfile: initial.phaseProfile ?? null,
    },
    socket: {
      workerMs: socket.workerLatencyMs,
      hostMs: socketHostMs,
      rowCount: socket.rowCount,
      changeCount: socket.changeCount,
      bytes: snapshotPayloadBytes(socket),
      phaseProfile: socket.phaseProfile ?? null,
      mutationProfile: socketMutationProfile,
    },
    api: {
      workerMs: api.workerLatencyMs,
      hostMs: apiHostMs,
      rowCount: api.rowCount,
      changeCount: api.changeCount,
      bytes: snapshotPayloadBytes(api),
      phaseProfile: api.phaseProfile ?? null,
      mutationProfile: apiMutationProfile,
    },
  }
}

async function maybeWaitForMutationProfile(client, scenarioId, phase) {
  try {
    const message = await client.waitFor(
      (candidate) =>
        candidate.type === 'mutation-profile' &&
        candidate.scenarioId === scenarioId &&
        candidate.phase === phase,
      1_000,
    )
    return message.mutationProfile ?? null
  } catch {
    return null
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

function buildMarkdownReport({ engineOrder, engineResults }) {
  const lines = []
  lines.push('# Live Query Worker Compare')
  lines.push('')
  lines.push(`Generated: ${nowIso()}`)
  lines.push('')
  lines.push('## Setup')
  lines.push('')
  lines.push(
    `- Dataset: ${formatCount(DATASET_CONFIG.issueCount)} issues, ${formatCount(DATASET_CONFIG.projectCount)} projects, ${formatCount(DATASET_CONFIG.userCount)} users, ${formatCount(DATASET_CONFIG.teamCount)} teams, ${formatCount(DATASET_CONFIG.organizationCount)} orgs, ${formatCount(DATASET_CONFIG.milestoneCount)} milestones`,
  )
  lines.push(`- Engines: ${engineOrder.map((engineId) => ENGINE_DEFS[engineId].label).join(', ')}`)
  lines.push(`- Warmup rounds: ${WARMUP_ROUNDS}`)
  lines.push(`- Measured rounds: ${MEASURED_ROUNDS}`)
  lines.push(`- Scenario variant: ${SCENARIO_VARIANT}`)
  if (SCENARIO_VARIANT === 'trace_capability_aligned') {
    lines.push(
      '- Trace capability alignment: benchmark runs only non-blocking scenarios, and TanStack disables ORDER BY / LIMIT to match Cynos trace semantics.',
    )
  }
  lines.push(`- Socket patch count per round: ${SOCKET_PATCH_COUNT}`)
  lines.push(`- API refresh count per round: ${API_REFRESH_COUNT}`)
  lines.push(
    `- Host row payloads: ${INCLUDE_ROWS ? 'enabled (full snapshot rows are structured-cloned back to host)' : 'disabled (worker returns counts only)'}`,
  )
  lines.push('')
  lines.push('## Results')
  lines.push('')
  lines.push(
    '| Scenario | Engine | Init worker | Initial worker | Initial host | Snapshot rows | Payload | Socket worker | Socket host | API worker | API host |',
  )
  lines.push(
    '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
  )

  for (const scenarioId of ACTIVE_SCENARIO_ORDER) {
    const scenario = SCENARIOS[scenarioId]
    for (const engineId of engineOrder) {
      const engineSummary = engineResults[engineId]
      const summary = engineSummary.scenarioResults[scenarioId].summary
      lines.push(
        `| ${scenario.label} | ${ENGINE_DEFS[engineId].label} | ${formatMs(engineSummary.initMsMedian)} | ${formatMs(summary.initial.workerMsMedian)} | ${formatMs(summary.initial.hostMsMedian)} | ${formatCount(summary.initial.rowCountMedian)} | ${formatBytes(summary.initial.payloadBytesMedian)} | ${formatMs(summary.socket.workerMsMedian)} | ${formatMs(summary.socket.hostMsMedian)} | ${formatMs(summary.api.workerMsMedian)} | ${formatMs(summary.api.hostMsMedian)} |`,
      )
    }
  }

  lines.push('')
  lines.push('## Engine Notes')
  lines.push('')
  for (const engineId of engineOrder) {
    lines.push(`### ${ENGINE_DEFS[engineId].label}`)
    lines.push('')
    lines.push(
      `- Worker init median: ${formatMs(engineResults[engineId].initMsMedian)} (mean ${formatMs(engineResults[engineId].initMsMean)}) across ${formatCount(engineResults[engineId].initSamples)} isolated worker runs`,
    )
    lines.push('Base collection sizes:')
    for (const [tableName, count] of Object.entries(
      engineResults[engineId].dataset,
    )) {
      lines.push(`- ${tableName}: ${formatCount(count)}`)
    }
    for (const note of ENGINE_DEFS[engineId].notes) {
      lines.push(`- ${note}`)
    }
    lines.push('')
  }

  lines.push('## Scenario Details')
  lines.push('')
  for (const scenarioId of ACTIVE_SCENARIO_ORDER) {
    const scenario = SCENARIOS[scenarioId]
    lines.push(`### ${scenario.label}`)
    lines.push('')
    lines.push(`- ${scenario.description}`)
    for (const engineId of engineOrder) {
      const summary = engineResults[engineId].scenarioResults[scenarioId].summary
      lines.push(
        `- ${ENGINE_DEFS[engineId].label}: initial ${formatMs(summary.initial.hostMsMedian)} host / ${formatMs(summary.initial.workerMsMedian)} worker; socket ${formatMs(summary.socket.hostMsMedian)} host; api ${formatMs(summary.api.hostMsMedian)} host; payload ${formatBytes(summary.initial.payloadBytesMedian)}`,
      )
    }
    lines.push('')
  }

  return `${lines.join('\n')}\n`
}

async function runEngine(engineId) {
  const initSamples = []
  const initProfiles = []
  let dataset = null

  async function runIsolatedRound(scenarioId) {
    const engineDef = ENGINE_DEFS[engineId]
    const client = new BenchmarkWorkerClient(engineDef.workerUrl)

    try {
      client.post({
        type: 'init',
        ...(engineDef.initMessage ?? {}),
        scenarioVariant: SCENARIO_VARIANT,
      })
      const readyMessage = await client.waitFor(
        (message) => message.type === 'ready',
        COMPARE_TIMEOUT_MS,
      )
      const round = await runRound(client, scenarioId, engineDef)

      client.post({ type: 'shutdown' })
      await client.waitFor(
        (message) => message.type === 'shutdown-complete',
        COMPARE_TIMEOUT_MS,
      )

      return {
        readyMessage,
        round,
      }
    } finally {
      await client.terminate()
    }
  }

  const scenarioResults = {}
  for (const scenarioId of ACTIVE_SCENARIO_ORDER) {
    const warmupRounds = []
    for (let index = 0; index < WARMUP_ROUNDS; index += 1) {
      const { readyMessage, round } = await runIsolatedRound(scenarioId)
      initSamples.push(readyMessage.initMs)
      initProfiles.push(readyMessage.initProfile ?? null)
      dataset ??= readyMessage.dataset
      warmupRounds.push(round)
    }

    const measuredRounds = []
    for (let index = 0; index < MEASURED_ROUNDS; index += 1) {
      const { readyMessage, round } = await runIsolatedRound(scenarioId)
      initSamples.push(readyMessage.initMs)
      initProfiles.push(readyMessage.initProfile ?? null)
      dataset ??= readyMessage.dataset
      measuredRounds.push(round)
    }

    scenarioResults[scenarioId] = {
      warmupRounds,
      measuredRounds,
      summary: summarizeScenario(measuredRounds),
    }
  }

  return {
    dataset,
    initMsMedian: median(initSamples),
    initMsMean: mean(initSamples),
    initSamples: initSamples.length,
    initProfiles,
    scenarioResults,
  }
}

async function main() {
  await ensureTmpDir()

  const engineOrder = parseEngines()
  const engineResults = {}

  for (const engineId of engineOrder) {
    process.stdout.write(`Running ${ENGINE_DEFS[engineId].label}...\n`)
    engineResults[engineId] = await runEngine(engineId)
  }

  const report = buildMarkdownReport({ engineOrder, engineResults })
  await fs.writeFile(REPORT_PATH, report, 'utf8')
  await fs.writeFile(
    JSON_REPORT_PATH,
    JSON.stringify(
      {
        generatedAt: nowIso(),
        datasetConfig: DATASET_CONFIG,
        includeRows: INCLUDE_ROWS,
        scenarioVariant: SCENARIO_VARIANT,
        engineOrder,
        engineResults,
      },
      null,
      2,
    ),
    'utf8',
  )

  process.stdout.write(report)
  process.stdout.write(`\nSaved Markdown report to ${REPORT_PATH}\n`)
  process.stdout.write(`Saved JSON report to ${JSON_REPORT_PATH}\n`)
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
