import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { parentPort } from 'node:worker_threads'
import { performance } from 'node:perf_hooks'
import initWasm, {
  Database,
  JsDataType,
  JsSortOrder,
  ColumnOptions,
  col,
} from '../js/packages/core/dist/wasm.js'
import {
  ResultSet,
  snapshotSchemaLayout,
} from '../js/packages/core/dist/index.js'
import {
  API_REFRESH_COUNT,
  DATASET_CONFIG,
  SOCKET_PATCH_COUNT,
} from './tanstack_db_benchmark_shared.mjs'
import {
  extractProjectIds,
  extractProjectIdsFromResultSet,
  scenarioRowKey,
  snapshotRowsForScenario,
} from './cynos_benchmark_row_shape.mjs'
import {
  buildServerDataset,
  summarizeDataset,
} from './live_query_benchmark_dataset.mjs'

if (!parentPort) {
  throw new Error('This module must run inside a worker thread.')
}

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url))
const ROOT_DIR = path.resolve(SCRIPT_DIR, '..')
const WASM_PATH = path.join(ROOT_DIR, 'js', 'packages', 'core', 'dist', 'cynos.wasm')
const MAX_TRACKED_PROJECT_IDS = Math.max(
  256,
  SOCKET_PATCH_COUNT,
  API_REFRESH_COUNT,
)

const runtime = {
  initialized: false,
  db: null,
  server: null,
  active: null,
  queryMode: 'changes',
  scenarioVariant: 'default',
  outputMode: 'object',
  traceCallbackMode: 'snapshot-materialize',
}

function usesAlignedFilters(scenarioVariant) {
  return scenarioVariant === 'trace_aligned'
}

function normalizeScenarioVariant(scenarioVariant) {
  if (scenarioVariant === 'trace_aligned') {
    return 'trace_aligned'
  }

  if (scenarioVariant === 'trace_capability_aligned') {
    return 'trace_capability_aligned'
  }

  return 'default'
}

function pkOptions() {
  return new ColumnOptions().primaryKey(true)
}

function nullableOptions() {
  return new ColumnOptions().setNullable(true)
}

function post(message, transferList) {
  parentPort.postMessage(message, transferList)
}

function createTables(db) {
  db.registerTable(
    db.createTable('organizations')
      .column('id', JsDataType.Int64, pkOptions())
      .column('name', JsDataType.String, null)
      .column('tier', JsDataType.String, null)
      .column('region', JsDataType.String, null)
      .column('metadata', JsDataType.Jsonb, null)
      .index('idx_organizations_region', 'region'),
  )

  db.registerTable(
    db.createTable('teams')
      .column('id', JsDataType.Int64, pkOptions())
      .column('organizationId', JsDataType.Int64, null)
      .column('name', JsDataType.String, null)
      .column('function', JsDataType.String, null)
      .column('metadata', JsDataType.Jsonb, null)
      .index('idx_teams_organizationId', 'organizationId'),
  )

  db.registerTable(
    db.createTable('users')
      .column('id', JsDataType.Int64, pkOptions())
      .column('teamId', JsDataType.Int64, null)
      .column('name', JsDataType.String, null)
      .column('role', JsDataType.String, null)
      .column('metadata', JsDataType.Jsonb, null)
      .index('idx_users_teamId', 'teamId'),
  )

  db.registerTable(
    db.createTable('projects')
      .column('id', JsDataType.Int64, pkOptions())
      .column('organizationId', JsDataType.Int64, null)
      .column('teamId', JsDataType.Int64, null)
      .column('leadUserId', JsDataType.Int64, null)
      .column('name', JsDataType.String, null)
      .column('state', JsDataType.String, null)
      .column('healthScore', JsDataType.Int32, null)
      .column('updatedAt', JsDataType.Int64, null)
      .column('priorityBand', JsDataType.String, null)
      .column('metadata', JsDataType.Jsonb, null)
      .index('idx_projects_organizationId', 'organizationId')
      .index('idx_projects_teamId', 'teamId')
      .index('idx_projects_leadUserId', 'leadUserId')
      .index('idx_projects_state', 'state')
      .index('idx_projects_healthScore', 'healthScore')
      .index('idx_projects_updatedAt', 'updatedAt')
      .jsonbIndex(
        'metadata',
        ['$.risk.bucket', '$.risk.score', '$.flags.strategic'],
      ),
  )

  db.registerTable(
    db.createTable('projectSnapshots')
      .column('projectId', JsDataType.Int64, pkOptions())
      .column('velocity', JsDataType.Int32, null)
      .column('completionRate', JsDataType.Float64, null)
      .column('blockedRatio', JsDataType.Float64, null)
      .column('updatedAt', JsDataType.Int64, null)
      .index('idx_projectSnapshots_velocity', 'velocity'),
  )

  db.registerTable(
    db.createTable('projectCounters')
      .column('projectId', JsDataType.Int64, pkOptions())
      .column('openIssueCount', JsDataType.Int32, null)
      .column('blockerCount', JsDataType.Int32, null)
      .column('staleIssueCount', JsDataType.Int32, null)
      .column('updatedAt', JsDataType.Int64, null)
      .index('idx_projectCounters_openIssueCount', 'openIssueCount'),
  )

  db.registerTable(
    db.createTable('currentMilestones')
      .column('id', JsDataType.Int64, pkOptions())
      .column('projectId', JsDataType.Int64, null)
      .column('name', JsDataType.String, null)
      .column('dueAt', JsDataType.Int64, null)
      .column('status', JsDataType.String, null)
      .column('metadata', JsDataType.Jsonb, null)
      .index('idx_currentMilestones_projectId', 'projectId')
      .index('idx_currentMilestones_dueAt', 'dueAt'),
  )

  db.registerTable(
    db.createTable('issues')
      .column('id', JsDataType.Int64, pkOptions())
      .column('projectId', JsDataType.Int64, null)
      .column('assigneeId', JsDataType.Int64, null)
      .column('currentMilestoneId', JsDataType.Int64, nullableOptions())
      .column('title', JsDataType.String, null)
      .column('status', JsDataType.String, null)
      .column('priority', JsDataType.String, null)
      .column('estimate', JsDataType.Int32, null)
      .column('updatedAt', JsDataType.Int64, null)
      .column('metadata', JsDataType.Jsonb, null)
      .index('idx_issues_projectId', 'projectId')
      .index('idx_issues_assigneeId', 'assigneeId')
      .index('idx_issues_currentMilestoneId', 'currentMilestoneId')
      .index('idx_issues_status', 'status')
      .index('idx_issues_estimate', 'estimate')
      .index('idx_issues_updatedAt', 'updatedAt')
      .jsonbIndex(
        'metadata',
        ['$.severityRank', '$.customer.tier', '$.workflow.lane'],
      ),
  )
}

async function insertTableInBatches(db, tableName, rows) {
  const batchSize =
    tableName === 'issues'
      ? 4_000
      : tableName === 'users' || tableName === 'currentMilestones'
        ? 2_000
        : 1_000

  for (let index = 0; index < rows.length; index += batchSize) {
    await db
      .insert(tableName)
      .values(rows.slice(index, index + batchSize))
      .exec()
  }
}

function buildIssueWindowQuery(queryMode, limit, scenarioVariant) {
  const alignedFilters = usesAlignedFilters(scenarioVariant)
  const query = runtime.db
    .select([
      'issues.id',
      'project.id',
      'issues.title',
      'issues.status',
      'issues.priority',
      'issues.updatedAt',
      'issues.metadata',
      'project.name',
      'project.state',
      'project.healthScore',
      'project.metadata',
      'org.name',
      'team.name',
      'assignee.name',
      'milestone.name',
      'counter.openIssueCount',
      'counter.blockerCount',
      'snapshot.velocity',
    ])
    .from('issues')
    .leftJoin('projects as project', col('issues.projectId').eq(col('project.id')))
    .leftJoin(
      'organizations as org',
      col('project.organizationId').eq(col('org.id')),
    )
    .leftJoin('teams as team', col('project.teamId').eq(col('team.id')))
    .leftJoin(
      'users as assignee',
      col('issues.assigneeId').eq(col('assignee.id')),
    )
    .leftJoin(
      'currentMilestones as milestone',
      col('issues.currentMilestoneId').eq(col('milestone.id')),
    )
    .leftJoin(
      'projectCounters as counter',
      col('project.id').eq(col('counter.projectId')),
    )
    .leftJoin(
      'projectSnapshots as snapshot',
      col('project.id').eq(col('snapshot.projectId')),
    )
    .where(
      alignedFilters
        ? col('issues.status')
            .eq('open')
            .or(col('issues.status').eq('in_progress'))
            .and(col('issues.estimate').gte(3))
            .and(
              col('issues.metadata')
                .get('$.customer.tier')
                .eq('enterprise')
                .or(col('issues.metadata').get('$.customer.tier').eq('mid_market')),
            )
        : col('issues.status')
            .eq('open')
            .or(col('issues.status').eq('in_progress'))
            .and(col('issues.estimate').gte(3))
            .and(
              col('issues.metadata')
                .get('$.customer.tier')
                .eq('enterprise')
                .or(col('issues.metadata').get('$.customer.tier').eq('mid_market')),
            )
            .and(col('project.healthScore').gte(45))
            .and(
              col('project.metadata')
                .get('$.risk.bucket')
                .eq('high')
                .or(col('project.metadata').get('$.risk.bucket').eq('critical')),
            )
            .and(col('counter.openIssueCount').gte(5))
            .and(col('snapshot.velocity').gte(18)),
    )

  if (queryMode === 'trace') {
    return query.trace()
  }

  if (!Number.isFinite(limit)) {
    return query.changes()
  }

  return query
    .orderBy('issues.updatedAt', JsSortOrder.Desc)
    .limit(limit)
    .changes()
}

function buildProjectBoardQuery(queryMode, limit, scenarioVariant) {
  const alignedFilters = usesAlignedFilters(scenarioVariant)
  const query = runtime.db
    .select([
      'projects.id',
      'projects.name',
      'projects.state',
      'projects.healthScore',
      'projects.updatedAt',
      'projects.metadata',
      'org.region',
      'org.name',
      'team.name',
      'lead.name',
      'lead.role',
      'milestone.name',
      'milestone.status',
      'counter.openIssueCount',
      'counter.blockerCount',
      'counter.staleIssueCount',
      'snapshot.velocity',
      'snapshot.blockedRatio',
    ])
    .from('projects')
    .leftJoin(
      'organizations as org',
      col('projects.organizationId').eq(col('org.id')),
    )
    .leftJoin('teams as team', col('projects.teamId').eq(col('team.id')))
    .leftJoin('users as lead', col('projects.leadUserId').eq(col('lead.id')))
    .leftJoin(
      'projectCounters as counter',
      col('projects.id').eq(col('counter.projectId')),
    )
    .leftJoin(
      'projectSnapshots as snapshot',
      col('projects.id').eq(col('snapshot.projectId')),
    )
    .leftJoin(
      'currentMilestones as milestone',
      col('projects.id').eq(col('milestone.projectId')),
    )
    .where(
      alignedFilters
        ? col('projects.state')
            .eq('active')
            .or(col('projects.state').eq('at_risk'))
            .and(col('projects.healthScore').gte(45))
            .and(
              col('projects.metadata')
                .get('$.risk.bucket')
                .eq('high')
                .or(col('projects.metadata').get('$.risk.bucket').eq('critical')),
            )
        : col('projects.state')
            .eq('active')
            .or(col('projects.state').eq('at_risk'))
            .and(col('projects.healthScore').gte(45))
            .and(
              col('projects.metadata')
                .get('$.risk.bucket')
                .eq('high')
                .or(col('projects.metadata').get('$.risk.bucket').eq('critical')),
            )
            .and(col('counter.openIssueCount').gte(4))
            .and(col('snapshot.velocity').gte(20)),
    )

  if (queryMode === 'trace') {
    return query.trace()
  }

  if (!Number.isFinite(limit)) {
    return query.changes()
  }

  return query
    .orderBy('projects.healthScore', JsSortOrder.Desc)
    .limit(limit)
    .changes()
}

function buildScenarioStream(scenarioId, queryMode, scenarioVariant) {
  if (scenarioId === 'issue_window_500') {
    return buildIssueWindowQuery(queryMode, 500, scenarioVariant)
  }

  if (scenarioId === 'issue_window_5000') {
    return buildIssueWindowQuery(queryMode, 5_000, scenarioVariant)
  }

  if (scenarioId === 'issue_stream_all') {
    return buildIssueWindowQuery(queryMode, Number.NaN, scenarioVariant)
  }

  if (scenarioId === 'project_board_2000') {
    return buildProjectBoardQuery(queryMode, 2_000, scenarioVariant)
  }

  if (scenarioId === 'project_board_stream_all') {
    return buildProjectBoardQuery(queryMode, Number.NaN, scenarioVariant)
  }

  throw new Error(`Unknown scenario: ${scenarioId}`)
}

async function initRuntime(message = {}) {
  if (runtime.initialized) {
    return {
      type: 'ready',
      queryMode: runtime.queryMode,
      scenarioVariant: runtime.scenarioVariant,
      outputMode: runtime.outputMode,
      traceCallbackMode: runtime.traceCallbackMode,
      dataset: summarizeDataset(runtime.server.tables),
    }
  }

  const startedAt = performance.now()
  const initProfile = {
    wasmFileReadMs: 0,
    wasmInitMs: 0,
    datasetBuildMs: 0,
    databaseCreateMs: 0,
    schemaRegistrationMs: 0,
    insertTotalMs: 0,
    insertByTableMs: {},
  }
  runtime.queryMode = message.queryMode === 'trace' ? 'trace' : 'changes'
  runtime.scenarioVariant = normalizeScenarioVariant(message.scenarioVariant)
  runtime.outputMode =
    runtime.queryMode === 'changes' && message.outputMode === 'binary'
      ? 'binary'
      : 'object'
  runtime.traceCallbackMode =
    runtime.queryMode === 'trace' && message.traceCallbackMode === 'delta-minimal'
      ? 'delta-minimal'
      : 'snapshot-materialize'
  const wasmReadStartedAt = performance.now()
  const wasmBytes = await fs.readFile(WASM_PATH)
  initProfile.wasmFileReadMs = performance.now() - wasmReadStartedAt
  const wasmInitStartedAt = performance.now()
  await initWasm({ module_or_path: wasmBytes })
  initProfile.wasmInitMs = performance.now() - wasmInitStartedAt

  const datasetBuildStartedAt = performance.now()
  runtime.server = buildServerDataset(DATASET_CONFIG)
  initProfile.datasetBuildMs = performance.now() - datasetBuildStartedAt
  const databaseCreateStartedAt = performance.now()
  runtime.db = new Database(`cynos_bench_${Date.now()}`)
  initProfile.databaseCreateMs = performance.now() - databaseCreateStartedAt
  const schemaRegistrationStartedAt = performance.now()
  createTables(runtime.db)
  initProfile.schemaRegistrationMs =
    performance.now() - schemaRegistrationStartedAt

  const insertStartedAt = performance.now()
  for (const [tableName, rows] of Object.entries(runtime.server.tables)) {
    const tableInsertStartedAt = performance.now()
    await insertTableInBatches(runtime.db, tableName, rows)
    initProfile.insertByTableMs[tableName] =
      performance.now() - tableInsertStartedAt
  }
  initProfile.insertTotalMs = performance.now() - insertStartedAt

  runtime.initialized = true
  return {
    type: 'ready',
    initMs: performance.now() - startedAt,
    initProfile,
    queryMode: runtime.queryMode,
    scenarioVariant: runtime.scenarioVariant,
    outputMode: runtime.outputMode,
    traceCallbackMode: runtime.traceCallbackMode,
    dataset: summarizeDataset(runtime.server.tables),
  }
}

function ensureInitialized() {
  if (!runtime.initialized || !runtime.db || !runtime.server) {
    throw new Error('Worker runtime is not initialized.')
  }
}

function unsubscribeActive() {
  if (!runtime.active) return
  runtime.active.unsubscribe?.()
  runtime.active.stream?.free?.()
  runtime.active = null
}

function createSnapshotMessage(
  active,
  phase,
  rows,
  changeCount = 1,
  phaseProfile = undefined,
) {
  return {
    type: 'snapshot',
    scenarioId: active.scenarioId,
    phase,
    queryMode: active.queryMode,
    payloadKind: 'object',
    workerLatencyMs: performance.now() - active.pending.startedAt,
    rowCount: rows.length,
    changeCount,
    phaseProfile,
    rows: active.includeRows ? rows : undefined,
  }
}

function createCountSnapshotMessage(
  active,
  phase,
  rowCount,
  changeCount = 1,
  phaseProfile = undefined,
) {
  return {
    type: 'snapshot',
    scenarioId: active.scenarioId,
    phase,
    queryMode: active.queryMode,
    payloadKind: 'object',
    workerLatencyMs: performance.now() - active.pending.startedAt,
    rowCount,
    changeCount,
    phaseProfile,
    rows: undefined,
  }
}

function createBinarySnapshotMessage(active, phase, payload) {
  return {
    type: 'snapshot',
    scenarioId: active.scenarioId,
    phase,
    queryMode: active.queryMode,
    payloadKind: 'binary',
    workerLatencyMs: performance.now() - payload.startedAt,
    rowCount: payload.rowCount,
    changeCount: payload.changeCount,
    layout: payload.layout,
    binaryBytes: active.includeRows ? payload.binaryBytes : undefined,
    binaryByteLength: payload.binaryByteLength,
  }
}

function updateTrackedProjectIds(active, projectIds) {
  if (projectIds.length > 0) {
    active.lastProjectIds = projectIds
  }
}

function postMutationProfile(active, phase, mutationProfile) {
  post({
    type: 'mutation-profile',
    scenarioId: active.scenarioId,
    phase,
    queryMode: active.queryMode,
    mutationProfile,
  })
}

function takeCommitProfile() {
  const profile = runtime.db?.takeLastCommitProfile?.()
  return profile && typeof profile === 'object' ? profile : null
}

function takeTraceInitProfile() {
  const profile = runtime.db?.takeLastTraceInitProfile?.()
  return profile && typeof profile === 'object' ? profile : null
}

function takeSnapshotInitProfile() {
  const profile = runtime.db?.takeLastSnapshotInitProfile?.()
  return profile && typeof profile === 'object' ? profile : null
}

function takeDeltaFlushProfile() {
  const profile = runtime.db?.takeLastDeltaFlushProfile?.()
  return profile && typeof profile === 'object' ? profile : null
}

function takeSnapshotFlushProfile() {
  const profile = runtime.db?.takeLastSnapshotFlushProfile?.()
  return profile && typeof profile === 'object' ? profile : null
}

function takeIvmBridgeProfile() {
  const profile = runtime.db?.takeLastIvmBridgeProfile?.()
  return profile && typeof profile === 'object' ? profile : null
}

function buildCommitProfiles() {
  const commitProfile = takeCommitProfile()
  const traceInitProfile = takeTraceInitProfile()
  const deltaFlushProfile = takeDeltaFlushProfile()
  const snapshotFlushProfile = takeSnapshotFlushProfile()
  const ivmBridgeProfile = takeIvmBridgeProfile()

  let observableInternalMs = null
  let registryNonDeltaMs = null
  let deltaFlushOverheadMs = null

  if (deltaFlushProfile && ivmBridgeProfile) {
    observableInternalMs = Math.max(
      0,
      Number(deltaFlushProfile.queryOnTableChangeMs ?? 0) -
        Number(ivmBridgeProfile.totalMs ?? 0),
    )
    deltaFlushOverheadMs = Math.max(
      0,
      Number(deltaFlushProfile.totalMs ?? 0) -
        Number(deltaFlushProfile.cloneMs ?? 0) -
        Number(deltaFlushProfile.queryOnTableChangeMs ?? 0),
    )
  }

  if (commitProfile && deltaFlushProfile) {
    registryNonDeltaMs = Math.max(
      0,
      Number(commitProfile.registryFlushMs ?? 0) -
        Number(deltaFlushProfile.totalMs ?? 0),
    )
  }

  return {
    commitProfile,
    traceInitProfile,
    deltaFlushProfile,
    snapshotFlushProfile,
    ivmBridgeProfile,
    observableInternalMs,
    registryNonDeltaMs,
    deltaFlushOverheadMs,
  }
}

function syncTrackedProjectIdsFromBinary(active, binary) {
  const resultSet = new ResultSet(binary, active.layoutSnapshot)
  updateTrackedProjectIds(
    active,
    extractProjectIdsFromResultSet(
      active.scenarioId,
      resultSet,
      MAX_TRACKED_PROJECT_IDS,
    ),
  )
  resultSet.free()
}

function buildBinarySnapshotPayload(
  active,
  phase,
  startedAt,
  binary,
  changeCount = 1,
) {
  const layout = active.layoutSnapshot

  if (active.includeRows) {
    const binaryBytes = binary.intoTransferable()
    const resultSet = new ResultSet(binaryBytes, layout)
    const rowCount = resultSet.length
    updateTrackedProjectIds(
      active,
      extractProjectIdsFromResultSet(active.scenarioId, resultSet, MAX_TRACKED_PROJECT_IDS),
    )
    resultSet.free()

    return {
      message: createBinarySnapshotMessage(active, phase, {
        startedAt,
        rowCount,
        changeCount,
        layout,
        binaryBytes,
        binaryByteLength: binaryBytes.byteLength,
      }),
      transferList: [binaryBytes.buffer],
    }
  }

  const binaryByteLength = binary.len()
  const resultSet = new ResultSet(binary, layout)
  const rowCount = resultSet.length
    updateTrackedProjectIds(
      active,
      extractProjectIdsFromResultSet(active.scenarioId, resultSet, MAX_TRACKED_PROJECT_IDS),
    )
  resultSet.free()

  return {
    message: createBinarySnapshotMessage(active, phase, {
      startedAt,
      rowCount,
      changeCount,
      layout,
      binaryByteLength,
    }),
    transferList: undefined,
  }
}

function projectNameToggle(current, suffix) {
  return current.name.endsWith(suffix)
    ? current.name.slice(0, -suffix.length)
    : `${current.name}${suffix}`
}

function projectPatchTransform(current, mutationMode, phase) {
  if (mutationMode === 'projection-stable') {
    const suffix = phase === 'socket' ? ' [socket]' : ' [api]'
    return {
      ...current,
      name: projectNameToggle(current, suffix),
    }
  }

  if (phase === 'socket') {
    const healthScore = current.healthScore >= 45 ? 24 : 82
    return {
      ...current,
      state: current.state === 'active' ? 'at_risk' : 'active',
      healthScore,
      updatedAt: current.updatedAt + 60_000,
    }
  }

  const nextRisk = current.healthScore >= 45 ? 18 : 76
  const nextHealth = current.healthScore >= 45 ? 28 : 88
  return {
    ...current,
    healthScore: nextHealth,
    updatedAt: current.updatedAt + 120_000,
    metadata: {
      ...current.metadata,
      risk: {
        ...current.metadata.risk,
        score: nextRisk,
        bucket: nextRisk >= 70 ? 'critical' : nextRisk >= 45 ? 'high' : 'medium',
      },
      flags: {
        ...current.metadata.flags,
        strategic: !current.metadata.flags.strategic,
      },
    },
  }
}

function subscribeScenario(message) {
  ensureInitialized()
  unsubscribeActive()

  const active = {
    scenarioId: message.scenarioId,
    includeRows: message.includeRows !== false,
    queryMode: runtime.queryMode,
    outputMode: runtime.outputMode,
    traceCallbackMode:
      runtime.queryMode === 'trace' &&
      (message.traceCallbackMode ?? runtime.traceCallbackMode) === 'delta-minimal'
        ? 'delta-minimal'
        : 'snapshot-materialize',
    stream: null,
    layoutSnapshot: null,
    currentRows: [],
    currentRowCount: 0,
    rawRowsByKey: null,
    lastProjectIds: [],
    pending: {
      phase: 'initial',
      startedAt: performance.now(),
    },
    unsubscribe: null,
  }

  runtime.active = active
  const buildStreamStartedAt = performance.now()
  active.stream = buildScenarioStream(
    message.scenarioId,
    runtime.queryMode,
    runtime.scenarioVariant,
  )
  const buildStreamMs = performance.now() - buildStreamStartedAt
  const traceInitProfile =
    runtime.queryMode === 'trace' ? takeTraceInitProfile() : null
  const snapshotInitProfile =
    runtime.queryMode === 'trace' ? null : takeSnapshotInitProfile()

  if (runtime.queryMode === 'trace') {
    active.rawRowsByKey = new Map()

    const getResultStartedAt = performance.now()
    const initialRawRows = active.stream.getResult()
    const getResultMs = performance.now() - getResultStartedAt
    let initialRows
    let materializeRowsMs = 0
    if (active.traceCallbackMode === 'snapshot-materialize') {
      for (const rawRow of initialRawRows) {
        const key = scenarioRowKey(active.scenarioId, rawRow)
        if (key == null) continue
        active.rawRowsByKey.set(String(key), rawRow)
      }

      const materializeStartedAt = performance.now()
      initialRows = snapshotRowsForScenario(
        active.scenarioId,
        Array.from(active.rawRowsByKey.values()),
      )
      materializeRowsMs = performance.now() - materializeStartedAt
      active.currentRows = initialRows
    } else {
      const materializeStartedAt = performance.now()
      initialRows = snapshotRowsForScenario(active.scenarioId, initialRawRows)
      materializeRowsMs = performance.now() - materializeStartedAt
      active.currentRows = active.includeRows ? initialRows : []
    }
    active.currentRowCount = initialRows.length

    const trackedIdsStartedAt = performance.now()
    updateTrackedProjectIds(
      active,
      extractProjectIds(initialRows, MAX_TRACKED_PROJECT_IDS),
    )
    const trackedProjectIdsMs = performance.now() - trackedIdsStartedAt

    const initialPending = active.pending
    if (initialPending) {
      post(
        createSnapshotMessage(
          active,
          initialPending.phase,
          initialRows,
          initialRows.length,
          {
            traceInitProfile,
            getResultMs,
            materializeRowsMs,
            trackedProjectIdsMs,
          },
        ),
      )
      active.pending = null
    }

    active.unsubscribe = active.stream.subscribe((delta) => {
      const pending = active.pending
      const removed = delta?.removed ?? []
      const added = delta?.added ?? []
      const callbackStartedAt = performance.now()
      if (active.traceCallbackMode === 'snapshot-materialize') {
        const mergeStartedAt = callbackStartedAt

        for (const rawRow of removed) {
          const key = scenarioRowKey(active.scenarioId, rawRow)
          if (key == null) continue
          active.rawRowsByKey.delete(String(key))
        }

        for (const rawRow of added) {
          const key = scenarioRowKey(active.scenarioId, rawRow)
          if (key == null) continue
          active.rawRowsByKey.set(String(key), rawRow)
        }
        const mergeDeltaMs = performance.now() - mergeStartedAt

        const materializeStartedAt = performance.now()
        const rows = snapshotRowsForScenario(
          active.scenarioId,
          Array.from(active.rawRowsByKey.values()),
        )
        const materializeRowsMs = performance.now() - materializeStartedAt
        active.currentRows = rows
        active.currentRowCount = rows.length

        const trackedIdsStartedAt = performance.now()
        updateTrackedProjectIds(
          active,
          extractProjectIds(rows, MAX_TRACKED_PROJECT_IDS),
        )
        const trackedProjectIdsMs = performance.now() - trackedIdsStartedAt
        const callbackTotalMs = performance.now() - callbackStartedAt

        if (!pending) return
        post(
          createSnapshotMessage(
            active,
            pending.phase,
            rows,
            added.length + removed.length,
            {
              deltaAddedCount: added.length,
              deltaRemovedCount: removed.length,
              mergeDeltaMs,
              materializeRowsMs,
              trackedProjectIdsMs,
              callbackTotalMs,
            },
          ),
        )
        active.pending = null
        return
      }

      active.currentRowCount += added.length - removed.length
      const callbackTotalMs = performance.now() - callbackStartedAt

      if (!pending) return
      post(
        createCountSnapshotMessage(
          active,
          pending.phase,
          active.currentRowCount,
          added.length + removed.length,
          {
            deltaAddedCount: added.length,
            deltaRemovedCount: removed.length,
            callbackTotalMs,
          },
        ),
      )
      active.pending = null
    })
    return
  }

  if (runtime.outputMode === 'binary') {
    active.layoutSnapshot = snapshotSchemaLayout(active.stream.getSchemaLayout())
    active.unsubscribe = active.stream.subscribeBinary((binary) => {
      try {
        const pending = active.pending
        if (!pending) {
          syncTrackedProjectIdsFromBinary(active, binary)
          return
        }

        const payload = buildBinarySnapshotPayload(
          active,
          pending.phase,
          pending.startedAt,
          binary,
          1,
        )
        post(payload.message, payload.transferList)
        active.pending = null
      } catch (error) {
        handleError(error, 'subscribe-binary')
      }
    })
    return
  }

  const captureInitialSubscribeProfile = snapshotInitProfile != null
  const subscribeStartedAt = captureInitialSubscribeProfile
    ? performance.now()
    : 0
  let initialEmission = null
  active.unsubscribe = active.stream.subscribe((rawRows) => {
    const pending = active.pending
    const subscribeBeforeCallbackMs =
      captureInitialSubscribeProfile && pending?.phase === 'initial'
        ? performance.now() - subscribeStartedAt
        : undefined
    const materializeStartedAt = performance.now()
    const rows = snapshotRowsForScenario(
      active.scenarioId,
      rawRows,
    )
    const materializeRowsMs = performance.now() - materializeStartedAt
    active.currentRows = rows
    const trackedIdsStartedAt = performance.now()
    updateTrackedProjectIds(
      active,
      extractProjectIds(rows, MAX_TRACKED_PROJECT_IDS),
    )
    const trackedProjectIdsMs = performance.now() - trackedIdsStartedAt
    if (!pending) return
    if (pending.phase === 'initial' && !captureInitialSubscribeProfile) {
      post(
        createSnapshotMessage(active, pending.phase, rows, 1, {
          materializeRowsMs,
          trackedProjectIdsMs,
        }),
      )
      active.pending = null
      return
    }

    const phaseProfile = {
      snapshotInitProfile,
      buildStreamMs,
      subscribeBeforeCallbackMs,
      materializeRowsMs,
      trackedProjectIdsMs,
    }

    if (pending.phase === 'initial') {
      initialEmission = {
        rows,
        changeCount: 1,
        phaseProfile,
      }
      return
    }

    post(createSnapshotMessage(active, pending.phase, rows, 1, phaseProfile))
    active.pending = null
  })
  if (captureInitialSubscribeProfile && initialEmission) {
    const subscribeReturnMs = performance.now() - subscribeStartedAt
    initialEmission.phaseProfile.subscribeReturnMs = subscribeReturnMs
    post(
      createSnapshotMessage(
        active,
        'initial',
        initialEmission.rows,
        initialEmission.changeCount,
        initialEmission.phaseProfile,
      ),
    )
    active.pending = null
  }
}

function collectActiveProjectIds(maxCount) {
  if (!runtime.active) {
    throw new Error('No active live query subscription.')
  }

  const currentIds = extractProjectIds(runtime.active.currentRows, maxCount)
  if (currentIds.length > 0) return currentIds

  if (runtime.active.lastProjectIds.length > 0) {
    const result = runtime.active.lastProjectIds.slice(0, maxCount)
    if (result.length >= maxCount) {
      return result
    }

    const seen = new Set(result)
    for (const row of runtime.server.tables.projects) {
      if (seen.has(row.id)) continue
      seen.add(row.id)
      result.push(row.id)
      if (result.length >= maxCount) break
    }

    return result
  }

  return runtime.server.tables.projects.slice(0, maxCount).map((row) => row.id)
}

function applyProjectPatch(projectId, transform) {
  const projects = runtime.server.tables.projects
  const index = projects.findIndex((row) => row.id === projectId)
  if (index < 0) return null
  const current = projects[index]
  const next = transform(current)
  projects[index] = next
  runtime.server.revisions.projects += 1
  return next
}

function runSocketPatchBurst(message) {
  ensureInitialized()
  if (!runtime.active) {
    throw new Error('Subscribe before running socket patches.')
  }

  const pending = {
    phase: 'socket',
    startedAt: performance.now(),
  }
  runtime.active.pending = pending
  const mutationMode =
    message.mutationMode === 'projection-stable'
      ? 'projection-stable'
      : 'default'
  const projectIds = collectActiveProjectIds(message.patchCount)
  const projectIdsCollectedAt = performance.now()

  const tx = runtime.db.transaction()
  const patchLoopStartedAt = performance.now()
  for (const projectId of projectIds) {
    const next = applyProjectPatch(projectId, (current) =>
      projectPatchTransform(current, mutationMode, 'socket'),
    )
    if (!next) continue
    if (mutationMode === 'projection-stable') {
      tx.update(
        'projects',
        {
          name: next.name,
        },
        col('id').eq(next.id),
      )
    } else {
      tx.update(
        'projects',
        {
          state: next.state,
          healthScore: next.healthScore,
          updatedAt: next.updatedAt,
        },
        col('id').eq(next.id),
      )
    }
  }
  const patchLoopCompletedAt = performance.now()
  const commitStartedAt = performance.now()
  tx.commit()
  const commitCompletedAt = performance.now()

  postMutationProfile(runtime.active, 'socket', {
    projectIdsCount: projectIds.length,
    collectProjectIdsMs: projectIdsCollectedAt - pending.startedAt,
    patchLoopMs: patchLoopCompletedAt - patchLoopStartedAt,
    commitCallMs: commitCompletedAt - commitStartedAt,
    ...buildCommitProfiles(),
  })
}

function runApiRefresh(message) {
  ensureInitialized()
  if (!runtime.active) {
    throw new Error('Subscribe before running API refresh.')
  }

  const pending = {
    phase: 'api',
    startedAt: performance.now(),
  }
  runtime.active.pending = pending
  const mutationMode =
    message.mutationMode === 'projection-stable'
      ? 'projection-stable'
      : 'default'
  const projectIds = collectActiveProjectIds(message.patchCount)
  const projectIdsCollectedAt = performance.now()

  const tx = runtime.db.transaction()
  const patchLoopStartedAt = performance.now()
  for (const projectId of projectIds) {
    const next = applyProjectPatch(projectId, (current) =>
      projectPatchTransform(current, mutationMode, 'api'),
    )
    if (!next) continue
    if (mutationMode === 'projection-stable') {
      tx.update(
        'projects',
        {
          name: next.name,
        },
        col('id').eq(next.id),
      )
    } else {
      tx.update(
        'projects',
        {
          healthScore: next.healthScore,
          updatedAt: next.updatedAt,
          metadata: next.metadata,
        },
        col('id').eq(next.id),
      )
    }
  }
  const patchLoopCompletedAt = performance.now()
  const commitStartedAt = performance.now()
  tx.commit()
  const commitCompletedAt = performance.now()

  postMutationProfile(runtime.active, 'api', {
    projectIdsCount: projectIds.length,
    collectProjectIdsMs: projectIdsCollectedAt - pending.startedAt,
    patchLoopMs: patchLoopCompletedAt - patchLoopStartedAt,
    commitCallMs: commitCompletedAt - commitStartedAt,
    ...buildCommitProfiles(),
  })
}

function handleError(error, context) {
  post({
    type: 'error',
    context,
    message: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? error.stack : undefined,
  })
}

parentPort.on('message', async (message) => {
  try {
    switch (message.type) {
      case 'init':
        post(await initRuntime(message))
        return
      case 'subscribe':
        subscribeScenario(message)
        return
      case 'socket-patch':
        runSocketPatchBurst(message)
        return
      case 'api-refresh':
        runApiRefresh(message)
        return
      case 'unsubscribe':
        unsubscribeActive()
        post({ type: 'unsubscribed', scenarioId: message.scenarioId })
        return
      case 'shutdown':
        unsubscribeActive()
        runtime.db?.free?.()
        post({ type: 'shutdown-complete' })
        return
      default:
        throw new Error(`Unknown worker message type: ${message.type}`)
    }
  } catch (error) {
    handleError(error, message.type)
  }
})
