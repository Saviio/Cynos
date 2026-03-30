import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { parentPort } from 'node:worker_threads'
import { performance } from 'node:perf_hooks'
import initWasm, {
  Database,
  JsDataType,
  ColumnOptions,
  ForeignKeyOptions,
  col,
} from '../js/packages/core/dist/wasm.js'
import { DATASET_CONFIG } from './tanstack_db_benchmark_shared.mjs'
import { extractProjectIds } from './cynos_benchmark_row_shape.mjs'
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

const runtime = {
  initialized: false,
  db: null,
  server: null,
  lookups: null,
  prepared: null,
  active: null,
}

function pkOptions() {
  return new ColumnOptions().primaryKey(true)
}

function nullableOptions() {
  return new ColumnOptions().setNullable(true)
}

function fk(fieldName, reverseFieldName) {
  return new ForeignKeyOptions()
    .fieldName(fieldName)
    .reverseFieldName(reverseFieldName)
}

function post(message) {
  parentPort.postMessage(message)
}

function rowValue(row, ...keys) {
  for (const key of keys) {
    if (key in row) return row[key]
  }
  return undefined
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
      .foreignKey(
        'fk_teams_organization',
        'organizationId',
        'organizations',
        'id',
        fk('organization', 'teams'),
      )
      .index('idx_teams_organizationId', 'organizationId'),
  )

  db.registerTable(
    db.createTable('users')
      .column('id', JsDataType.Int64, pkOptions())
      .column('teamId', JsDataType.Int64, null)
      .column('name', JsDataType.String, null)
      .column('role', JsDataType.String, null)
      .column('metadata', JsDataType.Jsonb, null)
      .foreignKey('fk_users_team', 'teamId', 'teams', 'id', fk('team', 'users'))
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
      .foreignKey(
        'fk_projects_organization',
        'organizationId',
        'organizations',
        'id',
        fk('organization', 'projects'),
      )
      .foreignKey('fk_projects_team', 'teamId', 'teams', 'id', fk('team', 'projects'))
      .foreignKey(
        'fk_projects_lead',
        'leadUserId',
        'users',
        'id',
        fk('lead', 'ledProjects'),
      )
      .index('idx_projects_organizationId', 'organizationId')
      .index('idx_projects_teamId', 'teamId')
      .index('idx_projects_leadUserId', 'leadUserId')
      .index('idx_projects_state', 'state')
      .index('idx_projects_healthScore', 'healthScore')
      .index('idx_projects_updatedAt', 'updatedAt')
      .jsonbIndex('metadata', ['$.risk.bucket', '$.risk.score', '$.flags.strategic']),
  )

  db.registerTable(
    db.createTable('projectSnapshots')
      .column('projectId', JsDataType.Int64, pkOptions())
      .column('velocity', JsDataType.Int32, null)
      .column('completionRate', JsDataType.Float64, null)
      .column('blockedRatio', JsDataType.Float64, null)
      .column('updatedAt', JsDataType.Int64, null)
      .foreignKey(
        'fk_project_snapshots_project',
        'projectId',
        'projects',
        'id',
        fk('project', 'snapshot'),
      )
      .index('idx_projectSnapshots_velocity', 'velocity'),
  )

  db.registerTable(
    db.createTable('projectCounters')
      .column('projectId', JsDataType.Int64, pkOptions())
      .column('openIssueCount', JsDataType.Int32, null)
      .column('blockerCount', JsDataType.Int32, null)
      .column('staleIssueCount', JsDataType.Int32, null)
      .column('updatedAt', JsDataType.Int64, null)
      .foreignKey(
        'fk_project_counters_project',
        'projectId',
        'projects',
        'id',
        fk('project', 'counter'),
      )
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
      .foreignKey(
        'fk_current_milestones_project',
        'projectId',
        'projects',
        'id',
        fk('project', 'milestone'),
      )
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
      .foreignKey(
        'fk_issues_project',
        'projectId',
        'projects',
        'id',
        fk('project', 'issues'),
      )
      .foreignKey(
        'fk_issues_assignee',
        'assigneeId',
        'users',
        'id',
        fk('assignee', 'assignedIssues'),
      )
      .foreignKey(
        'fk_issues_current_milestone',
        'currentMilestoneId',
        'currentMilestones',
        'id',
        fk('currentMilestone', 'issues'),
      )
      .index('idx_issues_projectId', 'projectId')
      .index('idx_issues_assigneeId', 'assigneeId')
      .index('idx_issues_currentMilestoneId', 'currentMilestoneId')
      .index('idx_issues_status', 'status')
      .index('idx_issues_estimate', 'estimate')
      .index('idx_issues_updatedAt', 'updatedAt')
      .jsonbIndex(
        'metadata',
        ['$.customer.tier', '$.severityRank', '$.workflow.lane'],
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

const ISSUE_FEED_SUBSCRIPTION = `
  subscription IssueFeed($rootLimit: Int!, $projectIds: [Long!]!) {
    issues(
      where: {
        AND: [
          { projectId: { in: $projectIds } }
          { OR: [{ status: { eq: "open" } }, { status: { eq: "in_progress" } }] }
          { estimate: { gte: 3 } }
          {
            OR: [
              { metadata: { path: "$.customer.tier", eq: "enterprise" } }
              { metadata: { path: "$.customer.tier", eq: "mid_market" } }
            ]
          }
        ]
      }
      orderBy: [{ field: UPDATEDAT, direction: DESC }]
      limit: $rootLimit
    ) {
      id
      title
      status
      priority
      estimate
      updatedAt
      metadata
      project {
        id
        name
        state
        healthScore
        updatedAt
        metadata
        organization {
          name
        }
        team {
          name
        }
        counter(where: { openIssueCount: { gte: 5 } }, limit: 1) {
          openIssueCount
          blockerCount
          staleIssueCount
          updatedAt
        }
        snapshot(where: { velocity: { gte: 18 } }, limit: 1) {
          velocity
          blockedRatio
          completionRate
          updatedAt
        }
      }
      assignee {
        name
      }
      currentMilestone {
        name
        status
      }
    }
  }
`

const ISSUE_FEED_QUERY = ISSUE_FEED_SUBSCRIPTION.replace(
  'subscription IssueFeed',
  'query IssueFeed',
)

const ISSUE_FEED_STREAM_SUBSCRIPTION = `
  subscription IssueFeedStream($projectIds: [Long!]!) {
    issues(
      where: {
        AND: [
          { projectId: { in: $projectIds } }
          { OR: [{ status: { eq: "open" } }, { status: { eq: "in_progress" } }] }
          { estimate: { gte: 3 } }
          {
            OR: [
              { metadata: { path: "$.customer.tier", eq: "enterprise" } }
              { metadata: { path: "$.customer.tier", eq: "mid_market" } }
            ]
          }
        ]
      }
    ) {
      id
      title
      status
      priority
      estimate
      updatedAt
      metadata
      project {
        id
        name
        state
        healthScore
        updatedAt
        metadata
        organization {
          name
        }
        team {
          name
        }
        counter(where: { openIssueCount: { gte: 5 } }, limit: 1) {
          openIssueCount
          blockerCount
          staleIssueCount
          updatedAt
        }
        snapshot(where: { velocity: { gte: 18 } }, limit: 1) {
          velocity
          blockedRatio
          completionRate
          updatedAt
        }
      }
      assignee {
        name
      }
      currentMilestone {
        name
        status
      }
    }
  }
`

const ISSUE_FEED_STREAM_QUERY = ISSUE_FEED_STREAM_SUBSCRIPTION.replace(
  'subscription IssueFeedStream',
  'query IssueFeedStream',
)

const PROJECT_BOARD_SUBSCRIPTION = `
  subscription ProjectBoard($rootLimit: Int!) {
    projects(
      where: {
        AND: [
          { OR: [{ state: { eq: "active" } }, { state: { eq: "at_risk" } }] }
          { healthScore: { gte: 45 } }
          {
            OR: [
              { metadata: { path: "$.risk.bucket", eq: "high" } }
              { metadata: { path: "$.risk.bucket", eq: "critical" } }
            ]
          }
        ]
      }
      orderBy: [{ field: HEALTHSCORE, direction: DESC }]
      limit: $rootLimit
    ) {
      id
      name
      state
      healthScore
      updatedAt
      metadata
      organization {
        region
        name
      }
      team {
        name
      }
      lead {
        name
        role
      }
      counter(where: { openIssueCount: { gte: 4 } }, limit: 1) {
        openIssueCount
        blockerCount
        staleIssueCount
        updatedAt
      }
      snapshot(where: { velocity: { gte: 20 } }, limit: 1) {
        velocity
        blockedRatio
        completionRate
        updatedAt
      }
      milestone(limit: 1) {
        name
        status
      }
    }
  }
`

const PROJECT_BOARD_QUERY = PROJECT_BOARD_SUBSCRIPTION.replace(
  'subscription ProjectBoard',
  'query ProjectBoard',
)

const PROJECT_BOARD_STREAM_SUBSCRIPTION = `
  subscription ProjectBoardStream {
    projects(
      where: {
        AND: [
          { OR: [{ state: { eq: "active" } }, { state: { eq: "at_risk" } }] }
          { healthScore: { gte: 45 } }
          {
            OR: [
              { metadata: { path: "$.risk.bucket", eq: "high" } }
              { metadata: { path: "$.risk.bucket", eq: "critical" } }
            ]
          }
        ]
      }
    ) {
      id
      name
      state
      healthScore
      updatedAt
      metadata
      organization {
        region
        name
      }
      team {
        name
      }
      lead {
        name
        role
      }
      counter(where: { openIssueCount: { gte: 4 } }, limit: 1) {
        openIssueCount
        blockerCount
        staleIssueCount
        updatedAt
      }
      snapshot(where: { velocity: { gte: 20 } }, limit: 1) {
        velocity
        blockedRatio
        completionRate
        updatedAt
      }
      milestone(limit: 1) {
        name
        status
      }
    }
  }
`

const PROJECT_BOARD_STREAM_QUERY = PROJECT_BOARD_STREAM_SUBSCRIPTION.replace(
  'subscription ProjectBoardStream',
  'query ProjectBoardStream',
)

function issueRootLimit(visibleLimit) {
  return visibleLimit
}

function boardRootLimit() {
  return DATASET_CONFIG.projectCount
}

function scenarioVisibleLimit(scenarioId) {
  if (scenarioId === 'issue_window_500') return 500
  if (scenarioId === 'issue_window_5000') return 5_000
  if (scenarioId === 'issue_stream_all') return Number.POSITIVE_INFINITY
  if (scenarioId === 'project_board_2000') return 2_000
  if (scenarioId === 'project_board_stream_all') return Number.POSITIVE_INFINITY
  throw new Error(`Unknown scenario: ${scenarioId}`)
}

function isIssueScenario(scenarioId) {
  return (
    scenarioId === 'issue_window_500' ||
    scenarioId === 'issue_window_5000' ||
    scenarioId === 'issue_stream_all'
  )
}

function byProjectId(rows) {
  const map = new Map()
  for (const row of rows ?? []) {
    map.set(row.projectId, row)
  }
  return map
}

function eligibleIssueProjectIds() {
  const countersByProjectId = runtime.lookups?.projectCounters
  const snapshotsByProjectId = runtime.lookups?.projectSnapshots
  const ids = []

  for (const project of runtime.server.tables.projects) {
    const counter = countersByProjectId?.get(project.id)
    const snapshot = snapshotsByProjectId?.get(project.id)
    const riskBucket = project.metadata?.risk?.bucket

    if (project.healthScore < 45) continue
    if (riskBucket !== 'high' && riskBucket !== 'critical') continue
    if (!counter || counter.openIssueCount < 5) continue
    if (!snapshot || snapshot.velocity < 18) continue

    ids.push(project.id)
  }

  return ids.length > 0 ? ids : [-1]
}

function scenarioVariables(scenarioId) {
  if (scenarioId === 'issue_window_500') {
    return {
      rootLimit: issueRootLimit(500),
      projectIds: eligibleIssueProjectIds(),
    }
  }
  if (scenarioId === 'issue_window_5000') {
    return {
      rootLimit: issueRootLimit(5_000),
      projectIds: eligibleIssueProjectIds(),
    }
  }
  if (scenarioId === 'issue_stream_all') {
    return {
      projectIds: eligibleIssueProjectIds(),
    }
  }
  if (scenarioId === 'project_board_2000') {
    return { rootLimit: boardRootLimit() }
  }
  if (scenarioId === 'project_board_stream_all') {
    return {}
  }
  throw new Error(`Unknown scenario: ${scenarioId}`)
}

function buildScenarioSubscription(scenarioId, variables) {
  if (scenarioId === 'issue_window_500' || scenarioId === 'issue_window_5000') {
    return runtime.prepared.issueFeed.subscribe(variables)
  }
  if (scenarioId === 'issue_stream_all') {
    return runtime.prepared.issueFeedStream.subscribe(variables)
  }

  if (scenarioId === 'project_board_2000') {
    return runtime.prepared.projectBoard.subscribe(variables)
  }
  if (scenarioId === 'project_board_stream_all') {
    return runtime.prepared.projectBoardStream.subscribe(variables)
  }

  throw new Error(`Unknown scenario: ${scenarioId}`)
}

function executeScenarioQuery(scenarioId, variables) {
  if (scenarioId === 'issue_window_500' || scenarioId === 'issue_window_5000') {
    return runtime.prepared.issueFeedQuery.exec(variables)
  }
  if (scenarioId === 'issue_stream_all') {
    return runtime.prepared.issueFeedStreamQuery.exec(variables)
  }

  if (scenarioId === 'project_board_2000') {
    return runtime.prepared.projectBoardQuery.exec(variables)
  }
  if (scenarioId === 'project_board_stream_all') {
    return runtime.prepared.projectBoardStreamQuery.exec(variables)
  }

  throw new Error(`Unknown scenario: ${scenarioId}`)
}

function mapIssuePayloadRows(rows, visibleLimit) {
  const result = []

  for (const issue of rows ?? []) {
    const project = issue.project
    const counter = project?.counter?.[0]
    const snapshot = project?.snapshot?.[0]
    if (!project || !counter || !snapshot) continue

    result.push({
      issueId: issue.id,
      projectId: project.id,
      issueTitle: issue.title,
      issueStatus: issue.status,
      issuePriority: issue.priority,
      issueSeverityRank: issue.metadata?.severityRank ?? null,
      issueCustomerTier: issue.metadata?.customer?.tier ?? null,
      projectName: project.name,
      projectState: project.state,
      projectHealth: project.healthScore,
      projectRiskScore: project.metadata?.risk?.score ?? null,
      projectStrategic: project.metadata?.flags?.strategic ?? null,
      organizationName: project.organization?.name ?? null,
      teamName: project.team?.name ?? null,
      assigneeName: issue.assignee?.name ?? null,
      milestoneName: issue.currentMilestone?.name ?? null,
      openIssueCount: counter.openIssueCount ?? 0,
      blockerCount: counter.blockerCount ?? 0,
      velocity: snapshot.velocity ?? 0,
      updatedAt: issue.updatedAt,
    })
  }

  if (Number.isFinite(visibleLimit)) {
    result.sort((left, right) => right.updatedAt - left.updatedAt)
    return result.slice(0, visibleLimit)
  }

  return result
}

function mapProjectBoardPayloadRows(rows, visibleLimit) {
  const result = []

  for (const project of rows ?? []) {
    const counter = project.counter?.[0]
    const snapshot = project.snapshot?.[0]
    const milestone = project.milestone?.[0]
    if (!counter || !snapshot) continue

    result.push({
      projectId: project.id,
      projectName: project.name,
      projectState: project.state,
      projectHealth: project.healthScore,
      projectRiskScore: project.metadata?.risk?.score ?? null,
      projectStrategic: project.metadata?.flags?.strategic ?? null,
      region: project.organization?.region ?? null,
      organizationName: project.organization?.name ?? null,
      teamName: project.team?.name ?? null,
      leadName: project.lead?.name ?? null,
      leadRole: project.lead?.role ?? null,
      milestoneName: milestone?.name ?? null,
      milestoneStatus: milestone?.status ?? null,
      openIssueCount: counter.openIssueCount ?? 0,
      blockerCount: counter.blockerCount ?? 0,
      staleIssueCount: counter.staleIssueCount ?? 0,
      velocity: snapshot.velocity ?? 0,
      blockedRatio: snapshot.blockedRatio ?? 0,
      updatedAt: project.updatedAt,
    })
  }

  if (Number.isFinite(visibleLimit)) {
    result.sort((left, right) => right.projectHealth - left.projectHealth)
    return result.slice(0, visibleLimit)
  }

  return result
}

function mapPayloadForScenario(scenarioId, payload, visibleLimit) {
  const data = payload?.data ?? {}

  if (
    scenarioId === 'issue_window_500' ||
    scenarioId === 'issue_window_5000' ||
    scenarioId === 'issue_stream_all'
  ) {
    return mapIssuePayloadRows(data.issues ?? [], visibleLimit)
  }

  if (scenarioId === 'project_board_2000' || scenarioId === 'project_board_stream_all') {
    return mapProjectBoardPayloadRows(data.projects ?? [], visibleLimit)
  }

  throw new Error(`Unknown scenario: ${scenarioId}`)
}

function syncActiveRows(active, payload) {
  const rows = mapPayloadForScenario(active.scenarioId, payload, active.visibleLimit)
  active.currentRows = rows

  const nextProjectIds = extractProjectIds(rows, Number.POSITIVE_INFINITY)
  if (nextProjectIds.length > 0) {
    active.lastProjectIds = nextProjectIds
  }

  return rows
}

function flushPendingSnapshot(active, rows) {
  const pending = active.pending
  if (!pending) return
  post(createSnapshotMessage(active, pending.phase, rows))
  active.pending = null
}

function cancelScheduledReattach(active) {
  if (active.reattachHandle == null) return
  clearTimeout(active.reattachHandle)
  active.reattachHandle = null
}

function detachSubscription(active) {
  cancelScheduledReattach(active)
  active.unsubscribe?.()
  active.subscription?.free?.()
  active.unsubscribe = null
  active.subscription = null
}

function attachSubscription(active, variables = scenarioVariables(active.scenarioId)) {
  detachSubscription(active)

  active.variables = variables
  active.subscriptionGeneration = (active.subscriptionGeneration ?? 0) + 1
  const generation = active.subscriptionGeneration
  const subscription = buildScenarioSubscription(active.scenarioId, variables)

  active.subscription = subscription
  active.unsubscribe = subscription.subscribe((payload) => {
    if (runtime.active !== active) return
    if (active.subscriptionGeneration !== generation) return
    if (active.subscriptionMuted) return

    const rows = syncActiveRows(active, payload)
    flushPendingSnapshot(active, rows)
  })
}

function scheduleAttachSubscription(active, variables) {
  cancelScheduledReattach(active)
  active.reattachHandle = setTimeout(() => {
    active.reattachHandle = null
    if (runtime.active !== active) return
    attachSubscription(active, variables)
  }, 0)
}

function refreshIssueScenarioSnapshot(active) {
  active.subscriptionMuted = true
  const variables = scenarioVariables(active.scenarioId)
  const payload = executeScenarioQuery(active.scenarioId, variables)
  const rows = syncActiveRows(active, payload)
  flushPendingSnapshot(active, rows)
  active.subscriptionMuted = false
  scheduleAttachSubscription(active, variables)
}

async function initRuntime() {
  if (runtime.initialized) {
    return {
      type: 'ready',
      dataset: summarizeDataset(runtime.server.tables),
    }
  }

  const startedAt = performance.now()
  await initWasm({ module_or_path: await fs.readFile(WASM_PATH) })

  runtime.server = buildServerDataset(DATASET_CONFIG)
  runtime.lookups = {
    projectCounters: byProjectId(runtime.server.tables.projectCounters),
    projectSnapshots: byProjectId(runtime.server.tables.projectSnapshots),
  }
  runtime.db = new Database(`cynos_bench_${Date.now()}`)
  createTables(runtime.db)

  for (const [tableName, rows] of Object.entries(runtime.server.tables)) {
    await insertTableInBatches(runtime.db, tableName, rows)
  }

  runtime.prepared = {
    issueFeed: runtime.db.prepareGraphql(ISSUE_FEED_SUBSCRIPTION, 'IssueFeed'),
    issueFeedQuery: runtime.db.prepareGraphql(ISSUE_FEED_QUERY, 'IssueFeed'),
    issueFeedStream: runtime.db.prepareGraphql(
      ISSUE_FEED_STREAM_SUBSCRIPTION,
      'IssueFeedStream',
    ),
    issueFeedStreamQuery: runtime.db.prepareGraphql(
      ISSUE_FEED_STREAM_QUERY,
      'IssueFeedStream',
    ),
    projectBoard: runtime.db.prepareGraphql(
      PROJECT_BOARD_SUBSCRIPTION,
      'ProjectBoard',
    ),
    projectBoardQuery: runtime.db.prepareGraphql(
      PROJECT_BOARD_QUERY,
      'ProjectBoard',
    ),
    projectBoardStream: runtime.db.prepareGraphql(
      PROJECT_BOARD_STREAM_SUBSCRIPTION,
      'ProjectBoardStream',
    ),
    projectBoardStreamQuery: runtime.db.prepareGraphql(
      PROJECT_BOARD_STREAM_QUERY,
      'ProjectBoardStream',
    ),
  }

  runtime.initialized = true
  return {
    type: 'ready',
    initMs: performance.now() - startedAt,
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
  detachSubscription(runtime.active)
  runtime.active = null
}

function createSnapshotMessage(active, phase, rows) {
  return {
    type: 'snapshot',
    scenarioId: active.scenarioId,
    phase,
    workerLatencyMs: performance.now() - active.pending.startedAt,
    rowCount: rows.length,
    changeCount: 1,
    rows: active.includeRows ? rows : undefined,
  }
}

function subscribeScenario(message) {
  ensureInitialized()
  unsubscribeActive()

  const active = {
    scenarioId: message.scenarioId,
    includeRows: message.includeRows !== false,
    visibleLimit: scenarioVisibleLimit(message.scenarioId),
    subscription: null,
    currentRows: [],
    lastProjectIds: [],
    pending: {
      phase: 'initial',
      startedAt: performance.now(),
    },
    unsubscribe: null,
    variables: null,
    subscriptionGeneration: 0,
    subscriptionMuted: false,
    reattachHandle: null,
  }

  runtime.active = active
  attachSubscription(active)
}

function collectActiveProjectIds(maxCount) {
  if (!runtime.active) {
    throw new Error('No active live query subscription.')
  }

  const currentIds = extractProjectIds(runtime.active.currentRows, maxCount)
  if (currentIds.length > 0) return currentIds

  if (runtime.active.lastProjectIds.length > 0) {
    return runtime.active.lastProjectIds.slice(0, maxCount)
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

  const projectIds = collectActiveProjectIds(message.patchCount)
  runtime.active.pending = {
    phase: 'socket',
    startedAt: performance.now(),
  }

  const tx = runtime.db.transaction()
  for (const projectId of projectIds) {
    const next = applyProjectPatch(projectId, (current) => {
      const healthScore = current.healthScore >= 45 ? 24 : 82
      return {
        ...current,
        state: current.state === 'active' ? 'at_risk' : 'active',
        healthScore,
        updatedAt: current.updatedAt + 60_000,
      }
    })
    if (!next) continue
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
  const shouldRefreshIssueScenario = isIssueScenario(runtime.active.scenarioId)
  if (shouldRefreshIssueScenario) {
    detachSubscription(runtime.active)
  }
  tx.commit()
  if (shouldRefreshIssueScenario) {
    refreshIssueScenarioSnapshot(runtime.active)
  }
}

function runApiRefresh(message) {
  ensureInitialized()
  if (!runtime.active) {
    throw new Error('Subscribe before running API refresh.')
  }

  const projectIds = collectActiveProjectIds(message.patchCount)
  runtime.active.pending = {
    phase: 'api',
    startedAt: performance.now(),
  }

  const tx = runtime.db.transaction()
  for (const projectId of projectIds) {
    const next = applyProjectPatch(projectId, (current) => {
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
    })
    if (!next) continue
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
  const shouldRefreshIssueScenario = isIssueScenario(runtime.active.scenarioId)
  if (shouldRefreshIssueScenario) {
    detachSubscription(runtime.active)
  }
  tx.commit()
  if (shouldRefreshIssueScenario) {
    refreshIssueScenarioSnapshot(runtime.active)
  }
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
        post(await initRuntime())
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
