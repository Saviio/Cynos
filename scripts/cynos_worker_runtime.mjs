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
import { DATASET_CONFIG } from './tanstack_db_benchmark_shared.mjs'
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
  active: null,
}

function pkOptions() {
  return new ColumnOptions().primaryKey(true)
}

function nullableOptions() {
  return new ColumnOptions().setNullable(true)
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

function buildIssueWindowStream(limit) {
  return runtime.db
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
      col('issues.status')
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
    .orderBy('issues.updatedAt', JsSortOrder.Desc)
    .limit(limit)
    .changes()
}

function buildProjectBoardStream() {
  return runtime.db
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
      col('projects.state')
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
    .orderBy('projects.healthScore', JsSortOrder.Desc)
    .limit(2_000)
    .changes()
}

function buildScenarioStream(scenarioId) {
  if (scenarioId === 'issue_window_500') {
    return buildIssueWindowStream(500)
  }

  if (scenarioId === 'issue_window_5000') {
    return buildIssueWindowStream(5_000)
  }

  if (scenarioId === 'project_board_2000') {
    return buildProjectBoardStream()
  }

  throw new Error(`Unknown scenario: ${scenarioId}`)
}

function mapIssueRow(row) {
  const issueMetadata = rowValue(row, 'issues.metadata', 'metadata') ?? {}
  const projectMetadata = row['project.metadata'] ?? {}

  return {
    issueId: rowValue(row, 'issues.id', 'id'),
    projectId: rowValue(row, 'project.id'),
    issueTitle: rowValue(row, 'title', 'issues.title'),
    issueStatus: rowValue(row, 'status', 'issues.status'),
    issuePriority: rowValue(row, 'priority', 'issues.priority'),
    issueSeverityRank: issueMetadata.severityRank ?? null,
    issueCustomerTier: issueMetadata.customer?.tier ?? null,
    projectName: rowValue(row, 'project.name', 'name'),
    projectState: rowValue(row, 'state', 'project.state'),
    projectHealth: rowValue(row, 'healthScore', 'project.healthScore'),
    projectRiskScore: projectMetadata.risk?.score ?? null,
    projectStrategic: projectMetadata.flags?.strategic ?? null,
    organizationName: row['org.name'] ?? null,
    teamName: row['team.name'] ?? null,
    assigneeName: row['assignee.name'] ?? null,
    milestoneName: row['milestone.name'] ?? null,
    openIssueCount: rowValue(row, 'openIssueCount', 'counter.openIssueCount') ?? 0,
    blockerCount: rowValue(row, 'blockerCount', 'counter.blockerCount') ?? 0,
    velocity: rowValue(row, 'velocity', 'snapshot.velocity') ?? 0,
    updatedAt: rowValue(row, 'updatedAt', 'issues.updatedAt'),
  }
}

function mapProjectBoardRow(row) {
  const projectMetadata = rowValue(row, 'projects.metadata', 'metadata') ?? {}

  return {
    projectId: rowValue(row, 'projects.id', 'id'),
    projectName: row['projects.name'] ?? null,
    projectState: rowValue(row, 'state', 'projects.state'),
    projectHealth: rowValue(row, 'healthScore', 'projects.healthScore'),
    projectRiskScore: projectMetadata.risk?.score ?? null,
    projectStrategic: projectMetadata.flags?.strategic ?? null,
    region: rowValue(row, 'region', 'org.region') ?? null,
    organizationName: row['org.name'] ?? null,
    teamName: row['team.name'] ?? null,
    leadName: row['lead.name'] ?? null,
    leadRole: rowValue(row, 'role', 'lead.role') ?? null,
    milestoneName: row['milestone.name'] ?? null,
    milestoneStatus: rowValue(row, 'status', 'milestone.status') ?? null,
    openIssueCount: rowValue(row, 'openIssueCount', 'counter.openIssueCount') ?? 0,
    blockerCount: rowValue(row, 'blockerCount', 'counter.blockerCount') ?? 0,
    staleIssueCount:
      rowValue(row, 'staleIssueCount', 'counter.staleIssueCount') ?? 0,
    velocity: rowValue(row, 'velocity', 'snapshot.velocity') ?? 0,
    blockedRatio: rowValue(row, 'blockedRatio', 'snapshot.blockedRatio') ?? 0,
    updatedAt: rowValue(row, 'updatedAt', 'projects.updatedAt'),
  }
}

function mapRowsForScenario(scenarioId, rows) {
  if (scenarioId === 'issue_window_500' || scenarioId === 'issue_window_5000') {
    return rows.map(mapIssueRow)
  }

  if (scenarioId === 'project_board_2000') {
    return rows.map(mapProjectBoardRow)
  }

  throw new Error(`Unknown scenario: ${scenarioId}`)
}

function extractProjectIds(rows, maxCount) {
  const result = []
  const seen = new Set()
  for (const row of rows) {
    if (row.projectId == null || seen.has(row.projectId)) continue
    seen.add(row.projectId)
    result.push(row.projectId)
    if (result.length >= maxCount) break
  }
  return result
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
  runtime.db = new Database(`cynos_bench_${Date.now()}`)
  createTables(runtime.db)

  for (const [tableName, rows] of Object.entries(runtime.server.tables)) {
    await insertTableInBatches(runtime.db, tableName, rows)
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
  runtime.active.unsubscribe?.()
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
    stream: null,
    currentRows: [],
    lastProjectIds: [],
    pending: {
      phase: 'initial',
      startedAt: performance.now(),
    },
    unsubscribe: null,
  }

  runtime.active = active
  active.stream = buildScenarioStream(message.scenarioId)

  active.unsubscribe = active.stream.subscribe((rawRows) => {
    const pending = active.pending
    const rows = mapRowsForScenario(active.scenarioId, rawRows)
    active.currentRows = rows
    const nextProjectIds = extractProjectIds(rows, Number.POSITIVE_INFINITY)
    if (nextProjectIds.length > 0) {
      active.lastProjectIds = nextProjectIds
    }
    if (!pending) return
    post(createSnapshotMessage(active, pending.phase, rows))
    active.pending = null
  })
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
  tx.commit()
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
  tx.commit()
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
