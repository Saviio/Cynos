import fs from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { performance } from 'node:perf_hooks'
import initWasm, {
  Database,
  JsDataType,
  JsSortOrder,
  ColumnOptions,
  col,
} from '../js/packages/core/dist/wasm.js'
import { ResultSet, snapshotSchemaLayout } from '../js/packages/core/dist/index.js'
import { DATASET_CONFIG } from './tanstack_db_benchmark_shared.mjs'
import {
  buildServerDataset,
  summarizeDataset,
} from './live_query_benchmark_dataset.mjs'

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url))
const ROOT_DIR = path.resolve(SCRIPT_DIR, '..')
const WASM_PATH = path.join(ROOT_DIR, 'js', 'packages', 'core', 'dist', 'cynos.wasm')
const REPORT_PATH = path.join(ROOT_DIR, 'tmp', 'cynos_query_plan_probe.md')
const JSON_REPORT_PATH = path.join(ROOT_DIR, 'tmp', 'cynos_query_plan_probe.json')
const ROUNDS = Number.parseInt(process.env.CYNOS_QUERY_PROBE_ROUNDS ?? '3', 10)

function pkOptions() {
  return new ColumnOptions().primaryKey(true)
}

function nullableOptions() {
  return new ColumnOptions().setNullable(true)
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

function issueRootPredicate() {
  return col('issues.status')
    .eq('open')
    .or(col('issues.status').eq('in_progress'))
    .and(col('issues.estimate').gte(3))
    .and(
      col('issues.metadata')
        .get('$.customer.tier')
        .eq('enterprise')
        .or(col('issues.metadata').get('$.customer.tier').eq('mid_market')),
    )
}

function issueJoinedPredicate() {
  return issueRootPredicate()
    .and(col('project.healthScore').gte(45))
    .and(
      col('project.metadata')
        .get('$.risk.bucket')
        .eq('high')
        .or(col('project.metadata').get('$.risk.bucket').eq('critical')),
    )
    .and(col('counter.openIssueCount').gte(5))
    .and(col('snapshot.velocity').gte(18))
}

function issueJoinBase(db) {
  return db
    .select([
      'issues.id',
      'issues.updatedAt',
      'issues.status',
      'issues.estimate',
      'project.id',
      'project.healthScore',
      'project.metadata',
      'counter.openIssueCount',
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
}

function projectRootPredicate() {
  return col('projects.state')
    .eq('active')
    .or(col('projects.state').eq('at_risk'))
    .and(col('projects.healthScore').gte(45))
    .and(
      col('projects.metadata')
        .get('$.risk.bucket')
        .eq('high')
        .or(col('projects.metadata').get('$.risk.bucket').eq('critical')),
    )
}

function projectJoinedPredicate() {
  return projectRootPredicate()
    .and(col('counter.openIssueCount').gte(4))
    .and(col('snapshot.velocity').gte(20))
}

function projectJoinBase(db) {
  return db
    .select([
      'projects.id',
      'projects.healthScore',
      'projects.updatedAt',
      'projects.metadata',
      'counter.openIssueCount',
      'snapshot.velocity',
      'milestone.name',
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
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid]
}

function mean(values) {
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

function formatMs(value) {
  return value < 1 ? `${value.toFixed(3)} ms` : `${value.toFixed(2)} ms`
}

function normalizePlan(plan) {
  return {
    logical: String(plan.logical ?? ''),
    optimized: String(plan.optimized ?? ''),
    physical: String(plan.physical ?? ''),
  }
}

async function benchmarkPrepared(prepared, decodeBinary = false) {
  const layout = snapshotSchemaLayout(prepared.getSchemaLayout())
  const execBinaryMs = []
  const decodeBinaryMs = []
  const execObjectMs = []
  let lastRowCount = 0

  for (let round = 0; round < ROUNDS; round += 1) {
    let startedAt = performance.now()
    const binary = await prepared.execBinary()
    execBinaryMs.push(performance.now() - startedAt)

    startedAt = performance.now()
    const resultSet = new ResultSet(binary, layout)
    lastRowCount = resultSet.length
    if (decodeBinary) {
      resultSet.toArray()
    }
    decodeBinaryMs.push(performance.now() - startedAt)

    startedAt = performance.now()
    const objectRows = await prepared.exec()
    execObjectMs.push(performance.now() - startedAt)
    lastRowCount = Array.isArray(objectRows) ? objectRows.length : lastRowCount
  }

  return {
    rowCount: lastRowCount,
    execBinaryMs,
    decodeBinaryMs,
    execObjectMs,
    binaryMedianMs: median(execBinaryMs),
    binaryMeanMs: mean(execBinaryMs),
    decodeMedianMs: median(decodeBinaryMs),
    decodeMeanMs: mean(decodeBinaryMs),
    objectMedianMs: median(execObjectMs),
    objectMeanMs: mean(execObjectMs),
  }
}

function analyzeDataset(server) {
  const projectById = new Map(server.tables.projects.map((row) => [row.id, row]))
  const counterByProjectId = new Map(
    server.tables.projectCounters.map((row) => [row.projectId, row]),
  )
  const snapshotByProjectId = new Map(
    server.tables.projectSnapshots.map((row) => [row.projectId, row]),
  )

  const issueRootMatches = server.tables.issues.filter((issue) => {
    const statusMatch = issue.status === 'open' || issue.status === 'in_progress'
    const estimateMatch = issue.estimate >= 3
    const tier = issue.metadata?.customer?.tier
    const tierMatch = tier === 'enterprise' || tier === 'mid_market'
    return statusMatch && estimateMatch && tierMatch
  })

  const projectJoinedMatches = server.tables.projects.filter((project) => {
    const riskBucket = project.metadata?.risk?.bucket
    const counter = counterByProjectId.get(project.id)
    const snapshot = snapshotByProjectId.get(project.id)
    return (
      (project.state === 'active' || project.state === 'at_risk') &&
      project.healthScore >= 45 &&
      (riskBucket === 'high' || riskBucket === 'critical') &&
      (counter?.openIssueCount ?? 0) >= 4 &&
      (snapshot?.velocity ?? 0) >= 20
    )
  })

  const issueJoinedMatches = issueRootMatches.filter((issue) => {
    const project = projectById.get(issue.projectId)
    if (!project) return false
    const riskBucket = project.metadata?.risk?.bucket
    const counter = counterByProjectId.get(project.id)
    const snapshot = snapshotByProjectId.get(project.id)
    return (
      project.healthScore >= 45 &&
      (riskBucket === 'high' || riskBucket === 'critical') &&
      (counter?.openIssueCount ?? 0) >= 5 &&
      (snapshot?.velocity ?? 0) >= 18
    )
  })

  return {
    issueRootMatches: issueRootMatches.length,
    issueJoinedMatches: issueJoinedMatches.length,
    projectJoinedMatches: projectJoinedMatches.length,
  }
}

function issueVariants(db) {
  return [
    {
      id: 'issue_root_filter_only',
      label: 'Issues root filter only',
      build: () =>
        db
          .select(['issues.id', 'issues.updatedAt'])
          .from('issues')
          .where(issueRootPredicate()),
    },
    {
      id: 'issue_root_filter_topn_5000',
      label: 'Issues root filter + ORDER BY/LIMIT',
      build: () =>
        db
          .select(['issues.id', 'issues.updatedAt'])
          .from('issues')
          .where(issueRootPredicate())
          .orderBy('issues.updatedAt', JsSortOrder.Desc)
          .limit(5_000),
    },
    {
      id: 'issue_join_root_filter_only',
      label: '7-way join + root-table predicates only',
      build: () => issueJoinBase(db).where(issueRootPredicate()),
    },
    {
      id: 'issue_join_full_filter',
      label: '7-way join + full benchmark predicates',
      build: () => issueJoinBase(db).where(issueJoinedPredicate()),
    },
    {
      id: 'issue_join_full_filter_topn_5000',
      label: '7-way join + full predicates + ORDER BY/LIMIT',
      build: () =>
        issueJoinBase(db)
          .where(issueJoinedPredicate())
          .orderBy('issues.updatedAt', JsSortOrder.Desc)
          .limit(5_000),
    },
  ]
}

function projectVariants(db) {
  return [
    {
      id: 'project_root_filter_only',
      label: 'Projects root filter only',
      build: () =>
        db
          .select(['projects.id', 'projects.healthScore'])
          .from('projects')
          .where(projectRootPredicate()),
    },
    {
      id: 'project_join_full_filter',
      label: '6-way join + full board predicates',
      build: () => projectJoinBase(db).where(projectJoinedPredicate()),
    },
    {
      id: 'project_join_full_filter_topn_2000',
      label: '6-way join + full predicates + ORDER BY/LIMIT',
      build: () =>
        projectJoinBase(db)
          .where(projectJoinedPredicate())
          .orderBy('projects.healthScore', JsSortOrder.Desc)
          .limit(2_000),
    },
  ]
}

function summarizePlan(plan) {
  const text = `${plan.optimized}\n${plan.physical}`
  return {
    hasTopN: text.includes('TopN'),
    hasHashJoin: text.includes('HashJoin'),
    hasIndexNestedLoopJoin: text.includes('IndexNestedLoopJoin'),
    hasGinIndexScan: text.includes('GinIndexScan'),
    hasGinIndexScanMulti: text.includes('GinIndexScanMulti'),
    hasFilterAboveJoin: text.includes('Filter') && text.includes('Join'),
  }
}

async function main() {
  await initWasm({ module_or_path: await fs.readFile(WASM_PATH) })

  const server = buildServerDataset(DATASET_CONFIG)
  const db = new Database(`cynos_query_probe_${Date.now()}`)
  createTables(db)

  const insertStartedAt = performance.now()
  for (const [tableName, rows] of Object.entries(server.tables)) {
    await insertTableInBatches(db, tableName, rows)
  }
  const insertMs = performance.now() - insertStartedAt

  const results = []
  for (const variant of [...issueVariants(db), ...projectVariants(db)]) {
    const query = variant.build()
    const plan = normalizePlan(query.explain())
    const prepared = query.prepare()
    const measurement = await benchmarkPrepared(prepared)
    results.push({
      id: variant.id,
      label: variant.label,
      plan,
      planFlags: summarizePlan(plan),
      measurement,
    })
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    dataset: summarizeDataset(server.tables),
    datasetSelectivity: analyzeDataset(server),
    insertMs,
    rounds: ROUNDS,
    results,
  }

  const lines = []
  lines.push('# Cynos Query Plan Probe')
  lines.push('')
  lines.push(`Generated at: ${summary.generatedAt}`)
  lines.push(`Dataset: ${JSON.stringify(summary.dataset)}`)
  lines.push(`Dataset selectivity: ${JSON.stringify(summary.datasetSelectivity)}`)
  lines.push(`Insert time: ${formatMs(summary.insertMs)}`)
  lines.push('')

  for (const result of results) {
    lines.push(`## ${result.label}`)
    lines.push('')
    lines.push(
      `Rows: ${result.measurement.rowCount}, execBinary median: ${formatMs(result.measurement.binaryMedianMs)}, exec() median: ${formatMs(result.measurement.objectMedianMs)}, binary decode median: ${formatMs(result.measurement.decodeMedianMs)}`,
    )
    lines.push(`Plan flags: ${JSON.stringify(result.planFlags)}`)
    lines.push('')
    lines.push('### Physical')
    lines.push('```')
    lines.push(result.plan.physical)
    lines.push('```')
    lines.push('')
    lines.push('### Optimized')
    lines.push('```')
    lines.push(result.plan.optimized)
    lines.push('```')
    lines.push('')
  }

  await fs.writeFile(REPORT_PATH, `${lines.join('\n')}\n`)
  await fs.writeFile(JSON_REPORT_PATH, `${JSON.stringify(summary, null, 2)}\n`)

  console.log(`Wrote report to ${REPORT_PATH}`)
  console.log(`Wrote JSON to ${JSON_REPORT_PATH}`)
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
