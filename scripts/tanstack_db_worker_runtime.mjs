import { parentPort } from 'node:worker_threads'
import { performance } from 'node:perf_hooks'
import { QueryClient } from '@tanstack/query-core'
import {
  BasicIndex,
  BTreeIndex,
  and,
  createCollection,
  createLiveQueryCollection,
  eq,
  gte,
  or,
} from '@tanstack/db'
import { queryCollectionOptions } from '@tanstack/query-db-collection'
import {
  API_REFRESH_COUNT,
  DATASET_CONFIG,
  SOCKET_PATCH_COUNT,
} from './tanstack_db_benchmark_shared.mjs'
import { extractProjectIds } from './cynos_benchmark_row_shape.mjs'

if (!parentPort) {
  throw new Error('This module must run inside a worker thread.')
}

const PROJECT_STATES = ['active', 'at_risk', 'planned', 'paused', 'archived']
const ORG_REGIONS = ['na', 'emea', 'apac', 'latam']
const TEAM_FUNCTIONS = ['product', 'design', 'engineering', 'ops', 'growth']
const CUSTOMER_TIERS = ['self_serve', 'mid_market', 'enterprise']
const ISSUE_LANES = ['backlog', 'triage', 'delivery', 'follow_up']
const PRIMARY_TAGS = ['ux', 'api', 'infra', 'growth', 'security', 'sales']
const SECONDARY_TAGS = ['mobile', 'web', 'sync', 'billing', 'search', 'ai']
const RISK_BUCKETS = ['low', 'medium', 'high', 'critical']
const MAX_TRACKED_PROJECT_IDS = Math.max(
  256,
  SOCKET_PATCH_COUNT,
  API_REFRESH_COUNT,
)

function createRandom(seed) {
  let value = seed >>> 0
  return () => {
    value += 0x6d2b79f5
    let result = Math.imul(value ^ (value >>> 15), 1 | value)
    result ^= result + Math.imul(result ^ (result >>> 7), 61 | result)
    return ((result ^ (result >>> 14)) >>> 0) / 4294967296
  }
}

function pick(random, values) {
  return values[Math.floor(random() * values.length)] ?? values[0]
}

function maybe(random, threshold = 0.5) {
  return random() < threshold
}

function intBetween(random, min, max) {
  return Math.floor(random() * (max - min + 1)) + min
}

function deepClone(value) {
  return structuredClone(value)
}

function stableModulo(id, modulo) {
  return ((id % modulo) + modulo) % modulo
}

function buildServerDataset(config) {
  const random = createRandom(config.seed)
  const now = Date.UTC(2026, 2, 27, 8, 0, 0)

  const organizations = Array.from({ length: config.organizationCount }, (_, idx) => {
    const id = idx + 1
    return {
      id,
      name: `Organization ${id}`,
      tier: CUSTOMER_TIERS[stableModulo(id, CUSTOMER_TIERS.length)],
      region: ORG_REGIONS[stableModulo(id, ORG_REGIONS.length)],
      metadata: {
        spendBand: intBetween(random, 1, 5),
        contract: {
          renewed: maybe(random, 0.72),
          seats: intBetween(random, 50, 5_000),
        },
      },
    }
  })

  const teams = Array.from({ length: config.teamCount }, (_, idx) => {
    const id = idx + 1
    const organizationId = stableModulo(id - 1, organizations.length) + 1
    return {
      id,
      organizationId,
      name: `Team ${id}`,
      function: TEAM_FUNCTIONS[stableModulo(id, TEAM_FUNCTIONS.length)],
      metadata: {
        timezoneOffset: stableModulo(id, 12) - 6,
        budgetCode: `BGT-${organizationId}-${id}`,
      },
    }
  })

  const users = Array.from({ length: config.userCount }, (_, idx) => {
    const id = idx + 1
    const teamId = stableModulo(id - 1, teams.length) + 1
    return {
      id,
      teamId,
      name: `User ${id}`,
      role: maybe(random, 0.08)
        ? 'staff'
        : maybe(random, 0.2)
          ? 'lead'
          : 'member',
      metadata: {
        locale: stableModulo(id, 2) === 0 ? 'en-US' : 'en-GB',
        focus: pick(random, ['product', 'platform', 'growth', 'design']),
        seniority: intBetween(random, 1, 6),
      },
    }
  })

  const teamUserIds = new Map()
  for (const user of users) {
    const existing = teamUserIds.get(user.teamId)
    if (existing) {
      existing.push(user.id)
    } else {
      teamUserIds.set(user.teamId, [user.id])
    }
  }

  const projects = Array.from({ length: config.projectCount }, (_, idx) => {
    const id = idx + 1
    const teamId = stableModulo(id - 1, teams.length) + 1
    const organizationId = teams[teamId - 1].organizationId
    const candidateUsers = teamUserIds.get(teamId) ?? [1]
    const leadUserId = candidateUsers[stableModulo(id, candidateUsers.length)]
    const healthScore = intBetween(random, 25, 95)
    return {
      id,
      organizationId,
      teamId,
      leadUserId,
      name: `Project ${id}`,
      state: pick(random, PROJECT_STATES),
      healthScore,
      updatedAt: now - id * 17_000,
      priorityBand: healthScore > 75 ? 'p0' : healthScore > 55 ? 'p1' : 'p2',
      metadata: {
        risk: {
          score: intBetween(random, 10, 95),
          bucket: pick(random, RISK_BUCKETS),
        },
        flags: {
          strategic: maybe(random, 0.28),
          regulated: maybe(random, 0.14),
        },
        topology: {
          shard: stableModulo(id, 32),
          market: pick(random, ORG_REGIONS),
        },
      },
    }
  })

  const milestones = []
  const milestonesByProject = new Map()
  let milestoneId = 1
  while (milestones.length < config.milestoneCount) {
    const project = projects[stableModulo(milestones.length, projects.length)]
    const existing = milestonesByProject.get(project.id) ?? []
    const row = {
      id: milestoneId,
      projectId: project.id,
      name: `Milestone ${milestoneId}`,
      dueAt: now + intBetween(random, 1, 180) * 86_400_000,
      status: maybe(random, 0.7) ? 'active' : 'planned',
      metadata: {
        quarter: `2026-Q${stableModulo(milestoneId, 4) + 1}`,
        slipDays: intBetween(random, 0, 18),
      },
    }
    milestones.push(row)
    existing.push(row.id)
    milestonesByProject.set(project.id, existing)
    milestoneId += 1
  }

  const issues = Array.from({ length: config.issueCount }, (_, idx) => {
    const id = idx + 1
    const project = projects[Math.floor(random() * projects.length)]
    const assigneePool = teamUserIds.get(project.teamId) ?? [project.leadUserId]
    const currentMilestoneIds = milestonesByProject.get(project.id) ?? []
    const currentMilestoneId =
      currentMilestoneIds.length > 0 && maybe(random, 0.78)
        ? currentMilestoneIds[Math.floor(random() * currentMilestoneIds.length)]
        : undefined
    const status =
      random() < 0.52
        ? 'open'
        : random() < 0.72
          ? 'in_progress'
          : random() < 0.88
            ? 'blocked'
            : 'closed'
    return {
      id,
      projectId: project.id,
      assigneeId: assigneePool[Math.floor(random() * assigneePool.length)],
      currentMilestoneId,
      title: `Issue ${id}`,
      status,
      priority: pick(random, ['low', 'medium', 'high', 'urgent']),
      estimate: intBetween(random, 1, 8),
      updatedAt: now - intBetween(random, 0, 14 * 24 * 60) * 60_000,
      metadata: {
        severityRank: intBetween(random, 1, 5),
        tags: {
          primary: pick(random, PRIMARY_TAGS),
          secondary: pick(random, SECONDARY_TAGS),
        },
        customer: {
          tier: pick(random, CUSTOMER_TIERS),
        },
        workflow: {
          lane: pick(random, ISSUE_LANES),
          slaHours: intBetween(random, 4, 96),
        },
      },
    }
  })

  const issueCounters = new Map()
  for (const issue of issues) {
    const current = issueCounters.get(issue.projectId) ?? {
      openIssueCount: 0,
      blockerCount: 0,
      staleIssueCount: 0,
      lastUpdatedAt: 0,
    }
    if (issue.status !== 'closed') current.openIssueCount += 1
    if (issue.status === 'blocked' || issue.metadata.severityRank >= 4) {
      current.blockerCount += 1
    }
    if (now - issue.updatedAt > 72 * 60 * 60 * 1000) {
      current.staleIssueCount += 1
    }
    if (issue.updatedAt > current.lastUpdatedAt) {
      current.lastUpdatedAt = issue.updatedAt
    }
    issueCounters.set(issue.projectId, current)
  }

  const projectCounters = projects.map((project) => {
    const counters = issueCounters.get(project.id) ?? {
      openIssueCount: 0,
      blockerCount: 0,
      staleIssueCount: 0,
      lastUpdatedAt: project.updatedAt,
    }
    return {
      projectId: project.id,
      openIssueCount: counters.openIssueCount,
      blockerCount: counters.blockerCount,
      staleIssueCount: counters.staleIssueCount,
      updatedAt: counters.lastUpdatedAt,
    }
  })

  const projectSnapshots = projects.map((project) => {
    const counters = issueCounters.get(project.id) ?? {
      openIssueCount: 0,
      blockerCount: 0,
      staleIssueCount: 0,
    }
    return {
      projectId: project.id,
      velocity: Math.max(8, 80 - counters.blockerCount * 2 - counters.staleIssueCount),
      completionRate: Math.max(0.1, Math.min(0.98, project.healthScore / 100)),
      blockedRatio:
        counters.openIssueCount === 0
          ? 0
          : Math.min(1, counters.blockerCount / counters.openIssueCount),
      updatedAt: project.updatedAt,
    }
  })

  const currentMilestones = projects
    .map((project) => {
      const ids = milestonesByProject.get(project.id) ?? []
      if (ids.length === 0) return null
      const firstId = ids[0]
      return milestones[firstId - 1]
    })
    .filter(Boolean)
    .map((row) => ({
      id: row.id,
      projectId: row.projectId,
      name: row.name,
      dueAt: row.dueAt,
      status: row.status,
      metadata: row.metadata,
    }))

  return {
    generatedAt: now,
    tables: {
      organizations,
      teams,
      users,
      projects,
      projectSnapshots,
      projectCounters,
      currentMilestones,
      issues,
    },
    revisions: {
      organizations: 1,
      teams: 1,
      users: 1,
      projects: 1,
      projectSnapshots: 1,
      projectCounters: 1,
      currentMilestones: 1,
      issues: 1,
    },
  }
}

function createApi(server) {
  return {
    async fetchTable(tableName) {
      const items = deepClone(server.tables[tableName])
      return {
        items,
        revision: server.revisions[tableName],
        total: items.length,
        source: 'mock-api',
      }
    },
  }
}

const runtime = {
  initialized: false,
  queryClient: null,
  api: null,
  server: null,
  collections: null,
  active: null,
  scenarioVariant: 'default',
}

function usesAlignedFilters(scenarioVariant) {
  return scenarioVariant === 'trace_aligned'
}

function usesTraceCapabilityAlignment(scenarioVariant) {
  return scenarioVariant === 'trace_capability_aligned'
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

function createBaseCollections(queryClient, api) {
  const buildCollection = (id, getKey) =>
    createCollection(
      queryCollectionOptions({
        id,
        queryKey: ['tanstack-bench', id],
        queryClient,
        getKey,
        retry: false,
        queryFn: async () => api.fetchTable(id),
        select: (response) => response.items,
      }),
    )

  const collections = {
    organizations: buildCollection('organizations', (row) => row.id),
    teams: buildCollection('teams', (row) => row.id),
    users: buildCollection('users', (row) => row.id),
    projects: buildCollection('projects', (row) => row.id),
    projectSnapshots: buildCollection('projectSnapshots', (row) => row.projectId),
    projectCounters: buildCollection('projectCounters', (row) => row.projectId),
    currentMilestones: buildCollection('currentMilestones', (row) => row.id),
    issues: buildCollection('issues', (row) => row.id),
  }

  collections.teams.createIndex((row) => row.organizationId, {
    indexType: BasicIndex,
  })
  collections.users.createIndex((row) => row.teamId, {
    indexType: BasicIndex,
  })
  collections.projects.createIndex((row) => row.organizationId, {
    indexType: BasicIndex,
  })
  collections.projects.createIndex((row) => row.teamId, {
    indexType: BasicIndex,
  })
  collections.projects.createIndex((row) => row.leadUserId, {
    indexType: BasicIndex,
  })
  collections.projects.createIndex((row) => row.state, {
    indexType: BasicIndex,
  })
  collections.projects.createIndex((row) => row.healthScore, {
    indexType: BTreeIndex,
  })
  collections.projects.createIndex((row) => row.metadata.risk.bucket, {
    indexType: BTreeIndex,
  })
  collections.projectSnapshots.createIndex((row) => row.projectId, {
    indexType: BasicIndex,
  })
  collections.projectSnapshots.createIndex((row) => row.velocity, {
    indexType: BTreeIndex,
  })
  collections.projectCounters.createIndex((row) => row.projectId, {
    indexType: BasicIndex,
  })
  collections.projectCounters.createIndex((row) => row.openIssueCount, {
    indexType: BTreeIndex,
  })
  collections.currentMilestones.createIndex((row) => row.projectId, {
    indexType: BasicIndex,
  })
  collections.issues.createIndex((row) => row.projectId, {
    indexType: BasicIndex,
  })
  collections.issues.createIndex((row) => row.assigneeId, {
    indexType: BasicIndex,
  })
  collections.issues.createIndex((row) => row.currentMilestoneId, {
    indexType: BasicIndex,
  })
  collections.issues.createIndex((row) => row.status, {
    indexType: BasicIndex,
  })
  collections.issues.createIndex((row) => row.updatedAt, {
    indexType: BTreeIndex,
  })
  collections.issues.createIndex((row) => row.estimate, {
    indexType: BTreeIndex,
  })
  collections.issues.createIndex((row) => row.metadata.customer.tier, {
    indexType: BTreeIndex,
  })

  return collections
}

function buildScenarioCollection(scenarioId, collections, scenarioVariant) {
  if (
    scenarioId === 'issue_window_500' ||
    scenarioId === 'issue_window_5000' ||
    scenarioId === 'issue_stream_all'
  ) {
    const limit =
      scenarioId === 'issue_window_500'
        ? 500
        : scenarioId === 'issue_window_5000'
          ? 5_000
          : null
    const alignedFilters = usesAlignedFilters(scenarioVariant)
    const disableBlockingOps = usesTraceCapabilityAlignment(scenarioVariant)
    return createLiveQueryCollection((q) =>
      {
        let query = q
        .from({ issue: collections.issues })
        .leftJoin({ project: collections.projects }, ({ issue, project }) =>
          eq(issue.projectId, project.id),
        )
        .leftJoin({ org: collections.organizations }, ({ project, org }) =>
          eq(project.organizationId, org.id),
        )
        .leftJoin({ team: collections.teams }, ({ project, team }) =>
          eq(project.teamId, team.id),
        )
        .leftJoin({ assignee: collections.users }, ({ issue, assignee }) =>
          eq(issue.assigneeId, assignee.id),
        )
        .leftJoin(
          { milestone: collections.currentMilestones },
          ({ issue, milestone }) => eq(issue.currentMilestoneId, milestone.id),
        )
        .leftJoin({ counter: collections.projectCounters }, ({ project, counter }) =>
          eq(project.id, counter.projectId),
        )
        .leftJoin({ snapshot: collections.projectSnapshots }, ({ project, snapshot }) =>
          eq(project.id, snapshot.projectId),
        )
        .where(({ issue, project, counter, snapshot }) =>
          alignedFilters
            ? and(
                or(eq(issue.status, 'open'), eq(issue.status, 'in_progress')),
                gte(issue.estimate, 3),
                or(
                  eq(issue.metadata.customer.tier, 'enterprise'),
                  eq(issue.metadata.customer.tier, 'mid_market'),
                ),
              )
            : and(
                or(eq(issue.status, 'open'), eq(issue.status, 'in_progress')),
                gte(issue.estimate, 3),
                or(
                  eq(issue.metadata.customer.tier, 'enterprise'),
                  eq(issue.metadata.customer.tier, 'mid_market'),
                ),
                gte(project.healthScore, 45),
                or(
                  eq(project.metadata.risk.bucket, 'high'),
                  eq(project.metadata.risk.bucket, 'critical'),
                ),
                gte(counter.openIssueCount, 5),
                gte(snapshot.velocity, 18),
              ),
        )
        .select(
          ({ issue, project, org, team, assignee, milestone, counter, snapshot }) => ({
            issueId: issue.id,
            projectId: project.id,
            issueTitle: issue.title,
            issueStatus: issue.status,
            issuePriority: issue.priority,
            issueSeverityRank: issue.metadata.severityRank,
            issueCustomerTier: issue.metadata.customer.tier,
            projectName: project.name,
            projectState: project.state,
            projectHealth: project.healthScore,
            projectRiskScore: project.metadata.risk.score,
            projectStrategic: project.metadata.flags.strategic,
            organizationName: org.name,
            teamName: team.name,
            assigneeName: assignee.name,
            milestoneName: milestone.name,
            openIssueCount: counter.openIssueCount,
            blockerCount: counter.blockerCount,
            velocity: snapshot.velocity,
            updatedAt: issue.updatedAt,
          }),
        )
        if (Number.isFinite(limit) && !disableBlockingOps) {
          query = query
            .orderBy(({ $selected }) => $selected.updatedAt, 'desc')
            .limit(limit)
        }
        return query
      },
    )
  }

  if (
    scenarioId === 'project_board_2000' ||
    scenarioId === 'project_board_stream_all'
  ) {
    const limit = scenarioId === 'project_board_2000' ? 2_000 : null
    const alignedFilters = usesAlignedFilters(scenarioVariant)
    const disableBlockingOps = usesTraceCapabilityAlignment(scenarioVariant)
    return createLiveQueryCollection((q) =>
      {
        let query = q
        .from({ project: collections.projects })
        .leftJoin({ org: collections.organizations }, ({ project, org }) =>
          eq(project.organizationId, org.id),
        )
        .leftJoin({ team: collections.teams }, ({ project, team }) =>
          eq(project.teamId, team.id),
        )
        .leftJoin({ lead: collections.users }, ({ project, lead }) =>
          eq(project.leadUserId, lead.id),
        )
        .leftJoin({ counter: collections.projectCounters }, ({ project, counter }) =>
          eq(project.id, counter.projectId),
        )
        .leftJoin({ snapshot: collections.projectSnapshots }, ({ project, snapshot }) =>
          eq(project.id, snapshot.projectId),
        )
        .leftJoin(
          { milestone: collections.currentMilestones },
          ({ project, milestone }) => eq(project.id, milestone.projectId),
        )
        .where(({ project, counter, snapshot }) =>
          alignedFilters
            ? and(
                or(eq(project.state, 'active'), eq(project.state, 'at_risk')),
                gte(project.healthScore, 45),
                or(
                  eq(project.metadata.risk.bucket, 'high'),
                  eq(project.metadata.risk.bucket, 'critical'),
                ),
              )
            : and(
                or(eq(project.state, 'active'), eq(project.state, 'at_risk')),
                gte(project.healthScore, 45),
                or(
                  eq(project.metadata.risk.bucket, 'high'),
                  eq(project.metadata.risk.bucket, 'critical'),
                ),
                gte(counter.openIssueCount, 4),
                gte(snapshot.velocity, 20),
              ),
        )
        .select(({ project, org, team, lead, counter, snapshot, milestone }) => ({
          projectId: project.id,
          projectName: project.name,
          projectState: project.state,
          projectHealth: project.healthScore,
          projectRiskScore: project.metadata.risk.score,
          projectStrategic: project.metadata.flags.strategic,
          region: org.region,
          organizationName: org.name,
          teamName: team.name,
          leadName: lead.name,
          leadRole: lead.role,
          milestoneName: milestone.name,
          milestoneStatus: milestone.status,
          openIssueCount: counter.openIssueCount,
          blockerCount: counter.blockerCount,
          staleIssueCount: counter.staleIssueCount,
            velocity: snapshot.velocity,
            blockedRatio: snapshot.blockedRatio,
            updatedAt: project.updatedAt,
          }))
        if (Number.isFinite(limit) && !disableBlockingOps) {
          query = query
            .orderBy(({ $selected }) => $selected.projectHealth, 'desc')
            .limit(limit)
        }
        return query
      },
    )
  }

  throw new Error(`Unknown scenario: ${scenarioId}`)
}

async function initRuntime(message = {}) {
  if (runtime.initialized) {
    return {
      type: 'ready',
      scenarioVariant: runtime.scenarioVariant,
      dataset: summarizeDataset(runtime.server.tables),
    }
  }

  const startedAt = performance.now()
  runtime.scenarioVariant = normalizeScenarioVariant(message.scenarioVariant)
  runtime.server = buildServerDataset(DATASET_CONFIG)
  runtime.api = createApi(runtime.server)
  runtime.queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  })
  runtime.collections = createBaseCollections(runtime.queryClient, runtime.api)
  await Promise.all(
    Object.values(runtime.collections).map((collection) => collection.preload()),
  )
  runtime.initialized = true
  return {
    type: 'ready',
    initMs: performance.now() - startedAt,
    scenarioVariant: runtime.scenarioVariant,
    dataset: summarizeDataset(runtime.server.tables),
  }
}

function summarizeDataset(tables) {
  return Object.fromEntries(
    Object.entries(tables).map(([tableName, rows]) => [tableName, rows.length]),
  )
}

function ensureInitialized() {
  if (!runtime.initialized || !runtime.collections || !runtime.server) {
    throw new Error('Worker runtime is not initialized.')
  }
}

function unsubscribeActive() {
  if (!runtime.active) return
  runtime.active.subscription?.unsubscribe?.()
  runtime.active = null
}

function post(message) {
  parentPort.postMessage(message)
}

function createSnapshotMessage(active, phase, changes, includeRows = true) {
  const rows = includeRows ? active.collection.toArray : undefined
  return {
    type: 'snapshot',
    scenarioId: active.scenarioId,
    phase,
    workerLatencyMs: performance.now() - active.pending.startedAt,
    rowCount: active.collection.size,
    changeCount: changes.length,
    rows,
  }
}

function subscribeScenario(message) {
  ensureInitialized()
  unsubscribeActive()

  const active = {
    scenarioId: message.scenarioId,
    includeRows: message.includeRows !== false,
    collection: null,
    lastProjectIds: [],
    pending: {
      phase: 'initial',
      startedAt: performance.now(),
    },
    subscription: null,
  }

  active.collection = buildScenarioCollection(
    message.scenarioId,
    runtime.collections,
    runtime.scenarioVariant,
  )

  active.subscription = active.collection.subscribeChanges(
    (changes) => {
      const pending = active.pending
      const nextProjectIds = extractProjectIds(
        active.collection.toArray,
        MAX_TRACKED_PROJECT_IDS,
      )
      if (nextProjectIds.length > 0) {
        active.lastProjectIds = nextProjectIds
      }
      if (!pending) return
      post(createSnapshotMessage(active, pending.phase, changes, active.includeRows))
      active.pending = null
    },
    {
      includeInitialState: true,
    },
  )

  runtime.active = active
}

function collectActiveProjectIds(maxCount) {
  if (!runtime.active) {
    throw new Error('No active live query subscription.')
  }

  const currentIds = extractProjectIds(runtime.active.collection.toArray, maxCount)
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

  runtime.collections.projects.utils.writeBatch(() => {
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
      runtime.collections.projects.utils.writeUpdate({
        id: next.id,
        state: next.state,
        healthScore: next.healthScore,
        updatedAt: next.updatedAt,
      })
    }
  })
}

async function runApiRefresh(message) {
  ensureInitialized()
  if (!runtime.active) {
    throw new Error('Subscribe before running API refresh.')
  }

  const projectIds = collectActiveProjectIds(message.patchCount)
  runtime.active.pending = {
    phase: 'api',
    startedAt: performance.now(),
  }

  for (const projectId of projectIds) {
    applyProjectPatch(projectId, (current) => {
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
  }

  await runtime.collections.projects.utils.refetch({ throwOnError: true })
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
        await runApiRefresh(message)
        return
      case 'unsubscribe':
        unsubscribeActive()
        post({ type: 'unsubscribed', scenarioId: message.scenarioId })
        return
      case 'shutdown':
        unsubscribeActive()
        post({ type: 'shutdown-complete' })
        return
      default:
        throw new Error(`Unknown worker message type: ${message.type}`)
    }
  } catch (error) {
    handleError(error, message.type)
  }
})
