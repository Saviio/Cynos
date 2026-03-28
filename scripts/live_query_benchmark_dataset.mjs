const PROJECT_STATES = ['active', 'at_risk', 'planned', 'paused', 'archived']
const ORG_REGIONS = ['na', 'emea', 'apac', 'latam']
const TEAM_FUNCTIONS = ['product', 'design', 'engineering', 'ops', 'growth']
const CUSTOMER_TIERS = ['self_serve', 'mid_market', 'enterprise']
const ISSUE_LANES = ['backlog', 'triage', 'delivery', 'follow_up']
const PRIMARY_TAGS = ['ux', 'api', 'infra', 'growth', 'security', 'sales']
const SECONDARY_TAGS = ['mobile', 'web', 'sync', 'billing', 'search', 'ai']
const RISK_BUCKETS = ['low', 'medium', 'high', 'critical']

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

export function deepClone(value) {
  return structuredClone(value)
}

function stableModulo(id, modulo) {
  return ((id % modulo) + modulo) % modulo
}

export function summarizeDataset(tables) {
  return Object.fromEntries(
    Object.entries(tables).map(([tableName, rows]) => [tableName, rows.length]),
  )
}

export function buildServerDataset(config) {
  const random = createRandom(config.seed)
  const now = Date.UTC(2026, 2, 27, 8, 0, 0)

  const organizations = Array.from(
    { length: config.organizationCount },
    (_, idx) => {
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
    },
  )

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
      velocity: Math.max(
        8,
        80 - counters.blockerCount * 2 - counters.staleIssueCount,
      ),
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
