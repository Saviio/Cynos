export function rowValue(row, ...keys) {
  for (const key of keys) {
    if (key in row) return row[key]
  }
  return undefined
}

function rawProjectIdColumn(scenarioId) {
  if (
    scenarioId === 'issue_window_500' ||
    scenarioId === 'issue_window_5000' ||
    scenarioId === 'issue_stream_all'
  ) {
    return 1
  }

  if (
    scenarioId === 'project_board_2000' ||
    scenarioId === 'project_board_stream_all'
  ) {
    return 0
  }

  throw new Error(`Unknown scenario: ${scenarioId}`)
}

export function mapIssueRow(row) {
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

export function mapProjectBoardRow(row) {
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

export function mapRowsForScenario(scenarioId, rows) {
  if (
    scenarioId === 'issue_window_500' ||
    scenarioId === 'issue_window_5000' ||
    scenarioId === 'issue_stream_all'
  ) {
    return rows.map(mapIssueRow)
  }

  if (
    scenarioId === 'project_board_2000' ||
    scenarioId === 'project_board_stream_all'
  ) {
    return rows.map(mapProjectBoardRow)
  }

  throw new Error(`Unknown scenario: ${scenarioId}`)
}

export function scenarioRowKey(scenarioId, row) {
  if (
    scenarioId === 'issue_window_500' ||
    scenarioId === 'issue_window_5000' ||
    scenarioId === 'issue_stream_all'
  ) {
    return rowValue(row, 'issues.id', 'issueId', 'id')
  }

  if (
    scenarioId === 'project_board_2000' ||
    scenarioId === 'project_board_stream_all'
  ) {
    return rowValue(row, 'projects.id', 'projectId', 'id')
  }

  throw new Error(`Unknown scenario: ${scenarioId}`)
}

export function snapshotRowsForScenario(scenarioId, rawRows) {
  return mapRowsForScenario(scenarioId, rawRows)
}

export function extractProjectIds(rows, maxCount) {
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

export function extractProjectIdsFromResultSet(scenarioId, resultSet, maxCount) {
  const columnIndex = rawProjectIdColumn(scenarioId)

  const result = []
  const seen = new Set()

  for (let rowIndex = 0; rowIndex < resultSet.length; rowIndex += 1) {
    const projectId = resultSet.getNumber(rowIndex, columnIndex)
    if (projectId == null || seen.has(projectId)) continue
    seen.add(projectId)
    result.push(projectId)
    if (result.length >= maxCount) break
  }

  return result
}

function materializeIssueRowsFromResultSet(resultSet) {
  const rows = new Array(resultSet.length)

  for (let rowIndex = 0; rowIndex < resultSet.length; rowIndex += 1) {
    const issueMetadata = resultSet.getJsonb(rowIndex, 9) ?? {}
    const projectMetadata = resultSet.getJsonb(rowIndex, 19) ?? {}

    rows[rowIndex] = {
      issueId: resultSet.getNumber(rowIndex, 0),
      projectId: resultSet.getNumber(rowIndex, 10),
      issueTitle: resultSet.getString(rowIndex, 4),
      issueStatus: resultSet.getString(rowIndex, 5),
      issuePriority: resultSet.getString(rowIndex, 6),
      issueSeverityRank: issueMetadata.severityRank ?? null,
      issueCustomerTier: issueMetadata.customer?.tier ?? null,
      projectName: resultSet.getString(rowIndex, 14),
      projectState: resultSet.getString(rowIndex, 15),
      projectHealth: resultSet.getNumber(rowIndex, 16),
      projectRiskScore: projectMetadata.risk?.score ?? null,
      projectStrategic: projectMetadata.flags?.strategic ?? null,
      organizationName: resultSet.getString(rowIndex, 21),
      teamName: resultSet.getString(rowIndex, 27),
      assigneeName: resultSet.getString(rowIndex, 32),
      milestoneName: resultSet.getString(rowIndex, 37),
      openIssueCount: resultSet.getNumber(rowIndex, 42) ?? 0,
      blockerCount: resultSet.getNumber(rowIndex, 43) ?? 0,
      velocity: resultSet.getNumber(rowIndex, 47) ?? 0,
      updatedAt: resultSet.getNumber(rowIndex, 8),
    }
  }

  return rows
}

function materializeProjectBoardRowsFromResultSet(resultSet) {
  const rows = new Array(resultSet.length)

  for (let rowIndex = 0; rowIndex < resultSet.length; rowIndex += 1) {
    const projectMetadata = resultSet.getJsonb(rowIndex, 9) ?? {}

    rows[rowIndex] = {
      projectId: resultSet.getNumber(rowIndex, 0),
      projectName: resultSet.getString(rowIndex, 4),
      projectState: resultSet.getString(rowIndex, 5),
      projectHealth: resultSet.getNumber(rowIndex, 6),
      projectRiskScore: projectMetadata.risk?.score ?? null,
      projectStrategic: projectMetadata.flags?.strategic ?? null,
      region: resultSet.getString(rowIndex, 13),
      organizationName: resultSet.getString(rowIndex, 11),
      teamName: resultSet.getString(rowIndex, 17),
      leadName: resultSet.getString(rowIndex, 22),
      leadRole: resultSet.getString(rowIndex, 23),
      milestoneName: resultSet.getString(rowIndex, 37),
      milestoneStatus: resultSet.getString(rowIndex, 39),
      openIssueCount: resultSet.getNumber(rowIndex, 26) ?? 0,
      blockerCount: resultSet.getNumber(rowIndex, 27) ?? 0,
      staleIssueCount: resultSet.getNumber(rowIndex, 28) ?? 0,
      velocity: resultSet.getNumber(rowIndex, 31) ?? 0,
      blockedRatio: resultSet.getNumber(rowIndex, 33) ?? 0,
      updatedAt: resultSet.getNumber(rowIndex, 7),
    }
  }

  return rows
}

export function materializeResultSetForScenario(scenarioId, resultSet) {
  let rows

  if (
    scenarioId === 'issue_window_500' ||
    scenarioId === 'issue_window_5000' ||
    scenarioId === 'issue_stream_all'
  ) {
    rows = materializeIssueRowsFromResultSet(resultSet)
  } else if (
    scenarioId === 'project_board_2000' ||
    scenarioId === 'project_board_stream_all'
  ) {
    rows = materializeProjectBoardRowsFromResultSet(resultSet)
  } else {
    throw new Error(`Unknown scenario: ${scenarioId}`)
  }

  return rows
}
