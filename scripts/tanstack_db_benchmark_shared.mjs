import path from 'node:path'
import { fileURLToPath } from 'node:url'

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url))
const ROOT_DIR = path.resolve(SCRIPT_DIR, '..')
const TMP_DIR = path.join(ROOT_DIR, 'tmp')

function readIntEnv(name, fallback) {
  const value = Number.parseInt(process.env[name] ?? '', 10)
  return Number.isFinite(value) && value > 0 ? value : fallback
}

function readNonNegativeIntEnv(name, fallback) {
  const value = Number.parseInt(process.env[name] ?? '', 10)
  return Number.isFinite(value) && value >= 0 ? value : fallback
}

export const DATASET_CONFIG = {
  organizationCount: readIntEnv('TANSTACK_BENCH_ORGS', 200),
  teamCount: readIntEnv('TANSTACK_BENCH_TEAMS', 1_000),
  userCount: readIntEnv('TANSTACK_BENCH_USERS', 12_000),
  projectCount: readIntEnv('TANSTACK_BENCH_PROJECTS', 3_000),
  milestoneCount: readIntEnv('TANSTACK_BENCH_MILESTONES', 9_000),
  issueCount: readIntEnv('TANSTACK_BENCH_ISSUES', 50_000),
  seed: readIntEnv('TANSTACK_BENCH_SEED', 20260327),
}

export const WARMUP_ROUNDS = readNonNegativeIntEnv('TANSTACK_BENCH_WARMUP', 1)
export const MEASURED_ROUNDS = readIntEnv('TANSTACK_BENCH_ROUNDS', 3)
export const SOCKET_PATCH_COUNT = readIntEnv('TANSTACK_BENCH_SOCKET_PATCHES', 24)
export const API_REFRESH_COUNT = readIntEnv('TANSTACK_BENCH_API_REFRESHES', 24)
export const MESSAGE_TIMEOUT_MS = readIntEnv('TANSTACK_BENCH_TIMEOUT_MS', 20_000)

export const SCENARIOS = {
  issue_window_500: {
    id: 'issue_window_500',
    label: 'Issue Feed · 7-way left join · limit 500',
    limit: 500,
    root: 'issues',
    updateSource: 'projects',
    description:
      '50K issues on top of project-management entities. Filters mix scalar and nested metadata predicates; host receives full snapshots from a worker subscription.',
  },
  issue_window_5000: {
    id: 'issue_window_5000',
    label: 'Issue Feed · 7-way left join · limit 5000',
    limit: 5_000,
    root: 'issues',
    updateSource: 'projects',
    description:
      'Same query shape as the interactive feed, but with a much larger snapshot to surface worker→host structured-clone costs.',
  },
  issue_stream_all: {
    id: 'issue_stream_all',
    label: 'Issue Feed · 7-way left join · no order/limit',
    limit: null,
    root: 'issues',
    updateSource: 'projects',
    description:
      'Control scenario for pure incremental join/filter maintenance: same issue-centric multi-join shape, but with ORDER BY / LIMIT removed so both engines can stay on a non-blocking live path.',
  },
  project_board_2000: {
    id: 'project_board_2000',
    label: 'Project Board · 6-way left join · limit 2000',
    limit: 2_000,
    root: 'projects',
    updateSource: 'projects',
    description:
      'Project-centric board shaped around project patches, nested metadata, and one-to-one rollup tables to emulate Linear-like project views.',
  },
  project_board_stream_all: {
    id: 'project_board_stream_all',
    label: 'Project Board · 6-way left join · no order/limit',
    limit: null,
    root: 'projects',
    updateSource: 'projects',
    description:
      'Control scenario for pure incremental maintenance on the project board join graph, with the same left joins and predicates but no blocking operators.',
  },
}

export const SCENARIO_ORDER = Object.keys(SCENARIOS)

export const REPORT_PATH = path.join(TMP_DIR, 'tanstack_db_worker_benchmark.md')
export const JSON_REPORT_PATH = path.join(
  TMP_DIR,
  'tanstack_db_worker_benchmark.json',
)

export function median(values) {
  if (values.length === 0) return NaN
  const sorted = [...values].sort((a, b) => a - b)
  const middle = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 0
    ? (sorted[middle - 1] + sorted[middle]) / 2
    : sorted[middle]
}

export function mean(values) {
  if (values.length === 0) return NaN
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

export function formatMs(value) {
  if (!Number.isFinite(value)) return 'N/A'
  if (value < 1) return `${value.toFixed(3)} ms`
  return `${value.toFixed(2)} ms`
}

export function formatCount(value) {
  if (!Number.isFinite(value)) return 'N/A'
  return Number(value).toLocaleString()
}

export function formatBytes(value) {
  if (!Number.isFinite(value)) return 'N/A'
  if (value < 1024) return `${value} B`
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`
  return `${(value / (1024 * 1024)).toFixed(2)} MiB`
}

export function nowIso() {
  return new Date().toISOString()
}
