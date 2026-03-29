use cynos_core::schema::TableBuilder;
use cynos_core::{DataType, JsonbValue, Row, Value};
use cynos_database::query_engine::{
    build_execution_context_for_plan, compile_plan, explain_plan, TableCacheDataSource,
};
use cynos_query::ast::Expr;
use cynos_query::executor::PhysicalPlanRunner;
use cynos_query::planner::{LogicalPlan, PhysicalPlan, QueryPlanner};
use cynos_storage::TableCache;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const ORGANIZATION_COUNT: usize = 200;
const TEAM_COUNT: usize = 1_000;
const USER_COUNT: usize = 12_000;
const PROJECT_COUNT: usize = 3_000;
const MILESTONE_COUNT: usize = 9_000;
const ISSUE_COUNT: usize = 50_000;
const SEED: u32 = 20260327;
const PROFILE_ROUNDS: usize = 5;

const PROJECT_STATES: &[&str] = &["active", "at_risk", "planned", "paused", "archived"];
const ORG_REGIONS: &[&str] = &["na", "emea", "apac", "latam"];
const TEAM_FUNCTIONS: &[&str] = &["product", "design", "engineering", "ops", "growth"];
const CUSTOMER_TIERS: &[&str] = &["self_serve", "mid_market", "enterprise"];
const ISSUE_LANES: &[&str] = &["backlog", "triage", "delivery", "follow_up"];
const PRIMARY_TAGS: &[&str] = &["ux", "api", "infra", "growth", "security", "sales"];
const SECONDARY_TAGS: &[&str] = &["mobile", "web", "sync", "billing", "search", "ai"];
const RISK_BUCKETS: &[&str] = &["low", "medium", "high", "critical"];

#[derive(Clone)]
struct ProjectInfo {
    id: i64,
    organization_id: i64,
    team_id: i64,
    lead_user_id: i64,
    state: &'static str,
    health_score: i32,
    updated_at: i64,
    priority_band: &'static str,
    metadata_json: String,
}

#[derive(Clone)]
struct MilestoneInfo {
    id: i64,
    project_id: i64,
    name: String,
    due_at: i64,
    status: &'static str,
    metadata_json: String,
}

#[derive(Clone)]
struct IssueInfo {
    id: i64,
    project_id: i64,
    assignee_id: i64,
    current_milestone_id: Option<i64>,
    title: String,
    status: &'static str,
    priority: &'static str,
    estimate: i32,
    updated_at: i64,
    severity_rank: i32,
    metadata_json: String,
}

#[derive(Clone, Copy, Default)]
struct CounterState {
    open_issue_count: i32,
    blocker_count: i32,
    stale_issue_count: i32,
    last_updated_at: i64,
}

#[derive(Clone, Copy)]
struct Mulberry32 {
    value: u32,
}

impl Mulberry32 {
    fn new(seed: u32) -> Self {
        Self { value: seed }
    }

    fn next_f64(&mut self) -> f64 {
        self.value = self.value.wrapping_add(0x6d2b79f5);
        let mut result = self.value;
        result = ((result ^ (result >> 15)).wrapping_mul(1 | result)) as u32;
        result ^= result.wrapping_add((result ^ (result >> 7)).wrapping_mul(61 | result));
        ((result ^ (result >> 14)) as f64) / 4_294_967_296.0
    }

    fn maybe(&mut self, threshold: f64) -> bool {
        self.next_f64() < threshold
    }

    fn int_between_i32(&mut self, min: i32, max: i32) -> i32 {
        let span = (max - min + 1) as f64;
        min + (self.next_f64() * span).floor() as i32
    }

    fn int_between_i64(&mut self, min: i64, max: i64) -> i64 {
        let span = (max - min + 1) as f64;
        min + (self.next_f64() * span).floor() as i64
    }

    fn pick<'a>(&mut self, values: &'a [&'a str]) -> &'a str {
        let idx = (self.next_f64() * values.len() as f64).floor() as usize;
        values.get(idx).copied().unwrap_or(values[0])
    }
}

#[derive(Clone)]
struct Scenario {
    id: &'static str,
    label: &'static str,
    root_table: &'static str,
    logical: LogicalPlan,
}

#[derive(Clone)]
struct Measure {
    median_ms: f64,
    mean_ms: f64,
    row_count: usize,
}

#[derive(Clone)]
struct PlanProfile {
    label: String,
    measure: Measure,
    children: Vec<PlanProfile>,
}

fn stable_modulo(value: i64, modulo: usize) -> usize {
    (((value % modulo as i64) + modulo as i64) % modulo as i64) as usize
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn format_ms(value: f64) -> String {
    if value < 1.0 {
        format!("{value:.3} ms")
    } else {
        format!("{value:.2} ms")
    }
}

fn create_cache() -> TableCache {
    let mut cache = TableCache::new();
    create_tables(&mut cache);
    populate_tables(&mut cache);
    cache
}

fn create_tables(cache: &mut TableCache) {
    let organizations = TableBuilder::new("organizations")
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("name", DataType::String)
        .unwrap()
        .add_column("tier", DataType::String)
        .unwrap()
        .add_column("region", DataType::String)
        .unwrap()
        .add_column("metadata", DataType::Jsonb)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .add_index("idx_organizations_region", &["region"], false)
        .unwrap()
        .build()
        .unwrap();

    let teams = TableBuilder::new("teams")
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("organizationId", DataType::Int64)
        .unwrap()
        .add_column("name", DataType::String)
        .unwrap()
        .add_column("function", DataType::String)
        .unwrap()
        .add_column("metadata", DataType::Jsonb)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .add_index("idx_teams_organizationId", &["organizationId"], false)
        .unwrap()
        .build()
        .unwrap();

    let users = TableBuilder::new("users")
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("teamId", DataType::Int64)
        .unwrap()
        .add_column("name", DataType::String)
        .unwrap()
        .add_column("role", DataType::String)
        .unwrap()
        .add_column("metadata", DataType::Jsonb)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .add_index("idx_users_teamId", &["teamId"], false)
        .unwrap()
        .build()
        .unwrap();

    let projects = TableBuilder::new("projects")
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("organizationId", DataType::Int64)
        .unwrap()
        .add_column("teamId", DataType::Int64)
        .unwrap()
        .add_column("leadUserId", DataType::Int64)
        .unwrap()
        .add_column("name", DataType::String)
        .unwrap()
        .add_column("state", DataType::String)
        .unwrap()
        .add_column("healthScore", DataType::Int32)
        .unwrap()
        .add_column("updatedAt", DataType::Int64)
        .unwrap()
        .add_column("priorityBand", DataType::String)
        .unwrap()
        .add_column("metadata", DataType::Jsonb)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .add_index("idx_projects_organizationId", &["organizationId"], false)
        .unwrap()
        .add_index("idx_projects_teamId", &["teamId"], false)
        .unwrap()
        .add_index("idx_projects_leadUserId", &["leadUserId"], false)
        .unwrap()
        .add_index("idx_projects_state", &["state"], false)
        .unwrap()
        .add_index("idx_projects_healthScore", &["healthScore"], false)
        .unwrap()
        .add_index("idx_projects_updatedAt", &["updatedAt"], false)
        .unwrap()
        .add_jsonb_index(
            "idx_projects_metadata_gin",
            "metadata",
            &["risk.bucket", "risk.score", "flags.strategic"],
        )
        .unwrap()
        .build()
        .unwrap();

    let project_snapshots = TableBuilder::new("projectSnapshots")
        .unwrap()
        .add_column("projectId", DataType::Int64)
        .unwrap()
        .add_column("velocity", DataType::Int32)
        .unwrap()
        .add_column("completionRate", DataType::Float64)
        .unwrap()
        .add_column("blockedRatio", DataType::Float64)
        .unwrap()
        .add_column("updatedAt", DataType::Int64)
        .unwrap()
        .add_primary_key(&["projectId"], false)
        .unwrap()
        .add_index("idx_projectSnapshots_velocity", &["velocity"], false)
        .unwrap()
        .build()
        .unwrap();

    let project_counters = TableBuilder::new("projectCounters")
        .unwrap()
        .add_column("projectId", DataType::Int64)
        .unwrap()
        .add_column("openIssueCount", DataType::Int32)
        .unwrap()
        .add_column("blockerCount", DataType::Int32)
        .unwrap()
        .add_column("staleIssueCount", DataType::Int32)
        .unwrap()
        .add_column("updatedAt", DataType::Int64)
        .unwrap()
        .add_primary_key(&["projectId"], false)
        .unwrap()
        .add_index(
            "idx_projectCounters_openIssueCount",
            &["openIssueCount"],
            false,
        )
        .unwrap()
        .build()
        .unwrap();

    let current_milestones = TableBuilder::new("currentMilestones")
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("projectId", DataType::Int64)
        .unwrap()
        .add_column("name", DataType::String)
        .unwrap()
        .add_column("dueAt", DataType::Int64)
        .unwrap()
        .add_column("status", DataType::String)
        .unwrap()
        .add_column("metadata", DataType::Jsonb)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .add_index("idx_currentMilestones_projectId", &["projectId"], false)
        .unwrap()
        .add_index("idx_currentMilestones_dueAt", &["dueAt"], false)
        .unwrap()
        .build()
        .unwrap();

    let issues = TableBuilder::new("issues")
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("projectId", DataType::Int64)
        .unwrap()
        .add_column("assigneeId", DataType::Int64)
        .unwrap()
        .add_column("currentMilestoneId", DataType::Int64)
        .unwrap()
        .add_column("title", DataType::String)
        .unwrap()
        .add_column("status", DataType::String)
        .unwrap()
        .add_column("priority", DataType::String)
        .unwrap()
        .add_column("estimate", DataType::Int32)
        .unwrap()
        .add_column("updatedAt", DataType::Int64)
        .unwrap()
        .add_column("metadata", DataType::Jsonb)
        .unwrap()
        .add_primary_key(&["id"], false)
        .unwrap()
        .add_index("idx_issues_projectId", &["projectId"], false)
        .unwrap()
        .add_index("idx_issues_assigneeId", &["assigneeId"], false)
        .unwrap()
        .add_index(
            "idx_issues_currentMilestoneId",
            &["currentMilestoneId"],
            false,
        )
        .unwrap()
        .add_index("idx_issues_status", &["status"], false)
        .unwrap()
        .add_index("idx_issues_estimate", &["estimate"], false)
        .unwrap()
        .add_index("idx_issues_updatedAt", &["updatedAt"], false)
        .unwrap()
        .add_jsonb_index(
            "idx_issues_metadata_gin",
            "metadata",
            &["severityRank", "customer.tier", "workflow.lane"],
        )
        .unwrap()
        .build()
        .unwrap();

    for table in [
        organizations,
        teams,
        users,
        projects,
        project_snapshots,
        project_counters,
        current_milestones,
        issues,
    ] {
        cache.create_table(table).unwrap();
    }
}

fn populate_tables(cache: &mut TableCache) {
    let mut random = Mulberry32::new(SEED);
    let now = 1_774_944_000_000i64;

    {
        let store = cache.get_table_mut("organizations").unwrap();
        for idx in 0..ORGANIZATION_COUNT {
            let id = (idx + 1) as i64;
            let tier = CUSTOMER_TIERS[stable_modulo(id, CUSTOMER_TIERS.len())];
            let region = ORG_REGIONS[stable_modulo(id, ORG_REGIONS.len())];
            let metadata = format!(
                "{{\"spendBand\":{},\"contract\":{{\"renewed\":{},\"seats\":{}}}}}",
                random.int_between_i32(1, 5),
                random.maybe(0.72),
                random.int_between_i32(50, 5_000),
            );
            store
                .insert(Row::new(
                    id as u64,
                    vec![
                        Value::Int64(id),
                        Value::String(format!("Organization {id}").into()),
                        Value::String(tier.into()),
                        Value::String(region.into()),
                        Value::Jsonb(JsonbValue(metadata.into_bytes())),
                    ],
                ))
                .unwrap();
        }
    }

    {
        let store = cache.get_table_mut("teams").unwrap();
        for idx in 0..TEAM_COUNT {
            let id = (idx + 1) as i64;
            let organization_id = stable_modulo(id - 1, ORGANIZATION_COUNT) as i64 + 1;
            let function = TEAM_FUNCTIONS[stable_modulo(id, TEAM_FUNCTIONS.len())];
            let metadata = format!(
                "{{\"timezoneOffset\":{},\"budgetCode\":\"BGT-{}-{}\"}}",
                stable_modulo(id, 12) as i64 - 6,
                organization_id,
                id,
            );
            store
                .insert(Row::new(
                    id as u64,
                    vec![
                        Value::Int64(id),
                        Value::Int64(organization_id),
                        Value::String(format!("Team {id}").into()),
                        Value::String(function.into()),
                        Value::Jsonb(JsonbValue(metadata.into_bytes())),
                    ],
                ))
                .unwrap();
        }
    }

    let mut team_user_ids: HashMap<i64, Vec<i64>> = HashMap::new();
    {
        let store = cache.get_table_mut("users").unwrap();
        for idx in 0..USER_COUNT {
            let id = (idx + 1) as i64;
            let team_id = stable_modulo(id - 1, TEAM_COUNT) as i64 + 1;
            let role = if random.maybe(0.08) {
                "staff"
            } else if random.maybe(0.2) {
                "lead"
            } else {
                "member"
            };
            let focus = random.pick(&["product", "platform", "growth", "design"]);
            let metadata = format!(
                "{{\"locale\":\"{}\",\"focus\":\"{}\",\"seniority\":{}}}",
                if stable_modulo(id, 2) == 0 {
                    "en-US"
                } else {
                    "en-GB"
                },
                focus,
                random.int_between_i32(1, 6),
            );
            team_user_ids.entry(team_id).or_default().push(id);
            store
                .insert(Row::new(
                    id as u64,
                    vec![
                        Value::Int64(id),
                        Value::Int64(team_id),
                        Value::String(format!("User {id}").into()),
                        Value::String(role.into()),
                        Value::Jsonb(JsonbValue(metadata.into_bytes())),
                    ],
                ))
                .unwrap();
        }
    }

    let mut projects = Vec::with_capacity(PROJECT_COUNT);
    {
        let store = cache.get_table_mut("projects").unwrap();
        for idx in 0..PROJECT_COUNT {
            let id = (idx + 1) as i64;
            let team_id = stable_modulo(id - 1, TEAM_COUNT) as i64 + 1;
            let organization_id = stable_modulo(team_id - 1, ORGANIZATION_COUNT) as i64 + 1;
            let candidate_users = team_user_ids
                .get(&team_id)
                .cloned()
                .unwrap_or_else(|| vec![1]);
            let lead_user_id = candidate_users[stable_modulo(id, candidate_users.len())];
            let health_score = random.int_between_i32(25, 95);
            let risk_score = random.int_between_i32(10, 95);
            let risk_bucket = random.pick(RISK_BUCKETS);
            let updated_at = now - id * 17_000;
            let metadata_json = format!(
                "{{\"risk\":{{\"score\":{},\"bucket\":\"{}\"}},\"flags\":{{\"strategic\":{},\"regulated\":{}}},\"topology\":{{\"shard\":{},\"market\":\"{}\"}}}}",
                risk_score,
                risk_bucket,
                random.maybe(0.28),
                random.maybe(0.14),
                stable_modulo(id, 32),
                random.pick(ORG_REGIONS),
            );
            let state = random.pick(PROJECT_STATES);
            let priority_band = if health_score > 75 {
                "p0"
            } else if health_score > 55 {
                "p1"
            } else {
                "p2"
            };
            projects.push(ProjectInfo {
                id,
                organization_id,
                team_id,
                lead_user_id,
                state,
                health_score,
                updated_at,
                priority_band,
                metadata_json: metadata_json.clone(),
            });
            store
                .insert(Row::new(
                    id as u64,
                    vec![
                        Value::Int64(id),
                        Value::Int64(organization_id),
                        Value::Int64(team_id),
                        Value::Int64(lead_user_id),
                        Value::String(format!("Project {id}").into()),
                        Value::String(state.into()),
                        Value::Int32(health_score),
                        Value::Int64(updated_at),
                        Value::String(priority_band.into()),
                        Value::Jsonb(JsonbValue(metadata_json.into_bytes())),
                    ],
                ))
                .unwrap();
        }
    }

    let mut milestones = Vec::with_capacity(MILESTONE_COUNT);
    let mut milestones_by_project: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut milestone_id = 1i64;
    while milestones.len() < MILESTONE_COUNT {
        let project = &projects[stable_modulo(milestones.len() as i64, projects.len())];
        let row = MilestoneInfo {
            id: milestone_id,
            project_id: project.id,
            name: format!("Milestone {milestone_id}"),
            due_at: now + random.int_between_i64(1, 180) * 86_400_000,
            status: if random.maybe(0.7) {
                "active"
            } else {
                "planned"
            },
            metadata_json: format!(
                "{{\"quarter\":\"2026-Q{}\",\"slipDays\":{}}}",
                stable_modulo(milestone_id, 4) + 1,
                random.int_between_i32(0, 18),
            ),
        };
        milestones_by_project
            .entry(project.id)
            .or_default()
            .push(row.id);
        milestones.push(row);
        milestone_id += 1;
    }

    {
        let store = cache.get_table_mut("currentMilestones").unwrap();
        for milestone in milestones_by_project
            .values()
            .filter_map(|ids| ids.first())
            .map(|id| milestones[(id - 1) as usize].clone())
        {
            store
                .insert(Row::new(
                    milestone.id as u64,
                    vec![
                        Value::Int64(milestone.id),
                        Value::Int64(milestone.project_id),
                        Value::String(milestone.name.into()),
                        Value::Int64(milestone.due_at),
                        Value::String(milestone.status.into()),
                        Value::Jsonb(JsonbValue(milestone.metadata_json.into_bytes())),
                    ],
                ))
                .unwrap();
        }
    }

    let mut issues = Vec::with_capacity(ISSUE_COUNT);
    let mut issue_counters: HashMap<i64, CounterState> = HashMap::new();
    {
        let store = cache.get_table_mut("issues").unwrap();
        for idx in 0..ISSUE_COUNT {
            let id = (idx + 1) as i64;
            let project = &projects[(random.next_f64() * projects.len() as f64).floor() as usize];
            let assignee_pool = team_user_ids
                .get(&project.team_id)
                .cloned()
                .unwrap_or_else(|| vec![project.lead_user_id]);
            let current_milestone_ids = milestones_by_project
                .get(&project.id)
                .cloned()
                .unwrap_or_default();
            let current_milestone_id = if !current_milestone_ids.is_empty() && random.maybe(0.78) {
                Some(
                    current_milestone_ids
                        [(random.next_f64() * current_milestone_ids.len() as f64).floor() as usize],
                )
            } else {
                None
            };
            let status = {
                let roll = random.next_f64();
                if roll < 0.52 {
                    "open"
                } else if roll < 0.72 {
                    "in_progress"
                } else if roll < 0.88 {
                    "blocked"
                } else {
                    "closed"
                }
            };
            let severity_rank = random.int_between_i32(1, 5);
            let updated_at = now - random.int_between_i64(0, 14 * 24 * 60) * 60_000;
            let tier = random.pick(CUSTOMER_TIERS);
            let metadata_json = format!(
                "{{\"severityRank\":{},\"tags\":{{\"primary\":\"{}\",\"secondary\":\"{}\"}},\"customer\":{{\"tier\":\"{}\"}},\"workflow\":{{\"lane\":\"{}\",\"slaHours\":{}}}}}",
                severity_rank,
                random.pick(PRIMARY_TAGS),
                random.pick(SECONDARY_TAGS),
                tier,
                random.pick(ISSUE_LANES),
                random.int_between_i32(4, 96),
            );
            let issue = IssueInfo {
                id,
                project_id: project.id,
                assignee_id: assignee_pool
                    [(random.next_f64() * assignee_pool.len() as f64).floor() as usize],
                current_milestone_id,
                title: format!("Issue {id}"),
                status,
                priority: random.pick(&["low", "medium", "high", "urgent"]),
                estimate: random.int_between_i32(1, 8),
                updated_at,
                severity_rank,
                metadata_json: metadata_json.clone(),
            };
            let counters = issue_counters.entry(issue.project_id).or_default();
            if issue.status != "closed" {
                counters.open_issue_count += 1;
            }
            if issue.status == "blocked" || issue.severity_rank >= 4 {
                counters.blocker_count += 1;
            }
            if now - issue.updated_at > 72 * 60 * 60 * 1000 {
                counters.stale_issue_count += 1;
            }
            if issue.updated_at > counters.last_updated_at {
                counters.last_updated_at = issue.updated_at;
            }
            store
                .insert(Row::new(
                    issue.id as u64,
                    vec![
                        Value::Int64(issue.id),
                        Value::Int64(issue.project_id),
                        Value::Int64(issue.assignee_id),
                        issue
                            .current_milestone_id
                            .map(Value::Int64)
                            .unwrap_or(Value::Null),
                        Value::String(issue.title.clone().into()),
                        Value::String(issue.status.into()),
                        Value::String(issue.priority.into()),
                        Value::Int32(issue.estimate),
                        Value::Int64(issue.updated_at),
                        Value::Jsonb(JsonbValue(metadata_json.into_bytes())),
                    ],
                ))
                .unwrap();
            issues.push(issue);
        }
    }

    {
        let store = cache.get_table_mut("projectCounters").unwrap();
        for project in &projects {
            let counters = issue_counters.get(&project.id).copied().unwrap_or_default();
            store
                .insert(Row::new(
                    project.id as u64,
                    vec![
                        Value::Int64(project.id),
                        Value::Int32(counters.open_issue_count),
                        Value::Int32(counters.blocker_count),
                        Value::Int32(counters.stale_issue_count),
                        Value::Int64(if counters.last_updated_at == 0 {
                            project.updated_at
                        } else {
                            counters.last_updated_at
                        }),
                    ],
                ))
                .unwrap();
        }
    }

    {
        let store = cache.get_table_mut("projectSnapshots").unwrap();
        for project in &projects {
            let counters = issue_counters.get(&project.id).copied().unwrap_or_default();
            let velocity = (80 - counters.blocker_count * 2 - counters.stale_issue_count).max(8);
            let completion_rate = (project.health_score as f64 / 100.0).clamp(0.1, 0.98);
            let blocked_ratio = if counters.open_issue_count == 0 {
                0.0
            } else {
                (counters.blocker_count as f64 / counters.open_issue_count as f64).min(1.0)
            };
            store
                .insert(Row::new(
                    project.id as u64,
                    vec![
                        Value::Int64(project.id),
                        Value::Int32(velocity),
                        Value::Float64(completion_rate),
                        Value::Float64(blocked_ratio),
                        Value::Int64(project.updated_at),
                    ],
                ))
                .unwrap();
        }
    }

    let _ = issues;
}

fn issue_status_predicate() -> Expr {
    Expr::or(
        Expr::eq(
            Expr::column("issues", "status", 5),
            Expr::literal(Value::String("open".into())),
        ),
        Expr::eq(
            Expr::column("issues", "status", 5),
            Expr::literal(Value::String("in_progress".into())),
        ),
    )
}

fn issue_estimate_predicate() -> Expr {
    Expr::ge(
        Expr::column("issues", "estimate", 7),
        Expr::literal(Value::Int32(3)),
    )
}

fn issue_customer_tier_predicate() -> Expr {
    Expr::or(
        Expr::jsonb_path_eq(
            Expr::column("issues", "metadata", 9),
            "$.customer.tier",
            Value::String("enterprise".into()),
        ),
        Expr::jsonb_path_eq(
            Expr::column("issues", "metadata", 9),
            "$.customer.tier",
            Value::String("mid_market".into()),
        ),
    )
}

fn issue_customer_tier_enterprise_predicate() -> Expr {
    Expr::jsonb_path_eq(
        Expr::column("issues", "metadata", 9),
        "$.customer.tier",
        Value::String("enterprise".into()),
    )
}

fn issue_root_predicate() -> Expr {
    Expr::and(
        Expr::and(issue_status_predicate(), issue_estimate_predicate()),
        issue_customer_tier_predicate(),
    )
}

fn issue_joined_predicate() -> Expr {
    Expr::and(
        Expr::and(
            issue_root_predicate(),
            Expr::ge(
                Expr::column("projects", "healthScore", 6),
                Expr::literal(Value::Int32(45)),
            ),
        ),
        Expr::and(
            Expr::or(
                Expr::jsonb_path_eq(
                    Expr::column("projects", "metadata", 9),
                    "$.risk.bucket",
                    Value::String("high".into()),
                ),
                Expr::jsonb_path_eq(
                    Expr::column("projects", "metadata", 9),
                    "$.risk.bucket",
                    Value::String("critical".into()),
                ),
            ),
            Expr::and(
                Expr::ge(
                    Expr::column("projectCounters", "openIssueCount", 1),
                    Expr::literal(Value::Int32(5)),
                ),
                Expr::ge(
                    Expr::column("projectSnapshots", "velocity", 1),
                    Expr::literal(Value::Int32(18)),
                ),
            ),
        ),
    )
}

fn issue_join_base() -> LogicalPlan {
    let issues = LogicalPlan::scan("issues");
    let projects = LogicalPlan::scan("projects");
    let organizations = LogicalPlan::scan("organizations");
    let teams = LogicalPlan::scan("teams");
    let users = LogicalPlan::scan("users");
    let milestones = LogicalPlan::scan("currentMilestones");
    let counters = LogicalPlan::scan("projectCounters");
    let snapshots = LogicalPlan::scan("projectSnapshots");

    let plan = LogicalPlan::left_join(
        issues,
        projects,
        Expr::eq(
            Expr::column("issues", "projectId", 1),
            Expr::column("projects", "id", 0),
        ),
    );
    let plan = LogicalPlan::left_join(
        plan,
        organizations,
        Expr::eq(
            Expr::column("projects", "organizationId", 1),
            Expr::column("organizations", "id", 0),
        ),
    );
    let plan = LogicalPlan::left_join(
        plan,
        teams,
        Expr::eq(
            Expr::column("projects", "teamId", 2),
            Expr::column("teams", "id", 0),
        ),
    );
    let plan = LogicalPlan::left_join(
        plan,
        users,
        Expr::eq(
            Expr::column("issues", "assigneeId", 2),
            Expr::column("users", "id", 0),
        ),
    );
    let plan = LogicalPlan::left_join(
        plan,
        milestones,
        Expr::eq(
            Expr::column("issues", "currentMilestoneId", 3),
            Expr::column("currentMilestones", "id", 0),
        ),
    );
    let plan = LogicalPlan::left_join(
        plan,
        counters,
        Expr::eq(
            Expr::column("projects", "id", 0),
            Expr::column("projectCounters", "projectId", 0),
        ),
    );
    LogicalPlan::left_join(
        plan,
        snapshots,
        Expr::eq(
            Expr::column("projects", "id", 0),
            Expr::column("projectSnapshots", "projectId", 0),
        ),
    )
}

fn issue_root_plan() -> LogicalPlan {
    LogicalPlan::project(
        LogicalPlan::filter(LogicalPlan::scan("issues"), issue_root_predicate()),
        vec![
            Expr::column("issues", "id", 0),
            Expr::column("issues", "updatedAt", 8),
        ],
    )
}

fn issue_status_only_plan() -> LogicalPlan {
    LogicalPlan::project(
        LogicalPlan::filter(LogicalPlan::scan("issues"), issue_status_predicate()),
        vec![
            Expr::column("issues", "id", 0),
            Expr::column("issues", "updatedAt", 8),
        ],
    )
}

fn issue_estimate_only_plan() -> LogicalPlan {
    LogicalPlan::project(
        LogicalPlan::filter(LogicalPlan::scan("issues"), issue_estimate_predicate()),
        vec![
            Expr::column("issues", "id", 0),
            Expr::column("issues", "updatedAt", 8),
        ],
    )
}

fn issue_customer_tier_only_plan() -> LogicalPlan {
    LogicalPlan::project(
        LogicalPlan::filter(LogicalPlan::scan("issues"), issue_customer_tier_predicate()),
        vec![
            Expr::column("issues", "id", 0),
            Expr::column("issues", "updatedAt", 8),
        ],
    )
}

fn issue_customer_tier_enterprise_only_plan() -> LogicalPlan {
    LogicalPlan::project(
        LogicalPlan::filter(
            LogicalPlan::scan("issues"),
            issue_customer_tier_enterprise_predicate(),
        ),
        vec![
            Expr::column("issues", "id", 0),
            Expr::column("issues", "updatedAt", 8),
        ],
    )
}

fn issue_status_estimate_plan() -> LogicalPlan {
    LogicalPlan::project(
        LogicalPlan::filter(
            LogicalPlan::scan("issues"),
            Expr::and(issue_status_predicate(), issue_estimate_predicate()),
        ),
        vec![
            Expr::column("issues", "id", 0),
            Expr::column("issues", "updatedAt", 8),
        ],
    )
}

fn issue_estimate_customer_tier_plan() -> LogicalPlan {
    LogicalPlan::project(
        LogicalPlan::filter(
            LogicalPlan::scan("issues"),
            Expr::and(issue_estimate_predicate(), issue_customer_tier_predicate()),
        ),
        vec![
            Expr::column("issues", "id", 0),
            Expr::column("issues", "updatedAt", 8),
        ],
    )
}

fn issue_join_root_plan() -> LogicalPlan {
    LogicalPlan::project(
        LogicalPlan::filter(issue_join_base(), issue_root_predicate()),
        vec![
            Expr::column("issues", "id", 0),
            Expr::column("issues", "updatedAt", 8),
        ],
    )
}

fn issue_full_plan() -> LogicalPlan {
    LogicalPlan::project(
        LogicalPlan::filter(issue_join_base(), issue_joined_predicate()),
        vec![
            Expr::column("issues", "id", 0),
            Expr::column("issues", "updatedAt", 8),
            Expr::column("issues", "status", 5),
            Expr::column("issues", "estimate", 7),
            Expr::column("projects", "id", 0),
            Expr::column("projects", "healthScore", 6),
            Expr::column("projects", "metadata", 9),
            Expr::column("projectCounters", "openIssueCount", 1),
            Expr::column("projectSnapshots", "velocity", 1),
        ],
    )
}

fn project_root_predicate() -> Expr {
    Expr::and(
        Expr::and(
            Expr::or(
                Expr::eq(
                    Expr::column("projects", "state", 5),
                    Expr::literal(Value::String("active".into())),
                ),
                Expr::eq(
                    Expr::column("projects", "state", 5),
                    Expr::literal(Value::String("at_risk".into())),
                ),
            ),
            Expr::ge(
                Expr::column("projects", "healthScore", 6),
                Expr::literal(Value::Int32(45)),
            ),
        ),
        Expr::or(
            Expr::jsonb_path_eq(
                Expr::column("projects", "metadata", 9),
                "$.risk.bucket",
                Value::String("high".into()),
            ),
            Expr::jsonb_path_eq(
                Expr::column("projects", "metadata", 9),
                "$.risk.bucket",
                Value::String("critical".into()),
            ),
        ),
    )
}

fn project_joined_predicate() -> Expr {
    Expr::and(
        project_root_predicate(),
        Expr::and(
            Expr::ge(
                Expr::column("projectCounters", "openIssueCount", 1),
                Expr::literal(Value::Int32(4)),
            ),
            Expr::ge(
                Expr::column("projectSnapshots", "velocity", 1),
                Expr::literal(Value::Int32(20)),
            ),
        ),
    )
}

fn project_join_base() -> LogicalPlan {
    let projects = LogicalPlan::scan("projects");
    let organizations = LogicalPlan::scan("organizations");
    let teams = LogicalPlan::scan("teams");
    let users = LogicalPlan::scan("users");
    let counters = LogicalPlan::scan("projectCounters");
    let snapshots = LogicalPlan::scan("projectSnapshots");
    let milestones = LogicalPlan::scan("currentMilestones");

    let plan = LogicalPlan::left_join(
        projects,
        organizations,
        Expr::eq(
            Expr::column("projects", "organizationId", 1),
            Expr::column("organizations", "id", 0),
        ),
    );
    let plan = LogicalPlan::left_join(
        plan,
        teams,
        Expr::eq(
            Expr::column("projects", "teamId", 2),
            Expr::column("teams", "id", 0),
        ),
    );
    let plan = LogicalPlan::left_join(
        plan,
        users,
        Expr::eq(
            Expr::column("projects", "leadUserId", 3),
            Expr::column("users", "id", 0),
        ),
    );
    let plan = LogicalPlan::left_join(
        plan,
        counters,
        Expr::eq(
            Expr::column("projects", "id", 0),
            Expr::column("projectCounters", "projectId", 0),
        ),
    );
    let plan = LogicalPlan::left_join(
        plan,
        snapshots,
        Expr::eq(
            Expr::column("projects", "id", 0),
            Expr::column("projectSnapshots", "projectId", 0),
        ),
    );
    LogicalPlan::left_join(
        plan,
        milestones,
        Expr::eq(
            Expr::column("projects", "id", 0),
            Expr::column("currentMilestones", "projectId", 1),
        ),
    )
}

fn project_root_plan() -> LogicalPlan {
    LogicalPlan::project(
        LogicalPlan::filter(LogicalPlan::scan("projects"), project_root_predicate()),
        vec![
            Expr::column("projects", "id", 0),
            Expr::column("projects", "healthScore", 6),
        ],
    )
}

fn project_full_plan() -> LogicalPlan {
    LogicalPlan::project(
        LogicalPlan::filter(project_join_base(), project_joined_predicate()),
        vec![
            Expr::column("projects", "id", 0),
            Expr::column("projects", "healthScore", 6),
            Expr::column("projects", "updatedAt", 7),
            Expr::column("projects", "metadata", 9),
            Expr::column("projectCounters", "openIssueCount", 1),
            Expr::column("projectSnapshots", "velocity", 1),
            Expr::column("currentMilestones", "name", 2),
        ],
    )
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            id: "issue_status_only",
            label: "Issues status predicate only",
            root_table: "issues",
            logical: issue_status_only_plan(),
        },
        Scenario {
            id: "issue_estimate_only",
            label: "Issues estimate predicate only",
            root_table: "issues",
            logical: issue_estimate_only_plan(),
        },
        Scenario {
            id: "issue_customer_tier_enterprise_only",
            label: "Issues JSON-path single equality",
            root_table: "issues",
            logical: issue_customer_tier_enterprise_only_plan(),
        },
        Scenario {
            id: "issue_customer_tier_only",
            label: "Issues JSON-path predicate only",
            root_table: "issues",
            logical: issue_customer_tier_only_plan(),
        },
        Scenario {
            id: "issue_status_estimate",
            label: "Issues scalar predicates only",
            root_table: "issues",
            logical: issue_status_estimate_plan(),
        },
        Scenario {
            id: "issue_estimate_customer_tier",
            label: "Issues estimate + JSON-path predicates",
            root_table: "issues",
            logical: issue_estimate_customer_tier_plan(),
        },
        Scenario {
            id: "issue_root_filter_only",
            label: "Issues root filter only",
            root_table: "issues",
            logical: issue_root_plan(),
        },
        Scenario {
            id: "issue_join_root_filter_only",
            label: "7-way join + root-table predicates only",
            root_table: "issues",
            logical: issue_join_root_plan(),
        },
        Scenario {
            id: "issue_join_full_filter",
            label: "7-way join + full benchmark predicates",
            root_table: "issues",
            logical: issue_full_plan(),
        },
        Scenario {
            id: "project_root_filter_only",
            label: "Projects root filter only",
            root_table: "projects",
            logical: project_root_plan(),
        },
        Scenario {
            id: "project_join_full_filter",
            label: "6-way join + full board predicates",
            root_table: "projects",
            logical: project_full_plan(),
        },
    ]
}

fn node_label(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::TableScan { table } => format!("TableScan[{table}]"),
        PhysicalPlan::IndexScan { table, index, .. } => format!("IndexScan[{table}.{index}]"),
        PhysicalPlan::IndexGet { table, index, .. } => format!("IndexGet[{table}.{index}]"),
        PhysicalPlan::IndexInGet { table, index, .. } => format!("IndexInGet[{table}.{index}]"),
        PhysicalPlan::GinIndexScan { table, index, .. } => format!("GinIndexScan[{table}.{index}]"),
        PhysicalPlan::GinIndexScanMulti { table, index, .. } => {
            format!("GinIndexScanMulti[{table}.{index}]")
        }
        PhysicalPlan::Filter { .. } => "Filter".into(),
        PhysicalPlan::Project { columns, .. } => format!("Project[{} cols]", columns.len()),
        PhysicalPlan::HashJoin {
            join_type,
            output_tables,
            ..
        } => {
            format!("HashJoin[{join_type:?}; {} tables]", output_tables.len())
        }
        PhysicalPlan::SortMergeJoin {
            join_type,
            output_tables,
            ..
        } => {
            format!(
                "SortMergeJoin[{join_type:?}; {} tables]",
                output_tables.len()
            )
        }
        PhysicalPlan::NestedLoopJoin {
            join_type,
            output_tables,
            ..
        } => {
            format!(
                "NestedLoopJoin[{join_type:?}; {} tables]",
                output_tables.len()
            )
        }
        PhysicalPlan::IndexNestedLoopJoin {
            join_type,
            inner_table,
            outer_is_left,
            ..
        } => format!(
            "IndexNestedLoopJoin[{join_type:?}; inner={inner_table}; outer_is_left={outer_is_left}]"
        ),
        PhysicalPlan::HashAggregate { .. } => "HashAggregate".into(),
        PhysicalPlan::Sort { .. } => "Sort".into(),
        PhysicalPlan::TopN { limit, offset, .. } => format!("TopN[limit={limit}, offset={offset}]"),
        PhysicalPlan::Limit { limit, offset, .. } => {
            format!("Limit[limit={limit}, offset={offset}]")
        }
        PhysicalPlan::CrossProduct { .. } => "CrossProduct".into(),
        PhysicalPlan::Union { all, .. } => format!("Union[all={all}]"),
        PhysicalPlan::NoOp { .. } => "NoOp".into(),
        PhysicalPlan::Empty => "Empty".into(),
    }
}

fn measure_streaming(cache: &TableCache, plan: &PhysicalPlan) -> Measure {
    let data_source = TableCacheDataSource::new(cache);
    let runner = PhysicalPlanRunner::new(&data_source);
    let artifact = runner.compile_execution_artifact_with_data_source(plan);
    let mut times = Vec::with_capacity(PROFILE_ROUNDS);
    let mut row_count = 0usize;

    for _ in 0..PROFILE_ROUNDS {
        let mut emitted = 0usize;
        let started_at = Instant::now();
        runner
            .execute_with_artifact_rows(plan, &artifact, |_row| {
                emitted += 1;
                Ok(true)
            })
            .unwrap();
        times.push(started_at.elapsed().as_secs_f64() * 1_000.0);
        row_count = emitted;
    }

    Measure {
        median_ms: median(&times),
        mean_ms: mean(&times),
        row_count,
    }
}

fn measure_collect(cache: &TableCache, plan: &PhysicalPlan) -> Measure {
    let data_source = TableCacheDataSource::new(cache);
    let runner = PhysicalPlanRunner::new(&data_source);
    let artifact = runner.compile_execution_artifact_with_data_source(plan);
    let mut times = Vec::with_capacity(PROFILE_ROUNDS);
    let mut row_count = 0usize;

    for _ in 0..PROFILE_ROUNDS {
        let started_at = Instant::now();
        let rows = runner
            .execute_with_artifact_row_vec(plan, &artifact)
            .unwrap();
        times.push(started_at.elapsed().as_secs_f64() * 1_000.0);
        row_count = rows.len();
    }

    Measure {
        median_ms: median(&times),
        mean_ms: mean(&times),
        row_count,
    }
}

fn measure_planning(cache: &TableCache, root_table: &str, logical: &LogicalPlan) -> f64 {
    let mut times = Vec::with_capacity(PROFILE_ROUNDS);
    for _ in 0..PROFILE_ROUNDS {
        let started_at = Instant::now();
        let _ = compile_plan(cache, root_table, logical.clone());
        times.push(started_at.elapsed().as_secs_f64() * 1_000.0);
    }
    median(&times)
}

fn measure_artifact_compile(cache: &TableCache, physical: &PhysicalPlan) -> f64 {
    let data_source = TableCacheDataSource::new(cache);
    let runner = PhysicalPlanRunner::new(&data_source);
    let mut times = Vec::with_capacity(PROFILE_ROUNDS);
    for _ in 0..PROFILE_ROUNDS {
        let started_at = Instant::now();
        let _ = runner.compile_execution_artifact_with_data_source(physical);
        times.push(started_at.elapsed().as_secs_f64() * 1_000.0);
    }
    median(&times)
}

fn profile_subtree(cache: &TableCache, plan: &PhysicalPlan) -> PlanProfile {
    let children = plan
        .inputs()
        .into_iter()
        .map(|child| profile_subtree(cache, child))
        .collect();
    PlanProfile {
        label: node_label(plan),
        measure: measure_streaming(cache, plan),
        children,
    }
}

fn render_profile_tree(profile: &PlanProfile, depth: usize, out: &mut String) {
    let indent = "  ".repeat(depth);
    let _ = writeln!(
        out,
        "{indent}- {}: {} median, {} rows",
        profile.label,
        format_ms(profile.measure.median_ms),
        profile.measure.row_count,
    );
    for child in &profile.children {
        render_profile_tree(child, depth + 1, out);
    }
}

fn main() {
    let cache = create_cache();
    let mut report = String::new();
    let output_path = PathBuf::from("tmp").join("cynos_native_query_profile.md");

    let _ = writeln!(report, "# Cynos Native Query Profile");
    let _ = writeln!(report);
    let _ = writeln!(
        report,
        "Dataset: orgs={}, teams={}, users={}, projects={}, milestones={}, issues={}",
        ORGANIZATION_COUNT, TEAM_COUNT, USER_COUNT, PROJECT_COUNT, MILESTONE_COUNT, ISSUE_COUNT
    );
    let _ = writeln!(report, "Rounds per measurement: {PROFILE_ROUNDS}");
    let _ = writeln!(report);

    for scenario in scenarios() {
        let explain = explain_plan(&cache, scenario.root_table, scenario.logical.clone());
        let planning_median = measure_planning(&cache, scenario.root_table, &scenario.logical);
        let physical = compile_plan(&cache, scenario.root_table, scenario.logical.clone());
        let artifact_compile_median = measure_artifact_compile(&cache, &physical);
        let stream_measure = measure_streaming(&cache, &physical);
        let collect_measure = measure_collect(&cache, &physical);
        let profile = profile_subtree(&cache, &physical);
        let ctx = build_execution_context_for_plan(&cache, scenario.root_table, &scenario.logical);
        let planner = QueryPlanner::new(ctx);
        let optimized_logical = planner.optimize_logical(scenario.logical.clone());

        let _ = writeln!(report, "## {}", scenario.label);
        let _ = writeln!(report);
        let _ = writeln!(report, "- id: `{}`", scenario.id);
        let _ = writeln!(report, "- planning median: {}", format_ms(planning_median));
        let _ = writeln!(
            report,
            "- artifact compile median: {}",
            format_ms(artifact_compile_median)
        );
        let _ = writeln!(
            report,
            "- compiled execute (streaming count) median: {}, rows={}",
            format_ms(stream_measure.median_ms),
            stream_measure.row_count
        );
        let _ = writeln!(
            report,
            "- compiled execute (collect rows) median: {}, rows={}",
            format_ms(collect_measure.median_ms),
            collect_measure.row_count
        );
        let _ = writeln!(report);
        let _ = writeln!(report, "### Optimized Logical");
        let _ = writeln!(report, "```text");
        let _ = writeln!(report, "{:#?}", optimized_logical);
        let _ = writeln!(report, "```");
        let _ = writeln!(report);
        let _ = writeln!(report, "### Physical");
        let _ = writeln!(report, "```text");
        let _ = writeln!(report, "{}", explain.physical_plan);
        let _ = writeln!(report, "```");
        let _ = writeln!(report);
        let _ = writeln!(report, "### Subtree Profile");
        render_profile_tree(&profile, 0, &mut report);
        let _ = writeln!(report);
    }

    fs::create_dir_all(output_path.parent().unwrap()).unwrap();
    fs::write(&output_path, report).unwrap();
    println!("Wrote native profile to {}", output_path.display());
}
