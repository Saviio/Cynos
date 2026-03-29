//! Unified query planner with ExecutionContext support.
//!
//! This module provides a unified entry point for query planning that handles
//! both logical and physical plan optimizations with proper ExecutionContext support.
//!
//! ## Architecture
//!
//! The query planning pipeline consists of:
//!
//! 1. **Logical Optimization** - Context-free transformations:
//!    - NotSimplification
//!    - AndPredicatePass
//!    - CrossProductPass
//!    - ImplicitJoinsPass
//!    - OuterJoinSimplification
//!    - PredicatePushdown
//!    - OuterJoinSimplification (rerun after pushdown to expose deeper join collapses)
//!    - PredicatePushdown
//!    - OuterJoinSimplification
//!    - JoinReorder
//!    - PredicatePushdown (rerun after join reorder to expose new single-table filters)
//!
//! 2. **Context-Aware Logical Optimization** - Requires ExecutionContext:
//!    - IndexSelection (converts Filter+Scan to IndexScan/IndexGet)
//!    - OuterJoinRemoval (removes unused cardinality-preserving outer joins)
//!
//! 3. **Physical Plan Conversion** - Converts logical to physical plan
//!
//! 4. **Physical Optimization** - Context-aware physical transformations:
//!    - TopNPushdown (converts Sort+Limit to TopN)
//!    - OrderByIndexPass (leverages indexes for sorting)
//!    - IndexJoinPass (uses indexed inner lookups for bounded joins)
//!    - LimitSkipByIndexPass (pushes limit/offset to IndexScan)
//!
//! ## Usage
//!
//! ```ignore
//! let ctx = build_execution_context(&cache, "users");
//! let planner = QueryPlanner::new(ctx);
//! let physical_plan = planner.plan(logical_plan);
//! ```

use crate::context::ExecutionContext;
use crate::optimizer::{
    AndPredicatePass, CrossProductPass, ImplicitJoinsPass, IndexJoinPass, IndexSelection,
    JoinReorder, LimitSkipByIndexPass, NotSimplification, OptimizerPass, OrderByIndexPass,
    OuterJoinRemoval, OuterJoinSimplification, PredicatePushdown, TopNPushdown,
};
use crate::planner::{LogicalPlan, PhysicalPlan};
use alloc::boxed::Box;
use alloc::vec::Vec;

/// Unified query planner that handles the complete optimization pipeline.
///
/// Unlike the basic `Optimizer`, `QueryPlanner` supports `ExecutionContext`
/// throughout the entire pipeline, enabling context-aware optimizations
/// for both logical and physical plans.
pub struct QueryPlanner {
    ctx: ExecutionContext,
    /// Logical optimization passes (context-free)
    logical_passes: Vec<Box<dyn OptimizerPass>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlannerProfile {
    Default,
    RootSubset,
}

impl QueryPlanner {
    fn default_logical_passes(ctx: &ExecutionContext) -> Vec<Box<dyn OptimizerPass>> {
        alloc::vec![
            Box::new(NotSimplification),
            Box::new(AndPredicatePass),
            Box::new(CrossProductPass),
            Box::new(ImplicitJoinsPass),
            Box::new(OuterJoinSimplification),
            Box::new(PredicatePushdown),
            // Predicate pushdown can surface new null-rejecting filters directly above
            // deeper outer joins, so rerun simplification before join reordering.
            Box::new(OuterJoinSimplification),
            // When an intermediate outer join collapses to inner, another pushdown/simplify
            // round can expose the next join in the chain.
            Box::new(PredicatePushdown),
            Box::new(OuterJoinSimplification),
            Box::new(JoinReorder::with_context(ctx.clone())),
            // Join reordering can surface a new join boundary for a filter that still
            // references only one side, so give pushdown one final pass before index selection.
            Box::new(PredicatePushdown),
        ]
    }

    fn root_subset_logical_passes(ctx: &ExecutionContext) -> Vec<Box<dyn OptimizerPass>> {
        // Root-subset planning now uses the ordinary logical pipeline and relies on
        // restricted-relation intent plus costed join/index choices to keep the anchor
        // relation as the driver, rather than disabling logical passes wholesale.
        Self::default_logical_passes(ctx)
    }

    /// Creates a new QueryPlanner with the given execution context.
    ///
    /// The planner is initialized with default optimization passes:
    /// - Logical: NotSimplification, AndPredicatePass, CrossProductPass,
    ///   ImplicitJoinsPass, OuterJoinSimplification, PredicatePushdown,
    ///   OuterJoinSimplification, PredicatePushdown, OuterJoinSimplification,
    ///   JoinReorder, PredicatePushdown
    /// - Context-aware logical: IndexSelection
    /// - Physical: TopNPushdown, OrderByIndexPass, LimitSkipByIndexPass
    pub fn new(ctx: ExecutionContext) -> Self {
        Self::for_profile(ctx, PlannerProfile::Default)
    }

    /// Creates a planner profile tailored for root-subset snapshot refresh.
    ///
    /// The subset profile now uses the normal logical pipeline and relies on
    /// restricted-relation planning intent plus physical costing to keep the
    /// declared root table as the driver.
    pub fn for_root_subset(ctx: ExecutionContext) -> Self {
        Self::for_profile(ctx, PlannerProfile::RootSubset)
    }

    pub fn for_profile(ctx: ExecutionContext, profile: PlannerProfile) -> Self {
        let logical_passes = match profile {
            PlannerProfile::Default => Self::default_logical_passes(&ctx),
            PlannerProfile::RootSubset => Self::root_subset_logical_passes(&ctx),
        };
        Self {
            ctx,
            logical_passes,
        }
    }

    /// Creates a QueryPlanner with custom logical passes.
    ///
    /// Context-aware passes (IndexSelection, OrderByIndexPass, etc.) are
    /// still applied automatically using the provided context.
    pub fn with_logical_passes(ctx: ExecutionContext, passes: Vec<Box<dyn OptimizerPass>>) -> Self {
        Self {
            ctx,
            logical_passes: passes,
        }
    }

    /// Returns a reference to the execution context.
    pub fn context(&self) -> &ExecutionContext {
        &self.ctx
    }

    fn optimize_context_aware_logical(&self, mut logical: LogicalPlan) -> LogicalPlan {
        let index_selection = IndexSelection::with_context(self.ctx.clone());
        logical = index_selection.optimize(logical);
        OuterJoinRemoval::new(&self.ctx).optimize(logical)
    }

    /// Plans a logical query into an optimized physical plan.
    ///
    /// This is the main entry point that runs the complete optimization pipeline:
    /// 1. Apply context-free logical optimizations
    /// 2. Apply context-aware logical optimizations (IndexSelection, OuterJoinRemoval)
    /// 3. Convert to physical plan
    /// 4. Apply physical optimizations (TopNPushdown, OrderByIndexPass, LimitSkipByIndexPass)
    pub fn plan(&self, plan: LogicalPlan) -> PhysicalPlan {
        // Phase 1: Context-free logical optimizations
        let mut logical = plan;
        for pass in &self.logical_passes {
            logical = pass.optimize(logical);
        }

        // Phase 2: Context-aware logical optimizations
        logical = self.optimize_context_aware_logical(logical);

        // Phase 3: Convert to physical plan
        self.optimize_physical(self.logical_to_physical(logical))
    }

    /// Optimizes only the logical plan without converting to physical.
    ///
    /// Useful for debugging or when you need to inspect the optimized logical plan.
    pub fn optimize_logical(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut logical = plan;

        // Context-free passes
        for pass in &self.logical_passes {
            logical = pass.optimize(logical);
        }

        // Context-aware passes
        self.optimize_context_aware_logical(logical)
    }

    /// Converts a logical plan to physical and applies physical optimizations.
    ///
    /// Assumes the logical plan has already been optimized.
    pub fn to_physical(&self, plan: LogicalPlan) -> PhysicalPlan {
        self.optimize_physical(self.logical_to_physical(plan))
    }

    /// Converts a logical plan to a physical plan without optimizations.
    fn logical_to_physical(&self, plan: LogicalPlan) -> PhysicalPlan {
        use crate::planner::JoinAlgorithm;

        match plan {
            LogicalPlan::Scan { table } => PhysicalPlan::table_scan(table),

            LogicalPlan::IndexScan {
                table,
                index,
                bounds,
            } => PhysicalPlan::IndexScan {
                table,
                index,
                bounds,
                limit: None,
                offset: None,
                reverse: false,
            },

            LogicalPlan::IndexGet { table, index, key } => {
                PhysicalPlan::index_get(table, index, key)
            }

            LogicalPlan::IndexInGet { table, index, keys } => {
                PhysicalPlan::index_in_get(table, index, keys)
            }

            LogicalPlan::GinIndexScan {
                table,
                index,
                column: _,
                column_index: _,
                path,
                value,
                query_type,
                recheck,
            } => {
                let key: alloc::string::String = path.trim_start_matches("$.").into();
                let value_str = value.map(|v| match v {
                    cynos_core::Value::String(s) => s,
                    cynos_core::Value::Int32(i) => alloc::format!("{}", i),
                    cynos_core::Value::Int64(i) => alloc::format!("{}", i),
                    cynos_core::Value::Float64(f) => alloc::format!("{}", f),
                    cynos_core::Value::Boolean(b) => alloc::format!("{}", b),
                    _ => alloc::format!("{:?}", v),
                });
                PhysicalPlan::gin_index_scan(table, index, key, value_str, query_type, recheck)
            }

            LogicalPlan::GinIndexScanMulti {
                table,
                index,
                column: _,
                pairs,
                match_all,
                recheck,
            } => {
                let string_pairs: Vec<(alloc::string::String, alloc::string::String)> = pairs
                    .into_iter()
                    .map(|(path, value)| {
                        let key: alloc::string::String = path.trim_start_matches("$.").into();
                        let value_str = match value {
                            cynos_core::Value::String(s) => s,
                            cynos_core::Value::Int32(i) => alloc::format!("{}", i),
                            cynos_core::Value::Int64(i) => alloc::format!("{}", i),
                            cynos_core::Value::Float64(f) => alloc::format!("{}", f),
                            cynos_core::Value::Boolean(b) => alloc::format!("{}", b),
                            _ => alloc::format!("{:?}", value),
                        };
                        (key, value_str)
                    })
                    .collect();
                PhysicalPlan::gin_index_scan_multi(table, index, string_pairs, match_all, recheck)
            }

            LogicalPlan::Filter { input, predicate } => {
                let input_physical = self.logical_to_physical(*input);
                PhysicalPlan::filter(input_physical, predicate)
            }

            LogicalPlan::Project { input, columns } => {
                let input_physical = self.logical_to_physical(*input);
                PhysicalPlan::project(input_physical, columns)
            }

            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => {
                let left_physical = self.logical_to_physical(*left);
                let right_physical = self.logical_to_physical(*right);
                let algorithm = self.choose_join_algorithm(&condition);

                match algorithm {
                    JoinAlgorithm::Hash => PhysicalPlan::hash_join_with_output_tables(
                        left_physical,
                        right_physical,
                        condition,
                        join_type,
                        output_tables,
                    ),
                    JoinAlgorithm::SortMerge => PhysicalPlan::sort_merge_join_with_output_tables(
                        left_physical,
                        right_physical,
                        condition,
                        join_type,
                        output_tables,
                    ),
                    JoinAlgorithm::NestedLoop | JoinAlgorithm::IndexNestedLoop => {
                        PhysicalPlan::nested_loop_join_with_output_tables(
                            left_physical,
                            right_physical,
                            condition,
                            join_type,
                            output_tables,
                        )
                    }
                }
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input_physical = self.logical_to_physical(*input);
                PhysicalPlan::hash_aggregate(input_physical, group_by, aggregates)
            }

            LogicalPlan::Sort { input, order_by } => {
                let input_physical = self.logical_to_physical(*input);
                PhysicalPlan::sort(input_physical, order_by)
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input_physical = self.logical_to_physical(*input);
                PhysicalPlan::limit(input_physical, limit, offset)
            }

            LogicalPlan::CrossProduct { left, right } => {
                let left_physical = self.logical_to_physical(*left);
                let right_physical = self.logical_to_physical(*right);
                PhysicalPlan::CrossProduct {
                    left: Box::new(left_physical),
                    right: Box::new(right_physical),
                }
            }

            LogicalPlan::Union { left, right, all } => {
                let left_physical = self.logical_to_physical(*left);
                let right_physical = self.logical_to_physical(*right);
                PhysicalPlan::union(left_physical, right_physical, all)
            }

            LogicalPlan::Empty => PhysicalPlan::Empty,
        }
    }

    fn choose_join_algorithm(&self, condition: &crate::ast::Expr) -> crate::planner::JoinAlgorithm {
        if condition.is_equi_join() {
            return crate::planner::JoinAlgorithm::Hash;
        }
        if condition.is_range_join() {
            return crate::planner::JoinAlgorithm::NestedLoop;
        }
        crate::planner::JoinAlgorithm::NestedLoop
    }

    fn optimize_physical(&self, mut physical: PhysicalPlan) -> PhysicalPlan {
        physical = TopNPushdown::new().optimize(physical);
        physical = OrderByIndexPass::new(&self.ctx).optimize(physical);
        physical = IndexJoinPass::new(&self.ctx).optimize(physical);
        LimitSkipByIndexPass::new(&self.ctx).optimize(physical)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Expr, JoinType, SortOrder};
    use crate::context::{IndexInfo, TableStats};
    use alloc::string::String;

    fn create_test_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                    IndexInfo::new("idx_name", alloc::vec!["name".into()], false),
                ],
            },
        );
        ctx
    }

    fn collect_scan_order(plan: &LogicalPlan, order: &mut Vec<String>) {
        match plan {
            LogicalPlan::Scan { table }
            | LogicalPlan::IndexScan { table, .. }
            | LogicalPlan::IndexGet { table, .. }
            | LogicalPlan::IndexInGet { table, .. }
            | LogicalPlan::GinIndexScan { table, .. }
            | LogicalPlan::GinIndexScanMulti { table, .. } => order.push(table.clone()),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => collect_scan_order(input, order),
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::CrossProduct { left, right }
            | LogicalPlan::Union { left, right, .. } => {
                collect_scan_order(left, order);
                collect_scan_order(right, order);
            }
            LogicalPlan::Empty => {}
        }
    }

    fn collect_join_types(plan: &LogicalPlan, join_types: &mut Vec<JoinType>) {
        match plan {
            LogicalPlan::Join {
                left,
                right,
                join_type,
                ..
            } => {
                join_types.push(*join_type);
                collect_join_types(left, join_types);
                collect_join_types(right, join_types);
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => collect_join_types(input, join_types),
            LogicalPlan::CrossProduct { left, right } | LogicalPlan::Union { left, right, .. } => {
                collect_join_types(left, join_types);
                collect_join_types(right, join_types);
            }
            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. }
            | LogicalPlan::Empty => {}
        }
    }

    fn build_issue_project_counter_outer_join_plan() -> LogicalPlan {
        LogicalPlan::filter(
            LogicalPlan::Join {
                left: Box::new(LogicalPlan::Join {
                    left: Box::new(LogicalPlan::scan("issues")),
                    right: Box::new(LogicalPlan::scan("projects")),
                    condition: Expr::eq(
                        Expr::column("issues", "project_id", 1),
                        Expr::column("projects", "id", 0),
                    ),
                    join_type: JoinType::LeftOuter,
                    output_tables: alloc::vec!["issues".into(), "projects".into()],
                }),
                right: Box::new(LogicalPlan::scan("project_counters")),
                condition: Expr::eq(
                    Expr::column("projects", "id", 0),
                    Expr::column("project_counters", "project_id", 0),
                ),
                join_type: JoinType::LeftOuter,
                output_tables: alloc::vec![
                    "issues".into(),
                    "projects".into(),
                    "project_counters".into(),
                ],
            },
            Expr::and(
                Expr::gte(
                    Expr::column("projects", "health_score", 1),
                    Expr::literal(45i64),
                ),
                Expr::gte(
                    Expr::column("project_counters", "open_issue_count", 1),
                    Expr::literal(5i64),
                ),
            ),
        )
    }

    #[test]
    fn test_query_planner_basic() {
        let ctx = create_test_context();
        let planner = QueryPlanner::new(ctx);

        let plan = LogicalPlan::scan("users");
        let physical = planner.plan(plan);

        assert!(matches!(physical, PhysicalPlan::TableScan { .. }));
    }

    #[test]
    fn test_query_planner_index_selection() {
        let ctx = create_test_context();
        let planner = QueryPlanner::new(ctx);

        // Filter: id = 42
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(42i64)),
        );

        let physical = planner.plan(plan);

        // Should use IndexGet
        assert!(matches!(physical, PhysicalPlan::IndexGet { .. }));
    }

    #[test]
    fn test_query_planner_union_lowers_to_physical_union() {
        let ctx = create_test_context();
        let planner = QueryPlanner::new(ctx);

        let plan = LogicalPlan::union(
            LogicalPlan::scan("users"),
            LogicalPlan::scan("users"),
            false,
        );
        let physical = planner.plan(plan);

        assert!(matches!(physical, PhysicalPlan::Union { all: false, .. }));
    }

    #[test]
    fn test_query_planner_order_by_index() {
        let ctx = create_test_context();
        let planner = QueryPlanner::new(ctx);

        // Sort by id ASC
        let plan = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::scan("users")),
            order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
        };

        let physical = planner.plan(plan);

        // Should use IndexScan instead of Sort
        assert!(matches!(physical, PhysicalPlan::IndexScan { .. }));
    }

    #[test]
    fn test_query_planner_topn_pushdown() {
        let ctx = create_test_context();
        let planner = QueryPlanner::new(ctx);

        // Sort by id DESC + Limit 10
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::scan("users")),
                order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Desc)],
            }),
            limit: 10,
            offset: 0,
        };

        let physical = planner.plan(plan);

        // Should become IndexScan with limit and reverse
        match physical {
            PhysicalPlan::IndexScan { limit, reverse, .. } => {
                assert_eq!(limit, Some(10));
                assert!(reverse);
            }
            _ => panic!("Expected IndexScan, got {:?}", physical),
        }
    }

    #[test]
    fn test_query_planner_uses_index_join_for_bounded_unique_join() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 100_000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_age", alloc::vec!["age".into()], false),
                    IndexInfo::new("idx_dept_id", alloc::vec!["dept_id".into()], false),
                ],
            },
        );
        ctx.register_table(
            "departments",
            TableStats {
                row_count: 100,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "pk_departments_id",
                    alloc::vec!["id".into()],
                    true,
                )],
            },
        );

        let planner = QueryPlanner::new(ctx);
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Join {
                left: Box::new(LogicalPlan::filter(
                    LogicalPlan::scan("users"),
                    Expr::gt(Expr::column("users", "age", 1), Expr::literal(60i64)),
                )),
                right: Box::new(LogicalPlan::scan("departments")),
                condition: Expr::eq(
                    Expr::column("users", "dept_id", 2),
                    Expr::column("departments", "id", 0),
                ),
                join_type: crate::ast::JoinType::Inner,
                output_tables: alloc::vec!["users".into(), "departments".into()],
            }),
            limit: 1_000,
            offset: 0,
        };

        let physical = planner.plan(plan);
        match physical {
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                assert_eq!(limit, 1_000);
                assert_eq!(offset, 0);
                assert!(matches!(*input, PhysicalPlan::IndexNestedLoopJoin { .. }));
            }
            other => panic!(
                "Expected limit over index nested loop join, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_query_planner_optimize_logical() {
        let ctx = create_test_context();
        let planner = QueryPlanner::new(ctx);

        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(42i64)),
        );

        let optimized = planner.optimize_logical(plan);

        // Should convert to IndexGet
        assert!(matches!(optimized, LogicalPlan::IndexGet { .. }));
    }

    #[test]
    fn test_query_planner_reorders_joins_but_preserves_logical_output_order() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "a",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );
        ctx.register_table(
            "b",
            TableStats {
                row_count: 10,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );
        ctx.register_table(
            "c",
            TableStats {
                row_count: 100,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );

        let planner = QueryPlanner::new(ctx);
        let plan = LogicalPlan::inner_join(
            LogicalPlan::inner_join(
                LogicalPlan::scan("a"),
                LogicalPlan::scan("c"),
                Expr::eq(Expr::column("a", "id", 0), Expr::column("c", "a_id", 0)),
            ),
            LogicalPlan::scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
        );

        let optimized = planner.optimize_logical(plan);
        let mut order = Vec::new();
        collect_scan_order(&optimized, &mut order);

        assert_eq!(
            order,
            alloc::vec![
                alloc::string::String::from("b"),
                alloc::string::String::from("a"),
                alloc::string::String::from("c")
            ]
        );
        assert_eq!(
            optimized.output_tables(),
            alloc::vec![
                alloc::string::String::from("a"),
                alloc::string::String::from("c"),
                alloc::string::String::from("b")
            ]
        );
    }

    #[test]
    fn test_query_planner_reruns_outer_join_simplification_after_pushdown() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "issues",
            TableStats {
                row_count: 10_000,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );
        ctx.register_table(
            "projects",
            TableStats {
                row_count: 1_000,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );
        ctx.register_table(
            "project_counters",
            TableStats {
                row_count: 1_000,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );

        let planner = QueryPlanner::new(ctx);
        let plan = build_issue_project_counter_outer_join_plan();

        let optimized = planner.optimize_logical(plan);
        let mut join_types = Vec::new();
        collect_join_types(&optimized, &mut join_types);

        assert!(!join_types.is_empty());
        assert!(join_types
            .iter()
            .all(|join_type| *join_type == JoinType::Inner));
    }

    #[test]
    fn test_root_subset_profile_keeps_anchor_table_as_driver() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "a",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );
        ctx.register_table(
            "b",
            TableStats {
                row_count: 10,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );
        ctx.register_table(
            "c",
            TableStats {
                row_count: 100,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );
        ctx.set_planner_feature_flags(crate::context::PlannerFeatureFlags {
            restricted_relation_cbo: true,
        });
        ctx.set_planning_intent(crate::context::PlanningIntent {
            mode: crate::context::PlanningMode::RestrictedRelation,
            restricted_table: Some("a".into()),
            exact_subset_rows: Some(32),
            subset_fraction: Some(0.032),
            preferred_access_mode: crate::context::RestrictedAccessMode::SubsetDriven,
            anchor_table: Some("a".into()),
            allow_global_fallback: true,
        });
        ctx.register_effective_row_count("a", 32);

        let planner = QueryPlanner::for_root_subset(ctx);
        let plan = LogicalPlan::inner_join(
            LogicalPlan::inner_join(
                LogicalPlan::scan("a"),
                LogicalPlan::scan("c"),
                Expr::eq(Expr::column("a", "id", 0), Expr::column("c", "a_id", 0)),
            ),
            LogicalPlan::scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
        );

        let optimized = planner.optimize_logical(plan);
        let mut order = Vec::new();
        collect_scan_order(&optimized, &mut order);

        assert!(!order.is_empty());
        assert_eq!(order.first().map(String::as_str), Some("a"));
    }
}
