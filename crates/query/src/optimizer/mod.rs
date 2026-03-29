//! Query optimizer module.

mod and_predicate;
mod cross_product;
mod get_row_count;
mod implicit_joins;
mod index_join;
mod index_selection;
mod join_reorder;
mod limit_skip_by_index;
mod multi_column_or;
mod not_simplification;
mod order_by_index;
mod outer_join_removal;
mod outer_join_simplification;
mod pass;
mod predicate_pushdown;
mod topn_pushdown;

pub use and_predicate::AndPredicatePass;
pub use cross_product::CrossProductPass;
pub use get_row_count::{GetRowCountPass, GetRowCountPlan};
pub use implicit_joins::ImplicitJoinsPass;
pub use index_join::IndexJoinPass;
pub use index_selection::IndexSelection;
pub use join_reorder::JoinReorder;
pub use limit_skip_by_index::LimitSkipByIndexPass;
pub use multi_column_or::{MultiColumnOrConfig, MultiColumnOrPass};
pub use not_simplification::NotSimplification;
pub use order_by_index::OrderByIndexPass;
pub use outer_join_removal::OuterJoinRemoval;
pub use outer_join_simplification::OuterJoinSimplification;
pub use pass::OptimizerPass;
pub use predicate_pushdown::PredicatePushdown;
pub use topn_pushdown::TopNPushdown;

use crate::planner::{JoinAlgorithm, LogicalPlan, PhysicalPlan};
use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use cynos_core::Value;

/// Query optimizer that applies optimization passes.
pub struct Optimizer {
    passes: Vec<Box<dyn OptimizerPass>>,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    /// Creates a new optimizer with default passes.
    ///
    /// The default passes are applied in this order:
    /// 1. NotSimplification - Simplify NOT expressions (double negation, De Morgan)
    /// 2. AndPredicatePass - Break down AND predicates into chained filters
    /// 3. CrossProductPass - Convert multi-way cross products to binary tree
    /// 4. ImplicitJoinsPass - Convert CrossProduct + Filter to Join
    /// 5. OuterJoinSimplification - Convert outer joins to inner when WHERE rejects NULL
    /// 6. PredicatePushdown - Push filters down the plan tree
    /// 7. JoinReorder - Reorder joins for better performance
    ///
    /// Note: IndexSelection is not included by default because it requires
    /// ExecutionContext with index information. Use `with_passes()` to add it.
    pub fn new() -> Self {
        Self {
            passes: alloc::vec![
                Box::new(NotSimplification),
                Box::new(AndPredicatePass),
                Box::new(CrossProductPass),
                Box::new(ImplicitJoinsPass),
                Box::new(OuterJoinSimplification),
                Box::new(PredicatePushdown),
                Box::new(JoinReorder::new()),
            ],
        }
    }

    /// Creates an optimizer with custom passes.
    pub fn with_passes(passes: Vec<Box<dyn OptimizerPass>>) -> Self {
        Self { passes }
    }

    /// Optimizes a logical plan.
    pub fn optimize(&self, mut plan: LogicalPlan) -> LogicalPlan {
        for pass in &self.passes {
            plan = pass.optimize(plan);
        }
        plan
    }

    /// Converts a logical plan to a physical plan.
    /// Also applies physical plan optimizations (TopNPushdown).
    pub fn to_physical(&self, plan: LogicalPlan) -> PhysicalPlan {
        let physical = self.logical_to_physical(plan);
        // Apply physical plan optimizations
        TopNPushdown::new().optimize(physical)
    }

    fn logical_to_physical(&self, plan: LogicalPlan) -> PhysicalPlan {
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
                // Extract the key from the JSON path (e.g., "$.category" -> "category")
                let key = path.trim_start_matches("$.").to_string();

                // Convert value to string for GIN index lookup
                let value_str = value.map(|v| match v {
                    Value::String(s) => s,
                    Value::Int32(i) => alloc::format!("{}", i),
                    Value::Int64(i) => alloc::format!("{}", i),
                    Value::Float64(f) => alloc::format!("{}", f),
                    Value::Boolean(b) => alloc::format!("{}", b),
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
                // Convert (path, value) pairs to (key, value_str) pairs
                let string_pairs: Vec<(String, String)> = pairs
                    .into_iter()
                    .map(|(path, value)| {
                        let key = path.trim_start_matches("$.").to_string();
                        let value_str = match value {
                            Value::String(s) => s,
                            Value::Int32(i) => alloc::format!("{}", i),
                            Value::Int64(i) => alloc::format!("{}", i),
                            Value::Float64(f) => alloc::format!("{}", f),
                            Value::Boolean(b) => alloc::format!("{}", b),
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

                // Choose join algorithm based on condition
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

    fn choose_join_algorithm(&self, condition: &crate::ast::Expr) -> JoinAlgorithm {
        // For equi-joins, prefer hash join
        if condition.is_equi_join() {
            return JoinAlgorithm::Hash;
        }

        // For range joins, use nested loop (could use sort-merge if sorted)
        if condition.is_range_join() {
            return JoinAlgorithm::NestedLoop;
        }

        // Default to nested loop for complex conditions
        JoinAlgorithm::NestedLoop
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;

    #[test]
    fn test_optimizer_default() {
        let optimizer = Optimizer::new();
        assert_eq!(optimizer.passes.len(), 7);
    }

    #[test]
    fn test_logical_to_physical_scan() {
        let optimizer = Optimizer::new();
        let logical = LogicalPlan::scan("users");
        let physical = optimizer.to_physical(logical);

        assert!(matches!(physical, PhysicalPlan::TableScan { table } if table == "users"));
    }

    #[test]
    fn test_logical_to_physical_filter() {
        let optimizer = Optimizer::new();
        let logical = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );
        let physical = optimizer.to_physical(logical);

        assert!(matches!(physical, PhysicalPlan::Filter { .. }));
    }

    #[test]
    fn test_logical_to_physical_join() {
        let optimizer = Optimizer::new();
        let logical = LogicalPlan::inner_join(
            LogicalPlan::scan("a"),
            LogicalPlan::scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
        );
        let physical = optimizer.to_physical(logical);

        // Should choose hash join for equi-join
        assert!(matches!(physical, PhysicalPlan::HashJoin { .. }));
    }
}
