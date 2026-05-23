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

use crate::planner::{logical_to_physical, LogicalPlan, PhysicalPlan};
use alloc::boxed::Box;
use alloc::vec::Vec;

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
        let physical = logical_to_physical(plan);
        // Apply physical plan optimizations
        TopNPushdown::new().optimize(physical)
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
