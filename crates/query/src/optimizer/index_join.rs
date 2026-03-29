//! Index join pass - converts eligible joins to index nested loop joins.
//!
//! This pass identifies Join nodes where one side has an index on the join column,
//! and converts them to IndexNestedLoopJoin for better performance.
//!
//! Example:
//! ```text
//! HashJoin(a.id = b.a_id)       =>    IndexNestedLoopJoin
//!    /            \                    outer: Scan(a)
//! Scan(a)      Scan(b)                 inner: b (using idx_a_id)
//! ```
//!
//! Index nested loop join is beneficial when:
//! 1. One side has an index on the join column
//! 2. The outer relation is small enough that index lookups are efficient
//! 3. The join is an inner equi-join

use crate::ast::{BinaryOp, ColumnRef, Expr, JoinType};
use crate::context::{ExecutionContext, IndexInfo};
use crate::planner::PhysicalPlan;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

const INDEX_JOIN_ALWAYS_OUTER_ROWS: usize = 64;
const INDEX_JOIN_MAX_OUTER_ROWS: usize = 4096;
const INDEX_JOIN_MAX_UNIQUE_EFFECTIVE_OUTER_ROWS: usize = 4096;
const INDEX_JOIN_ROW_GOAL_SMALL_INNER_ROWS: usize = 512;
const INDEX_JOIN_MIN_INNER_OUTER_RATIO: usize = 4;

/// Pass that converts eligible joins to index nested loop joins.
pub struct IndexJoinPass<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> IndexJoinPass<'a> {
    /// Creates a new IndexJoinPass with the given execution context.
    pub fn new(ctx: &'a ExecutionContext) -> Self {
        Self { ctx }
    }

    /// Optimizes the physical plan by converting eligible joins to index joins.
    pub fn optimize(&self, plan: PhysicalPlan) -> PhysicalPlan {
        self.traverse(plan, None)
    }

    fn traverse(&self, plan: PhysicalPlan, row_goal: Option<usize>) -> PhysicalPlan {
        match plan {
            // Check hash joins for index join optimization
            PhysicalPlan::HashJoin {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => {
                let left = self.traverse(*left, None);
                let right = self.traverse(*right, None);

                if !condition.is_equi_join() {
                    return PhysicalPlan::HashJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        condition,
                        join_type,
                        output_tables,
                    };
                }

                // Try to find an index on either side
                if let Some((outer, inner_table, inner_index, outer_is_left)) =
                    self.find_index_join_candidate(&left, &right, &condition, join_type, row_goal)
                {
                    return PhysicalPlan::IndexNestedLoopJoin {
                        outer: Box::new(outer),
                        inner_table,
                        inner_index,
                        condition,
                        join_type,
                        outer_is_left,
                        output_tables,
                    };
                }

                PhysicalPlan::HashJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    condition,
                    join_type,
                    output_tables,
                }
            }

            // Also check nested loop joins
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => {
                let left = self.traverse(*left, None);
                let right = self.traverse(*right, None);

                if !condition.is_equi_join() {
                    return PhysicalPlan::NestedLoopJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        condition,
                        join_type,
                        output_tables,
                    };
                }

                // Try to find an index on either side
                if let Some((outer, inner_table, inner_index, outer_is_left)) =
                    self.find_index_join_candidate(&left, &right, &condition, join_type, row_goal)
                {
                    return PhysicalPlan::IndexNestedLoopJoin {
                        outer: Box::new(outer),
                        inner_table,
                        inner_index,
                        condition,
                        join_type,
                        outer_is_left,
                        output_tables,
                    };
                }

                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    condition,
                    join_type,
                    output_tables,
                }
            }

            // Recursively process other nodes
            PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
                input: Box::new(self.traverse(*input, row_goal)),
                predicate,
            },

            PhysicalPlan::Project { input, columns } => PhysicalPlan::Project {
                input: Box::new(self.traverse(*input, row_goal)),
                columns,
            },

            PhysicalPlan::SortMergeJoin {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => PhysicalPlan::SortMergeJoin {
                left: Box::new(self.traverse(*left, None)),
                right: Box::new(self.traverse(*right, None)),
                condition,
                join_type,
                output_tables,
            },

            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => PhysicalPlan::HashAggregate {
                input: Box::new(self.traverse(*input, None)),
                group_by,
                aggregates,
            },

            PhysicalPlan::Sort { input, order_by } => PhysicalPlan::Sort {
                input: Box::new(self.traverse(*input, None)),
                order_by,
            },

            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => PhysicalPlan::Limit {
                input: Box::new(self.traverse(
                    *input,
                    Self::combine_row_goal(row_goal, limit.saturating_add(offset)),
                )),
                limit,
                offset,
            },

            PhysicalPlan::CrossProduct { left, right } => PhysicalPlan::CrossProduct {
                left: Box::new(self.traverse(*left, None)),
                right: Box::new(self.traverse(*right, None)),
            },

            PhysicalPlan::Union { left, right, all } => PhysicalPlan::Union {
                left: Box::new(self.traverse(*left, None)),
                right: Box::new(self.traverse(*right, None)),
                all,
            },

            PhysicalPlan::NoOp { input } => PhysicalPlan::NoOp {
                input: Box::new(self.traverse(*input, row_goal)),
            },

            PhysicalPlan::TopN {
                input,
                order_by,
                limit,
                offset,
            } => PhysicalPlan::TopN {
                input: Box::new(self.traverse(
                    *input,
                    Self::combine_row_goal(row_goal, limit.saturating_add(offset)),
                )),
                order_by,
                limit,
                offset,
            },

            // Leaf nodes and already-optimized nodes - no transformation
            plan @ (PhysicalPlan::TableScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::IndexGet { .. }
            | PhysicalPlan::IndexInGet { .. }
            | PhysicalPlan::IndexNestedLoopJoin { .. }
            | PhysicalPlan::Empty
            | PhysicalPlan::GinIndexScan { .. }
            | PhysicalPlan::GinIndexScanMulti { .. }) => plan,
        }
    }

    /// Finds a candidate for index join optimization.
    /// Returns (outer_plan, inner_table, inner_index) if found.
    fn find_index_join_candidate(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        condition: &Expr,
        join_type: JoinType,
        row_goal: Option<usize>,
    ) -> Option<(PhysicalPlan, String, String, bool)> {
        // Extract join columns from the condition
        let (cond_left_col, cond_right_col) = self.extract_join_columns(condition)?;
        let (left_col, right_col) =
            self.align_join_columns(left, right, cond_left_col, cond_right_col)?;

        match join_type {
            JoinType::Inner => {
                // Check if right side is a table scan with an index on the join column
                if let Some((table, index)) = self.get_indexed_table_scan(right, right_col) {
                    if self.should_use_index_join(left, &table, &index, row_goal) {
                        return Some((left.clone(), table, index.name.clone(), true));
                    }
                }

                // Check if left side is a table scan with an index on the join column
                if let Some((table, index)) = self.get_indexed_table_scan(left, left_col) {
                    if self.should_use_index_join(right, &table, &index, row_goal) {
                        return Some((right.clone(), table, index.name.clone(), false));
                    }
                }
            }
            JoinType::LeftOuter => {
                // Preserve LEFT JOIN semantics by probing the nullable right side.
                if let Some((table, index)) = self.get_indexed_table_scan(right, right_col) {
                    if self.should_use_index_join(left, &table, &index, row_goal) {
                        return Some((left.clone(), table, index.name.clone(), true));
                    }
                }
            }
            JoinType::RightOuter => {
                // Preserve RIGHT JOIN semantics by probing the nullable left side.
                if let Some((table, index)) = self.get_indexed_table_scan(left, left_col) {
                    if self.should_use_index_join(right, &table, &index, row_goal) {
                        return Some((right.clone(), table, index.name.clone(), false));
                    }
                }
            }
            JoinType::FullOuter | JoinType::Cross => {}
        }

        None
    }

    fn align_join_columns<'b>(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        first: &'b ColumnRef,
        second: &'b ColumnRef,
    ) -> Option<(&'b ColumnRef, &'b ColumnRef)> {
        let left_tables = left.collect_tables();
        let right_tables = right.collect_tables();

        let first_in_left = left_tables.iter().any(|table| table == &first.table);
        let first_in_right = right_tables.iter().any(|table| table == &first.table);
        let second_in_left = left_tables.iter().any(|table| table == &second.table);
        let second_in_right = right_tables.iter().any(|table| table == &second.table);

        if first_in_left && second_in_right && !first_in_right && !second_in_left {
            return Some((first, second));
        }

        if second_in_left && first_in_right && !second_in_right && !first_in_left {
            return Some((second, first));
        }

        None
    }

    /// Extracts the column names from an equi-join condition.
    fn extract_join_columns<'b>(
        &self,
        condition: &'b Expr,
    ) -> Option<(&'b ColumnRef, &'b ColumnRef)> {
        match condition {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                let left_col = self.extract_column_ref(left)?;
                let right_col = self.extract_column_ref(right)?;
                Some((left_col, right_col))
            }
            _ => None,
        }
    }

    fn extract_column_ref<'b>(&self, expr: &'b Expr) -> Option<&'b ColumnRef> {
        match expr {
            Expr::Column(col_ref) => Some(col_ref),
            _ => None,
        }
    }

    /// Checks if the plan is a table scan with an index on the given column.
    /// Returns (table_name, index_info) if found.
    fn get_indexed_table_scan(
        &self,
        plan: &PhysicalPlan,
        column: &ColumnRef,
    ) -> Option<(String, IndexInfo)> {
        match plan {
            PhysicalPlan::TableScan { table } => {
                if table != &column.table {
                    return None;
                }
                // Check if there's an index on this column
                let index = self.ctx.find_index(table, &[column.column.as_str()])?;
                if !index.supports_point_lookup() {
                    return None;
                }
                Some((table.clone(), index.clone()))
            }
            _ => None,
        }
    }

    fn should_use_index_join(
        &self,
        outer: &PhysicalPlan,
        inner_table: &str,
        inner_index: &IndexInfo,
        row_goal: Option<usize>,
    ) -> bool {
        let outer_rows = self.estimate_rows(outer);
        let effective_outer_rows = row_goal.map_or(outer_rows, |goal| outer_rows.min(goal));
        let inner_rows = self.ctx.row_count(inner_table);
        let point_lookup_rows = self
            .ctx
            .estimate_point_lookup_rows(inner_table, &inner_index.name, inner_index.is_unique);

        if effective_outer_rows == 0 || inner_rows == 0 {
            return false;
        }

        if effective_outer_rows <= INDEX_JOIN_ALWAYS_OUTER_ROWS {
            return true;
        }

        if inner_index.is_unique {
            return effective_outer_rows <= INDEX_JOIN_MAX_UNIQUE_EFFECTIVE_OUTER_ROWS;
        }

        if point_lookup_rows > 0 {
            let hash_join_cost = inner_rows.saturating_add(effective_outer_rows);
            let index_join_cost = effective_outer_rows.saturating_mul(point_lookup_rows);
            if index_join_cost <= hash_join_cost {
                return true;
            }
        }

        if row_goal.is_some()
            && effective_outer_rows <= INDEX_JOIN_MAX_OUTER_ROWS
            && inner_rows <= INDEX_JOIN_ROW_GOAL_SMALL_INNER_ROWS
        {
            return true;
        }

        effective_outer_rows <= INDEX_JOIN_MAX_OUTER_ROWS
            && inner_rows >= effective_outer_rows.saturating_mul(INDEX_JOIN_MIN_INNER_OUTER_RATIO)
    }

    fn combine_row_goal(existing: Option<usize>, candidate: usize) -> Option<usize> {
        Some(existing.map_or(candidate, |current| current.min(candidate)))
    }

    fn estimate_rows(&self, plan: &PhysicalPlan) -> usize {
        match plan {
            PhysicalPlan::TableScan { table } => {
                let count = self.ctx.row_count(table);
                if count > 0 { count } else { 1000 }
            }
            PhysicalPlan::IndexGet { .. } => 1,
            PhysicalPlan::IndexInGet { keys, .. } => keys.len(),
            PhysicalPlan::IndexScan { table, .. } => {
                let row_count = self.ctx.row_count(table);
                if row_count == 0 {
                    100
                } else {
                    core::cmp::max(row_count / 10, 1)
                }
            }
            PhysicalPlan::GinIndexScan {
                table,
                index,
                key,
                value,
                ..
            } => {
                if let Some(value) = value {
                    self.ctx
                        .gin_key_value_cost(table, index, key, value)
                        .filter(|cost| *cost > 0)
                        .unwrap_or_else(|| {
                            let row_count = self.ctx.row_count(table);
                            if row_count == 0 {
                                100
                            } else {
                                core::cmp::max(row_count / 10, 1)
                            }
                        })
                } else {
                    self.ctx
                        .gin_key_cost(table, index, key)
                        .filter(|cost| *cost > 0)
                        .unwrap_or_else(|| {
                            let row_count = self.ctx.row_count(table);
                            if row_count == 0 {
                                100
                            } else {
                                core::cmp::max(row_count / 10, 1)
                            }
                        })
                }
            }
            PhysicalPlan::GinIndexScanMulti {
                table,
                index,
                pairs,
                match_all,
                ..
            } => {
                let costs: Vec<usize> = pairs
                    .iter()
                    .filter_map(|(key, value)| {
                        self.ctx.gin_key_value_cost(table, index, key, value)
                    })
                    .collect();
                let estimated = if *match_all {
                    costs.iter().copied().min()
                } else {
                    Some(costs.iter().copied().sum())
                };
                estimated
                    .filter(|cost| *cost > 0)
                    .unwrap_or_else(|| {
                        let row_count = self.ctx.row_count(table);
                        if row_count == 0 {
                            50
                        } else {
                            core::cmp::max(row_count / 20, 1)
                        }
                    })
            }
            PhysicalPlan::Filter { input, .. } => core::cmp::max(self.estimate_rows(input) / 10, 1),
            PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::NoOp { input } => self.estimate_rows(input),
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            }
            | PhysicalPlan::TopN {
                input,
                limit,
                offset,
                ..
            } => core::cmp::min(self.estimate_rows(input), limit.saturating_add(*offset)),
            PhysicalPlan::HashAggregate {
                input, group_by, ..
            } => {
                if group_by.is_empty() {
                    1
                } else {
                    core::cmp::max(self.estimate_rows(input) / 10, 1)
                }
            }
            PhysicalPlan::HashJoin {
                left,
                right,
                condition,
                join_type,
                ..
            }
            | PhysicalPlan::SortMergeJoin {
                left,
                right,
                condition,
                join_type,
                ..
            }
            | PhysicalPlan::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
                ..
            } => self.estimate_join_rows(left, right, condition, *join_type),
            PhysicalPlan::CrossProduct { left, right } => core::cmp::max(
                self.estimate_rows(left)
                    .saturating_mul(self.estimate_rows(right)),
                1,
            ),
            PhysicalPlan::Union { left, right, .. } => self
                .estimate_rows(left)
                .saturating_add(self.estimate_rows(right)),
            PhysicalPlan::IndexNestedLoopJoin { outer, .. } => self.estimate_rows(outer),
            PhysicalPlan::Empty => 0,
        }
    }

    fn estimate_join_rows(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        condition: &Expr,
        join_type: JoinType,
    ) -> usize {
        let left_rows = self.estimate_rows(left);
        let right_rows = self.estimate_rows(right);

        if let Some((left_col, right_col)) = self.join_columns_for_sides(left, right, condition) {
            let left_unique = self.relation_has_unique_lookup(left, left_col);
            let right_unique = self.relation_has_unique_lookup(right, right_col);

            match join_type {
                JoinType::LeftOuter if right_unique => return left_rows.max(1),
                JoinType::RightOuter if left_unique => return right_rows.max(1),
                JoinType::Inner => {
                    if right_unique {
                        return left_rows.max(1);
                    }
                    if left_unique {
                        return right_rows.max(1);
                    }
                }
                JoinType::FullOuter | JoinType::Cross => {}
                JoinType::LeftOuter => return left_rows.max(1),
                JoinType::RightOuter => return right_rows.max(1),
            }
        }

        match join_type {
            JoinType::LeftOuter => left_rows.max(1),
            JoinType::RightOuter => right_rows.max(1),
            JoinType::FullOuter => left_rows.saturating_add(right_rows).max(1),
            JoinType::Cross => left_rows.saturating_mul(right_rows).max(1),
            JoinType::Inner => core::cmp::max(left_rows.saturating_mul(right_rows) / 10, 1),
        }
    }

    fn join_columns_for_sides<'b>(
        &self,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        condition: &'b Expr,
    ) -> Option<(&'b ColumnRef, &'b ColumnRef)> {
        let (first, second) = self.extract_join_columns(condition)?;
        self.align_join_columns(left, right, first, second)
    }

    fn relation_has_unique_lookup(&self, plan: &PhysicalPlan, column: &ColumnRef) -> bool {
        let tables = plan.collect_tables();
        if tables.len() != 1 || !tables.iter().any(|table| table == &column.table) {
            return false;
        }

        self.ctx
            .find_index(&column.table, &[column.column.as_str()])
            .map(|index| index.is_unique && index.supports_point_lookup())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;
    use crate::context::{IndexInfo, TableStats};

    fn create_test_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();

        // Table 'a' with no index
        ctx.register_table(
            "a",
            TableStats {
                row_count: 100,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );

        // Table 'b' with index on 'a_id'
        ctx.register_table(
            "b",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_a_id",
                    alloc::vec!["a_id".into()],
                    false
                )],
            },
        );

        ctx
    }

    #[test]
    fn test_hash_join_to_index_join() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        // Create: HashJoin(a.id = b.a_id, Scan(a), Scan(b))
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::Inner,
        );

        let result = pass.optimize(plan);

        // Should become IndexNestedLoopJoin
        assert!(matches!(result, PhysicalPlan::IndexNestedLoopJoin { .. }));
        if let PhysicalPlan::IndexNestedLoopJoin {
            inner_table,
            inner_index,
            ..
        } = result
        {
            assert_eq!(inner_table, "b");
            assert_eq!(inner_index, "idx_a_id");
        }
    }

    #[test]
    fn test_index_join_tracks_logical_left_when_outer_flips() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "employees",
            TableStats {
                row_count: 1_000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_dept_id",
                    alloc::vec!["dept_id".into()],
                    false,
                )],
            },
        );
        ctx.register_table(
            "departments",
            TableStats {
                row_count: 8,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );

        let pass = IndexJoinPass::new(&ctx);
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("employees"),
            PhysicalPlan::table_scan("departments"),
            Expr::eq(
                Expr::column("employees", "dept_id", 1),
                Expr::column("departments", "id", 0),
            ),
            JoinType::Inner,
        );

        let result = pass.optimize(plan);

        if let PhysicalPlan::IndexNestedLoopJoin {
            inner_table,
            outer_is_left,
            ..
        } = result
        {
            assert_eq!(inner_table, "employees");
            assert!(!outer_is_left);
        } else {
            panic!("expected index nested loop join");
        }
    }

    #[test]
    fn test_no_index_remains_hash_join() {
        let ctx = ExecutionContext::new(); // Empty context, no indexes
        let pass = IndexJoinPass::new(&ctx);

        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::Inner,
        );

        let result = pass.optimize(plan);

        // Should remain as HashJoin
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_left_outer_join_can_use_index_join() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::LeftOuter,
        );

        let result = pass.optimize(plan);
        if let PhysicalPlan::IndexNestedLoopJoin {
            inner_table,
            outer_is_left,
            join_type,
            ..
        } = result
        {
            assert_eq!(inner_table, "b");
            assert!(outer_is_left);
            assert_eq!(join_type, JoinType::LeftOuter);
        } else {
            panic!("expected left outer index nested loop join");
        }
    }

    #[test]
    fn test_right_outer_join_can_use_index_join() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "a",
            TableStats {
                row_count: 100,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new("idx_id", alloc::vec!["id".into()], true,)],
            },
        );
        ctx.register_table(
            "b",
            TableStats {
                row_count: 2,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );
        let pass = IndexJoinPass::new(&ctx);

        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::RightOuter,
        );

        let result = pass.optimize(plan);
        if let PhysicalPlan::IndexNestedLoopJoin {
            inner_table,
            outer_is_left,
            join_type,
            ..
        } = result
        {
            assert_eq!(inner_table, "a");
            assert!(!outer_is_left);
            assert_eq!(join_type, JoinType::RightOuter);
        } else {
            panic!("expected right outer index nested loop join");
        }
    }

    #[test]
    fn test_full_outer_join_remains_hash_join() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::FullOuter,
        );

        let result = pass.optimize(plan);
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_multi_table_inner_join_can_still_use_index_join() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "issues",
            TableStats {
                row_count: 1_000,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );
        ctx.register_table(
            "projects",
            TableStats {
                row_count: 100,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "pk_projects_id",
                    alloc::vec!["id".into()],
                    true,
                )],
            },
        );
        ctx.register_table(
            "project_counters",
            TableStats {
                row_count: 100,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "pk_project_counters_project_id",
                    alloc::vec!["project_id".into()],
                    true,
                )],
            },
        );

        let pass = IndexJoinPass::new(&ctx);
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::hash_join(
                PhysicalPlan::table_scan("issues"),
                PhysicalPlan::table_scan("projects"),
                Expr::eq(
                    Expr::column("issues", "project_id", 1),
                    Expr::column("projects", "id", 0),
                ),
                JoinType::Inner,
            ),
            PhysicalPlan::table_scan("project_counters"),
            Expr::eq(
                Expr::column("projects", "id", 0),
                Expr::column("project_counters", "project_id", 0),
            ),
            JoinType::Inner,
        );

        let result = pass.optimize(plan);

        match result {
            PhysicalPlan::IndexNestedLoopJoin {
                inner_table,
                join_type,
                outer,
                ..
            } => {
                assert_eq!(inner_table, "project_counters");
                assert_eq!(join_type, JoinType::Inner);
                assert!(matches!(*outer, PhysicalPlan::IndexNestedLoopJoin { .. }));
            }
            other => panic!(
                "expected nested index joins for multi-table inner join, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_non_equi_join_not_optimized() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        // Range join should not be converted to index join
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::gt(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::Inner,
        );

        let result = pass.optimize(plan);

        // Should remain as HashJoin
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_nested_joins() {
        let ctx = create_test_context();
        let pass = IndexJoinPass::new(&ctx);

        // Create nested join: HashJoin(HashJoin(a, b), c)
        let inner_join = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("a"),
            PhysicalPlan::table_scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::Inner,
        );

        let outer_join = PhysicalPlan::hash_join(
            inner_join,
            PhysicalPlan::table_scan("c"),
            Expr::eq(Expr::column("b", "id", 0), Expr::column("c", "b_id", 0)),
            JoinType::Inner,
        );

        let result = pass.optimize(outer_join);

        // Inner join should be converted to index join
        // Outer join remains as hash join (no index on c.b_id)
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
        if let PhysicalPlan::HashJoin { left, .. } = result {
            assert!(matches!(*left, PhysicalPlan::IndexNestedLoopJoin { .. }));
        }
    }

    #[test]
    fn test_large_outer_prefers_hash_join() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "big_outer",
            TableStats {
                row_count: 20_000,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );
        ctx.register_table(
            "small_inner",
            TableStats {
                row_count: 1_000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_outer_id",
                    alloc::vec!["outer_id".into()],
                    false,
                )],
            },
        );

        let pass = IndexJoinPass::new(&ctx);
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("big_outer"),
            PhysicalPlan::table_scan("small_inner"),
            Expr::eq(
                Expr::column("big_outer", "id", 0),
                Expr::column("small_inner", "outer_id", 0),
            ),
            JoinType::Inner,
        );

        let result = pass.optimize(plan);
        assert!(matches!(result, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_limit_row_goal_enables_unique_index_join() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 100_000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new("idx_age", alloc::vec!["age".into()], false,)],
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

        let pass = IndexJoinPass::new(&ctx);
        let plan = PhysicalPlan::Limit {
            input: Box::new(PhysicalPlan::hash_join(
                PhysicalPlan::IndexScan {
                    table: "users".into(),
                    index: "idx_age".into(),
                    bounds: crate::planner::IndexBounds::all(),
                    limit: None,
                    offset: None,
                    reverse: false,
                },
                PhysicalPlan::table_scan("departments"),
                Expr::eq(
                    Expr::column("users", "dept_id", 2),
                    Expr::column("departments", "id", 0),
                ),
                JoinType::Inner,
            )),
            limit: 1_000,
            offset: 0,
        };

        let result = pass.optimize(plan);
        match result {
            PhysicalPlan::Limit { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::IndexNestedLoopJoin { .. }));
            }
            other => panic!(
                "expected limit over index nested loop join, got {:?}",
                other
            ),
        }
    }
}
