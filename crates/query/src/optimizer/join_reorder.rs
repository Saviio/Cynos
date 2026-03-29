//! Join reorder optimization pass.
//!
//! Reorders joins to minimize intermediate result sizes using a greedy algorithm.
//! The algorithm prioritizes joining smaller tables first to reduce the size of
//! intermediate results.
//!
//! Example:
//! ```text
//! Join(Join(large_table, medium_table), small_table)
//!     =>
//! Join(Join(small_table, medium_table), large_table)
//! ```
//!
//! The greedy algorithm:
//! 1. Collect all tables involved in a chain of inner joins
//! 2. Sort tables by estimated cardinality (row count)
//! 3. Build a left-deep join tree starting with smallest tables
//!
//! Limitations:
//! - Only reorders inner joins (outer joins have semantic ordering)
//! - Uses simple row count estimation (no selectivity estimation)
//! - Produces left-deep trees (not bushy trees)

use crate::ast::{Expr, JoinType};
use crate::context::ExecutionContext;
use crate::optimizer::OptimizerPass;
use crate::planner::LogicalPlan;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

/// Join reorder optimization using greedy algorithm.
pub struct JoinReorder {
    /// Optional execution context for cardinality estimation.
    context: Option<ExecutionContext>,
}

impl Default for JoinReorder {
    fn default() -> Self {
        Self::new()
    }
}

impl JoinReorder {
    /// Creates a new JoinReorder pass without context.
    /// Will use default cardinality estimates.
    pub fn new() -> Self {
        Self { context: None }
    }

    /// Creates a new JoinReorder pass with execution context.
    pub fn with_context(context: ExecutionContext) -> Self {
        Self {
            context: Some(context),
        }
    }
}

impl OptimizerPass for JoinReorder {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.reorder(plan)
    }

    fn name(&self) -> &'static str {
        "join_reorder"
    }
}

/// Represents a table or subquery in the join graph.
#[derive(Clone, Debug)]
struct JoinNode {
    /// The logical plan for this node.
    plan: LogicalPlan,
    /// Estimated row count.
    cardinality: usize,
    /// Tables referenced by this node.
    tables: Vec<String>,
}

/// Represents a join condition between tables.
#[derive(Clone, Debug)]
struct JoinCondition {
    /// The join condition expression.
    condition: Expr,
    /// Tables referenced by the left side.
    left_tables: Vec<String>,
    /// Tables referenced by the right side.
    right_tables: Vec<String>,
}

impl JoinReorder {
    fn reorder(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => {
                let optimized_left = self.reorder(*left);
                let optimized_right = self.reorder(*right);
                let original_join = LogicalPlan::join_with_output_tables(
                    optimized_left,
                    optimized_right,
                    condition,
                    join_type,
                    output_tables,
                );

                if join_type != JoinType::Inner {
                    return original_join;
                }

                let original_output_tables = original_join.output_tables();
                let mut nodes = Vec::new();
                let mut conditions = Vec::new();
                self.collect_join_nodes(&original_join, &mut nodes, &mut conditions);

                if nodes.len() <= 2 {
                    if nodes.len() == 2 && !conditions.is_empty() {
                        let (left_node, right_node) = self.order_two_nodes(nodes);
                        return LogicalPlan::join_with_output_tables(
                            left_node.plan,
                            right_node.plan,
                            conditions.into_iter().next().unwrap().condition,
                            JoinType::Inner,
                            original_output_tables,
                        );
                    }
                    if let Some(node) = nodes.into_iter().next() {
                        return node.plan;
                    }
                    return LogicalPlan::Empty;
                }

                self.greedy_reorder(nodes, conditions, &original_output_tables)
            }

            LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(self.reorder(*input)),
                predicate,
            },

            LogicalPlan::Project { input, columns } => LogicalPlan::Project {
                input: Box::new(self.reorder(*input)),
                columns,
            },

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.reorder(*input)),
                group_by,
                aggregates,
            },

            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.reorder(*input)),
                order_by,
            },

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.reorder(*input)),
                limit,
                offset,
            },

            LogicalPlan::CrossProduct { left, right } => LogicalPlan::CrossProduct {
                left: Box::new(self.reorder(*left)),
                right: Box::new(self.reorder(*right)),
            },

            LogicalPlan::Union { left, right, all } => LogicalPlan::Union {
                left: Box::new(self.reorder(*left)),
                right: Box::new(self.reorder(*right)),
                all,
            },

            // Leaf nodes
            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. }
            | LogicalPlan::Empty => plan,
        }
    }

    /// Collect all nodes and conditions from a chain of inner joins.
    fn collect_join_nodes(
        &self,
        plan: &LogicalPlan,
        nodes: &mut Vec<JoinNode>,
        conditions: &mut Vec<JoinCondition>,
    ) {
        match plan {
            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type: JoinType::Inner,
                ..
            } => {
                // Recursively collect from left and right
                self.collect_join_nodes(left, nodes, conditions);
                self.collect_join_nodes(right, nodes, conditions);

                // Add the join condition
                let (left_tables, right_tables) = self.extract_condition_tables(condition);
                conditions.push(JoinCondition {
                    condition: condition.clone(),
                    left_tables,
                    right_tables,
                });
            }

            // Base case: a non-join node (table scan, filter, etc.)
            _ => {
                let tables = self.extract_plan_tables(plan);
                let cardinality = self.estimate_cardinality(plan, &tables);
                nodes.push(JoinNode {
                    plan: plan.clone(),
                    cardinality,
                    tables,
                });
            }
        }
    }

    /// Order two nodes by cardinality (smaller first).
    fn order_two_nodes(&self, mut nodes: Vec<JoinNode>) -> (JoinNode, JoinNode) {
        if nodes.len() != 2 {
            panic!("Expected exactly 2 nodes");
        }
        let second = nodes.pop().unwrap();
        let first = nodes.pop().unwrap();

        if first.cardinality <= second.cardinality {
            (first, second)
        } else {
            (second, first)
        }
    }

    /// Apply greedy join reordering.
    /// Strategy: Always join the two smallest relations that have a join condition.
    fn greedy_reorder(
        &self,
        mut nodes: Vec<JoinNode>,
        conditions: Vec<JoinCondition>,
        original_output_tables: &[String],
    ) -> LogicalPlan {
        if nodes.is_empty() {
            return LogicalPlan::Empty;
        }

        if nodes.len() == 1 {
            return nodes.pop().unwrap().plan;
        }

        // Sort nodes by cardinality (smallest first)
        nodes.sort_by_key(|n| n.cardinality);

        // Build left-deep tree greedily. Under restricted-relation planning, keep the
        // anchor table as the left-deep driver whenever it is present in an inner-join island.
        let start_idx = self.anchor_start_index(&nodes).unwrap_or(0);
        let mut result_node = nodes.remove(start_idx);
        let mut used_conditions: Vec<bool> = alloc::vec![false; conditions.len()];

        while !nodes.is_empty() {
            // Find the best node to join next (must have a join condition)
            let (best_idx, best_condition_idx) =
                self.find_best_join(&result_node, &nodes, &conditions, &used_conditions);

            // If no join condition found, try to find any node with a valid condition
            if best_condition_idx.is_none() {
                // Find any node that has a condition with result_node
                let mut found_idx = None;
                let mut found_cond_idx = None;

                for (i, node) in nodes.iter().enumerate() {
                    for (j, cond) in conditions.iter().enumerate() {
                        if used_conditions[j] {
                            continue;
                        }
                        // Check if this condition connects result_node and this node
                        let result_has_left = cond
                            .left_tables
                            .iter()
                            .any(|t| result_node.tables.contains(t));
                        let result_has_right = cond
                            .right_tables
                            .iter()
                            .any(|t| result_node.tables.contains(t));
                        let node_has_left =
                            cond.left_tables.iter().any(|t| node.tables.contains(t));
                        let node_has_right =
                            cond.right_tables.iter().any(|t| node.tables.contains(t));

                        if (result_has_left && node_has_right)
                            || (result_has_right && node_has_left)
                        {
                            found_idx = Some(i);
                            found_cond_idx = Some(j);
                            break;
                        }
                    }
                    if found_idx.is_some() {
                        break;
                    }
                }

                if let (Some(idx), Some(cond_idx)) = (found_idx, found_cond_idx) {
                    let next_node = nodes.remove(idx);
                    used_conditions[cond_idx] = true;
                    let mut new_tables = result_node.tables;
                    new_tables.extend(next_node.tables);
                    let output_tables =
                        Self::filter_output_tables(original_output_tables, &new_tables);

                    let new_plan = LogicalPlan::join_with_output_tables(
                        result_node.plan,
                        next_node.plan,
                        conditions[cond_idx].condition.clone(),
                        JoinType::Inner,
                        output_tables.clone(),
                    );

                    result_node = JoinNode {
                        plan: new_plan,
                        cardinality: self.estimate_join_cardinality(
                            result_node.cardinality,
                            next_node.cardinality,
                        ),
                        tables: new_tables,
                    };
                    continue;
                }

                // No valid join condition found, use cross product as last resort
                let next_node = nodes.remove(0);
                let mut new_tables = result_node.tables;
                new_tables.extend(next_node.tables);
                let output_tables = Self::filter_output_tables(original_output_tables, &new_tables);
                let new_plan = LogicalPlan::join_with_output_tables(
                    result_node.plan,
                    next_node.plan,
                    Expr::literal(true),
                    JoinType::Inner,
                    output_tables.clone(),
                );

                result_node = JoinNode {
                    plan: new_plan,
                    cardinality: self
                        .estimate_join_cardinality(result_node.cardinality, next_node.cardinality),
                    tables: new_tables,
                };
                continue;
            }

            let next_node = nodes.remove(best_idx);

            // Get the join condition
            let condition = if let Some(cond_idx) = best_condition_idx {
                used_conditions[cond_idx] = true;
                conditions[cond_idx].condition.clone()
            } else {
                // No specific condition found, use a cross product condition
                // This shouldn't happen in well-formed queries
                Expr::literal(true)
            };

            // Create the join
            // Update result node
            let mut new_tables = result_node.tables;
            new_tables.extend(next_node.tables);
            let output_tables = Self::filter_output_tables(original_output_tables, &new_tables);
            let new_plan = LogicalPlan::join_with_output_tables(
                result_node.plan,
                next_node.plan,
                condition,
                JoinType::Inner,
                output_tables.clone(),
            );

            result_node = JoinNode {
                plan: new_plan,
                // Estimate new cardinality (simplified: product with selectivity factor)
                cardinality: self
                    .estimate_join_cardinality(result_node.cardinality, next_node.cardinality),
                tables: new_tables,
            };
        }

        // After building the join tree, check if there are any unused conditions
        // that should be applied as filters
        let mut final_plan = result_node.plan;
        for (i, cond) in conditions.iter().enumerate() {
            if !used_conditions[i] {
                // This condition wasn't used in any join, apply it as a filter
                final_plan = LogicalPlan::Filter {
                    input: Box::new(final_plan),
                    predicate: cond.condition.clone(),
                };
            }
        }

        final_plan
    }

    /// Find the best node to join next based on cardinality and available conditions.
    fn find_best_join(
        &self,
        current: &JoinNode,
        candidates: &[JoinNode],
        conditions: &[JoinCondition],
        used_conditions: &[bool],
    ) -> (usize, Option<usize>) {
        let mut best_idx = 0;
        let mut best_condition_idx = None;
        let mut best_score = usize::MAX;

        for (i, candidate) in candidates.iter().enumerate() {
            // Check if there's a join condition between current and candidate
            let condition_idx = self.find_applicable_condition(
                &current.tables,
                &candidate.tables,
                conditions,
                used_conditions,
            );

            // Score based on cardinality (prefer smaller)
            let score = candidate.cardinality;

            // Prefer candidates with applicable conditions
            let adjusted_score = if condition_idx.is_some() {
                score
            } else {
                score.saturating_mul(10) // Penalize cross products
            };

            if adjusted_score < best_score {
                best_score = adjusted_score;
                best_idx = i;
                best_condition_idx = condition_idx;
            }
        }

        (best_idx, best_condition_idx)
    }

    fn anchor_start_index(&self, nodes: &[JoinNode]) -> Option<usize> {
        let anchor_table = self
            .context
            .as_ref()?
            .planning_intent()
            .anchor_table
            .as_ref()?;

        nodes.iter()
            .enumerate()
            .filter(|(_, node)| node.tables.iter().any(|table| table == anchor_table))
            .min_by_key(|(_, node)| node.cardinality)
            .map(|(idx, _)| idx)
    }

    /// Find a join condition that applies between two sets of tables.
    fn find_applicable_condition(
        &self,
        left_tables: &[String],
        right_tables: &[String],
        conditions: &[JoinCondition],
        used_conditions: &[bool],
    ) -> Option<usize> {
        for (i, cond) in conditions.iter().enumerate() {
            if used_conditions[i] {
                continue;
            }

            // Check if condition connects left and right tables
            let left_matches = cond
                .left_tables
                .iter()
                .any(|t| left_tables.contains(t) || right_tables.contains(t));
            let right_matches = cond
                .right_tables
                .iter()
                .any(|t| left_tables.contains(t) || right_tables.contains(t));

            if left_matches && right_matches {
                return Some(i);
            }
        }
        None
    }

    /// Extract tables referenced by a join condition.
    fn extract_condition_tables(&self, condition: &Expr) -> (Vec<String>, Vec<String>) {
        match condition {
            Expr::BinaryOp { left, right, .. } => {
                let left_tables = self.extract_expr_tables(left);
                let right_tables = self.extract_expr_tables(right);
                (left_tables, right_tables)
            }
            _ => (Vec::new(), Vec::new()),
        }
    }

    /// Extract tables referenced by an expression.
    fn extract_expr_tables(&self, expr: &Expr) -> Vec<String> {
        let mut tables = Vec::new();
        self.collect_expr_tables(expr, &mut tables);
        tables
    }

    fn collect_expr_tables(&self, expr: &Expr, tables: &mut Vec<String>) {
        match expr {
            Expr::Column(col) => {
                if !tables.contains(&col.table) {
                    tables.push(col.table.clone());
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_expr_tables(left, tables);
                self.collect_expr_tables(right, tables);
            }
            Expr::UnaryOp { expr, .. } => {
                self.collect_expr_tables(expr, tables);
            }
            _ => {}
        }
    }

    /// Extract tables referenced by a plan.
    fn extract_plan_tables(&self, plan: &LogicalPlan) -> Vec<String> {
        let mut tables = Vec::new();
        self.collect_plan_tables(plan, &mut tables);
        tables
    }

    fn collect_plan_tables(&self, plan: &LogicalPlan, tables: &mut Vec<String>) {
        match plan {
            LogicalPlan::Scan { table } => {
                tables.push(table.clone());
            }
            LogicalPlan::IndexScan { table, .. }
            | LogicalPlan::IndexGet { table, .. }
            | LogicalPlan::IndexInGet { table, .. }
            | LogicalPlan::GinIndexScan { table, .. }
            | LogicalPlan::GinIndexScanMulti { table, .. } => {
                tables.push(table.clone());
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => {
                self.collect_plan_tables(input, tables);
            }
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::CrossProduct { left, right }
            | LogicalPlan::Union { left, right, .. } => {
                self.collect_plan_tables(left, tables);
                self.collect_plan_tables(right, tables);
            }
            LogicalPlan::Empty => {}
        }
    }

    /// Estimate cardinality for a plan.
    fn estimate_cardinality(&self, plan: &LogicalPlan, tables: &[String]) -> usize {
        // Try to get from context
        if let Some(ctx) = &self.context {
            if tables.len() == 1 {
                let count = ctx.row_count(&tables[0]);
                if count > 0 {
                    return count;
                }
            }
        }

        // Default estimates based on plan type
        match plan {
            LogicalPlan::Scan { .. } => 1000,  // Default table size
            LogicalPlan::IndexGet { .. } => 1, // Point lookup
            LogicalPlan::IndexInGet { keys, .. } => keys.len(), // Multi-point lookup
            LogicalPlan::IndexScan { .. } => 100, // Range scan
            LogicalPlan::Filter { input, .. } => {
                // Assume 10% selectivity
                self.estimate_cardinality(input, tables) / 10
            }
            LogicalPlan::Limit { limit, .. } => *limit,
            _ => 1000,
        }
    }

    /// Estimate cardinality after a join.
    fn estimate_join_cardinality(&self, left_card: usize, right_card: usize) -> usize {
        // Simple estimate: assume 10% selectivity for equi-joins
        // This is a rough heuristic; real systems use histograms
        let product = left_card.saturating_mul(right_card);
        core::cmp::max(product / 10, 1)
    }

    fn filter_output_tables(
        original_output_tables: &[String],
        included_tables: &[String],
    ) -> Vec<String> {
        let mut filtered = Vec::new();
        for table in original_output_tables {
            if included_tables.contains(table) {
                filtered.push(table.clone());
            }
        }
        filtered
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::TableStats;

    fn create_test_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();

        ctx.register_table(
            "small",
            TableStats {
                row_count: 100,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );

        ctx.register_table(
            "medium",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );

        ctx.register_table(
            "large",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![],
            },
        );

        ctx
    }

    #[test]
    fn test_join_reorder_basic() {
        let pass = JoinReorder::new();

        let plan = LogicalPlan::join(
            LogicalPlan::scan("a"),
            LogicalPlan::scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::Inner,
        );

        let optimized = pass.optimize(plan);
        assert!(matches!(optimized, LogicalPlan::Join { .. }));
    }

    #[test]
    fn test_join_reorder_with_context() {
        let ctx = create_test_context();
        let pass = JoinReorder::with_context(ctx);

        // Create: large JOIN medium JOIN small
        // Should reorder to: small JOIN medium JOIN large
        let plan = LogicalPlan::join(
            LogicalPlan::join(
                LogicalPlan::scan("large"),
                LogicalPlan::scan("medium"),
                Expr::eq(
                    Expr::column("large", "id", 0),
                    Expr::column("medium", "large_id", 0),
                ),
                JoinType::Inner,
            ),
            LogicalPlan::scan("small"),
            Expr::eq(
                Expr::column("medium", "id", 0),
                Expr::column("small", "medium_id", 0),
            ),
            JoinType::Inner,
        );

        let optimized = pass.optimize(plan);

        // Verify it's still a valid join tree
        assert!(matches!(optimized, LogicalPlan::Join { .. }));

        // The smallest table should be involved early in the tree
        // (exact structure depends on implementation details)
    }

    #[test]
    fn test_outer_join_not_reordered() {
        let pass = JoinReorder::new();

        // Left outer join should not be reordered
        let plan = LogicalPlan::join(
            LogicalPlan::scan("a"),
            LogicalPlan::scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
            JoinType::LeftOuter,
        );

        let optimized = pass.optimize(plan);

        if let LogicalPlan::Join { join_type, .. } = optimized {
            assert_eq!(join_type, JoinType::LeftOuter);
        } else {
            panic!("Expected Join");
        }
    }

    #[test]
    fn test_nested_inner_joins() {
        let ctx = create_test_context();
        let pass = JoinReorder::with_context(ctx);

        // (a JOIN b) JOIN c
        let plan = LogicalPlan::join(
            LogicalPlan::join(
                LogicalPlan::scan("large"),
                LogicalPlan::scan("small"),
                Expr::eq(
                    Expr::column("large", "id", 0),
                    Expr::column("small", "large_id", 0),
                ),
                JoinType::Inner,
            ),
            LogicalPlan::scan("medium"),
            Expr::eq(
                Expr::column("small", "id", 0),
                Expr::column("medium", "small_id", 0),
            ),
            JoinType::Inner,
        );

        let optimized = pass.optimize(plan);
        assert!(matches!(optimized, LogicalPlan::Join { .. }));
    }

    #[test]
    fn test_single_table_unchanged() {
        let pass = JoinReorder::new();

        let plan = LogicalPlan::scan("users");
        let optimized = pass.optimize(plan.clone());

        assert!(matches!(optimized, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn test_filter_preserved() {
        let pass = JoinReorder::new();

        let plan = LogicalPlan::filter(
            LogicalPlan::Join {
                left: Box::new(LogicalPlan::scan("a")),
                right: Box::new(LogicalPlan::scan("b")),
                condition: Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
                join_type: JoinType::Inner,
                output_tables: alloc::vec!["a".into(), "b".into()],
            },
            Expr::gt(Expr::column("a", "value", 1), Expr::literal(100i64)),
        );

        let optimized = pass.optimize(plan);

        // Filter should be preserved
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn test_extract_condition_tables() {
        let pass = JoinReorder::new();

        let condition = Expr::eq(
            Expr::column("users", "id", 0),
            Expr::column("orders", "user_id", 0),
        );

        let (left, right) = pass.extract_condition_tables(&condition);
        assert!(left.contains(&"users".into()));
        assert!(right.contains(&"orders".into()));
    }

    #[test]
    fn test_estimate_cardinality() {
        let ctx = create_test_context();
        let pass = JoinReorder::with_context(ctx);

        let plan = LogicalPlan::scan("small");
        let tables = pass.extract_plan_tables(&plan);
        let card = pass.estimate_cardinality(&plan, &tables);

        assert_eq!(card, 100);
    }
}
