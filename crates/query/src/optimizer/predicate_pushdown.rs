//! Predicate pushdown optimization pass.
//!
//! Pushes filter predicates down the plan tree as close to the data source
//! as possible to reduce the amount of data processed.
//!
//! Key optimizations:
//! 1. Push filters through Sort (doesn't change semantics)
//! 2. Merge consecutive filters into AND predicates
//! 3. Push filters into Join when predicate references only one side
//! 4. Cannot push through Aggregate or Limit (changes semantics)

use crate::ast::{Expr, JoinType};
use crate::optimizer::OptimizerPass;
use crate::planner::LogicalPlan;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use hashbrown::HashSet;

/// Predicate pushdown optimization.
pub struct PredicatePushdown;

impl OptimizerPass for PredicatePushdown {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.pushdown(plan)
    }

    fn name(&self) -> &'static str {
        "predicate_pushdown"
    }
}

impl PredicatePushdown {
    fn pushdown(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let optimized_input = self.pushdown(*input);
                self.try_push_filter(optimized_input, predicate)
            }

            LogicalPlan::Project { input, columns } => LogicalPlan::Project {
                input: Box::new(self.pushdown(*input)),
                columns,
            },

            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => LogicalPlan::Join {
                left: Box::new(self.pushdown(*left)),
                right: Box::new(self.pushdown(*right)),
                condition,
                join_type,
                output_tables,
            },

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.pushdown(*input)),
                group_by,
                aggregates,
            },

            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.pushdown(*input)),
                order_by,
            },

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.pushdown(*input)),
                limit,
                offset,
            },

            LogicalPlan::CrossProduct { left, right } => LogicalPlan::CrossProduct {
                left: Box::new(self.pushdown(*left)),
                right: Box::new(self.pushdown(*right)),
            },

            LogicalPlan::Union { left, right, all } => LogicalPlan::Union {
                left: Box::new(self.pushdown(*left)),
                right: Box::new(self.pushdown(*right)),
                all,
            },

            // Leaf nodes - no transformation
            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. }
            | LogicalPlan::Empty => plan,
        }
    }

    fn try_push_filter(&self, input: LogicalPlan, predicate: Expr) -> LogicalPlan {
        match input {
            // Push filter below projection if predicate doesn't reference projected columns
            LogicalPlan::Project {
                input: proj_input,
                columns,
            } => {
                // For simplicity, we don't push through projection in this basic implementation
                // A full implementation would check if the predicate can be evaluated before projection
                LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::Project {
                        input: proj_input,
                        columns,
                    }),
                    predicate,
                }
            }

            // Push filter into join if predicate references only one side
            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => self.push_filter_into_join(
                *left,
                *right,
                condition,
                join_type,
                output_tables,
                predicate,
            ),

            // Can't push filter below aggregate
            LogicalPlan::Aggregate { .. } => LogicalPlan::Filter {
                input: Box::new(input),
                predicate,
            },

            // Push filter below sort
            LogicalPlan::Sort {
                input: sort_input,
                order_by,
            } => LogicalPlan::Sort {
                input: Box::new(self.try_push_filter(*sort_input, predicate)),
                order_by,
            },

            // Push filter below limit (careful - this changes semantics for LIMIT)
            // For correctness, we don't push below LIMIT
            LogicalPlan::Limit { .. } => LogicalPlan::Filter {
                input: Box::new(input),
                predicate,
            },

            // Filter on scan - keep as is
            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. } => LogicalPlan::Filter {
                input: Box::new(input),
                predicate,
            },

            // Merge consecutive filters
            LogicalPlan::Filter {
                input: inner_input,
                predicate: inner_pred,
            } => LogicalPlan::Filter {
                input: inner_input,
                predicate: Expr::and(inner_pred, predicate),
            },

            _ => LogicalPlan::Filter {
                input: Box::new(input),
                predicate,
            },
        }
    }

    /// Push filter into join based on which tables the predicate references.
    fn push_filter_into_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Expr,
        join_type: JoinType,
        output_tables: Vec<String>,
        predicate: Expr,
    ) -> LogicalPlan {
        let left_tables = self.extract_tables(&left);
        let right_tables = self.extract_tables(&right);
        let mut left_predicates = Vec::new();
        let mut right_predicates = Vec::new();
        let mut remaining_predicates = Vec::new();

        for predicate in Self::split_conjuncts(predicate) {
            let pred_tables = self.extract_predicate_tables(&predicate);
            let refs_left = pred_tables.iter().any(|t| left_tables.contains(t));
            let refs_right = pred_tables.iter().any(|t| right_tables.contains(t));

            match join_type {
                JoinType::Inner => {
                    if refs_left && !refs_right {
                        left_predicates.push(predicate);
                    } else if refs_right && !refs_left {
                        right_predicates.push(predicate);
                    } else {
                        remaining_predicates.push(predicate);
                    }
                }
                JoinType::LeftOuter => {
                    if refs_left && !refs_right {
                        left_predicates.push(predicate);
                    } else {
                        remaining_predicates.push(predicate);
                    }
                }
                JoinType::RightOuter => {
                    if refs_right && !refs_left {
                        right_predicates.push(predicate);
                    } else {
                        remaining_predicates.push(predicate);
                    }
                }
                JoinType::FullOuter | JoinType::Cross => {
                    remaining_predicates.push(predicate);
                }
            }
        }

        let left = if left_predicates.is_empty() {
            left
        } else {
            self.try_push_filter(left, Self::combine_predicates(left_predicates))
        };
        let right = if right_predicates.is_empty() {
            right
        } else {
            self.try_push_filter(right, Self::combine_predicates(right_predicates))
        };

        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            condition,
            join_type,
            output_tables,
        };

        if remaining_predicates.is_empty() {
            join
        } else {
            LogicalPlan::Filter {
                input: Box::new(join),
                predicate: Self::combine_predicates(remaining_predicates),
            }
        }
    }

    fn split_conjuncts(predicate: Expr) -> Vec<Expr> {
        let mut predicates = Vec::new();
        Self::split_conjuncts_into(predicate, &mut predicates);
        predicates
    }

    fn split_conjuncts_into(predicate: Expr, predicates: &mut Vec<Expr>) {
        match predicate {
            Expr::BinaryOp {
                left,
                op: crate::ast::BinaryOp::And,
                right,
            } => {
                Self::split_conjuncts_into(*left, predicates);
                Self::split_conjuncts_into(*right, predicates);
            }
            other => predicates.push(other),
        }
    }

    fn combine_predicates(predicates: Vec<Expr>) -> Expr {
        predicates
            .into_iter()
            .reduce(Expr::and)
            .expect("combine_predicates requires at least one predicate")
    }

    /// Extract all table names referenced by a plan.
    fn extract_tables(&self, plan: &LogicalPlan) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.collect_tables(plan, &mut tables);
        tables
    }

    fn collect_tables(&self, plan: &LogicalPlan, tables: &mut HashSet<String>) {
        match plan {
            LogicalPlan::Scan { table } => {
                tables.insert(table.clone());
            }
            LogicalPlan::IndexScan { table, .. }
            | LogicalPlan::IndexGet { table, .. }
            | LogicalPlan::IndexInGet { table, .. }
            | LogicalPlan::GinIndexScan { table, .. }
            | LogicalPlan::GinIndexScanMulti { table, .. } => {
                tables.insert(table.clone());
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => {
                self.collect_tables(input, tables);
            }
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::CrossProduct { left, right }
            | LogicalPlan::Union { left, right, .. } => {
                self.collect_tables(left, tables);
                self.collect_tables(right, tables);
            }
            LogicalPlan::Empty => {}
        }
    }

    /// Extract all table names referenced by a predicate expression.
    fn extract_predicate_tables(&self, expr: &Expr) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.collect_expr_tables(expr, &mut tables);
        tables
    }

    fn collect_expr_tables(&self, expr: &Expr, tables: &mut HashSet<String>) {
        match expr {
            Expr::Column(col) => {
                tables.insert(col.table.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_expr_tables(left, tables);
                self.collect_expr_tables(right, tables);
            }
            Expr::UnaryOp { expr, .. } => {
                self.collect_expr_tables(expr, tables);
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    self.collect_expr_tables(arg, tables);
                }
            }
            Expr::Aggregate { expr, .. } => {
                if let Some(e) = expr {
                    self.collect_expr_tables(e, tables);
                }
            }
            Expr::Between { expr, low, high } => {
                self.collect_expr_tables(expr, tables);
                self.collect_expr_tables(low, tables);
                self.collect_expr_tables(high, tables);
            }
            Expr::In { expr, list } => {
                self.collect_expr_tables(expr, tables);
                for item in list {
                    self.collect_expr_tables(item, tables);
                }
            }
            Expr::Like { expr, .. } => {
                self.collect_expr_tables(expr, tables);
            }
            Expr::NotBetween { expr, low, high } => {
                self.collect_expr_tables(expr, tables);
                self.collect_expr_tables(low, tables);
                self.collect_expr_tables(high, tables);
            }
            Expr::NotIn { expr, list } => {
                self.collect_expr_tables(expr, tables);
                for item in list {
                    self.collect_expr_tables(item, tables);
                }
            }
            Expr::NotLike { expr, .. } => {
                self.collect_expr_tables(expr, tables);
            }
            Expr::Match { expr, .. } => {
                self.collect_expr_tables(expr, tables);
            }
            Expr::NotMatch { expr, .. } => {
                self.collect_expr_tables(expr, tables);
            }
            Expr::Literal(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BinaryOp, SortOrder};
    use alloc::format;

    #[test]
    fn test_predicate_pushdown_basic() {
        let pass = PredicatePushdown;

        // Filter on scan should stay as is
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );

        let optimized = pass.optimize(plan);
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn test_predicate_pushdown_through_sort() {
        let pass = PredicatePushdown;

        // Filter above sort should be pushed below sort
        let plan = LogicalPlan::filter(
            LogicalPlan::sort(
                LogicalPlan::scan("users"),
                alloc::vec![(Expr::column("users", "name", 1), SortOrder::Asc)],
            ),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );

        let optimized = pass.optimize(plan);

        // Should be Sort(Filter(Scan))
        assert!(matches!(optimized, LogicalPlan::Sort { .. }));
    }

    #[test]
    fn test_merge_consecutive_filters() {
        let pass = PredicatePushdown;

        // Two consecutive filters should be merged
        let plan = LogicalPlan::filter(
            LogicalPlan::filter(
                LogicalPlan::scan("users"),
                Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
            ),
            Expr::eq(Expr::column("users", "active", 2), Expr::literal(true)),
        );

        let optimized = pass.optimize(plan);

        // Should be a single Filter with AND predicate
        if let LogicalPlan::Filter { predicate, .. } = optimized {
            assert!(matches!(
                predicate,
                Expr::BinaryOp {
                    op: BinaryOp::And,
                    ..
                }
            ));
        } else {
            panic!("Expected Filter");
        }
    }

    #[test]
    fn test_push_filter_into_inner_join_left() {
        let pass = PredicatePushdown;

        // Filter on left table should be pushed into left side of inner join
        let plan = LogicalPlan::filter(
            LogicalPlan::inner_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::eq(Expr::column("users", "active", 1), Expr::literal(true)),
        );

        let optimized = pass.optimize(plan);

        // Should be Join(Filter(Scan(users)), Scan(orders))
        if let LogicalPlan::Join { left, .. } = optimized {
            assert!(matches!(*left, LogicalPlan::Filter { .. }));
        } else {
            panic!("Expected Join, got {:?}", optimized);
        }
    }

    #[test]
    fn test_push_filter_into_inner_join_right() {
        let pass = PredicatePushdown;

        // Filter on right table should be pushed into right side of inner join
        let plan = LogicalPlan::filter(
            LogicalPlan::inner_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::gt(Expr::column("orders", "amount", 1), Expr::literal(100i64)),
        );

        let optimized = pass.optimize(plan);

        // Should be Join(Scan(users), Filter(Scan(orders)))
        if let LogicalPlan::Join { right, .. } = optimized {
            assert!(matches!(*right, LogicalPlan::Filter { .. }));
        } else {
            panic!("Expected Join, got {:?}", optimized);
        }
    }

    #[test]
    fn test_pushdown_splits_and_across_inner_join_sides() {
        let pass = PredicatePushdown;

        let plan = LogicalPlan::filter(
            LogicalPlan::inner_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::and(
                Expr::eq(Expr::column("users", "active", 1), Expr::literal(true)),
                Expr::gt(Expr::column("orders", "amount", 1), Expr::literal(100i64)),
            ),
        );

        let optimized = pass.optimize(plan);

        match optimized {
            LogicalPlan::Join { left, right, .. } => {
                assert!(matches!(*left, LogicalPlan::Filter { .. }));
                assert!(matches!(*right, LogicalPlan::Filter { .. }));
            }
            other => panic!("Expected Join with pushed predicates, got {:?}", other),
        }
    }

    #[test]
    fn test_filter_on_both_sides_stays_above_join() {
        let pass = PredicatePushdown;

        // Filter referencing both tables should stay above join
        let plan = LogicalPlan::filter(
            LogicalPlan::inner_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::gt(
                Expr::column("users", "balance", 2),
                Expr::column("orders", "amount", 1),
            ),
        );

        let optimized = pass.optimize(plan);

        // Should be Filter(Join(...))
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
        if let LogicalPlan::Filter { input, .. } = optimized {
            assert!(matches!(*input, LogicalPlan::Join { .. }));
        }
    }

    #[test]
    fn test_left_join_push_to_left_only() {
        let pass = PredicatePushdown;

        // For left outer join, can only push predicates on left side
        let plan = LogicalPlan::filter(
            LogicalPlan::left_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::eq(Expr::column("users", "active", 1), Expr::literal(true)),
        );

        let optimized = pass.optimize(plan);

        // Should push to left side
        if let LogicalPlan::Join {
            left, join_type, ..
        } = optimized
        {
            assert_eq!(join_type, JoinType::LeftOuter);
            assert!(matches!(*left, LogicalPlan::Filter { .. }));
        } else {
            panic!("Expected Join, got {:?}", optimized);
        }
    }

    #[test]
    fn test_left_join_right_predicate_stays_above() {
        let pass = PredicatePushdown;

        // For left outer join, predicates on right side must stay above
        let plan = LogicalPlan::filter(
            LogicalPlan::left_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::gt(Expr::column("orders", "amount", 1), Expr::literal(100i64)),
        );

        let optimized = pass.optimize(plan);

        // Should stay above join
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
        if let LogicalPlan::Filter { input, .. } = optimized {
            assert!(matches!(*input, LogicalPlan::Join { .. }));
        }
    }

    #[test]
    fn test_left_join_splits_and_pushes_left_only() {
        let pass = PredicatePushdown;

        let plan = LogicalPlan::filter(
            LogicalPlan::left_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
            ),
            Expr::and(
                Expr::eq(Expr::column("users", "active", 1), Expr::literal(true)),
                Expr::gt(Expr::column("orders", "amount", 1), Expr::literal(100i64)),
            ),
        );

        let optimized = pass.optimize(plan);

        match optimized {
            LogicalPlan::Filter { input, predicate } => {
                let predicate_debug = format!("{:?}", predicate).to_lowercase();
                assert!(predicate_debug.contains("orders"));
                match *input {
                    LogicalPlan::Join { left, right, .. } => {
                        assert!(matches!(*left, LogicalPlan::Filter { .. }));
                        assert!(matches!(*right, LogicalPlan::Scan { .. }));
                    }
                    other => panic!("Expected Join under Filter, got {:?}", other),
                }
            }
            other => panic!("Expected Filter over Join, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_tables() {
        let pass = PredicatePushdown;

        let plan = LogicalPlan::inner_join(
            LogicalPlan::scan("users"),
            LogicalPlan::filter(
                LogicalPlan::scan("orders"),
                Expr::gt(Expr::column("orders", "amount", 0), Expr::literal(0i64)),
            ),
            Expr::eq(
                Expr::column("users", "id", 0),
                Expr::column("orders", "user_id", 1),
            ),
        );

        let tables = pass.extract_tables(&plan);
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_extract_predicate_tables() {
        let pass = PredicatePushdown;

        let pred = Expr::and(
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
            Expr::gt(
                Expr::column("orders", "amount", 0),
                Expr::column("products", "price", 0),
            ),
        );

        let tables = pass.extract_predicate_tables(&pred);
        assert!(tables.contains("users"));
        assert!(tables.contains("orders"));
        assert!(tables.contains("products"));
        assert_eq!(tables.len(), 3);
    }
}
