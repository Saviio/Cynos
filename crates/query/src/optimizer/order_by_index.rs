//! Order-by optimization using index order and lightweight physical properties.

use crate::ast::{Expr, SortOrder};
use crate::context::ExecutionContext;
use crate::planner::{IndexBounds, PhysicalPlan, PhysicalProperties};
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

/// Pass that leverages indexes to avoid explicit sorting.
pub struct OrderByIndexPass<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> OrderByIndexPass<'a> {
    /// Creates a new OrderByIndexPass with the given execution context.
    pub fn new(ctx: &'a ExecutionContext) -> Self {
        Self { ctx }
    }

    /// Optimizes the physical plan by leveraging indexes for sorting.
    pub fn optimize(&self, plan: PhysicalPlan) -> PhysicalPlan {
        self.traverse(plan)
    }

    fn traverse(&self, plan: PhysicalPlan) -> PhysicalPlan {
        match plan {
            PhysicalPlan::Sort { input, order_by } => {
                let optimized_input = self.traverse(*input);

                if self.input_satisfies_order(&optimized_input, &order_by) {
                    return optimized_input;
                }

                if let Some(rewritten_input) =
                    self.rewrite_for_order(optimized_input.clone(), &order_by, None)
                {
                    if self.input_satisfies_order(&rewritten_input, &order_by) {
                        return rewritten_input;
                    }
                }

                PhysicalPlan::Sort {
                    input: Box::new(optimized_input),
                    order_by,
                }
            }

            PhysicalPlan::TopN {
                input,
                order_by,
                limit,
                offset,
            } => {
                let optimized_input = self.traverse(*input);

                if self.input_satisfies_order(&optimized_input, &order_by) {
                    return PhysicalPlan::Limit {
                        input: Box::new(optimized_input),
                        limit,
                        offset,
                    };
                }

                if let Some(rewritten_input) = self.rewrite_for_order(
                    optimized_input.clone(),
                    &order_by,
                    Some((limit, offset)),
                ) {
                    if self.input_satisfies_order(&rewritten_input, &order_by) {
                        return if self.can_push_limit_into_scan(&rewritten_input) {
                            rewritten_input
                        } else {
                            PhysicalPlan::Limit {
                                input: Box::new(rewritten_input),
                                limit,
                                offset,
                            }
                        };
                    }
                }

                PhysicalPlan::TopN {
                    input: Box::new(optimized_input),
                    order_by,
                    limit,
                    offset,
                }
            }

            PhysicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
                input: Box::new(self.traverse(*input)),
                predicate,
            },
            PhysicalPlan::Project { input, columns } => PhysicalPlan::Project {
                input: Box::new(self.traverse(*input)),
                columns,
            },
            PhysicalPlan::HashJoin {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => PhysicalPlan::HashJoin {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
                condition,
                join_type,
                output_tables,
            },
            PhysicalPlan::SortMergeJoin {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => PhysicalPlan::SortMergeJoin {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
                condition,
                join_type,
                output_tables,
            },
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => PhysicalPlan::NestedLoopJoin {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
                condition,
                join_type,
                output_tables,
            },
            PhysicalPlan::IndexNestedLoopJoin {
                outer,
                inner_table,
                inner_index,
                condition,
                join_type,
                outer_is_left,
                output_tables,
            } => PhysicalPlan::IndexNestedLoopJoin {
                outer: Box::new(self.traverse(*outer)),
                inner_table,
                inner_index,
                condition,
                join_type,
                outer_is_left,
                output_tables,
            },
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => PhysicalPlan::HashAggregate {
                input: Box::new(self.traverse(*input)),
                group_by,
                aggregates,
            },
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => PhysicalPlan::Limit {
                input: Box::new(self.traverse(*input)),
                limit,
                offset,
            },
            PhysicalPlan::CrossProduct { left, right } => PhysicalPlan::CrossProduct {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
            },
            PhysicalPlan::Union { left, right, all } => PhysicalPlan::Union {
                left: Box::new(self.traverse(*left)),
                right: Box::new(self.traverse(*right)),
                all,
            },
            PhysicalPlan::NoOp { input } => PhysicalPlan::NoOp {
                input: Box::new(self.traverse(*input)),
            },
            plan @ (PhysicalPlan::TableScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::IndexGet { .. }
            | PhysicalPlan::IndexInGet { .. }
            | PhysicalPlan::GinIndexScan { .. }
            | PhysicalPlan::GinIndexScanMulti { .. }
            | PhysicalPlan::Empty) => plan,
        }
    }

    fn input_satisfies_order(&self, plan: &PhysicalPlan, order_by: &[(Expr, SortOrder)]) -> bool {
        PhysicalProperties::derive(plan, self.ctx)
            .ordering
            .map(|ordering| ordering.satisfies(order_by))
            .unwrap_or(false)
    }

    fn rewrite_for_order(
        &self,
        plan: PhysicalPlan,
        order_by: &[(Expr, SortOrder)],
        scan_limit: Option<(usize, usize)>,
    ) -> Option<PhysicalPlan> {
        match plan {
            PhysicalPlan::TableScan { table } => self.build_order_scan(table, order_by, scan_limit),
            PhysicalPlan::IndexScan {
                table,
                index,
                bounds,
                limit: existing_limit,
                offset: existing_offset,
                ..
            } => {
                let index_info = self.ctx.find_index_by_name(&table, &index)?;
                if !index_info.supports_ordering() {
                    return None;
                }
                let reverse = self.check_order_match(order_by, &index_info.columns)?;
                let (limit, offset) = match scan_limit {
                    Some((limit, offset)) => {
                        if existing_limit.is_some() || existing_offset.unwrap_or(0) > 0 {
                            return None;
                        }
                        (Some(limit), Some(offset))
                    }
                    None => (existing_limit, existing_offset),
                };
                Some(PhysicalPlan::IndexScan {
                    table,
                    index,
                    bounds,
                    limit,
                    offset,
                    reverse,
                })
            }
            PhysicalPlan::GinIndexScan { table, recheck, .. } => {
                scan_limit?;
                let recheck = recheck?;
                let input = self.build_order_scan(table, order_by, None)?;
                Some(PhysicalPlan::Filter {
                    input: Box::new(input),
                    predicate: recheck,
                })
            }
            PhysicalPlan::GinIndexScanMulti { table, recheck, .. } => {
                scan_limit?;
                let recheck = recheck?;
                let input = self.build_order_scan(table, order_by, None)?;
                Some(PhysicalPlan::Filter {
                    input: Box::new(input),
                    predicate: recheck,
                })
            }
            PhysicalPlan::Project { input, columns } => {
                let rewritten_input = self.rewrite_for_order(*input, order_by, scan_limit)?;
                Some(PhysicalPlan::Project {
                    input: Box::new(rewritten_input),
                    columns,
                })
            }
            PhysicalPlan::Filter { input, predicate } => {
                let original_input = *input;
                if let Some(rewritten_input) =
                    self.rewrite_for_order(original_input.clone(), order_by, None)
                {
                    return Some(PhysicalPlan::Filter {
                        input: Box::new(rewritten_input),
                        predicate,
                    });
                }

                if scan_limit.is_none() {
                    return None;
                }

                match original_input {
                    PhysicalPlan::GinIndexScan { table, recheck, .. }
                    | PhysicalPlan::GinIndexScanMulti { table, recheck, .. } => {
                        let input = self.build_order_scan(table, order_by, None)?;
                        Some(PhysicalPlan::Filter {
                            input: Box::new(input),
                            predicate: match recheck {
                                Some(source_recheck) => Expr::and(predicate, source_recheck),
                                None => predicate,
                            },
                        })
                    }
                    _ => return None,
                }
            }
            PhysicalPlan::NoOp { input } => {
                let rewritten_input = self.rewrite_for_order(*input, order_by, scan_limit)?;
                Some(PhysicalPlan::NoOp {
                    input: Box::new(rewritten_input),
                })
            }
            _ => None,
        }
    }

    fn build_order_scan(
        &self,
        table: String,
        order_by: &[(Expr, SortOrder)],
        scan_limit: Option<(usize, usize)>,
    ) -> Option<PhysicalPlan> {
        let order_columns = self.extract_order_columns(order_by)?;
        let column_refs: Vec<&str> = order_columns.iter().map(|column| column.as_str()).collect();
        let index = self.ctx.find_index_prefix(&table, &column_refs)?;
        let reverse = self.check_order_match(order_by, &index.columns)?;
        let (limit, offset) = scan_limit
            .map(|(limit, offset)| (Some(limit), Some(offset)))
            .unwrap_or((None, None));

        Some(PhysicalPlan::IndexScan {
            table,
            index: index.name.clone(),
            bounds: IndexBounds::all(),
            limit,
            offset,
            reverse,
        })
    }

    fn can_push_limit_into_scan(&self, plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::IndexScan { .. } => true,
            PhysicalPlan::Project { input, .. } | PhysicalPlan::NoOp { input } => {
                self.can_push_limit_into_scan(input)
            }
            _ => false,
        }
    }

    fn extract_order_columns(&self, order_by: &[(Expr, SortOrder)]) -> Option<Vec<String>> {
        let mut columns = Vec::with_capacity(order_by.len());
        for (expr, _) in order_by {
            let Expr::Column(col) = expr else {
                return None;
            };
            columns.push(col.column.clone());
        }
        Some(columns)
    }

    fn check_order_match(
        &self,
        order_by: &[(Expr, SortOrder)],
        index_columns: &[String],
    ) -> Option<bool> {
        if order_by.len() > index_columns.len() {
            return None;
        }

        for (i, (expr, _)) in order_by.iter().enumerate() {
            let Expr::Column(col) = expr else {
                return None;
            };
            if col.column != index_columns[i] {
                return None;
            }
        }

        let first_order = order_by.first().map(|(_, order)| *order)?;
        if order_by.iter().all(|(_, order)| *order == first_order) {
            Some(first_order == SortOrder::Desc)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::JoinType;
    use crate::context::{IndexInfo, TableStats};

    fn create_test_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                    IndexInfo::new("idx_score", alloc::vec!["score".into()], false),
                ],
            },
        );
        ctx.register_table(
            "documents",
            TableStats {
                row_count: 100_000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_updated_at", alloc::vec!["updated_at".into()], false),
                    IndexInfo::new_gin("idx_metadata", alloc::vec!["metadata".into()],),
                ],
            },
        );
        ctx
    }

    #[test]
    fn test_sort_to_index_scan() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::table_scan("users")),
            order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
        };

        let result = pass.optimize(plan);
        match result {
            PhysicalPlan::IndexScan { table, index, .. } => {
                assert_eq!(table, "users");
                assert_eq!(index, "idx_id");
            }
            other => panic!("expected index scan, got {:?}", other),
        }
    }

    #[test]
    fn test_sort_over_filter_index_scan_is_removed_via_properties() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::IndexScan {
                    table: "users".into(),
                    index: "idx_score".into(),
                    bounds: IndexBounds::all(),
                    limit: None,
                    offset: None,
                    reverse: false,
                }),
                predicate: Expr::eq(Expr::column("users", "id", 0), Expr::literal(10i64)),
            }),
            order_by: alloc::vec![(Expr::column("users", "score", 1), SortOrder::Asc)],
        };

        let result = pass.optimize(plan);
        match result {
            PhysicalPlan::Filter { input, .. } => {
                assert!(matches!(
                    *input,
                    PhysicalPlan::IndexScan { reverse: false, .. }
                ));
            }
            other => panic!("expected filter over index scan, got {:?}", other),
        }
    }

    #[test]
    fn test_sort_desc_over_filter_index_scan_rewrites_reverse() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::IndexScan {
                    table: "users".into(),
                    index: "idx_score".into(),
                    bounds: IndexBounds::all(),
                    limit: None,
                    offset: None,
                    reverse: false,
                }),
                predicate: Expr::gt(Expr::column("users", "id", 0), Expr::literal(5i64)),
            }),
            order_by: alloc::vec![(Expr::column("users", "score", 1), SortOrder::Desc)],
        };

        let result = pass.optimize(plan);
        match result {
            PhysicalPlan::Filter { input, .. } => match *input {
                PhysicalPlan::IndexScan { reverse, .. } => assert!(reverse),
                other => panic!("expected index scan, got {:?}", other),
            },
            other => panic!("expected filter, got {:?}", other),
        }
    }

    #[test]
    fn test_topn_over_filter_index_scan_becomes_limit() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);
        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::IndexScan {
                    table: "users".into(),
                    index: "idx_score".into(),
                    bounds: IndexBounds::all(),
                    limit: None,
                    offset: None,
                    reverse: false,
                }),
                predicate: Expr::eq(Expr::column("users", "id", 0), Expr::literal(9i64)),
            }),
            order_by: alloc::vec![(Expr::column("users", "score", 1), SortOrder::Asc)],
            limit: 10,
            offset: 3,
        };

        let result = pass.optimize(plan);
        match result {
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                assert_eq!(limit, 10);
                assert_eq!(offset, 3);
                assert!(matches!(*input, PhysicalPlan::Filter { .. }));
            }
            other => panic!("expected limit, got {:?}", other),
        }
    }

    #[test]
    fn test_topn_table_scan_pushes_limit_into_index_scan() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);
        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::table_scan("users")),
            order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Desc)],
            limit: 5,
            offset: 2,
        };

        let result = pass.optimize(plan);
        match result {
            PhysicalPlan::IndexScan {
                index,
                reverse,
                limit,
                offset,
                ..
            } => {
                assert_eq!(index, "idx_id");
                assert!(reverse);
                assert_eq!(limit, Some(5));
                assert_eq!(offset, Some(2));
            }
            other => panic!("expected index scan, got {:?}", other),
        }
    }

    #[test]
    fn test_topn_gin_scan_rewrites_to_ordered_index_filter() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);
        let recheck = Expr::Function {
            name: "JSONB_PATH_EQ".into(),
            args: alloc::vec![
                Expr::column("documents", "metadata", 3),
                Expr::literal(cynos_core::Value::String("$.category".into())),
                Expr::literal(cynos_core::Value::String("tech".into())),
            ],
        };
        let plan = PhysicalPlan::TopN {
            input: Box::new(PhysicalPlan::GinIndexScanMulti {
                table: "documents".into(),
                index: "idx_metadata".into(),
                pairs: alloc::vec![("category".into(), "tech".into())],
                match_all: true,
                recheck: Some(recheck.clone()),
            }),
            order_by: alloc::vec![(Expr::column("documents", "updated_at", 2), SortOrder::Desc)],
            limit: 20,
            offset: 0,
        };

        let result = pass.optimize(plan);
        match result {
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                assert_eq!(limit, 20);
                assert_eq!(offset, 0);
                match *input {
                    PhysicalPlan::Filter { input, predicate } => {
                        assert_eq!(
                            alloc::format!("{:?}", predicate),
                            alloc::format!("{:?}", recheck)
                        );
                        match *input {
                            PhysicalPlan::IndexScan {
                                table,
                                index,
                                reverse,
                                limit,
                                offset,
                                ..
                            } => {
                                assert_eq!(table, "documents");
                                assert_eq!(index, "idx_updated_at");
                                assert!(reverse);
                                assert_eq!(limit, None);
                                assert_eq!(offset, None);
                            }
                            other => panic!("expected index scan, got {:?}", other),
                        }
                    }
                    other => panic!("expected filter, got {:?}", other),
                }
            }
            other => panic!("expected limit, got {:?}", other),
        }
    }

    #[test]
    fn test_sort_after_join_not_optimized() {
        let ctx = create_test_context();
        let pass = OrderByIndexPass::new(&ctx);
        let plan = PhysicalPlan::Sort {
            input: Box::new(PhysicalPlan::HashJoin {
                left: Box::new(PhysicalPlan::table_scan("users")),
                right: Box::new(PhysicalPlan::table_scan("orders")),
                condition: Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 0),
                ),
                join_type: JoinType::Inner,
                output_tables: alloc::vec!["users".into(), "orders".into()],
            }),
            order_by: alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
        };

        assert!(matches!(pass.optimize(plan), PhysicalPlan::Sort { .. }));
    }
}
