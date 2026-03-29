//! Redundant outer-join removal.
//!
//! This pass removes LEFT/RIGHT OUTER JOINs when the nullable side is provably
//! redundant:
//! - columns from the nullable side are not needed by ancestors
//! - the join cannot multiply rows from the preserved side because the nullable
//!   side is constrained by a unique key
//!
//! The rule is intentionally conservative and mirrors the core idea used by
//! mainstream optimizers such as PostgreSQL's join removal and SQLite's omit
//! OUTER JOIN optimization: only eliminate an outer join when the dropped side
//! is both unused and cardinality-preserving.

use crate::ast::{BinaryOp, Expr, JoinType};
use crate::context::ExecutionContext;
use crate::optimizer::OptimizerPass;
use crate::planner::LogicalPlan;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use hashbrown::HashSet;

pub struct OuterJoinRemoval<'a> {
    ctx: &'a ExecutionContext,
}

impl<'a> OuterJoinRemoval<'a> {
    pub fn new(ctx: &'a ExecutionContext) -> Self {
        Self { ctx }
    }

    fn rewrite(&self, plan: LogicalPlan, required_tables: &HashSet<String>) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let mut child_required = required_tables.clone();
                self.collect_expr_tables(&predicate, &mut child_required);
                LogicalPlan::Filter {
                    input: Box::new(self.rewrite(*input, &child_required)),
                    predicate,
                }
            }

            LogicalPlan::Project { input, columns } => {
                let child_required = self.tables_in_exprs(&columns);
                LogicalPlan::Project {
                    input: Box::new(self.rewrite(*input, &child_required)),
                    columns,
                }
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let mut child_required = self.tables_in_exprs(&group_by);
                for (_, expr) in &aggregates {
                    self.collect_expr_tables(expr, &mut child_required);
                }
                LogicalPlan::Aggregate {
                    input: Box::new(self.rewrite(*input, &child_required)),
                    group_by,
                    aggregates,
                }
            }

            LogicalPlan::Sort { input, order_by } => {
                let mut child_required = required_tables.clone();
                for (expr, _) in &order_by {
                    self.collect_expr_tables(expr, &mut child_required);
                }
                LogicalPlan::Sort {
                    input: Box::new(self.rewrite(*input, &child_required)),
                    order_by,
                }
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.rewrite(*input, required_tables)),
                limit,
                offset,
            },

            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => self.rewrite_join(
                *left,
                *right,
                condition,
                join_type,
                output_tables,
                required_tables,
            ),

            LogicalPlan::CrossProduct { left, right } => {
                let left_tables = self.plan_tables(&left);
                let right_tables = self.plan_tables(&right);
                let left_required = self.filter_required_tables(required_tables, &left_tables);
                let right_required = self.filter_required_tables(required_tables, &right_tables);
                LogicalPlan::CrossProduct {
                    left: Box::new(self.rewrite(*left, &left_required)),
                    right: Box::new(self.rewrite(*right, &right_required)),
                }
            }

            LogicalPlan::Union { left, right, all } => LogicalPlan::Union {
                left: Box::new(self.rewrite(*left, required_tables)),
                right: Box::new(self.rewrite(*right, required_tables)),
                all,
            },

            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. }
            | LogicalPlan::Empty => plan,
        }
    }

    fn rewrite_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Expr,
        join_type: JoinType,
        output_tables: Vec<String>,
        required_tables: &HashSet<String>,
    ) -> LogicalPlan {
        let left_tables = self.plan_tables(&left);
        let right_tables = self.plan_tables(&right);

        match join_type {
            JoinType::LeftOuter => {
                if !self.any_required(required_tables, &right_tables)
                    && self.can_remove_outer_side(&left_tables, &right, &condition)
                {
                    let left_required = self.filter_required_tables(required_tables, &left_tables);
                    return self.rewrite(left, &left_required);
                }
            }
            JoinType::RightOuter => {
                if !self.any_required(required_tables, &left_tables)
                    && self.can_remove_outer_side(&right_tables, &left, &condition)
                {
                    let right_required =
                        self.filter_required_tables(required_tables, &right_tables);
                    return self.rewrite(right, &right_required);
                }
            }
            JoinType::Inner | JoinType::FullOuter | JoinType::Cross => {}
        }

        let condition_tables = self.tables_in_expr(&condition);
        let left_required =
            self.side_required_tables(required_tables, &condition_tables, &left_tables);
        let right_required =
            self.side_required_tables(required_tables, &condition_tables, &right_tables);

        let rewritten_left = self.rewrite(left, &left_required);
        let rewritten_right = self.rewrite(right, &right_required);
        let filtered_output_tables = Self::filter_output_tables(
            &output_tables,
            &rewritten_left.output_tables(),
            &rewritten_right.output_tables(),
        );

        LogicalPlan::join_with_output_tables(
            rewritten_left,
            rewritten_right,
            condition,
            join_type,
            filtered_output_tables,
        )
    }

    fn can_remove_outer_side(
        &self,
        preserved_tables: &HashSet<String>,
        nullable_side: &LogicalPlan,
        condition: &Expr,
    ) -> bool {
        let Some(source) = self.analyze_single_table_source(nullable_side) else {
            return false;
        };

        // Duplicate base-table names indicate self-joins or repeated references.
        // Those need alias-aware reasoning and are intentionally left alone.
        if preserved_tables.contains(&source.table) {
            return false;
        }

        if source.at_most_one_row {
            return true;
        }

        let mut constrained_columns = HashSet::new();
        self.collect_join_constrained_columns(condition, &source.table, &mut constrained_columns);
        for predicate in &source.local_predicates {
            self.collect_local_constrained_columns(
                predicate,
                &source.table,
                &mut constrained_columns,
            );
        }

        self.ctx
            .get_stats(&source.table)
            .map(|stats| {
                stats.indexes.iter().any(|index| {
                    index.is_unique
                        && !index.is_gin()
                        && !index.columns.is_empty()
                        && index
                            .columns
                            .iter()
                            .all(|column| constrained_columns.contains(column))
                })
            })
            .unwrap_or(false)
    }

    fn analyze_single_table_source(&self, plan: &LogicalPlan) -> Option<SingleTableSource> {
        match plan {
            LogicalPlan::Scan { table }
            | LogicalPlan::IndexScan { table, .. }
            | LogicalPlan::IndexInGet { table, .. }
            | LogicalPlan::GinIndexScan { table, .. }
            | LogicalPlan::GinIndexScanMulti { table, .. } => Some(SingleTableSource {
                table: table.clone(),
                local_predicates: Vec::new(),
                at_most_one_row: false,
            }),
            LogicalPlan::IndexGet { table, index, .. } => Some(SingleTableSource {
                table: table.clone(),
                local_predicates: Vec::new(),
                at_most_one_row: self
                    .ctx
                    .find_index_by_name(table, index)
                    .map(|info| info.is_unique)
                    .unwrap_or(false),
            }),
            LogicalPlan::Filter { input, predicate } => {
                let mut source = self.analyze_single_table_source(input)?;
                source.local_predicates.push(predicate.clone());
                Some(source)
            }
            LogicalPlan::Sort { input, .. } => self.analyze_single_table_source(input),
            LogicalPlan::Project { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::CrossProduct { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Empty => None,
        }
    }

    fn root_required_tables(&self, plan: &LogicalPlan) -> HashSet<String> {
        match plan {
            LogicalPlan::Project { columns, .. } => self.tables_in_exprs(columns),
            LogicalPlan::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                let mut tables = self.tables_in_exprs(group_by);
                for (_, expr) in aggregates {
                    self.collect_expr_tables(expr, &mut tables);
                }
                tables
            }
            LogicalPlan::Filter { input, predicate } => {
                let mut tables = self.root_required_tables(input);
                self.collect_expr_tables(predicate, &mut tables);
                tables
            }
            LogicalPlan::Sort { input, order_by } => {
                let mut tables = self.root_required_tables(input);
                for (expr, _) in order_by {
                    self.collect_expr_tables(expr, &mut tables);
                }
                tables
            }
            LogicalPlan::Limit { input, .. } => self.root_required_tables(input),
            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::CrossProduct { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Empty => plan.output_tables().into_iter().collect(),
        }
    }

    fn side_required_tables(
        &self,
        required_tables: &HashSet<String>,
        condition_tables: &HashSet<String>,
        side_tables: &HashSet<String>,
    ) -> HashSet<String> {
        let mut side_required = self.filter_required_tables(required_tables, side_tables);
        for table in condition_tables {
            if side_tables.contains(table) {
                side_required.insert(table.clone());
            }
        }
        side_required
    }

    fn filter_required_tables(
        &self,
        required_tables: &HashSet<String>,
        side_tables: &HashSet<String>,
    ) -> HashSet<String> {
        required_tables
            .iter()
            .filter(|table| side_tables.contains(*table))
            .cloned()
            .collect()
    }

    fn any_required(
        &self,
        required_tables: &HashSet<String>,
        side_tables: &HashSet<String>,
    ) -> bool {
        required_tables
            .iter()
            .any(|table| side_tables.contains(table))
    }

    fn plan_tables(&self, plan: &LogicalPlan) -> HashSet<String> {
        plan.collect_tables().into_iter().collect()
    }

    fn tables_in_exprs(&self, exprs: &[Expr]) -> HashSet<String> {
        let mut tables = HashSet::new();
        for expr in exprs {
            self.collect_expr_tables(expr, &mut tables);
        }
        tables
    }

    fn tables_in_expr(&self, expr: &Expr) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.collect_expr_tables(expr, &mut tables);
        tables
    }

    fn collect_expr_tables(&self, expr: &Expr, tables: &mut HashSet<String>) {
        match expr {
            Expr::Column(col) => {
                if !col.table.is_empty() {
                    tables.insert(col.table.clone());
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_expr_tables(left, tables);
                self.collect_expr_tables(right, tables);
            }
            Expr::UnaryOp { expr, .. } => self.collect_expr_tables(expr, tables),
            Expr::Function { args, .. } => {
                for arg in args {
                    self.collect_expr_tables(arg, tables);
                }
            }
            Expr::Aggregate { expr, .. } => {
                if let Some(expr) = expr {
                    self.collect_expr_tables(expr, tables);
                }
            }
            Expr::Between { expr, low, high } | Expr::NotBetween { expr, low, high } => {
                self.collect_expr_tables(expr, tables);
                self.collect_expr_tables(low, tables);
                self.collect_expr_tables(high, tables);
            }
            Expr::In { expr, list } | Expr::NotIn { expr, list } => {
                self.collect_expr_tables(expr, tables);
                for item in list {
                    self.collect_expr_tables(item, tables);
                }
            }
            Expr::Like { expr, .. }
            | Expr::NotLike { expr, .. }
            | Expr::Match { expr, .. }
            | Expr::NotMatch { expr, .. } => self.collect_expr_tables(expr, tables),
            Expr::Literal(_) => {}
        }
    }

    fn collect_join_constrained_columns(
        &self,
        expr: &Expr,
        table: &str,
        constrained_columns: &mut HashSet<String>,
    ) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.collect_join_constrained_columns(left, table, constrained_columns);
                self.collect_join_constrained_columns(right, table, constrained_columns);
            }
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                self.collect_constraint_from_equality(left, right, table, constrained_columns);
                self.collect_constraint_from_equality(right, left, table, constrained_columns);
            }
            _ => {}
        }
    }

    fn collect_local_constrained_columns(
        &self,
        expr: &Expr,
        table: &str,
        constrained_columns: &mut HashSet<String>,
    ) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.collect_local_constrained_columns(left, table, constrained_columns);
                self.collect_local_constrained_columns(right, table, constrained_columns);
            }
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                self.collect_local_constraint_from_equality(
                    left,
                    right,
                    table,
                    constrained_columns,
                );
                self.collect_local_constraint_from_equality(
                    right,
                    left,
                    table,
                    constrained_columns,
                );
            }
            _ => {}
        }
    }

    fn collect_constraint_from_equality(
        &self,
        candidate: &Expr,
        other: &Expr,
        table: &str,
        constrained_columns: &mut HashSet<String>,
    ) {
        let Expr::Column(column) = candidate else {
            return;
        };

        if column.table != table || self.expr_references_table(other, table) {
            return;
        }

        constrained_columns.insert(column.column.clone());
    }

    fn collect_local_constraint_from_equality(
        &self,
        candidate: &Expr,
        other: &Expr,
        table: &str,
        constrained_columns: &mut HashSet<String>,
    ) {
        let Expr::Column(column) = candidate else {
            return;
        };

        if column.table != table || !matches!(other, Expr::Literal(_)) {
            return;
        }

        constrained_columns.insert(column.column.clone());
    }

    fn expr_references_table(&self, expr: &Expr, table: &str) -> bool {
        match expr {
            Expr::Column(column) => column.table == table,
            Expr::BinaryOp { left, right, .. } => {
                self.expr_references_table(left, table) || self.expr_references_table(right, table)
            }
            Expr::UnaryOp { expr, .. } => self.expr_references_table(expr, table),
            Expr::Function { args, .. } => args
                .iter()
                .any(|arg| self.expr_references_table(arg, table)),
            Expr::Aggregate { expr, .. } => expr
                .as_ref()
                .map(|expr| self.expr_references_table(expr, table))
                .unwrap_or(false),
            Expr::Between { expr, low, high } | Expr::NotBetween { expr, low, high } => {
                self.expr_references_table(expr, table)
                    || self.expr_references_table(low, table)
                    || self.expr_references_table(high, table)
            }
            Expr::In { expr, list } | Expr::NotIn { expr, list } => {
                self.expr_references_table(expr, table)
                    || list
                        .iter()
                        .any(|item| self.expr_references_table(item, table))
            }
            Expr::Like { expr, .. }
            | Expr::NotLike { expr, .. }
            | Expr::Match { expr, .. }
            | Expr::NotMatch { expr, .. } => self.expr_references_table(expr, table),
            Expr::Literal(_) => false,
        }
    }

    fn filter_output_tables(
        original_output_tables: &[String],
        left_output_tables: &[String],
        right_output_tables: &[String],
    ) -> Vec<String> {
        let mut filtered = Vec::new();
        for table in original_output_tables {
            if left_output_tables.contains(table) || right_output_tables.contains(table) {
                filtered.push(table.clone());
            }
        }
        filtered
    }
}

impl OptimizerPass for OuterJoinRemoval<'_> {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let required_tables = self.root_required_tables(&plan);
        self.rewrite(plan, &required_tables)
    }

    fn name(&self) -> &'static str {
        "outer_join_removal"
    }
}

struct SingleTableSource {
    table: String,
    local_predicates: Vec<Expr>,
    at_most_one_row: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;
    use crate::context::{IndexInfo, TableStats};
    use alloc::string::ToString;

    fn create_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "issues",
            TableStats {
                row_count: 50_000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("pk_issues", alloc::vec!["id".into()], true),
                    IndexInfo::new(
                        "idx_issues_project_id",
                        alloc::vec!["project_id".into()],
                        false
                    ),
                ],
            },
        );
        ctx.register_table(
            "projects",
            TableStats {
                row_count: 3_000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "pk_projects",
                    alloc::vec!["id".into()],
                    true
                )],
            },
        );
        ctx.register_table(
            "orgs",
            TableStats {
                row_count: 200,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new("pk_orgs", alloc::vec!["id".into()], true)],
            },
        );
        ctx.register_table(
            "project_tags",
            TableStats {
                row_count: 8_000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_project_tags_project_id",
                    alloc::vec!["project_id".into()],
                    false,
                )],
            },
        );
        ctx
    }

    #[test]
    fn removes_unused_left_join_with_unique_right_side() {
        let ctx = create_context();
        let pass = OuterJoinRemoval::new(&ctx);

        let plan = LogicalPlan::project(
            LogicalPlan::left_join(
                LogicalPlan::left_join(
                    LogicalPlan::scan("issues"),
                    LogicalPlan::scan("projects"),
                    Expr::eq(
                        Expr::column("issues", "project_id", 1),
                        Expr::column("projects", "id", 0),
                    ),
                ),
                LogicalPlan::scan("orgs"),
                Expr::eq(
                    Expr::column("projects", "org_id", 2),
                    Expr::column("orgs", "id", 0),
                ),
            ),
            alloc::vec![
                Expr::column("issues", "id", 0),
                Expr::column("projects", "id", 0),
            ],
        );

        let optimized = pass.optimize(plan);
        let text = alloc::format!("{:#?}", optimized);

        assert!(text.contains("table: \"issues\""));
        assert!(text.contains("table: \"projects\""));
        assert!(!text.contains("table: \"orgs\""));
    }

    #[test]
    fn keeps_left_join_when_nullable_side_is_projected() {
        let ctx = create_context();
        let pass = OuterJoinRemoval::new(&ctx);

        let plan = LogicalPlan::project(
            LogicalPlan::left_join(
                LogicalPlan::scan("issues"),
                LogicalPlan::scan("projects"),
                Expr::eq(
                    Expr::column("issues", "project_id", 1),
                    Expr::column("projects", "id", 0),
                ),
            ),
            alloc::vec![
                Expr::column("issues", "id", 0),
                Expr::column("projects", "id", 0),
            ],
        );

        let optimized = pass.optimize(plan);
        let text = alloc::format!("{:#?}", optimized);
        assert!(text.contains("Join"));
        assert!(text.contains("table: \"projects\""));
    }

    #[test]
    fn keeps_left_join_when_nullable_side_can_multiply_rows() {
        let ctx = create_context();
        let pass = OuterJoinRemoval::new(&ctx);

        let plan = LogicalPlan::project(
            LogicalPlan::left_join(
                LogicalPlan::scan("projects"),
                LogicalPlan::scan("project_tags"),
                Expr::eq(
                    Expr::column("projects", "id", 0),
                    Expr::column("project_tags", "project_id", 0),
                ),
            ),
            alloc::vec![Expr::column("projects", "id", 0)],
        );

        let optimized = pass.optimize(plan);
        let text = alloc::format!("{:#?}", optimized);
        assert!(text.contains("table: \"project_tags\""));
    }

    #[test]
    fn removes_unused_right_join_symmetrically() {
        let ctx = create_context();
        let pass = OuterJoinRemoval::new(&ctx);

        let plan = LogicalPlan::project(
            LogicalPlan::Join {
                left: Box::new(LogicalPlan::scan("orgs")),
                right: Box::new(LogicalPlan::scan("projects")),
                condition: Expr::eq(
                    Expr::column("orgs", "id", 0),
                    Expr::column("projects", "org_id", 2),
                ),
                join_type: JoinType::RightOuter,
                output_tables: alloc::vec!["orgs".to_string(), "projects".to_string()],
            },
            alloc::vec![Expr::column("projects", "id", 0)],
        );

        let optimized = pass.optimize(plan);
        let text = alloc::format!("{:#?}", optimized);

        assert!(text.contains("table: \"projects\""));
        assert!(!text.contains("table: \"orgs\""));
    }
}
