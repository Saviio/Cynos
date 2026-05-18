//! Logical query plan definitions.

use crate::ast::JoinType;
use crate::ast::{AggregateFunc, Expr, SortOrder};
use crate::planner::IndexBounds;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::Value;

/// Logical query plan node.
#[derive(Clone, Debug)]
pub enum LogicalPlan {
    /// Table scan.
    Scan { table: String },

    /// Index scan with a key range.
    IndexScan {
        table: String,
        index: String,
        bounds: IndexBounds,
    },

    /// Index point lookup.
    IndexGet {
        table: String,
        index: String,
        key: Value,
    },

    /// Index multi-point lookup (for IN queries).
    /// Performs multiple index lookups and unions the results.
    IndexInGet {
        table: String,
        index: String,
        keys: Vec<Value>,
    },

    /// GIN index scan for JSONB queries.
    GinIndexScan {
        table: String,
        index: String,
        /// The JSONB column being queried.
        column: String,
        /// The column index in the table schema.
        column_index: usize,
        /// The JSON path being queried (e.g., "$.city").
        path: String,
        /// The value to match (for equality queries).
        value: Option<Value>,
        /// Query type: "eq", "contains", or "exists".
        query_type: String,
        /// Optional recheck predicate preserved for later physical rewrites.
        recheck: Option<Expr>,
    },

    /// GIN index scan for multiple JSONB predicates (AND combination).
    /// More efficient than multiple single GIN scans followed by set operations.
    GinIndexScanMulti {
        table: String,
        index: String,
        /// The JSONB column being queried.
        column: String,
        /// Multiple (path, value) pairs to match.
        pairs: Vec<(String, Value)>,
        /// `true` for AND semantics, `false` for OR semantics.
        match_all: bool,
        /// Optional recheck predicate preserved for later physical rewrites.
        recheck: Option<Expr>,
    },

    /// Filter (WHERE clause).
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },

    /// Projection (SELECT columns).
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<Expr>,
    },

    /// Join two relations.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: Expr,
        join_type: JoinType,
        output_tables: Vec<String>,
    },

    /// Aggregation (GROUP BY).
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<(AggregateFunc, Expr)>,
    },

    /// Sort (ORDER BY).
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<(Expr, SortOrder)>,
    },

    /// Limit and offset.
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
        offset: usize,
    },

    /// Cross product (cartesian join).
    CrossProduct {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },

    /// Union of two relations.
    Union {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        all: bool,
    },

    /// Empty relation.
    Empty,
}

impl LogicalPlan {
    /// Creates a table scan plan.
    pub fn scan(table: impl Into<String>) -> Self {
        LogicalPlan::Scan {
            table: table.into(),
        }
    }

    /// Creates a filter plan.
    pub fn filter(input: LogicalPlan, predicate: Expr) -> Self {
        LogicalPlan::Filter {
            input: Box::new(input),
            predicate,
        }
    }

    /// Creates a projection plan.
    pub fn project(input: LogicalPlan, columns: Vec<Expr>) -> Self {
        LogicalPlan::Project {
            input: Box::new(input),
            columns,
        }
    }

    /// Creates a join plan.
    pub fn join(
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Expr,
        join_type: JoinType,
    ) -> Self {
        let output_tables = Self::combined_output_tables(&left, &right);
        Self::join_with_output_tables(left, right, condition, join_type, output_tables)
    }

    /// Creates a join plan with an explicit output table order.
    pub fn join_with_output_tables(
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Expr,
        join_type: JoinType,
        output_tables: Vec<String>,
    ) -> Self {
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            condition,
            join_type,
            output_tables,
        }
    }

    /// Creates an inner join plan.
    pub fn inner_join(left: LogicalPlan, right: LogicalPlan, condition: Expr) -> Self {
        Self::join(left, right, condition, JoinType::Inner)
    }

    /// Creates a left outer join plan.
    pub fn left_join(left: LogicalPlan, right: LogicalPlan, condition: Expr) -> Self {
        Self::join(left, right, condition, JoinType::LeftOuter)
    }

    /// Creates an aggregation plan.
    pub fn aggregate(
        input: LogicalPlan,
        group_by: Vec<Expr>,
        aggregates: Vec<(AggregateFunc, Expr)>,
    ) -> Self {
        LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by,
            aggregates,
        }
    }

    /// Creates a sort plan.
    pub fn sort(input: LogicalPlan, order_by: Vec<(Expr, SortOrder)>) -> Self {
        LogicalPlan::Sort {
            input: Box::new(input),
            order_by,
        }
    }

    /// Creates a limit plan.
    pub fn limit(input: LogicalPlan, limit: usize, offset: usize) -> Self {
        LogicalPlan::Limit {
            input: Box::new(input),
            limit,
            offset,
        }
    }

    /// Creates a cross product plan.
    pub fn cross_product(left: LogicalPlan, right: LogicalPlan) -> Self {
        LogicalPlan::CrossProduct {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Creates a union plan.
    pub fn union(left: LogicalPlan, right: LogicalPlan, all: bool) -> Self {
        LogicalPlan::Union {
            left: Box::new(left),
            right: Box::new(right),
            all,
        }
    }

    fn combined_output_tables(left: &LogicalPlan, right: &LogicalPlan) -> Vec<String> {
        let mut tables = left.output_tables();
        tables.extend(right.output_tables());
        tables
    }

    /// Returns the visible table order produced by this plan.
    pub fn output_tables(&self) -> Vec<String> {
        match self {
            LogicalPlan::Scan { table }
            | LogicalPlan::IndexScan { table, .. }
            | LogicalPlan::IndexGet { table, .. }
            | LogicalPlan::IndexInGet { table, .. }
            | LogicalPlan::GinIndexScan { table, .. }
            | LogicalPlan::GinIndexScanMulti { table, .. } => alloc::vec![table.clone()],
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => input.output_tables(),
            LogicalPlan::Join { output_tables, .. } => output_tables.clone(),
            LogicalPlan::CrossProduct { left, right } | LogicalPlan::Union { left, right, .. } => {
                Self::combined_output_tables(left, right)
            }
            LogicalPlan::Empty => Vec::new(),
        }
    }

    /// Returns the input plan(s) of this node.
    pub fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. }
            | LogicalPlan::Empty => alloc::vec![],
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => alloc::vec![input.as_ref()],
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::CrossProduct { left, right }
            | LogicalPlan::Union { left, right, .. } => alloc::vec![left.as_ref(), right.as_ref()],
        }
    }

    /// Collects all table names referenced by this plan.
    pub fn collect_tables(&self) -> Vec<String> {
        let mut tables = Vec::new();
        self.collect_tables_into(&mut tables);
        tables
    }

    fn collect_tables_into(&self, tables: &mut Vec<String>) {
        match self {
            LogicalPlan::Scan { table }
            | LogicalPlan::IndexScan { table, .. }
            | LogicalPlan::IndexGet { table, .. }
            | LogicalPlan::IndexInGet { table, .. }
            | LogicalPlan::GinIndexScan { table, .. }
            | LogicalPlan::GinIndexScanMulti { table, .. } => {
                if !tables.contains(table) {
                    tables.push(table.clone());
                }
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => input.collect_tables_into(tables),
            LogicalPlan::Join { output_tables, .. } => {
                for table in output_tables {
                    if !tables.contains(table) {
                        tables.push(table.clone());
                    }
                }
            }
            LogicalPlan::CrossProduct { left, right } | LogicalPlan::Union { left, right, .. } => {
                left.collect_tables_into(tables);
                right.collect_tables_into(tables);
            }
            LogicalPlan::Empty => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;

    #[test]
    fn test_logical_plan_builders() {
        let scan = LogicalPlan::scan("users");
        assert!(matches!(scan, LogicalPlan::Scan { table } if table == "users"));

        let filter = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );
        assert!(matches!(filter, LogicalPlan::Filter { .. }));

        let project = LogicalPlan::project(
            LogicalPlan::scan("users"),
            alloc::vec![Expr::column("users", "name", 1)],
        );
        assert!(matches!(project, LogicalPlan::Project { .. }));
    }

    #[test]
    fn test_logical_plan_inputs() {
        let scan = LogicalPlan::scan("users");
        assert!(scan.inputs().is_empty());

        let filter = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );
        assert_eq!(filter.inputs().len(), 1);

        let join = LogicalPlan::inner_join(
            LogicalPlan::scan("a"),
            LogicalPlan::scan("b"),
            Expr::eq(Expr::column("a", "id", 0), Expr::column("b", "a_id", 0)),
        );
        assert_eq!(join.inputs().len(), 2);
    }

    #[test]
    fn test_logical_plan_collect_tables() {
        let plan = LogicalPlan::filter(
            LogicalPlan::inner_join(
                LogicalPlan::scan("users"),
                LogicalPlan::scan("orders"),
                Expr::eq(
                    Expr::column("users", "id", 0),
                    Expr::column("orders", "user_id", 1),
                ),
            ),
            Expr::gt(Expr::column("orders", "amount", 2), Expr::literal(100i64)),
        );

        let tables = plan.collect_tables();
        assert_eq!(
            tables,
            alloc::vec![
                alloc::string::String::from("users"),
                alloc::string::String::from("orders")
            ]
        );
    }
}
