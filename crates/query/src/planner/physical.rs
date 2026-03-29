//! Physical query plan definitions.

use crate::ast::JoinType;
use crate::ast::{AggregateFunc, Expr, SortOrder};
use crate::planner::IndexBounds;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::Value;

/// Join algorithm selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinAlgorithm {
    /// Hash join - best for equi-joins with no index.
    Hash,
    /// Sort-merge join - best when both inputs are sorted.
    SortMerge,
    /// Nested loop join - fallback for non-equi joins.
    NestedLoop,
    /// Index nested loop join - when one side has an index.
    IndexNestedLoop,
}

/// Physical query plan node.
#[derive(Clone, Debug)]
pub enum PhysicalPlan {
    /// Full table scan.
    TableScan { table: String },

    /// Index scan with a key range.
    IndexScan {
        table: String,
        index: String,
        bounds: IndexBounds,
        /// Optional limit for early termination.
        limit: Option<usize>,
        /// Optional offset to skip rows.
        offset: Option<usize>,
        /// Whether to scan in reverse order (for DESC sorting).
        reverse: bool,
    },

    /// Index point lookup.
    IndexGet {
        table: String,
        index: String,
        key: Value,
        /// Optional limit for early termination.
        limit: Option<usize>,
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
        /// The key to search for (JSON path segment).
        key: String,
        /// The value to match (for equality queries).
        value: Option<String>,
        /// Query type: "eq", "contains", or "exists".
        query_type: String,
        /// Optional recheck predicate preserved for later physical rewrites.
        recheck: Option<Expr>,
    },

    /// GIN index scan for multiple JSONB predicates.
    GinIndexScanMulti {
        table: String,
        index: String,
        /// Multiple key-value pairs to match.
        pairs: Vec<(String, String)>,
        /// `true` for AND semantics, `false` for OR semantics.
        match_all: bool,
        /// Optional recheck predicate preserved for later physical rewrites.
        recheck: Option<Expr>,
    },

    /// Filter operator.
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },

    /// Projection operator.
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<Expr>,
    },

    /// Hash join.
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Expr,
        join_type: JoinType,
        output_tables: Vec<String>,
    },

    /// Sort-merge join.
    SortMergeJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Expr,
        join_type: JoinType,
        output_tables: Vec<String>,
    },

    /// Nested loop join.
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Expr,
        join_type: JoinType,
        output_tables: Vec<String>,
    },

    /// Index nested loop join.
    IndexNestedLoopJoin {
        outer: Box<PhysicalPlan>,
        inner_table: String,
        inner_index: String,
        condition: Expr,
        join_type: JoinType,
        outer_is_left: bool,
        output_tables: Vec<String>,
    },

    /// Hash aggregate.
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<(AggregateFunc, Expr)>,
    },

    /// Sort operator.
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<(Expr, SortOrder)>,
    },

    /// TopN operator - combines Sort and Limit for efficient top-k selection.
    /// Uses a heap to maintain only the top N elements, avoiding full sort.
    TopN {
        input: Box<PhysicalPlan>,
        order_by: Vec<(Expr, SortOrder)>,
        limit: usize,
        offset: usize,
    },

    /// Limit and offset.
    Limit {
        input: Box<PhysicalPlan>,
        limit: usize,
        offset: usize,
    },

    /// Cross product.
    CrossProduct {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },

    /// Union of two relations.
    Union {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        all: bool,
    },

    /// No-op step (passes through input).
    NoOp { input: Box<PhysicalPlan> },

    /// Empty result.
    Empty,
}

impl PhysicalPlan {
    /// Creates a table scan plan.
    pub fn table_scan(table: impl Into<String>) -> Self {
        PhysicalPlan::TableScan {
            table: table.into(),
        }
    }

    /// Creates an index scan plan.
    pub fn index_scan(
        table: impl Into<String>,
        index: impl Into<String>,
        range_start: Option<Value>,
        range_end: Option<Value>,
    ) -> Self {
        PhysicalPlan::IndexScan {
            table: table.into(),
            index: index.into(),
            bounds: IndexBounds::from_scalar_range(range_start, range_end, true, true),
            limit: None,
            offset: None,
            reverse: false,
        }
    }

    /// Creates an index scan plan with limit and offset.
    pub fn index_scan_with_limit(
        table: impl Into<String>,
        index: impl Into<String>,
        range_start: Option<Value>,
        range_end: Option<Value>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Self {
        PhysicalPlan::IndexScan {
            table: table.into(),
            index: index.into(),
            bounds: IndexBounds::from_scalar_range(range_start, range_end, true, true),
            limit,
            offset,
            reverse: false,
        }
    }

    /// Creates an index scan plan with limit, offset, and reverse option.
    pub fn index_scan_with_options(
        table: impl Into<String>,
        index: impl Into<String>,
        range_start: Option<Value>,
        range_end: Option<Value>,
        limit: Option<usize>,
        offset: Option<usize>,
        reverse: bool,
    ) -> Self {
        PhysicalPlan::IndexScan {
            table: table.into(),
            index: index.into(),
            bounds: IndexBounds::from_scalar_range(range_start, range_end, true, true),
            limit,
            offset,
            reverse,
        }
    }

    /// Creates an index scan plan with explicit bounds.
    pub fn index_scan_with_bounds(
        table: impl Into<String>,
        index: impl Into<String>,
        bounds: IndexBounds,
        limit: Option<usize>,
        offset: Option<usize>,
        reverse: bool,
    ) -> Self {
        PhysicalPlan::IndexScan {
            table: table.into(),
            index: index.into(),
            bounds,
            limit,
            offset,
            reverse,
        }
    }

    /// Creates an index point lookup plan.
    pub fn index_get(table: impl Into<String>, index: impl Into<String>, key: Value) -> Self {
        PhysicalPlan::IndexGet {
            table: table.into(),
            index: index.into(),
            key,
            limit: None,
        }
    }

    /// Creates an index point lookup plan with limit.
    pub fn index_get_with_limit(
        table: impl Into<String>,
        index: impl Into<String>,
        key: Value,
        limit: Option<usize>,
    ) -> Self {
        PhysicalPlan::IndexGet {
            table: table.into(),
            index: index.into(),
            key,
            limit,
        }
    }

    /// Creates an index multi-point lookup plan (for IN queries).
    pub fn index_in_get(
        table: impl Into<String>,
        index: impl Into<String>,
        keys: Vec<Value>,
    ) -> Self {
        PhysicalPlan::IndexInGet {
            table: table.into(),
            index: index.into(),
            keys,
        }
    }

    /// Creates a GIN index scan plan.
    pub fn gin_index_scan(
        table: impl Into<String>,
        index: impl Into<String>,
        key: impl Into<String>,
        value: Option<String>,
        query_type: impl Into<String>,
        recheck: Option<Expr>,
    ) -> Self {
        PhysicalPlan::GinIndexScan {
            table: table.into(),
            index: index.into(),
            key: key.into(),
            value,
            query_type: query_type.into(),
            recheck,
        }
    }

    /// Creates a GIN index scan plan for multiple key-value pairs.
    pub fn gin_index_scan_multi(
        table: impl Into<String>,
        index: impl Into<String>,
        pairs: Vec<(String, String)>,
        match_all: bool,
        recheck: Option<Expr>,
    ) -> Self {
        PhysicalPlan::GinIndexScanMulti {
            table: table.into(),
            index: index.into(),
            pairs,
            match_all,
            recheck,
        }
    }

    /// Creates a filter plan.
    pub fn filter(input: PhysicalPlan, predicate: Expr) -> Self {
        PhysicalPlan::Filter {
            input: Box::new(input),
            predicate,
        }
    }

    /// Creates a projection plan.
    pub fn project(input: PhysicalPlan, columns: Vec<Expr>) -> Self {
        PhysicalPlan::Project {
            input: Box::new(input),
            columns,
        }
    }

    /// Creates a hash join plan.
    pub fn hash_join(
        left: PhysicalPlan,
        right: PhysicalPlan,
        condition: Expr,
        join_type: JoinType,
    ) -> Self {
        let output_tables = Self::combined_output_tables(&left, &right);
        Self::hash_join_with_output_tables(left, right, condition, join_type, output_tables)
    }

    /// Creates a hash join plan with an explicit output table order.
    pub fn hash_join_with_output_tables(
        left: PhysicalPlan,
        right: PhysicalPlan,
        condition: Expr,
        join_type: JoinType,
        output_tables: Vec<String>,
    ) -> Self {
        PhysicalPlan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            condition,
            join_type,
            output_tables,
        }
    }

    /// Creates a sort-merge join plan.
    pub fn sort_merge_join(
        left: PhysicalPlan,
        right: PhysicalPlan,
        condition: Expr,
        join_type: JoinType,
    ) -> Self {
        let output_tables = Self::combined_output_tables(&left, &right);
        Self::sort_merge_join_with_output_tables(left, right, condition, join_type, output_tables)
    }

    /// Creates a sort-merge join plan with an explicit output table order.
    pub fn sort_merge_join_with_output_tables(
        left: PhysicalPlan,
        right: PhysicalPlan,
        condition: Expr,
        join_type: JoinType,
        output_tables: Vec<String>,
    ) -> Self {
        PhysicalPlan::SortMergeJoin {
            left: Box::new(left),
            right: Box::new(right),
            condition,
            join_type,
            output_tables,
        }
    }

    /// Creates a nested loop join plan.
    pub fn nested_loop_join(
        left: PhysicalPlan,
        right: PhysicalPlan,
        condition: Expr,
        join_type: JoinType,
    ) -> Self {
        let output_tables = Self::combined_output_tables(&left, &right);
        Self::nested_loop_join_with_output_tables(left, right, condition, join_type, output_tables)
    }

    /// Creates a nested loop join plan with an explicit output table order.
    pub fn nested_loop_join_with_output_tables(
        left: PhysicalPlan,
        right: PhysicalPlan,
        condition: Expr,
        join_type: JoinType,
        output_tables: Vec<String>,
    ) -> Self {
        PhysicalPlan::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
            condition,
            join_type,
            output_tables,
        }
    }

    /// Creates a hash aggregate plan.
    pub fn hash_aggregate(
        input: PhysicalPlan,
        group_by: Vec<Expr>,
        aggregates: Vec<(AggregateFunc, Expr)>,
    ) -> Self {
        PhysicalPlan::HashAggregate {
            input: Box::new(input),
            group_by,
            aggregates,
        }
    }

    /// Creates a sort plan.
    pub fn sort(input: PhysicalPlan, order_by: Vec<(Expr, SortOrder)>) -> Self {
        PhysicalPlan::Sort {
            input: Box::new(input),
            order_by,
        }
    }

    /// Creates a TopN plan for efficient top-k selection.
    pub fn top_n(
        input: PhysicalPlan,
        order_by: Vec<(Expr, SortOrder)>,
        limit: usize,
        offset: usize,
    ) -> Self {
        PhysicalPlan::TopN {
            input: Box::new(input),
            order_by,
            limit,
            offset,
        }
    }

    /// Creates a limit plan.
    pub fn limit(input: PhysicalPlan, limit: usize, offset: usize) -> Self {
        PhysicalPlan::Limit {
            input: Box::new(input),
            limit,
            offset,
        }
    }

    /// Creates a union plan.
    pub fn union(left: PhysicalPlan, right: PhysicalPlan, all: bool) -> Self {
        PhysicalPlan::Union {
            left: Box::new(left),
            right: Box::new(right),
            all,
        }
    }

    fn combined_output_tables(left: &PhysicalPlan, right: &PhysicalPlan) -> Vec<String> {
        let mut tables = left.output_tables();
        tables.extend(right.output_tables());
        tables
    }

    /// Returns the visible table order produced by this plan.
    pub fn output_tables(&self) -> Vec<String> {
        match self {
            PhysicalPlan::TableScan { table }
            | PhysicalPlan::IndexScan { table, .. }
            | PhysicalPlan::IndexGet { table, .. }
            | PhysicalPlan::IndexInGet { table, .. }
            | PhysicalPlan::GinIndexScan { table, .. }
            | PhysicalPlan::GinIndexScanMulti { table, .. } => alloc::vec![table.clone()],
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::TopN { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::NoOp { input } => input.output_tables(),
            PhysicalPlan::HashJoin { output_tables, .. }
            | PhysicalPlan::SortMergeJoin { output_tables, .. }
            | PhysicalPlan::NestedLoopJoin { output_tables, .. }
            | PhysicalPlan::IndexNestedLoopJoin { output_tables, .. } => output_tables.clone(),
            PhysicalPlan::CrossProduct { left, right }
            | PhysicalPlan::Union { left, right, .. } => Self::combined_output_tables(left, right),
            PhysicalPlan::Empty => Vec::new(),
        }
    }

    /// Checks if this plan can be incrementalized.
    pub fn is_incrementalizable(&self) -> bool {
        match self {
            PhysicalPlan::TableScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::IndexGet { .. }
            | PhysicalPlan::IndexInGet { .. }
            | PhysicalPlan::Filter { .. }
            | PhysicalPlan::Project { .. }
            | PhysicalPlan::HashJoin { .. }
            | PhysicalPlan::HashAggregate { .. } => true,
            PhysicalPlan::Sort { .. } | PhysicalPlan::Limit { .. } | PhysicalPlan::TopN { .. } => {
                false
            }
            PhysicalPlan::SortMergeJoin { .. }
            | PhysicalPlan::NestedLoopJoin { .. }
            | PhysicalPlan::IndexNestedLoopJoin { .. } => true,
            PhysicalPlan::CrossProduct { .. } => true,
            PhysicalPlan::Union { .. } => false,
            PhysicalPlan::NoOp { input } => input.is_incrementalizable(),
            PhysicalPlan::Empty => true,
            PhysicalPlan::GinIndexScan { .. } | PhysicalPlan::GinIndexScanMulti { .. } => true,
        }
    }

    /// Returns the input plan(s) of this node.
    pub fn inputs(&self) -> Vec<&PhysicalPlan> {
        match self {
            PhysicalPlan::TableScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::IndexGet { .. }
            | PhysicalPlan::IndexInGet { .. }
            | PhysicalPlan::GinIndexScan { .. }
            | PhysicalPlan::GinIndexScanMulti { .. }
            | PhysicalPlan::Empty => alloc::vec![],
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::TopN { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::NoOp { input } => alloc::vec![input.as_ref()],
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::SortMergeJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::CrossProduct { left, right }
            | PhysicalPlan::Union { left, right, .. } => {
                alloc::vec![left.as_ref(), right.as_ref()]
            }
            PhysicalPlan::IndexNestedLoopJoin { outer, .. } => alloc::vec![outer.as_ref()],
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
            PhysicalPlan::TableScan { table }
            | PhysicalPlan::IndexScan { table, .. }
            | PhysicalPlan::IndexGet { table, .. }
            | PhysicalPlan::IndexInGet { table, .. }
            | PhysicalPlan::GinIndexScan { table, .. }
            | PhysicalPlan::GinIndexScanMulti { table, .. } => {
                if !tables.contains(table) {
                    tables.push(table.clone());
                }
            }
            PhysicalPlan::HashJoin { output_tables, .. }
            | PhysicalPlan::SortMergeJoin { output_tables, .. }
            | PhysicalPlan::NestedLoopJoin { output_tables, .. }
            | PhysicalPlan::IndexNestedLoopJoin { output_tables, .. } => {
                for table in output_tables {
                    if !tables.contains(table) {
                        tables.push(table.clone());
                    }
                }
            }
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::TopN { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::NoOp { input } => {
                input.collect_tables_into(tables);
            }
            PhysicalPlan::CrossProduct { left, right }
            | PhysicalPlan::Union { left, right, .. } => {
                left.collect_tables_into(tables);
                right.collect_tables_into(tables);
            }
            PhysicalPlan::Empty => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;

    #[test]
    fn test_physical_plan_builders() {
        let scan = PhysicalPlan::table_scan("users");
        assert!(matches!(scan, PhysicalPlan::TableScan { table } if table == "users"));

        let index_scan = PhysicalPlan::index_scan(
            "users",
            "idx_id",
            Some(Value::Int64(1)),
            Some(Value::Int64(100)),
        );
        assert!(matches!(index_scan, PhysicalPlan::IndexScan { .. }));
    }

    #[test]
    fn test_is_incrementalizable() {
        let scan = PhysicalPlan::table_scan("users");
        assert!(scan.is_incrementalizable());

        let sort = PhysicalPlan::sort(
            PhysicalPlan::table_scan("users"),
            alloc::vec![(Expr::column("users", "id", 0), SortOrder::Asc)],
        );
        assert!(!sort.is_incrementalizable());

        let limit = PhysicalPlan::limit(PhysicalPlan::table_scan("users"), 10, 0);
        assert!(!limit.is_incrementalizable());
    }
}
