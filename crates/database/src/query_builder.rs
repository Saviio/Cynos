//! Query builders for SELECT, INSERT, UPDATE, DELETE operations.
//!
//! This module provides fluent API builders for constructing and executing
//! database queries.

use crate::binary_protocol::{SchemaLayout, SchemaLayoutCache};
use crate::convert::{js_array_to_rows, js_to_value, projected_rows_to_js_array, rows_to_js_array};
use crate::dataflow_compiler::compile_to_dataflow;
use crate::expr::{Expr, ExprInner};
use crate::live_runtime::{
    collect_trace_bootstrap_source_bindings, LiveDependencySet, LivePlan, LiveRegistry,
    RowsProjection, RowsSnapshotDependencyGraph, RowsSnapshotDirectedJoinEdge,
    RowsSnapshotJoinEdge, RowsSnapshotLookupPrimitive, RowsSnapshotOrderKey,
    RowsSnapshotPartialRefreshMetadata, RowsSnapshotPartialRefreshState,
    RowsSnapshotRootSubsetMetadata, RowsSnapshotRootSubsetPlan, RowsSnapshotRootSubsetVariants,
};
#[cfg(feature = "benchmark")]
use crate::profiling::SnapshotInitProfile;
use crate::profiling::{now_ms, TraceInitProfile};
use crate::query_engine::{
    compile_cached_plan_with_profile, compile_plan, execute_compiled_physical_plan,
    execute_compiled_physical_plan_with_summary, execute_physical_plan, execute_plan, explain_plan,
    CompilePlanProfile, CompiledPhysicalPlan, QueryResultSummary, RootSubsetPlanningProfile,
};
use crate::reactive_bridge::{JsChangesStream, JsIvmObservableQuery, JsObservableQuery};
use crate::JsSortOrder;
use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::schema::Table;
use cynos_core::{reserve_row_ids, DataType, Row, Value};
use cynos_incremental::{CompiledBootstrapPlan, CompiledIvmPlan, Delta};
use cynos_query::ast::{AggregateFunc, SortOrder};
use cynos_query::plan_cache::{compute_plan_fingerprint, PlanCache};
use cynos_query::planner::{LogicalPlan, PhysicalPlan};
use cynos_reactive::TableId;
#[cfg(feature = "benchmark")]
use cynos_storage::StorageInsertProfile;
use cynos_storage::TableCache;
use wasm_bindgen::prelude::*;

/// SELECT query builder.
#[wasm_bindgen]
pub struct SelectBuilder {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<LiveRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    schema_layout_cache: Rc<RefCell<SchemaLayoutCache>>,
    plan_cache: Rc<RefCell<PlanCache>>,
    columns: JsValue,
    from_table: Option<String>,
    where_clause: Option<Expr>,
    order_by: Vec<(String, SortOrder)>,
    limit_val: Option<usize>,
    offset_val: Option<usize>,
    joins: Vec<JoinClause>,
    group_by_cols: Vec<String>,
    aggregates: Vec<(AggregateFunc, Option<String>)>, // (func, column_name or None for COUNT(*))
    frozen_base: Option<FrozenQueryBase>,
}

#[wasm_bindgen]
pub struct PreparedSelectQuery {
    cache: Rc<RefCell<TableCache>>,
    compiled_plan: CompiledPhysicalPlan,
    result_mapper: QueryResultMapper,
    binary_layout: SchemaLayout,
}

#[derive(Clone)]
enum QueryResultMapper {
    Full { schema: Table },
    Columns { column_names: Vec<String> },
}

impl QueryResultMapper {
    fn map_rows(&self, rows: &[Rc<Row>]) -> JsValue {
        match self {
            Self::Full { schema } => rows_to_js_array(rows, schema),
            Self::Columns { column_names } => projected_rows_to_js_array(rows, column_names),
        }
    }
}

#[derive(Clone)]
struct OutputColumn {
    name: String,
    data_type: DataType,
    is_nullable: bool,
}

#[derive(Clone)]
struct QueryOutput {
    schema: Table,
    columns: Vec<OutputColumn>,
}

impl QueryOutput {
    fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|col| col.name.clone()).collect()
    }

    fn resolve_column(&self, name: &str) -> Option<(usize, &OutputColumn)> {
        if let Some((index, column)) = self
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == name)
        {
            return Some((index, column));
        }

        let simple_name = name.rsplit('.').next().unwrap_or(name);
        let mut matches = self.columns.iter().enumerate().filter(|(_, column)| {
            column
                .name
                .rsplit('.')
                .next()
                .unwrap_or(column.name.as_str())
                == simple_name
        });
        let first = matches.next()?;
        if matches.next().is_some() {
            None
        } else {
            Some(first)
        }
    }

    fn layout(&self) -> SchemaLayout {
        use crate::binary_protocol::{BinaryDataType, ColumnLayout};

        let mut columns = Vec::with_capacity(self.columns.len());
        let mut offset = 0usize;

        for column in &self.columns {
            let binary_type = BinaryDataType::from(column.data_type);
            let fixed_size = binary_type.fixed_size();
            columns.push(ColumnLayout {
                name: column.name.clone(),
                data_type: binary_type,
                fixed_size,
                is_nullable: column.is_nullable,
                offset,
            });
            offset += fixed_size;
        }

        let null_mask_size = (columns.len() + 7) / 8;
        let data_size: usize = columns.iter().map(|column| column.fixed_size).sum();
        let row_stride = null_mask_size + data_size;

        SchemaLayout::new(columns, row_stride, null_mask_size)
    }

    fn is_compatible_with(&self, other: &Self) -> bool {
        self.columns.len() == other.columns.len()
            && self
                .columns
                .iter()
                .zip(other.columns.iter())
                .all(|(left, right)| left.data_type == right.data_type)
    }
}

#[derive(Clone)]
struct FrozenQueryBase {
    plan: LogicalPlan,
    output: QueryOutput,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct JoinClause {
    table: String,         // The actual table name (for schema lookup)
    alias: Option<String>, // Optional alias (for column reference)
    condition: Expr,
    join_type: JoinType,
}

impl JoinClause {
    /// Returns the name to use for column references (alias if present, otherwise table name)
    fn reference_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
enum JoinType {
    Inner,
    Left,
    Right,
}

impl SelectBuilder {
    fn partial_refresh_overscan(limit: usize) -> usize {
        core::cmp::max(256, core::cmp::min(limit / 4, 1_024))
    }

    fn plan_cache_fingerprint(plan: &LogicalPlan, profile: CompilePlanProfile) -> u64 {
        let fingerprint = compute_plan_fingerprint(plan);
        match profile {
            CompilePlanProfile::Default => fingerprint,
            CompilePlanProfile::RootSubset(root_subset_profile) => {
                const ROOT_SUBSET_TAG: u64 = 0xD8A4_81F2_5B39_C6E7;
                let variant_tag = match root_subset_profile.variant {
                    crate::query_engine::RootSubsetPlanVariant::Small => 0x91u64,
                    crate::query_engine::RootSubsetPlanVariant::Large => 0xE3u64,
                };
                fingerprint.rotate_left(17) ^ ROOT_SUBSET_TAG ^ variant_tag
            }
        }
    }

    fn get_or_compile_cached_plan(
        &self,
        cache: &TableCache,
        table_name: &str,
        plan: LogicalPlan,
        profile: CompilePlanProfile,
    ) -> CompiledPhysicalPlan {
        let fingerprint = Self::plan_cache_fingerprint(&plan, profile);
        let mut plan_cache = self.plan_cache.borrow_mut();
        plan_cache
            .get_or_insert_compiled_with(fingerprint, || {
                compile_cached_plan_with_profile(cache, table_name, plan, profile)
            })
            .clone()
    }

    pub(crate) fn new(
        cache: Rc<RefCell<TableCache>>,
        query_registry: Rc<RefCell<LiveRegistry>>,
        table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
        schema_layout_cache: Rc<RefCell<SchemaLayoutCache>>,
        plan_cache: Rc<RefCell<PlanCache>>,
        columns: JsValue,
    ) -> Self {
        Self {
            cache,
            query_registry,
            table_id_map,
            schema_layout_cache,
            plan_cache,
            columns,
            from_table: None,
            where_clause: None,
            order_by: Vec::new(),
            limit_val: None,
            offset_val: None,
            joins: Vec::new(),
            group_by_cols: Vec::new(),
            aggregates: Vec::new(),
            frozen_base: None,
        }
    }

    fn get_schema(&self) -> Option<Table> {
        self.from_table.as_ref().and_then(|name| {
            self.cache
                .borrow()
                .get_table(name)
                .map(|s| s.schema().clone())
        })
    }

    fn output_schema_context(&self) -> Result<Table, JsValue> {
        if let Some(base) = &self.frozen_base {
            Ok(base.output.schema.clone())
        } else {
            self.representative_schema()
        }
    }

    fn get_frozen_output_column_info(&self, col_name: &str) -> Option<(String, usize, DataType)> {
        let base = self.frozen_base.as_ref()?;
        base.output
            .resolve_column(col_name)
            .map(|(index, column)| (String::new(), index, column.data_type))
    }

    fn get_modifier_column_info(&self, col_name: &str) -> Option<(String, usize, DataType)> {
        if self.frozen_base.is_some() {
            self.get_frozen_output_column_info(col_name)
        } else {
            self.get_plan_column_info_any_table(col_name)
        }
    }

    fn get_join_output_column_info(&self, col_name: &str) -> Option<(String, usize, DataType)> {
        let (target_table, target_col) = if let Some(dot_pos) = col_name.find('.') {
            (Some(&col_name[..dot_pos]), &col_name[dot_pos + 1..])
        } else {
            (None, col_name)
        };

        let mut offset = 0usize;

        if let Some(main_table) = &self.from_table {
            if let Some(schema) = self.get_schema() {
                if target_table.is_none() || target_table == Some(main_table.as_str()) {
                    if let Some(col) = schema.get_column(target_col) {
                        return Some((String::new(), offset + col.index(), col.data_type()));
                    }
                }
                offset += schema.columns().len();
            }
        }

        let cache = self.cache.borrow();
        for join in &self.joins {
            if let Some(store) = cache.get_table(&join.table) {
                let schema = store.schema();
                let ref_name = join.reference_name();
                if target_table.is_none()
                    || target_table == Some(ref_name)
                    || target_table == Some(join.table.as_str())
                {
                    if let Some(col) = schema.get_column(target_col) {
                        return Some((String::new(), offset + col.index(), col.data_type()));
                    }
                }
                offset += schema.columns().len();
            }
        }

        None
    }

    fn can_preserve_projection_table_identity(&self) -> bool {
        let mut seen_tables = hashbrown::HashSet::new();

        if let Some(main_table) = &self.from_table {
            seen_tables.insert(main_table.clone());
        }

        for join in &self.joins {
            if !seen_tables.insert(join.table.clone()) {
                return false;
            }
        }

        true
    }

    /// Gets column info for any table (main table or joined tables).
    /// Supports qualified column names like `users.name` and table aliases.
    fn get_column_info_any_table(&self, col_name: &str) -> Option<(String, usize, DataType)> {
        // Check if column name is qualified (contains '.')
        if let Some(dot_pos) = col_name.find('.') {
            let table_part = &col_name[..dot_pos];
            let col_part = &col_name[dot_pos + 1..];

            // First check if table_part matches the main table
            if let Some(main_table) = &self.from_table {
                if main_table == table_part {
                    if let Some(schema) = self.get_schema() {
                        if let Some(col) = schema.get_column(col_part) {
                            return Some((table_part.to_string(), col.index(), col.data_type()));
                        }
                    }
                }
            }

            // Check if table_part matches any join's alias or table name
            for join in &self.joins {
                let ref_name = join.reference_name();
                if ref_name == table_part {
                    // Use the actual table name for schema lookup
                    if let Some(store) = self.cache.borrow().get_table(&join.table) {
                        if let Some(col) = store.schema().get_column(col_part) {
                            // Return the reference name (alias if present) for consistency
                            return Some((ref_name.to_string(), col.index(), col.data_type()));
                        }
                    }
                }
            }

            // Try direct table lookup (for cases without alias)
            if let Some(info) = self.cache.borrow().get_table(table_part).and_then(|store| {
                store
                    .schema()
                    .get_column(col_part)
                    .map(|c| (table_part.to_string(), c.index(), c.data_type()))
            }) {
                return Some(info);
            }
        }

        // Try the main table for unqualified column names
        if let Some(table_name) = &self.from_table {
            if let Some(schema) = self.get_schema() {
                if let Some(col) = schema.get_column(col_name) {
                    return Some((table_name.clone(), col.index(), col.data_type()));
                }
            }
        }

        // Try all joined tables
        for join in &self.joins {
            if let Some(info) = self
                .cache
                .borrow()
                .get_table(&join.table)
                .and_then(|store| {
                    store
                        .schema()
                        .get_column(col_name)
                        .map(|c| (join.reference_name().to_string(), c.index(), c.data_type()))
                })
            {
                return Some(info);
            }
        }

        None
    }

    /// Gets column info for plan construction while preserving the real table
    /// identity for optimizer passes like predicate pushdown and outer-join
    /// simplification.
    ///
    /// Unlike `get_column_info_any_table`, this resolves aliases back to the
    /// underlying table name so filters and order-by expressions on join queries
    /// are still attributable to their source tables.
    fn get_plan_column_info_any_table(&self, col_name: &str) -> Option<(String, usize, DataType)> {
        if let Some(dot_pos) = col_name.find('.') {
            let table_part = &col_name[..dot_pos];
            let col_part = &col_name[dot_pos + 1..];

            if let Some(main_table) = &self.from_table {
                if main_table == table_part {
                    if let Some(schema) = self.get_schema() {
                        if let Some(col) = schema.get_column(col_part) {
                            return Some((main_table.clone(), col.index(), col.data_type()));
                        }
                    }
                }
            }

            for join in &self.joins {
                let ref_name = join.reference_name();
                if ref_name == table_part || join.table == table_part {
                    if let Some(store) = self.cache.borrow().get_table(&join.table) {
                        if let Some(col) = store.schema().get_column(col_part) {
                            return Some((join.table.clone(), col.index(), col.data_type()));
                        }
                    }
                }
            }

            if let Some(info) = self.cache.borrow().get_table(table_part).and_then(|store| {
                store
                    .schema()
                    .get_column(col_part)
                    .map(|c| (table_part.to_string(), c.index(), c.data_type()))
            }) {
                return Some(info);
            }
        }

        if let Some(table_name) = &self.from_table {
            if let Some(schema) = self.get_schema() {
                if let Some(col) = schema.get_column(col_name) {
                    return Some((table_name.clone(), col.index(), col.data_type()));
                }
            }
        }

        for join in &self.joins {
            if let Some(info) = self
                .cache
                .borrow()
                .get_table(&join.table)
                .and_then(|store| {
                    store
                        .schema()
                        .get_column(col_name)
                        .map(|c| (join.table.clone(), c.index(), c.data_type()))
                })
            {
                return Some(info);
            }
        }

        None
    }

    /// Parses the columns JsValue into a list of column names.
    /// Returns None if selecting all columns (empty array, undefined, or contains "*").
    fn parse_columns(&self) -> Option<Vec<String>> {
        if self.columns.is_undefined() || self.columns.is_null() {
            return None;
        }

        if let Some(arr) = self.columns.dyn_ref::<js_sys::Array>() {
            if arr.length() == 0 {
                return None; // Empty array means select all
            }

            // Check if first element is an array (nested array case from variadic)
            // e.g., select(['name', 'age']) becomes [['name', 'age']] with variadic
            let first = arr.get(0);
            if let Some(inner_arr) = first.dyn_ref::<js_sys::Array>() {
                // Handle nested array: [['name', 'age']]
                let cols: Vec<String> = inner_arr.iter().filter_map(|v| v.as_string()).collect();
                if cols.is_empty() {
                    return None;
                } else if cols.len() == 1 && cols[0] == "*" {
                    return None; // ["*"] means select all
                } else {
                    return Some(cols);
                }
            }

            // Handle flat array: ['name', 'age'] (variadic args)
            let cols: Vec<String> = arr.iter().filter_map(|v| v.as_string()).collect();
            if cols.is_empty() {
                None
            } else if cols.len() == 1 && cols[0] == "*" {
                None // ["*"] means select all
            } else {
                Some(cols)
            }
        } else if let Some(s) = self.columns.as_string() {
            if s == "*" {
                None // "*" means select all
            } else {
                Some(alloc::vec![s])
            }
        } else {
            None
        }
    }

    /// Builds the scan/join root for a non-set-operation query.
    fn build_source_plan(&self, table_name: &str) -> LogicalPlan {
        let mut plan = LogicalPlan::Scan {
            table: table_name.to_string(),
        };

        let mut table_offsets: hashbrown::HashMap<String, usize> = hashbrown::HashMap::new();

        if let Some(schema) = self.get_schema() {
            table_offsets.insert(table_name.to_string(), 0);
            let mut current_offset = schema.columns().len();

            for join in &self.joins {
                let right_plan = LogicalPlan::Scan {
                    table: join.table.clone(),
                };

                let ref_name = join.reference_name().to_string();
                table_offsets.insert(ref_name.clone(), current_offset);

                let get_col_info = |name: &str| {
                    self.get_column_info_for_join_with_offsets_alias(name, join, &table_offsets)
                };
                let ast_condition = join.condition.to_ast_with_table(&get_col_info);

                plan = match join.join_type {
                    JoinType::Inner => LogicalPlan::inner_join(plan, right_plan, ast_condition),
                    JoinType::Left => LogicalPlan::left_join(plan, right_plan, ast_condition),
                    JoinType::Right => LogicalPlan::left_join(right_plan, plan, ast_condition),
                };

                if let Some(store) = self.cache.borrow().get_table(&join.table) {
                    current_offset += store.schema().columns().len();
                }
            }
        } else {
            for join in &self.joins {
                let right_plan = LogicalPlan::Scan {
                    table: join.table.clone(),
                };

                let get_col_info = |name: &str| self.get_column_info_for_join(name, &join.table);
                let ast_condition = join.condition.to_ast_with_table(&get_col_info);

                plan = match join.join_type {
                    JoinType::Inner => LogicalPlan::inner_join(plan, right_plan, ast_condition),
                    JoinType::Left => LogicalPlan::left_join(plan, right_plan, ast_condition),
                    JoinType::Right => LogicalPlan::left_join(right_plan, plan, ast_condition),
                };
            }
        }

        plan
    }

    /// Applies WHERE / GROUP BY / ORDER BY / LIMIT / projection clauses on top of a root plan.
    fn apply_query_modifiers(&self, mut plan: LogicalPlan) -> LogicalPlan {
        if let Some(ref predicate) = self.where_clause {
            let get_col_info = |name: &str| self.get_modifier_column_info(name);
            let ast_predicate = predicate.to_ast_with_table(&get_col_info);
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: ast_predicate,
            };
        }

        if !self.group_by_cols.is_empty() || !self.aggregates.is_empty() {
            let group_by_exprs: Vec<_> = self
                .group_by_cols
                .iter()
                .filter_map(|col| {
                    self.get_column_info_for_projection(col)
                        .map(|(tbl, idx, _)| {
                            let col_name = if let Some(dot_pos) = col.find('.') {
                                &col[dot_pos + 1..]
                            } else {
                                col.as_str()
                            };
                            cynos_query::ast::Expr::column(&tbl, col_name, idx)
                        })
                })
                .collect();

            let agg_exprs: Vec<_> = self
                .aggregates
                .iter()
                .filter_map(|(func, col_opt)| {
                    if let Some(col) = col_opt {
                        self.get_column_info_for_projection(col)
                            .map(|(tbl, idx, _)| {
                                let col_name = if let Some(dot_pos) = col.find('.') {
                                    &col[dot_pos + 1..]
                                } else {
                                    col.as_str()
                                };
                                (*func, cynos_query::ast::Expr::column(&tbl, col_name, idx))
                            })
                    } else {
                        Some((
                            *func,
                            cynos_query::ast::Expr::literal(cynos_core::Value::Int64(1)),
                        ))
                    }
                })
                .collect();

            plan = LogicalPlan::aggregate(plan, group_by_exprs, agg_exprs);
        }

        if !self.order_by.is_empty() {
            let order_exprs: Vec<_> = self
                .order_by
                .iter()
                .filter_map(|(col, order)| {
                    self.get_order_column_info(col).map(|(tbl, idx, _)| {
                        let col_name = if let Some(dot_pos) = col.find('.') {
                            &col[dot_pos + 1..]
                        } else {
                            col.as_str()
                        };
                        (cynos_query::ast::Expr::column(&tbl, col_name, idx), *order)
                    })
                })
                .collect();
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: order_exprs,
            };
        }

        if self.limit_val.is_some() || self.offset_val.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit: self.limit_val.unwrap_or(1_000_000_000),
                offset: self.offset_val.unwrap_or(0),
            };
        }

        if let Some(cols) = self.parse_columns() {
            let project_exprs: Vec<_> = cols
                .iter()
                .filter_map(|col| {
                    self.get_column_info_for_projection(col)
                        .map(|(tbl, idx, _)| {
                            let col_name = if let Some(dot_pos) = col.find('.') {
                                &col[dot_pos + 1..]
                            } else {
                                col.as_str()
                            };
                            cynos_query::ast::Expr::column(&tbl, col_name, idx)
                        })
                })
                .collect();

            if !project_exprs.is_empty() {
                plan = LogicalPlan::Project {
                    input: Box::new(plan),
                    columns: project_exprs,
                };
            }
        }

        plan
    }

    /// Builds a LogicalPlan from the query builder state.
    fn build_logical_plan(&self, table_name: &str) -> LogicalPlan {
        let root = self
            .frozen_base
            .as_ref()
            .map(|base| base.plan.clone())
            .unwrap_or_else(|| self.build_source_plan(table_name));
        self.apply_query_modifiers(root)
    }

    fn rewrite_limit_for_candidate_window(
        plan: LogicalPlan,
        candidate_limit: usize,
    ) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
                input: Box::new(Self::rewrite_limit_for_candidate_window(
                    *input,
                    candidate_limit,
                )),
                predicate,
            },
            LogicalPlan::Project { input, columns } => LogicalPlan::Project {
                input: Box::new(Self::rewrite_limit_for_candidate_window(
                    *input,
                    candidate_limit,
                )),
                columns,
            },
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(Self::rewrite_limit_for_candidate_window(
                    *input,
                    candidate_limit,
                )),
                group_by,
                aggregates,
            },
            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(Self::rewrite_limit_for_candidate_window(
                    *input,
                    candidate_limit,
                )),
                order_by,
            },
            LogicalPlan::Limit { input, .. } => LogicalPlan::Limit {
                input,
                limit: candidate_limit,
                offset: 0,
            },
            other => other,
        }
    }

    fn resolve_partial_output_index(output: &QueryOutput, name: &str) -> Option<usize> {
        output.resolve_column(name).map(|(index, _)| index)
    }

    fn resolve_partial_column_ref(
        &self,
        lookup: &str,
        explicit_name: Option<&str>,
    ) -> Option<(String, String, usize)> {
        let (table, index, _) = self.get_plan_column_info_any_table(lookup)?;
        let column = explicit_name
            .map(ToString::to_string)
            .unwrap_or_else(|| lookup.rsplit('.').next().unwrap_or(lookup).to_string());
        Some((table, column, index))
    }

    fn resolve_partial_expr_column(
        &self,
        column: &crate::expr::Column,
    ) -> Option<(String, String, usize)> {
        let name = column.name();
        if let Some(table_name) = column.table_name() {
            self.resolve_partial_column_ref(&alloc::format!("{}.{}", table_name, name), Some(&name))
        } else {
            self.resolve_partial_column_ref(&name, Some(&name))
        }
    }

    fn resolve_partial_value_column(&self, value: &JsValue) -> Option<(String, String, usize)> {
        if let Some(lookup) = value.as_string() {
            let explicit_name = lookup.rsplit('.').next().unwrap_or(lookup.as_str());
            return self.resolve_partial_column_ref(&lookup, Some(explicit_name));
        }

        if !value.is_object() {
            return None;
        }

        let name = js_sys::Reflect::get(value, &JsValue::from_str("name"))
            .ok()
            .and_then(|v| v.as_string())?;
        let lookup = js_sys::Reflect::get(value, &JsValue::from_str("tableName"))
            .ok()
            .and_then(|v| v.as_string())
            .map(|table| alloc::format!("{}.{}", table, name))
            .unwrap_or_else(|| name.clone());
        self.resolve_partial_column_ref(&lookup, Some(&name))
    }

    fn collect_partial_join_edges(
        &self,
        expr: &Expr,
        edges: &mut Vec<RowsSnapshotJoinEdge>,
    ) -> bool {
        match expr.inner() {
            ExprInner::And { left, right } => {
                self.collect_partial_join_edges(left, edges)
                    && self.collect_partial_join_edges(right, edges)
            }
            ExprInner::Comparison { column, op, value } if *op == crate::expr::ComparisonOp::Eq => {
                let Some((left_table, left_column, _)) = self.resolve_partial_expr_column(column)
                else {
                    return false;
                };
                let Some((right_table, right_column, _)) = self.resolve_partial_value_column(value)
                else {
                    return false;
                };
                if left_table == right_table {
                    return false;
                }
                edges.push(RowsSnapshotJoinEdge {
                    left_table,
                    left_column,
                    right_table,
                    right_column,
                });
                true
            }
            _ => false,
        }
    }

    fn resolve_table_id(&self, table_name: &str) -> Result<TableId, JsValue> {
        self.table_id_map
            .borrow()
            .get(table_name)
            .copied()
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table ID not found: {}", table_name)))
    }

    fn compiled_lookup_primitive(schema: &Table, column_name: &str) -> RowsSnapshotLookupPrimitive {
        if let Some(primary_key) = schema.primary_key() {
            if primary_key.columns().len() == 1
                && primary_key
                    .columns()
                    .first()
                    .map(|column| column.name.as_str())
                    == Some(column_name)
            {
                return RowsSnapshotLookupPrimitive::PrimaryKey;
            }
        }

        if let Some(index_name) = schema
            .indices()
            .iter()
            .find(|index| {
                index.get_index_type() != cynos_core::schema::IndexType::Gin
                    && index.columns().len() == 1
                    && index.columns().first().map(|column| column.name.as_str())
                        == Some(column_name)
            })
            .map(|index| index.name().to_string())
        {
            return RowsSnapshotLookupPrimitive::SingleColumnIndex { index_name };
        }

        RowsSnapshotLookupPrimitive::ScanFallback
    }

    fn compile_rows_snapshot_dependency_graph(
        &self,
        root_table: &str,
        join_edges: &[RowsSnapshotJoinEdge],
    ) -> Result<RowsSnapshotDependencyGraph, JsValue> {
        let root_table_id = self.resolve_table_id(root_table)?;
        let mut table_names = hashbrown::HashMap::new();
        table_names.insert(root_table_id, root_table.to_string());

        let mut undirected_edges_by_source =
            hashbrown::HashMap::<TableId, Vec<RowsSnapshotDirectedJoinEdge>>::new();
        let cache = self.cache.borrow();

        for edge in join_edges {
            let left_table_id = self.resolve_table_id(&edge.left_table)?;
            let right_table_id = self.resolve_table_id(&edge.right_table)?;

            let left_store = cache.get_table(&edge.left_table).ok_or_else(|| {
                JsValue::from_str(&alloc::format!("Table not found: {}", edge.left_table))
            })?;
            let right_store = cache.get_table(&edge.right_table).ok_or_else(|| {
                JsValue::from_str(&alloc::format!("Table not found: {}", edge.right_table))
            })?;

            let left_column_index = left_store
                .schema()
                .get_column_index(&edge.left_column)
                .ok_or_else(|| {
                    JsValue::from_str(&alloc::format!(
                        "Column not found: {}.{}",
                        edge.left_table,
                        edge.left_column
                    ))
                })?;
            let right_column_index = right_store
                .schema()
                .get_column_index(&edge.right_column)
                .ok_or_else(|| {
                    JsValue::from_str(&alloc::format!(
                        "Column not found: {}.{}",
                        edge.right_table,
                        edge.right_column
                    ))
                })?;

            table_names.insert(left_table_id, edge.left_table.clone());
            table_names.insert(right_table_id, edge.right_table.clone());

            undirected_edges_by_source
                .entry(left_table_id)
                .or_insert_with(Vec::new)
                .push(RowsSnapshotDirectedJoinEdge {
                    source_column_index: left_column_index,
                    target_table_id: right_table_id,
                    target_table: edge.right_table.clone(),
                    target_column_index: right_column_index,
                    lookup: Self::compiled_lookup_primitive(
                        right_store.schema(),
                        &edge.right_column,
                    ),
                });
            undirected_edges_by_source
                .entry(right_table_id)
                .or_insert_with(Vec::new)
                .push(RowsSnapshotDirectedJoinEdge {
                    source_column_index: right_column_index,
                    target_table_id: left_table_id,
                    target_table: edge.left_table.clone(),
                    target_column_index: left_column_index,
                    lookup: Self::compiled_lookup_primitive(left_store.schema(), &edge.left_column),
                });
        }

        let mut distance_to_root = hashbrown::HashMap::<TableId, usize>::new();
        let mut queue = VecDeque::new();
        distance_to_root.insert(root_table_id, 0);
        queue.push_back(root_table_id);

        while let Some(table_id) = queue.pop_front() {
            let next_distance = distance_to_root
                .get(&table_id)
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            for edge in undirected_edges_by_source
                .get(&table_id)
                .map(Vec::as_slice)
                .unwrap_or(&[])
            {
                if distance_to_root.contains_key(&edge.target_table_id) {
                    continue;
                }
                distance_to_root.insert(edge.target_table_id, next_distance);
                queue.push_back(edge.target_table_id);
            }
        }

        if distance_to_root.len() != table_names.len() {
            return Err(JsValue::from_str(
                "rows snapshot dependency graph is disconnected from the root table",
            ));
        }

        let mut edges_by_source =
            hashbrown::HashMap::<TableId, Vec<RowsSnapshotDirectedJoinEdge>>::new();
        for (source_table_id, candidate_edges) in undirected_edges_by_source {
            let Some(source_distance) = distance_to_root.get(&source_table_id).copied() else {
                continue;
            };
            let directed_edges: Vec<_> = candidate_edges
                .into_iter()
                .filter(|edge| {
                    distance_to_root
                        .get(&edge.target_table_id)
                        .map(|target_distance| *target_distance < source_distance)
                        .unwrap_or(false)
                })
                .collect();
            if !directed_edges.is_empty() {
                edges_by_source.insert(source_table_id, directed_edges);
            }
        }

        Ok(RowsSnapshotDependencyGraph {
            root_table_id,
            table_names,
            edges_by_source,
        })
    }

    fn build_rows_snapshot_partial_refresh_plan(
        &self,
        output: &QueryOutput,
    ) -> Result<Option<RowsSnapshotPartialRefreshMetadata>, JsValue> {
        if self.frozen_base.is_some()
            || !self.aggregates.is_empty()
            || !self.group_by_cols.is_empty()
            || self.order_by.is_empty()
        {
            return Ok(None);
        }

        let visible_limit = match self.limit_val {
            Some(limit) if limit > 0 => limit,
            _ => return Ok(None),
        };
        let visible_offset = self.offset_val.unwrap_or(0);

        if self
            .joins
            .iter()
            .any(|join| join.join_type == JoinType::Right)
        {
            return Ok(None);
        }

        let root_table = self
            .from_table
            .clone()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let mut seen_tables = hashbrown::HashSet::new();
        if !seen_tables.insert(root_table.clone()) {
            return Ok(None);
        }
        for join in &self.joins {
            if !seen_tables.insert(join.table.clone()) {
                return Ok(None);
            }
        }

        let cache = self.cache.borrow();
        let root_store = cache
            .get_table(&root_table)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", root_table)))?;
        let root_pk = match root_store.schema().primary_key() {
            Some(pk) if !pk.columns().is_empty() => pk,
            _ => return Ok(None),
        };
        let mut root_pk_output_indices = Vec::with_capacity(root_pk.columns().len());
        for pk_column in root_pk.columns() {
            let qualified = alloc::format!("{}.{}", root_table, pk_column.name);
            let Some(output_index) = Self::resolve_partial_output_index(output, &qualified)
                .or_else(|| Self::resolve_partial_output_index(output, &pk_column.name))
            else {
                return Ok(None);
            };
            root_pk_output_indices.push(output_index);
        }

        let mut order_keys = Vec::with_capacity(self.order_by.len());
        for (column_name, order) in &self.order_by {
            let Some(output_index) = Self::resolve_partial_output_index(output, column_name) else {
                return Ok(None);
            };
            order_keys.push(RowsSnapshotOrderKey {
                output_index,
                order: *order,
            });
        }

        let mut join_edges = Vec::new();
        for join in &self.joins {
            if !self.collect_partial_join_edges(&join.condition, &mut join_edges) {
                return Ok(None);
            }
        }
        let dependency_graph =
            self.compile_rows_snapshot_dependency_graph(&root_table, &join_edges)?;

        let overscan = Self::partial_refresh_overscan(visible_limit);
        let candidate_limit = visible_offset
            .saturating_add(visible_limit)
            .saturating_add(overscan.saturating_mul(2));

        Ok(Some(RowsSnapshotPartialRefreshMetadata {
            root_table,
            root_pk_output_indices,
            order_keys,
            visible_offset,
            visible_limit,
            overscan,
            candidate_limit,
            dependency_graph,
        }))
    }

    fn plan_supports_root_subset_refresh(plan: &PhysicalPlan, root_table: &str) -> bool {
        plan.collect_tables()
            .iter()
            .any(|table| table == root_table)
            && Self::plan_allows_root_subset_refresh(plan)
    }

    fn plan_allows_root_subset_refresh(plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::TableScan { .. }
            | PhysicalPlan::IndexScan { .. }
            | PhysicalPlan::IndexGet { .. }
            | PhysicalPlan::IndexInGet { .. }
            | PhysicalPlan::GinIndexScan { .. }
            | PhysicalPlan::GinIndexScanMulti { .. }
            | PhysicalPlan::Empty => true,
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::NoOp { input } => Self::plan_allows_root_subset_refresh(input),
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::SortMergeJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                Self::plan_allows_root_subset_refresh(left)
                    && Self::plan_allows_root_subset_refresh(right)
            }
            PhysicalPlan::IndexNestedLoopJoin { outer, .. } => {
                Self::plan_allows_root_subset_refresh(outer)
            }
            PhysicalPlan::HashAggregate { .. }
            | PhysicalPlan::Sort { .. }
            | PhysicalPlan::TopN { .. }
            | PhysicalPlan::Limit { .. }
            | PhysicalPlan::CrossProduct { .. }
            | PhysicalPlan::Union { .. } => false,
        }
    }

    fn build_rows_snapshot_root_subset_plan(
        &self,
        cache: &TableCache,
        output: &QueryOutput,
        logical_plan: &LogicalPlan,
        compiled_plan: &CompiledPhysicalPlan,
    ) -> Result<Option<RowsSnapshotRootSubsetPlan>, JsValue> {
        if self.frozen_base.is_some()
            || !self.aggregates.is_empty()
            || !self.group_by_cols.is_empty()
            || !self.order_by.is_empty()
            || self.limit_val.is_some()
            || self.offset_val.unwrap_or(0) > 0
        {
            return Ok(None);
        }

        if self
            .joins
            .iter()
            .any(|join| join.join_type == JoinType::Right)
        {
            return Ok(None);
        }

        let root_table = self
            .from_table
            .clone()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;
        if !Self::plan_supports_root_subset_refresh(compiled_plan.physical_plan(), &root_table) {
            return Ok(None);
        }

        let mut seen_tables = hashbrown::HashSet::new();
        if !seen_tables.insert(root_table.clone()) {
            return Ok(None);
        }
        for join in &self.joins {
            if !seen_tables.insert(join.table.clone()) {
                return Ok(None);
            }
        }

        let root_store = cache
            .get_table(&root_table)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", root_table)))?;
        let root_pk = match root_store.schema().primary_key() {
            Some(pk) if !pk.columns().is_empty() => pk,
            _ => return Ok(None),
        };
        let mut root_pk_output_indices = Vec::with_capacity(root_pk.columns().len());
        let mut root_pk_store_indices = Vec::with_capacity(root_pk.columns().len());
        for pk_column in root_pk.columns() {
            let qualified = alloc::format!("{}.{}", root_table, pk_column.name);
            let Some(output_index) = Self::resolve_partial_output_index(output, &qualified)
                .or_else(|| Self::resolve_partial_output_index(output, &pk_column.name))
            else {
                return Ok(None);
            };
            let Some(store_index) = root_store.schema().get_column_index(&pk_column.name) else {
                return Ok(None);
            };
            root_pk_store_indices.push(store_index);
            root_pk_output_indices.push(output_index);
        }

        let mut join_edges = Vec::new();
        for join in &self.joins {
            if !self.collect_partial_join_edges(&join.condition, &mut join_edges) {
                return Ok(None);
            }
        }
        let dependency_graph =
            self.compile_rows_snapshot_dependency_graph(&root_table, &join_edges)?;

        let subset_compiled_plan_small = self.get_or_compile_cached_plan(
            cache,
            &root_table,
            logical_plan.clone(),
            CompilePlanProfile::RootSubset(RootSubsetPlanningProfile::small()),
        );
        let subset_compiled_plan_large = self.get_or_compile_cached_plan(
            cache,
            &root_table,
            logical_plan.clone(),
            CompilePlanProfile::RootSubset(RootSubsetPlanningProfile::large()),
        );

        Ok(Some(RowsSnapshotRootSubsetPlan {
            metadata: RowsSnapshotRootSubsetMetadata {
                root_table,
                root_pk_store_indices,
                root_pk_output_indices,
                dependency_graph,
            },
            compiled_plans: RowsSnapshotRootSubsetVariants {
                small: subset_compiled_plan_small,
                large: subset_compiled_plan_large,
            },
        }))
    }

    /// Gets column info for projection, calculating the correct index for JOIN queries.
    /// For JOIN queries, returns the table-relative index (not the absolute offset).
    /// The absolute index will be computed at runtime based on actual table order.
    /// Supports table aliases.
    fn get_column_info_for_projection(&self, col_name: &str) -> Option<(String, usize, DataType)> {
        if self.frozen_base.is_some() {
            return self.get_frozen_output_column_info(col_name);
        }

        if !self.joins.is_empty() {
            if self.can_preserve_projection_table_identity() {
                return self.get_plan_column_info_any_table(col_name);
            }
            return self.get_join_output_column_info(col_name);
        }

        // Check if column name is qualified (contains '.')
        let (target_table, target_col) = if let Some(dot_pos) = col_name.find('.') {
            (Some(&col_name[..dot_pos]), &col_name[dot_pos + 1..])
        } else {
            (None, col_name)
        };

        // First check the main table
        if let Some(main_table) = &self.from_table {
            if let Some(schema) = self.get_schema() {
                if target_table.is_none() || target_table == Some(main_table.as_str()) {
                    if let Some(col) = schema.get_column(target_col) {
                        // Return table-relative index, not absolute offset
                        return Some((main_table.clone(), col.index(), col.data_type()));
                    }
                }
            }
        }

        // Then check joined tables in order
        for join in &self.joins {
            if let Some(store) = self.cache.borrow().get_table(&join.table) {
                let schema = store.schema();
                let ref_name = join.reference_name();
                // Match against both the reference name (alias) and the actual table name
                if target_table.is_none()
                    || target_table == Some(ref_name)
                    || target_table == Some(join.table.as_str())
                {
                    if let Some(col) = schema.get_column(target_col) {
                        // Return table-relative index, not absolute offset
                        return Some((ref_name.to_string(), col.index(), col.data_type()));
                    }
                }
            }
        }

        None
    }

    fn get_order_column_info(&self, col_name: &str) -> Option<(String, usize, DataType)> {
        if self.frozen_base.is_some()
            || !self.group_by_cols.is_empty()
            || !self.aggregates.is_empty()
        {
            let output = self.describe_output().ok()?;
            return output
                .resolve_column(col_name)
                .map(|(index, column)| (String::new(), index, column.data_type));
        }

        self.get_plan_column_info_any_table(col_name)
    }

    fn representative_schema(&self) -> Result<Table, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        self.cache
            .borrow()
            .get_table(table_name)
            .map(|store| store.schema().clone())
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))
    }

    fn normalize_column_names(column_names: &[String]) -> Vec<String> {
        let mut name_counts: hashbrown::HashMap<&str, usize> = hashbrown::HashMap::new();
        for col_name in column_names {
            let simple_name = if let Some(dot_pos) = col_name.find('.') {
                &col_name[dot_pos + 1..]
            } else {
                col_name.as_str()
            };
            *name_counts.entry(simple_name).or_insert(0) += 1;
        }

        column_names
            .iter()
            .map(|name| {
                if let Some(dot_pos) = name.find('.') {
                    let simple_name = &name[dot_pos + 1..];
                    if name_counts.get(simple_name).copied().unwrap_or(0) > 1 {
                        name.clone()
                    } else {
                        simple_name.to_string()
                    }
                } else {
                    name.clone()
                }
            })
            .collect()
    }

    fn build_projection_output(&self, column_names: &[String]) -> Result<QueryOutput, JsValue> {
        if let Some(base) = &self.frozen_base {
            let columns = column_names
                .iter()
                .filter_map(|column_name| {
                    base.output
                        .resolve_column(column_name)
                        .map(|(_, column)| column.clone())
                })
                .collect();

            return Ok(QueryOutput {
                schema: base.output.schema.clone(),
                columns,
            });
        }

        let normalized_names = Self::normalize_column_names(column_names);
        let columns: Vec<OutputColumn> = column_names
            .iter()
            .zip(normalized_names.into_iter())
            .filter_map(|(column_name, output_name)| {
                self.get_column_info_any_table(column_name)
                    .map(|(_, _, data_type)| OutputColumn {
                        name: output_name,
                        data_type,
                        is_nullable: true,
                    })
            })
            .collect();

        Ok(QueryOutput {
            schema: self.output_schema_context()?,
            columns,
        })
    }

    fn build_join_output(&self) -> Result<QueryOutput, JsValue> {
        let main_schema = self.representative_schema()?;
        let mut sources = alloc::vec![(main_schema.name().to_string(), main_schema.clone(), false)];

        {
            let cache = self.cache.borrow();
            for join in &self.joins {
                let join_store = cache.get_table(&join.table).ok_or_else(|| {
                    JsValue::from_str(&alloc::format!("Join table not found: {}", join.table))
                })?;
                sources.push((
                    join.reference_name().to_string(),
                    join_store.schema().clone(),
                    true,
                ));
            }
        }

        let mut name_counts: hashbrown::HashMap<&str, usize> = hashbrown::HashMap::new();
        for (_, schema, _) in &sources {
            for col in schema.columns() {
                *name_counts.entry(col.name()).or_insert(0) += 1;
            }
        }

        let mut columns = Vec::new();
        for (source_name, schema, force_nullable) in &sources {
            for col in schema.columns() {
                let output_name = if name_counts.get(col.name()).copied().unwrap_or(0) > 1 {
                    alloc::format!("{}.{}", source_name, col.name())
                } else {
                    col.name().to_string()
                };
                columns.push(OutputColumn {
                    name: output_name,
                    data_type: col.data_type(),
                    is_nullable: *force_nullable || col.is_nullable(),
                });
            }
        }

        Ok(QueryOutput {
            schema: main_schema,
            columns,
        })
    }

    fn aggregate_output_type(func: AggregateFunc, input_type: Option<DataType>) -> DataType {
        match func {
            AggregateFunc::Count | AggregateFunc::Distinct => DataType::Int64,
            AggregateFunc::Avg | AggregateFunc::StdDev | AggregateFunc::GeoMean => {
                DataType::Float64
            }
            AggregateFunc::Sum => match input_type {
                Some(DataType::Float64) => DataType::Float64,
                _ => DataType::Int64,
            },
            AggregateFunc::Min | AggregateFunc::Max => input_type.unwrap_or(DataType::Float64),
        }
    }

    fn describe_output(&self) -> Result<QueryOutput, JsValue> {
        if !self.group_by_cols.is_empty() || !self.aggregates.is_empty() {
            let group_columns = self.group_by_cols.iter().filter_map(|col| {
                self.get_column_info_for_projection(col)
                    .map(|(_, _, data_type)| OutputColumn {
                        name: if let Some(dot_pos) = col.find('.') {
                            col[dot_pos + 1..].to_string()
                        } else {
                            col.clone()
                        },
                        data_type,
                        is_nullable: true,
                    })
            });

            let aggregate_columns = self.aggregates.iter().map(|(func, col_opt)| {
                let input_type = col_opt
                    .as_deref()
                    .and_then(|col| self.get_column_info_for_projection(col))
                    .map(|(_, _, data_type)| data_type);
                let name = if let Some(col) = col_opt {
                    let simple_name = if let Some(dot_pos) = col.find('.') {
                        &col[dot_pos + 1..]
                    } else {
                        col.as_str()
                    };
                    alloc::format!(
                        "{}_{}",
                        match func {
                            AggregateFunc::Count => "count",
                            AggregateFunc::Sum => "sum",
                            AggregateFunc::Avg => "avg",
                            AggregateFunc::Min => "min",
                            AggregateFunc::Max => "max",
                            AggregateFunc::Distinct => "distinct",
                            AggregateFunc::StdDev => "stddev",
                            AggregateFunc::GeoMean => "geomean",
                        },
                        simple_name
                    )
                } else {
                    "count".to_string()
                };

                OutputColumn {
                    name,
                    data_type: Self::aggregate_output_type(*func, input_type),
                    is_nullable: true,
                }
            });

            return Ok(QueryOutput {
                schema: self.output_schema_context()?,
                columns: group_columns.chain(aggregate_columns).collect(),
            });
        }

        if let Some(cols) = self.parse_columns() {
            return self.build_projection_output(&cols);
        }

        if let Some(base) = &self.frozen_base {
            return Ok(base.output.clone());
        }

        if !self.joins.is_empty() {
            return self.build_join_output();
        }

        let schema = self.representative_schema()?;
        let columns = schema
            .columns()
            .iter()
            .map(|col| OutputColumn {
                name: col.name().to_string(),
                data_type: col.data_type(),
                is_nullable: col.is_nullable(),
            })
            .collect();

        Ok(QueryOutput { schema, columns })
    }

    /// Creates a SchemaLayout for projected columns, supporting multi-table column references.
    fn create_projection_layout(
        &self,
        column_names: &[String],
    ) -> crate::binary_protocol::SchemaLayout {
        use crate::binary_protocol::{BinaryDataType, ColumnLayout, SchemaLayout};

        // Extract just the column part from qualified names and count occurrences
        let mut name_counts: hashbrown::HashMap<&str, usize> = hashbrown::HashMap::new();
        for col_name in column_names {
            let simple_name = if let Some(dot_pos) = col_name.find('.') {
                &col_name[dot_pos + 1..]
            } else {
                col_name.as_str()
            };
            *name_counts.entry(simple_name).or_insert(0) += 1;
        }

        let mut columns: Vec<ColumnLayout> = Vec::new();
        let mut offset = 0usize;

        for name in column_names {
            // Look up column info from any table
            if let Some((_, _, data_type)) = self.get_column_info_any_table(name) {
                let binary_type = BinaryDataType::from(data_type);
                let fixed_size = binary_type.fixed_size();

                // Determine the final column name - use simple name when unique
                let final_name = if let Some(dot_pos) = name.find('.') {
                    let simple_name = &name[dot_pos + 1..];
                    if name_counts.get(simple_name).copied().unwrap_or(0) > 1 {
                        // Duplicate - keep qualified name
                        name.clone()
                    } else {
                        // Unique - use simple name
                        simple_name.to_string()
                    }
                } else {
                    name.clone()
                };

                columns.push(ColumnLayout {
                    name: final_name,
                    data_type: binary_type,
                    fixed_size,
                    is_nullable: true, // Conservative: assume nullable
                    offset,
                });
                offset += fixed_size;
            }
        }

        let null_mask_size = (columns.len() + 7) / 8;
        let data_size: usize = columns.iter().map(|c| c.fixed_size).sum();
        let row_stride = null_mask_size + data_size;

        SchemaLayout::new(columns, row_stride, null_mask_size)
    }

    fn binary_output_layout(
        &self,
        table_name: &str,
        schema: &Table,
    ) -> Result<SchemaLayout, JsValue> {
        if self.frozen_base.is_some()
            || !self.joins.is_empty()
            || !self.group_by_cols.is_empty()
            || !self.aggregates.is_empty()
        {
            return Ok(self.describe_output()?.layout());
        }

        if let Some(cols) = self.parse_columns() {
            Ok(self.create_projection_layout(&cols))
        } else {
            Ok(self
                .schema_layout_cache
                .borrow_mut()
                .get_or_create_full(table_name, schema)
                .clone())
        }
    }

    fn uses_full_row_mapping(&self) -> bool {
        self.frozen_base.is_none()
            && self.joins.is_empty()
            && self.group_by_cols.is_empty()
            && self.aggregates.is_empty()
            && self.parse_columns().is_none()
    }

    fn build_result_mapper(&self, schema: &Table) -> Result<QueryResultMapper, JsValue> {
        if self.uses_full_row_mapping() {
            Ok(QueryResultMapper::Full {
                schema: schema.clone(),
            })
        } else {
            Ok(QueryResultMapper::Columns {
                column_names: self.describe_output()?.column_names(),
            })
        }
    }

    fn map_rows_to_js(&self, rows: &[Rc<Row>], schema: &Table) -> Result<JsValue, JsValue> {
        Ok(self.build_result_mapper(schema)?.map_rows(rows))
    }

    /// Gets column info for JOIN conditions, checking both the main table and the join table.
    /// Returns (table_name, column_index, data_type).
    ///
    /// For JOIN conditions like `col('dept_id').eq('id')`:
    /// - The left column (dept_id) should come from the main table
    /// - The right column (id) should come from the join table
    ///
    /// Also supports qualified column names like `col('orders.year')`.
    fn get_column_info_for_join(
        &self,
        col_name: &str,
        join_table: &str,
    ) -> Option<(String, usize, DataType)> {
        // Check if column name is qualified (contains '.')
        if let Some(dot_pos) = col_name.find('.') {
            let table_part = &col_name[..dot_pos];
            let col_part = &col_name[dot_pos + 1..];

            // Try to find the table and column
            if let Some(info) = self.cache.borrow().get_table(table_part).and_then(|store| {
                store
                    .schema()
                    .get_column(col_part)
                    .map(|c| (table_part.to_string(), c.index(), c.data_type()))
            }) {
                return Some(info);
            }

            // Also check if it matches the main table
            if let Some(table_name) = &self.from_table {
                if table_name == table_part {
                    if let Some(schema) = self.get_schema() {
                        if let Some(col) = schema.get_column(col_part) {
                            return Some((table_name.clone(), col.index(), col.data_type()));
                        }
                    }
                }
            }
        }

        // First try the join table (for the right side of JOIN conditions)
        if let Some(info) = self.cache.borrow().get_table(join_table).and_then(|store| {
            store
                .schema()
                .get_column(col_name)
                .map(|c| (join_table.to_string(), c.index(), c.data_type()))
        }) {
            return Some(info);
        }

        // Then try the main table
        if let Some(table_name) = &self.from_table {
            if let Some(schema) = self.get_schema() {
                if let Some(col) = schema.get_column(col_name) {
                    return Some((table_name.clone(), col.index(), col.data_type()));
                }
            }
        }

        None
    }

    /// Gets column info for JOIN conditions with pre-computed table offsets and alias support.
    /// This is used for multi-table JOINs where column indices need to account for
    /// the combined row structure and table aliases.
    fn get_column_info_for_join_with_offsets_alias(
        &self,
        col_name: &str,
        current_join: &JoinClause,
        _table_offsets: &hashbrown::HashMap<String, usize>,
    ) -> Option<(String, usize, DataType)> {
        let current_ref_name = current_join.reference_name();

        // Check if column name is qualified (contains '.')
        if let Some(dot_pos) = col_name.find('.') {
            let table_part = &col_name[..dot_pos];
            let col_part = &col_name[dot_pos + 1..];

            // Check if table_part matches the current join's reference name (alias or table)
            if table_part == current_ref_name {
                if let Some(store) = self.cache.borrow().get_table(&current_join.table) {
                    if let Some(col) = store.schema().get_column(col_part) {
                        // Current join table uses original index (no offset)
                        // Return actual table name for Relation compatibility
                        return Some((current_join.table.clone(), col.index(), col.data_type()));
                    }
                }
            }

            // Check if table_part matches the main table
            if let Some(main_table) = &self.from_table {
                if table_part == main_table {
                    if let Some(schema) = self.get_schema() {
                        if let Some(col) = schema.get_column(col_part) {
                            return Some((main_table.clone(), col.index(), col.data_type()));
                        }
                    }
                }
            }

            // Check if table_part matches any other join's reference name
            for join in &self.joins {
                let ref_name = join.reference_name();
                if table_part == ref_name && ref_name != current_ref_name {
                    if let Some(store) = self.cache.borrow().get_table(&join.table) {
                        if let Some(col) = store.schema().get_column(col_part) {
                            return Some((join.table.clone(), col.index(), col.data_type()));
                        }
                    }
                }
            }

            // Try direct table lookup (for cases without alias)
            if let Some(store) = self.cache.borrow().get_table(table_part) {
                if let Some(col) = store.schema().get_column(col_part) {
                    return Some((table_part.to_string(), col.index(), col.data_type()));
                }
            }
        }

        // For unqualified column names, try current join table first
        if let Some(store) = self.cache.borrow().get_table(&current_join.table) {
            if let Some(col) = store.schema().get_column(col_name) {
                // Current join table uses original index (no offset)
                return Some((current_ref_name.to_string(), col.index(), col.data_type()));
            }
        }

        // Then try the main table
        if let Some(table_name) = &self.from_table {
            if let Some(schema) = self.get_schema() {
                if let Some(col) = schema.get_column(col_name) {
                    return Some((table_name.clone(), col.index(), col.data_type()));
                }
            }
        }

        None
    }

    fn clear_query_modifiers(&mut self) {
        self.columns = JsValue::UNDEFINED;
        self.where_clause = None;
        self.order_by.clear();
        self.limit_val = None;
        self.offset_val = None;
        self.group_by_cols.clear();
        self.aggregates.clear();
    }

    fn compose_union(mut self, other: &SelectBuilder, all: bool) -> Result<Self, JsValue> {
        let left_table = self
            .from_table
            .clone()
            .ok_or_else(|| JsValue::from_str("Left side of UNION is missing FROM"))?;
        let right_table = other
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("Right side of UNION is missing FROM"))?;

        let left_plan = self.build_logical_plan(&left_table);
        let right_plan = other.build_logical_plan(right_table);
        let left_output = self.describe_output()?;
        let right_output = other.describe_output()?;

        if !left_output.is_compatible_with(&right_output) {
            return Err(JsValue::from_str(
                "UNION operands must produce the same number of columns with matching types",
            ));
        }

        self.frozen_base = Some(FrozenQueryBase {
            plan: LogicalPlan::union(left_plan, right_plan, all),
            output: left_output,
        });
        self.clear_query_modifiers();
        Ok(self)
    }
}

#[wasm_bindgen]
impl SelectBuilder {
    /// Sets the FROM table.
    pub fn from(mut self, table: &str) -> Self {
        self.from_table = Some(table.to_string());
        self
    }

    /// Sets or extends the WHERE clause.
    /// Multiple calls to where_() are combined with AND.
    #[wasm_bindgen(js_name = "where")]
    pub fn where_(mut self, predicate: &Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => Expr::and(&existing, predicate),
            None => predicate.clone(),
        });
        self
    }

    /// Adds an ORDER BY clause.
    #[wasm_bindgen(js_name = orderBy)]
    pub fn order_by(mut self, column: &str, order: JsSortOrder) -> Self {
        self.order_by.push((column.to_string(), order.into()));
        self
    }

    /// Sets the LIMIT.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit_val = Some(n);
        self
    }

    /// Sets the OFFSET.
    pub fn offset(mut self, n: usize) -> Self {
        self.offset_val = Some(n);
        self
    }

    /// Combines this query with another query using UNION (distinct).
    pub fn union(self, other: &SelectBuilder) -> Result<Self, JsValue> {
        self.compose_union(other, false)
    }

    /// Combines this query with another query using UNION ALL.
    #[wasm_bindgen(js_name = unionAll)]
    pub fn union_all(self, other: &SelectBuilder) -> Result<Self, JsValue> {
        self.compose_union(other, true)
    }

    /// Parses a table specification that may include an alias.
    /// Supports formats: "table_name" or "table_name as alias"
    fn parse_table_spec(table_spec: &str) -> (String, Option<String>) {
        // Check for " as " (case insensitive)
        let lower = table_spec.to_lowercase();
        if let Some(pos) = lower.find(" as ") {
            let table = table_spec[..pos].trim().to_string();
            let alias = table_spec[pos + 4..].trim().to_string();
            (table, Some(alias))
        } else {
            (table_spec.trim().to_string(), None)
        }
    }

    /// Adds an INNER JOIN.
    #[wasm_bindgen(js_name = innerJoin)]
    pub fn inner_join(mut self, table: &str, condition: &Expr) -> Self {
        let (table_name, alias) = Self::parse_table_spec(table);
        self.joins.push(JoinClause {
            table: table_name,
            alias,
            condition: condition.clone(),
            join_type: JoinType::Inner,
        });
        self
    }

    /// Adds a LEFT JOIN.
    #[wasm_bindgen(js_name = leftJoin)]
    pub fn left_join(mut self, table: &str, condition: &Expr) -> Self {
        let (table_name, alias) = Self::parse_table_spec(table);
        self.joins.push(JoinClause {
            table: table_name,
            alias,
            condition: condition.clone(),
            join_type: JoinType::Left,
        });
        self
    }

    /// Adds a GROUP BY clause.
    #[wasm_bindgen(js_name = groupBy)]
    pub fn group_by(mut self, columns: &JsValue) -> Self {
        if let Some(arr) = columns.dyn_ref::<js_sys::Array>() {
            self.group_by_cols = arr.iter().filter_map(|v| v.as_string()).collect();
        } else if let Some(s) = columns.as_string() {
            self.group_by_cols = alloc::vec![s];
        }
        self
    }

    /// Adds a COUNT(*) aggregate.
    #[wasm_bindgen(js_name = count)]
    pub fn count(mut self) -> Self {
        self.aggregates.push((AggregateFunc::Count, None));
        self
    }

    /// Adds a COUNT(column) aggregate.
    #[wasm_bindgen(js_name = countCol)]
    pub fn count_col(mut self, column: &str) -> Self {
        self.aggregates
            .push((AggregateFunc::Count, Some(column.to_string())));
        self
    }

    /// Adds a SUM(column) aggregate.
    #[wasm_bindgen(js_name = sum)]
    pub fn sum(mut self, column: &str) -> Self {
        self.aggregates
            .push((AggregateFunc::Sum, Some(column.to_string())));
        self
    }

    /// Adds an AVG(column) aggregate.
    #[wasm_bindgen(js_name = avg)]
    pub fn avg(mut self, column: &str) -> Self {
        self.aggregates
            .push((AggregateFunc::Avg, Some(column.to_string())));
        self
    }

    /// Adds a MIN(column) aggregate.
    #[wasm_bindgen(js_name = min)]
    pub fn min(mut self, column: &str) -> Self {
        self.aggregates
            .push((AggregateFunc::Min, Some(column.to_string())));
        self
    }

    /// Adds a MAX(column) aggregate.
    #[wasm_bindgen(js_name = max)]
    pub fn max(mut self, column: &str) -> Self {
        self.aggregates
            .push((AggregateFunc::Max, Some(column.to_string())));
        self
    }

    /// Adds a STDDEV(column) aggregate.
    #[wasm_bindgen(js_name = stddev)]
    pub fn stddev(mut self, column: &str) -> Self {
        self.aggregates
            .push((AggregateFunc::StdDev, Some(column.to_string())));
        self
    }

    /// Adds a GEOMEAN(column) aggregate.
    #[wasm_bindgen(js_name = geomean)]
    pub fn geomean(mut self, column: &str) -> Self {
        self.aggregates
            .push((AggregateFunc::GeoMean, Some(column.to_string())));
        self
    }

    /// Adds a DISTINCT(column) aggregate (returns count of distinct values).
    #[wasm_bindgen(js_name = distinct)]
    pub fn distinct(mut self, column: &str) -> Self {
        self.aggregates
            .push((AggregateFunc::Distinct, Some(column.to_string())));
        self
    }

    /// Executes the query and returns results.
    pub async fn exec(&self) -> Result<JsValue, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache = self.cache.borrow();
        let store = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        // Build logical plan using query engine
        // ORDER BY, LIMIT, and OFFSET are now handled in the logical plan
        let plan = self.build_logical_plan(table_name);

        // Execute using query engine (with index optimization)
        let rows = execute_plan(&cache, table_name, plan)
            .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?;
        let schema = store.schema().clone();
        self.map_rows_to_js(&rows, &schema)
    }

    /// Compiles the current query into a reusable prepared handle.
    pub fn prepare(&self) -> Result<PreparedSelectQuery, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache = self.cache.borrow();
        let store = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        let plan = self.build_logical_plan(table_name);
        let result_mapper = self.build_result_mapper(store.schema())?;
        let binary_layout = self.binary_output_layout(table_name, store.schema())?;
        let compiled_plan =
            self.get_or_compile_cached_plan(&cache, table_name, plan, CompilePlanProfile::Default);

        Ok(PreparedSelectQuery {
            cache: self.cache.clone(),
            compiled_plan,
            result_mapper,
            binary_layout,
        })
    }

    /// Explains the query plan without executing it.
    ///
    /// Returns an object with:
    /// - `logical`: The original logical plan
    /// - `optimized`: The optimized logical plan (after index selection, etc.)
    /// - `physical`: The final physical execution plan
    pub fn explain(&self) -> Result<JsValue, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache = self.cache.borrow();
        let _ = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        // Build logical plan
        let plan = self.build_logical_plan(table_name);

        // Get explain result
        let result = explain_plan(&cache, table_name, plan);

        // Convert to JS object
        let obj = js_sys::Object::new();
        js_sys::Reflect::set(&obj, &"logical".into(), &result.logical_plan.into())?;
        js_sys::Reflect::set(&obj, &"optimized".into(), &result.optimized_plan.into())?;
        js_sys::Reflect::set(&obj, &"physical".into(), &result.physical_plan.into())?;

        Ok(obj.into())
    }

    /// Creates an observable query using the cached execution path.
    /// When data changes, the engine reuses the compiled plan and can apply
    /// row-local patches for simple single-table pipelines instead of always
    /// re-executing the full query.
    pub fn observe(&self) -> Result<JsObservableQuery, JsValue> {
        #[cfg(feature = "benchmark")]
        let observe_started_at = now_ms();
        #[cfg(feature = "benchmark")]
        let mut snapshot_init_profile = SnapshotInitProfile::default();
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache_ref = self.cache.clone();
        let cache = cache_ref.borrow();
        let store = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        // Build logical plan and compile to a cached execution artifact for re-execution.
        #[cfg(feature = "benchmark")]
        let logical_plan_started_at = now_ms();
        let logical_plan = self.build_logical_plan(table_name);
        #[cfg(feature = "benchmark")]
        {
            snapshot_init_profile.logical_plan_ms = now_ms() - logical_plan_started_at;
        }
        #[cfg(feature = "benchmark")]
        let describe_output_started_at = now_ms();
        let output = self.describe_output()?;
        #[cfg(feature = "benchmark")]
        {
            snapshot_init_profile.describe_output_ms = now_ms() - describe_output_started_at;
        }
        let output_columns = output.column_names();
        let schema = store.schema().clone();
        #[cfg(feature = "benchmark")]
        let binary_layout_started_at = now_ms();
        let binary_layout = if self.frozen_base.is_some() {
            output.layout()
        } else if self.joins.is_empty() {
            if let Some(cols) = self.parse_columns() {
                SchemaLayout::from_projection(&schema, &cols)
            } else {
                SchemaLayout::from_schema(&schema)
            }
        } else {
            let mut schemas: Vec<&Table> = Vec::with_capacity(1 + self.joins.len());
            schemas.push(store.schema());
            for join in &self.joins {
                let join_store = cache.get_table(&join.table).ok_or_else(|| {
                    JsValue::from_str(&alloc::format!("Join table not found: {}", join.table))
                })?;
                schemas.push(join_store.schema());
            }
            SchemaLayout::from_schemas(&schemas)
        };
        #[cfg(feature = "benchmark")]
        {
            snapshot_init_profile.binary_layout_ms = now_ms() - binary_layout_started_at;
        }
        #[cfg(feature = "benchmark")]
        let partial_refresh_started_at = now_ms();
        let partial_refresh = self.build_rows_snapshot_partial_refresh_plan(&output)?;
        #[cfg(feature = "benchmark")]
        {
            snapshot_init_profile.partial_refresh_plan_ms = now_ms() - partial_refresh_started_at;
            snapshot_init_profile.partial_refresh_enabled = partial_refresh.is_some();
        }
        let compiled_logical_plan = if let Some(partial) = &partial_refresh {
            Self::rewrite_limit_for_candidate_window(logical_plan.clone(), partial.candidate_limit)
        } else {
            logical_plan.clone()
        };
        #[cfg(feature = "benchmark")]
        let compile_main_plan_started_at = now_ms();
        let compiled_plan = self.get_or_compile_cached_plan(
            &cache,
            table_name,
            compiled_logical_plan,
            CompilePlanProfile::Default,
        );
        #[cfg(feature = "benchmark")]
        {
            snapshot_init_profile.compile_main_plan_ms = now_ms() - compile_main_plan_started_at;
        }
        #[cfg(feature = "benchmark")]
        let root_subset_started_at = now_ms();
        let root_subset_refresh = if partial_refresh.is_none() {
            self.build_rows_snapshot_root_subset_plan(
                &cache,
                &output,
                &logical_plan,
                &compiled_plan,
            )?
        } else {
            None
        };
        #[cfg(feature = "benchmark")]
        {
            snapshot_init_profile.root_subset_plan_ms = now_ms() - root_subset_started_at;
            snapshot_init_profile.root_subset_enabled = root_subset_refresh.is_some();
        }

        // Get initial result using the compiled plan artifact.
        #[cfg(feature = "benchmark")]
        let initial_query_started_at = now_ms();
        let initial_output = execute_compiled_physical_plan_with_summary(&cache, &compiled_plan)
            .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?;
        #[cfg(feature = "benchmark")]
        {
            snapshot_init_profile.initial_query_ms = now_ms() - initial_query_started_at;
            snapshot_init_profile.initial_row_count = initial_output.rows.len();
        }

        #[cfg(feature = "benchmark")]
        let dependency_bindings_started_at = now_ms();
        let (dependencies, dependency_table_bindings) = {
            let table_id_map = self.table_id_map.borrow();
            let mut bindings = logical_plan
                .collect_tables()
                .into_iter()
                .map(|table| {
                    table_id_map
                        .get(&table)
                        .copied()
                        .map(|table_id| (table_id, table.clone()))
                        .ok_or_else(|| {
                            JsValue::from_str(&alloc::format!("Table ID not found: {}", table))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            bindings.sort_unstable_by(|(left_id, left_name), (right_id, right_name)| {
                left_id
                    .cmp(right_id)
                    .then_with(|| left_name.cmp(right_name))
            });
            bindings.dedup_by(|left, right| left.0 == right.0);
            let table_ids = bindings.iter().map(|(table_id, _)| *table_id).collect();
            (LiveDependencySet::snapshot(table_ids), bindings)
        };
        #[cfg(feature = "benchmark")]
        {
            snapshot_init_profile.dependency_bindings_ms =
                now_ms() - dependency_bindings_started_at;
        }

        let projection = if self.frozen_base.is_some()
            || !self.aggregates.is_empty()
            || !self.group_by_cols.is_empty()
        {
            RowsProjection::Projection {
                schema: output.schema,
                columns: output_columns,
            }
        } else if let Some(cols) = self.parse_columns() {
            RowsProjection::Projection {
                schema,
                columns: cols,
            }
        } else {
            RowsProjection::Full { schema }
        };

        drop(cache); // Release borrow

        #[cfg(feature = "benchmark")]
        let initial_result_adapt_started_at = now_ms();
        let (initial_rows, initial_summary, partial_refresh) =
            if let Some(partial) = partial_refresh {
                let visible_rows: Vec<Rc<Row>> = initial_output
                    .rows
                    .iter()
                    .skip(partial.visible_offset)
                    .take(partial.visible_limit)
                    .cloned()
                    .collect();
                let visible_summary = QueryResultSummary::from_rows(&visible_rows);
                (
                    visible_rows,
                    visible_summary,
                    Some(RowsSnapshotPartialRefreshState {
                        metadata: partial,
                        initial_candidate_rows: initial_output.rows,
                    }),
                )
            } else {
                (initial_output.rows, initial_output.summary, None)
            };
        #[cfg(feature = "benchmark")]
        {
            snapshot_init_profile.initial_result_adapt_ms =
                now_ms() - initial_result_adapt_started_at;
            snapshot_init_profile.visible_row_count = initial_rows.len();
        }

        let live_plan = LivePlan::rows_snapshot(
            dependencies,
            compiled_plan,
            initial_rows,
            initial_summary,
            projection,
            binary_layout,
            dependency_table_bindings,
            partial_refresh,
            root_subset_refresh,
        );

        #[cfg(feature = "benchmark")]
        let observable_init_started_at = now_ms();
        let observable =
            live_plan.materialize_rows_snapshot(cache_ref.clone(), self.query_registry.clone());
        #[cfg(feature = "benchmark")]
        {
            snapshot_init_profile.observable_init_ms = now_ms() - observable_init_started_at;
            snapshot_init_profile.total_ms = now_ms() - observe_started_at;
            self.query_registry
                .borrow()
                .record_snapshot_init_profile(snapshot_init_profile);
        }

        Ok(observable)
    }

    /// Creates a changes stream (initial + incremental).
    pub fn changes(&self) -> Result<JsChangesStream, JsValue> {
        let observable = self.observe()?;
        Ok(JsChangesStream::from_observable(observable))
    }

    /// Creates an IVM-based observable query using DBSP incremental dataflow.
    ///
    /// Unlike `observe()` which re-executes the full query on every change (O(result_set)),
    /// `trace()` compiles the query into a dataflow graph and propagates only deltas (O(delta)).
    ///
    /// Returns an error if the query is not incrementalizable (e.g. contains ORDER BY / LIMIT).
    pub fn trace(&self) -> Result<JsIvmObservableQuery, JsValue> {
        let trace_started_at = now_ms();
        let mut trace_init_profile = TraceInitProfile::default();
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache_ref = self.cache.clone();
        let cache = cache_ref.borrow();
        let store = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        // Build logical plan and compile to physical plan
        let logical_plan = self.build_logical_plan(table_name);
        let output = self.describe_output()?;
        let output_columns = output.column_names();
        let schema = store.schema().clone();
        let binary_layout = if self.frozen_base.is_some() {
            output.layout()
        } else if self.joins.is_empty() {
            if let Some(cols) = self.parse_columns() {
                SchemaLayout::from_projection(&schema, &cols)
            } else {
                SchemaLayout::from_schema(&schema)
            }
        } else {
            let mut schemas: Vec<&Table> = Vec::with_capacity(1 + self.joins.len());
            schemas.push(store.schema());
            for join in &self.joins {
                let join_store = cache.get_table(&join.table).ok_or_else(|| {
                    JsValue::from_str(&alloc::format!("Join table not found: {}", join.table))
                })?;
                schemas.push(join_store.schema());
            }
            SchemaLayout::from_schemas(&schemas)
        };
        let compile_plan_started_at = now_ms();
        let physical_plan = compile_plan(&cache, table_name, logical_plan);
        trace_init_profile.compile_plan_ms = now_ms() - compile_plan_started_at;
        let mut table_schemas = hashbrown::HashMap::new();
        table_schemas.insert(table_name.clone(), store.schema().clone());
        for join in &self.joins {
            let join_store = cache.get_table(&join.table).ok_or_else(|| {
                JsValue::from_str(&alloc::format!("Join table not found: {}", join.table))
            })?;
            table_schemas.insert(join.table.clone(), join_store.schema().clone());
        }

        // Compile physical plan to dataflow — errors if not incrementalizable
        let table_id_map = self.table_id_map.borrow();
        let compile_to_dataflow_started_at = now_ms();
        let compile_result = compile_to_dataflow(&physical_plan, &table_id_map, &table_schemas)
            .ok_or_else(|| JsValue::from_str(
                "Query is not incrementalizable (contains ORDER BY, LIMIT, or other non-streamable operators). Use observe() instead."
            ))?;
        trace_init_profile.compile_to_dataflow_ms = now_ms() - compile_to_dataflow_started_at;

        let compile_trace_program_started_at = now_ms();
        let compiled_ivm_plan = CompiledIvmPlan::compile(&compile_result.dataflow);
        let compile_ivm_finished_at = now_ms();
        trace_init_profile.compile_ivm_plan_ms =
            compile_ivm_finished_at - compile_trace_program_started_at;
        let compiled_bootstrap_plan = CompiledBootstrapPlan::compile(&compile_result.dataflow);
        trace_init_profile.compile_trace_program_ms = now_ms() - compile_trace_program_started_at;

        // Get initial result using the compiled physical plan
        let initial_query_started_at = now_ms();
        let initial_rows = execute_physical_plan(&cache, &physical_plan)
            .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?;
        trace_init_profile.initial_query_ms = now_ms() - initial_query_started_at;

        let dependencies =
            LiveDependencySet::snapshot(compile_result.table_ids.values().copied().collect());
        let source_bindings = collect_trace_bootstrap_source_bindings(
            &physical_plan,
            &compile_result.table_ids,
            &table_schemas,
        );
        drop(cache);
        drop(table_id_map);

        let projection = if self.frozen_base.is_some()
            || !self.aggregates.is_empty()
            || !self.group_by_cols.is_empty()
        {
            RowsProjection::Projection {
                schema: output.schema,
                columns: output_columns,
            }
        } else if let Some(cols) = self.parse_columns() {
            RowsProjection::Projection {
                schema,
                columns: cols,
            }
        } else {
            RowsProjection::Full { schema }
        };

        let initial_row_count = initial_rows.len();
        let live_plan = LivePlan::rows_delta(
            dependencies,
            compile_result.dataflow,
            compiled_ivm_plan,
            compiled_bootstrap_plan,
            initial_rows,
            source_bindings,
            projection,
            binary_layout,
            TraceInitProfile {
                total_ms: now_ms() - trace_started_at,
                initial_row_count,
                ..trace_init_profile
            },
        );

        Ok(live_plan.materialize_rows_delta(self.cache.clone(), self.query_registry.clone()))
    }

    /// Gets the schema layout for binary decoding.
    /// The layout can be cached by JS for repeated queries on the same table.
    #[wasm_bindgen(js_name = getSchemaLayout)]
    pub fn get_schema_layout(&self) -> Result<crate::binary_protocol::SchemaLayout, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache = self.cache.borrow();
        let store = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        let schema = store.schema();
        self.binary_output_layout(table_name, schema)
    }

    /// Executes the query and returns a binary result buffer.
    /// Use with getSchemaLayout() for zero-copy decoding in JS.
    #[wasm_bindgen(js_name = execBinary)]
    pub async fn exec_binary(&self) -> Result<crate::binary_protocol::BinaryResult, JsValue> {
        let table_name = self
            .from_table
            .as_ref()
            .ok_or_else(|| JsValue::from_str("FROM table not specified"))?;

        let cache = self.cache.borrow();
        let store = cache
            .get_table(table_name)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?;

        // Build logical plan
        let plan = self.build_logical_plan(table_name);
        let schema = store.schema();
        let layout = self.binary_output_layout(table_name, schema)?;

        let compiled_plan =
            self.get_or_compile_cached_plan(&cache, table_name, plan, CompilePlanProfile::Default);
        let rows = execute_compiled_physical_plan(&cache, &compiled_plan)
            .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?;

        // Encode to binary
        let mut encoder = crate::binary_protocol::BinaryEncoder::new(layout, rows.len());
        encoder.encode_rows(&rows);
        let buffer = encoder.finish();

        Ok(crate::binary_protocol::BinaryResult::new(buffer))
    }
}

#[wasm_bindgen]
impl PreparedSelectQuery {
    /// Executes the prepared query and returns JS objects.
    pub async fn exec(&self) -> Result<JsValue, JsValue> {
        let cache = self.cache.borrow();
        let rows = execute_compiled_physical_plan(&cache, &self.compiled_plan)
            .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?;
        Ok(self.result_mapper.map_rows(&rows))
    }

    /// Executes the prepared query and returns a binary result buffer.
    #[wasm_bindgen(js_name = execBinary)]
    pub async fn exec_binary(&self) -> Result<crate::binary_protocol::BinaryResult, JsValue> {
        let cache = self.cache.borrow();
        let rows = execute_compiled_physical_plan(&cache, &self.compiled_plan)
            .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?;

        let mut encoder =
            crate::binary_protocol::BinaryEncoder::new(self.binary_layout.clone(), rows.len());
        encoder.encode_rows(&rows);
        Ok(crate::binary_protocol::BinaryResult::new(encoder.finish()))
    }

    /// Gets the schema layout for binary decoding.
    #[wasm_bindgen(js_name = getSchemaLayout)]
    pub fn get_schema_layout(&self) -> crate::binary_protocol::SchemaLayout {
        self.binary_layout.clone()
    }
}

/// INSERT query builder.
#[wasm_bindgen]
pub struct InsertBuilder {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<LiveRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    #[cfg(feature = "benchmark")]
    last_insert_profile: Rc<RefCell<Option<StorageInsertProfile>>>,
    table_name: String,
    values_data: Option<JsValue>,
}

impl InsertBuilder {
    pub(crate) fn new(
        cache: Rc<RefCell<TableCache>>,
        query_registry: Rc<RefCell<LiveRegistry>>,
        table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
        #[cfg(feature = "benchmark")] last_insert_profile: Rc<
            RefCell<Option<StorageInsertProfile>>,
        >,
        table: &str,
    ) -> Self {
        Self {
            cache,
            query_registry,
            table_id_map,
            #[cfg(feature = "benchmark")]
            last_insert_profile,
            table_name: table.to_string(),
            values_data: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InsertExecutionStats {
    row_count: usize,
}

fn execute_insert_values(
    cache: &Rc<RefCell<TableCache>>,
    query_registry: &Rc<RefCell<LiveRegistry>>,
    table_id_map: &Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    #[cfg(feature = "benchmark")] last_insert_profile: &Rc<RefCell<Option<StorageInsertProfile>>>,
    table_name: &str,
    values: &JsValue,
) -> Result<InsertExecutionStats, JsValue> {
    let schema = cache
        .borrow()
        .get_table(table_name)
        .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table_name)))?
        .schema()
        .clone();

    let arr = js_sys::Array::from(values);
    let start_row_id = reserve_row_ids(arr.length() as u64);
    let rows = js_array_to_rows(values, &schema, start_row_id)?;

    execute_insert_rows(
        cache,
        query_registry,
        table_id_map,
        #[cfg(feature = "benchmark")]
        last_insert_profile,
        table_name,
        rows,
    )
    .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))
}

fn execute_insert_rows(
    cache: &Rc<RefCell<TableCache>>,
    query_registry: &Rc<RefCell<LiveRegistry>>,
    table_id_map: &Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    #[cfg(feature = "benchmark")] last_insert_profile: &Rc<RefCell<Option<StorageInsertProfile>>>,
    table_name: &str,
    rows: Vec<Row>,
) -> cynos_core::Result<InsertExecutionStats> {
    let table_id = table_id_map.borrow().get(table_name).copied();
    let should_notify = table_id.is_some() && query_registry.borrow().query_count() > 0;
    let row_count = rows.len();

    let inserted_ids = should_notify.then(|| {
        rows.iter()
            .map(|row| row.id())
            .collect::<hashbrown::HashSet<_>>()
    });
    let deltas = should_notify.then(|| {
        rows.iter()
            .map(|row| Delta::insert(row.clone()))
            .collect::<Vec<_>>()
    });

    let mut cache = cache.borrow_mut();
    let store = cache
        .get_table_mut(table_name)
        .ok_or_else(|| cynos_core::Error::table_not_found(table_name))?;
    #[cfg(feature = "benchmark")]
    {
        let (inserted, profile) = store.insert_batch_profiled(rows, now_ms)?;
        debug_assert_eq!(inserted, row_count);
        *last_insert_profile.borrow_mut() = Some(profile);
    }
    #[cfg(not(feature = "benchmark"))]
    {
        store.insert_batch(rows)?;
    }
    drop(cache);

    if let (Some(table_id), Some(inserted_ids), Some(deltas)) = (table_id, inserted_ids, deltas) {
        query_registry
            .borrow_mut()
            .on_table_change_delta(table_id, deltas, &inserted_ids);
    }

    Ok(InsertExecutionStats { row_count })
}

#[wasm_bindgen]
impl InsertBuilder {
    /// Sets the values to insert.
    pub fn values(mut self, data: &JsValue) -> Self {
        self.values_data = Some(data.clone());
        self
    }

    /// Executes the insert operation.
    pub async fn exec(&self) -> Result<JsValue, JsValue> {
        let values = self
            .values_data
            .as_ref()
            .ok_or_else(|| JsValue::from_str("No values specified"))?;
        let stats = execute_insert_values(
            &self.cache,
            &self.query_registry,
            &self.table_id_map,
            #[cfg(feature = "benchmark")]
            &self.last_insert_profile,
            &self.table_name,
            values,
        )?;
        Ok(JsValue::from_f64(stats.row_count as f64))
    }
}

/// UPDATE query builder.
#[wasm_bindgen]
pub struct UpdateBuilder {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<LiveRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    table_name: String,
    set_values: Vec<(String, JsValue)>,
    where_clause: Option<Expr>,
}

impl UpdateBuilder {
    pub(crate) fn new(
        cache: Rc<RefCell<TableCache>>,
        query_registry: Rc<RefCell<LiveRegistry>>,
        table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
        table: &str,
    ) -> Self {
        Self {
            cache,
            query_registry,
            table_id_map,
            table_name: table.to_string(),
            set_values: Vec::new(),
            where_clause: None,
        }
    }
}

#[wasm_bindgen]
impl UpdateBuilder {
    /// Sets column values.
    /// Can be called with either:
    /// - An object: set({ column: value, ... })
    /// - Two arguments: set(column, value)
    pub fn set(mut self, column_or_obj: &JsValue, value: Option<JsValue>) -> Self {
        if let Some(val) = value {
            // Two-argument form: set(column, value)
            if let Some(col_name) = column_or_obj.as_string() {
                self.set_values.push((col_name, val));
            }
        } else if let Some(obj) = column_or_obj.dyn_ref::<js_sys::Object>() {
            // Object form: set({ column: value, ... })
            let keys = js_sys::Object::keys(obj);
            for key in keys.iter() {
                if let Some(col_name) = key.as_string() {
                    let value = js_sys::Reflect::get(obj, &key).unwrap_or(JsValue::NULL);
                    self.set_values.push((col_name, value));
                }
            }
        }
        self
    }

    /// Sets or extends the WHERE clause.
    /// Multiple calls to where_() are combined with AND.
    #[wasm_bindgen(js_name = "where")]
    pub fn where_(mut self, predicate: &Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => Expr::and(&existing, predicate),
            None => predicate.clone(),
        });
        self
    }

    /// Executes the update operation.
    pub async fn exec(&self) -> Result<JsValue, JsValue> {
        let schema = {
            let cache = self.cache.borrow();
            let store = cache.get_table(&self.table_name).ok_or_else(|| {
                JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name))
            })?;
            store.schema().clone()
        };

        // Find rows to update using query engine (with index optimization)
        let rows_to_update: Vec<Row> = if let Some(ref predicate) = self.where_clause {
            // Build logical plan: SELECT * FROM table WHERE predicate
            let get_col_info = |name: &str| -> Option<(String, usize, DataType)> {
                schema
                    .get_column(name)
                    .map(|col| (self.table_name.clone(), col.index(), col.data_type()))
            };
            let ast_predicate = predicate.to_ast_with_table(&get_col_info);

            let plan = LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: self.table_name.clone(),
                }),
                predicate: ast_predicate,
            };

            // Execute using query engine (with index optimization)
            let cache = self.cache.borrow();
            execute_plan(&cache, &self.table_name, plan)
                .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?
                .into_iter()
                .map(|rc| (*rc).clone())
                .collect()
        } else {
            // No WHERE clause - update all rows (full scan is necessary)
            let cache = self.cache.borrow();
            let store = cache.get_table(&self.table_name).ok_or_else(|| {
                JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name))
            })?;
            store.scan().map(|rc| (*rc).clone()).collect()
        };

        let mut cache = self.cache.borrow_mut();
        let store = cache.get_table_mut(&self.table_name).ok_or_else(|| {
            JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name))
        })?;

        let mut deltas = Vec::new();
        let mut update_count = 0;
        let mut updated_ids = hashbrown::HashSet::new();

        for old_row in rows_to_update {
            // Create new row with updated values
            let mut new_values = old_row.values().to_vec();

            for (col_name, js_val) in &self.set_values {
                if let Some(col) = schema.get_column(col_name) {
                    let idx = col.index();
                    let value = js_to_value(js_val, col.data_type())?;
                    if idx < new_values.len() {
                        new_values[idx] = value;
                    }
                }
            }

            // Create new row with incremented version
            let new_version = old_row.version().wrapping_add(1);
            let new_row = Row::new_with_version(old_row.id(), new_version, new_values);

            // Build deltas
            deltas.push(Delta::delete(old_row.clone()));
            deltas.push(Delta::insert(new_row.clone()));

            // Track updated row ID
            updated_ids.insert(old_row.id());

            // Update in store
            store
                .update(old_row.id(), new_row)
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

            update_count += 1;
        }

        // Notify query registry with changed IDs and deltas
        if let Some(table_id) = self.table_id_map.borrow().get(&self.table_name).copied() {
            drop(cache);
            self.query_registry
                .borrow_mut()
                .on_table_change_delta(table_id, deltas, &updated_ids);
        }

        Ok(JsValue::from_f64(update_count as f64))
    }
}

/// DELETE query builder.
#[wasm_bindgen]
pub struct DeleteBuilder {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<LiveRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    table_name: String,
    where_clause: Option<Expr>,
}

impl DeleteBuilder {
    pub(crate) fn new(
        cache: Rc<RefCell<TableCache>>,
        query_registry: Rc<RefCell<LiveRegistry>>,
        table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
        table: &str,
    ) -> Self {
        Self {
            cache,
            query_registry,
            table_id_map,
            table_name: table.to_string(),
            where_clause: None,
        }
    }
}

#[wasm_bindgen]
impl DeleteBuilder {
    /// Sets or extends the WHERE clause.
    /// Multiple calls to where_() are combined with AND.
    #[wasm_bindgen(js_name = "where")]
    pub fn where_(mut self, predicate: &Expr) -> Self {
        self.where_clause = Some(match self.where_clause {
            Some(existing) => Expr::and(&existing, predicate),
            None => predicate.clone(),
        });
        self
    }

    /// Executes the delete operation.
    pub async fn exec(&self) -> Result<JsValue, JsValue> {
        let schema = {
            let cache = self.cache.borrow();
            let store = cache.get_table(&self.table_name).ok_or_else(|| {
                JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name))
            })?;
            store.schema().clone()
        };

        // Fast path: DELETE without WHERE clause - use clear() for O(1) deletion
        if self.where_clause.is_none() {
            // Collect all rows for IVM notification before clearing
            let (delete_count, deltas, deleted_ids) = {
                let cache = self.cache.borrow();
                let store = cache.get_table(&self.table_name).ok_or_else(|| {
                    JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name))
                })?;

                let rows: Vec<_> = store.scan().collect();
                let count = rows.len();
                let deltas: Vec<Delta<Row>> =
                    rows.iter().map(|r| Delta::delete((**r).clone())).collect();
                let ids: hashbrown::HashSet<_> = rows.iter().map(|r| r.id()).collect();
                (count, deltas, ids)
            };

            // Clear the table (O(1) operation)
            {
                let mut cache = self.cache.borrow_mut();
                let store = cache.get_table_mut(&self.table_name).ok_or_else(|| {
                    JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name))
                })?;
                store.clear();
            }

            // Notify query registry
            if let Some(table_id) = self.table_id_map.borrow().get(&self.table_name).copied() {
                self.query_registry.borrow_mut().on_table_change_delta(
                    table_id,
                    deltas,
                    &deleted_ids,
                );
            }

            return Ok(JsValue::from_f64(delete_count as f64));
        }

        // Slow path: DELETE with WHERE clause - need to find matching rows
        let rows_to_delete: Vec<Row> = {
            let predicate = self.where_clause.as_ref().unwrap();
            // Build logical plan: SELECT * FROM table WHERE predicate
            let get_col_info = |name: &str| -> Option<(String, usize, DataType)> {
                schema
                    .get_column(name)
                    .map(|col| (self.table_name.clone(), col.index(), col.data_type()))
            };
            let ast_predicate = predicate.to_ast_with_table(&get_col_info);

            let plan = LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: self.table_name.clone(),
                }),
                predicate: ast_predicate,
            };

            // Execute using query engine (with index optimization)
            let cache = self.cache.borrow();
            execute_plan(&cache, &self.table_name, plan)
                .map_err(|e| JsValue::from_str(&alloc::format!("Query execution error: {:?}", e)))?
                .into_iter()
                .map(|rc| (*rc).clone())
                .collect()
        };

        // Collect row IDs for batch deletion
        let row_ids: Vec<_> = rows_to_delete.iter().map(|r| r.id()).collect();
        let deleted_ids: hashbrown::HashSet<_> = row_ids.iter().copied().collect();
        let delete_count = row_ids.len();

        // Build deltas for IVM notification
        let deltas: Vec<Delta<Row>> = rows_to_delete
            .iter()
            .map(|r| Delta::delete(r.clone()))
            .collect();

        // Use batch delete for better performance
        {
            let mut cache = self.cache.borrow_mut();
            let store = cache.get_table_mut(&self.table_name).ok_or_else(|| {
                JsValue::from_str(&alloc::format!("Table not found: {}", self.table_name))
            })?;
            store.delete_batch(&row_ids);
        }

        // Notify query registry with changed IDs and deltas
        if let Some(table_id) = self.table_id_map.borrow().get(&self.table_name).copied() {
            self.query_registry
                .borrow_mut()
                .on_table_change_delta(table_id, deltas, &deleted_ids);
        }

        Ok(JsValue::from_f64(delete_count as f64))
    }
}

// ---------------------------------------------------------------------------
// JSONB helpers for evaluate_predicate
// ---------------------------------------------------------------------------

/// Minimal JSON text parser producing `cynos_jsonb::JsonbValue`.
///
/// Handles: null, true, false, numbers, quoted strings, arrays, objects.
/// This mirrors the logic in `PhysicalPlanRunner::parse_json_str` so that
/// the re-query filter path and the query executor agree on semantics.
fn parse_json_text(s: &str) -> Option<cynos_jsonb::JsonbValue> {
    let s = s.trim();
    if s == "null" {
        return Some(cynos_jsonb::JsonbValue::Null);
    }
    if s == "true" {
        return Some(cynos_jsonb::JsonbValue::Bool(true));
    }
    if s == "false" {
        return Some(cynos_jsonb::JsonbValue::Bool(false));
    }
    if let Ok(n) = s.parse::<f64>() {
        return Some(cynos_jsonb::JsonbValue::Number(n));
    }
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        return Some(cynos_jsonb::JsonbValue::String(unescape_json(inner)));
    }
    if s.starts_with('{') {
        return parse_json_object(s);
    }
    if s.starts_with('[') {
        return parse_json_array(s);
    }
    None
}

fn unescape_json(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
                Some('r') => result.push('\r'),
                Some('"') => result.push('"'),
                Some('\\') => result.push('\\'),
                Some('/') => result.push('/'),
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn parse_json_object(s: &str) -> Option<cynos_jsonb::JsonbValue> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return None;
    }
    let inner = s[1..s.len() - 1].trim();
    if inner.is_empty() {
        return Some(cynos_jsonb::JsonbValue::Object(
            cynos_jsonb::JsonbObject::new(),
        ));
    }
    let mut obj = cynos_jsonb::JsonbObject::new();
    for pair in split_json_top_level(inner, ',') {
        let pair = pair.trim();
        // Find the colon separating key from value
        let colon_pos = find_json_colon(pair)?;
        let key_str = pair[..colon_pos].trim();
        let val_str = pair[colon_pos + 1..].trim();
        // Key must be a quoted string
        if key_str.starts_with('"') && key_str.ends_with('"') && key_str.len() >= 2 {
            let key = unescape_json(&key_str[1..key_str.len() - 1]);
            let val = parse_json_text(val_str)?;
            obj.insert(key, val);
        } else {
            return None;
        }
    }
    Some(cynos_jsonb::JsonbValue::Object(obj))
}

fn parse_json_array(s: &str) -> Option<cynos_jsonb::JsonbValue> {
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        return None;
    }
    let inner = s[1..s.len() - 1].trim();
    if inner.is_empty() {
        return Some(cynos_jsonb::JsonbValue::Array(Vec::new()));
    }
    let mut arr = Vec::new();
    for elem in split_json_top_level(inner, ',') {
        arr.push(parse_json_text(elem.trim())?);
    }
    Some(cynos_jsonb::JsonbValue::Array(arr))
}

/// Split a JSON string at top-level occurrences of `sep`,
/// respecting nested braces, brackets, and quoted strings.
fn split_json_top_level(s: &str, sep: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if c == '\\' && in_string {
            escape = true;
            continue;
        }
        if c == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        if c == '{' || c == '[' {
            depth += 1;
        } else if c == '}' || c == ']' {
            depth -= 1;
        } else if c == sep && depth == 0 {
            parts.push(&s[start..i]);
            start = i + c.len_utf8();
        }
    }
    if start <= s.len() {
        parts.push(&s[start..]);
    }
    parts
}

/// Find the first colon at top level (outside strings and nested structures).
fn find_json_colon(s: &str) -> Option<usize> {
    let mut in_string = false;
    let mut escape = false;
    for (i, c) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if c == '\\' && in_string {
            escape = true;
            continue;
        }
        if c == '"' {
            in_string = !in_string;
            continue;
        }
        if !in_string && c == ':' {
            return Some(i);
        }
    }
    None
}

/// Compare a `cynos_jsonb::JsonbValue` with a `cynos_core::Value`.
fn compare_jsonb_with_value(jsonb: &cynos_jsonb::JsonbValue, value: &Value) -> bool {
    match (jsonb, value) {
        (cynos_jsonb::JsonbValue::Null, Value::Null) => true,
        (cynos_jsonb::JsonbValue::Bool(a), Value::Boolean(b)) => a == b,
        (cynos_jsonb::JsonbValue::Number(a), Value::Int32(b)) => {
            (*a - *b as f64).abs() < f64::EPSILON
        }
        (cynos_jsonb::JsonbValue::Number(a), Value::Int64(b)) => {
            (*a - *b as f64).abs() < f64::EPSILON
        }
        (cynos_jsonb::JsonbValue::Number(a), Value::Float64(b)) => (*a - *b).abs() < f64::EPSILON,
        (cynos_jsonb::JsonbValue::String(a), Value::String(b)) => a == b,
        _ => false,
    }
}

/// Convert a `cynos_jsonb::JsonbValue` to its string representation.
fn jsonb_value_to_string(v: &cynos_jsonb::JsonbValue) -> String {
    match v {
        cynos_jsonb::JsonbValue::Null => String::from("null"),
        cynos_jsonb::JsonbValue::Bool(b) => {
            if *b {
                String::from("true")
            } else {
                String::from("false")
            }
        }
        cynos_jsonb::JsonbValue::Number(n) => {
            use alloc::format;
            format!("{}", n)
        }
        cynos_jsonb::JsonbValue::String(s) => s.clone(),
        _ => {
            use alloc::format;
            format!("{:?}", v)
        }
    }
}

/// Evaluates a predicate against a row.
pub(crate) fn evaluate_predicate(predicate: &Expr, row: &Row, schema: &Table) -> bool {
    match predicate.inner() {
        ExprInner::Comparison { column, op, value } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let col = col.unwrap();
            let idx = col.index();

            let row_val = match row.get(idx) {
                Some(v) => v,
                None => return false,
            };

            let cmp_val = match js_to_value(value, col.data_type()) {
                Ok(v) => v,
                Err(_) => return false,
            };

            use crate::expr::ComparisonOp;
            match op {
                ComparisonOp::Eq => row_val == &cmp_val,
                ComparisonOp::Ne => row_val != &cmp_val,
                ComparisonOp::Gt => row_val > &cmp_val,
                ComparisonOp::Gte => row_val >= &cmp_val,
                ComparisonOp::Lt => row_val < &cmp_val,
                ComparisonOp::Lte => row_val <= &cmp_val,
            }
        }
        ExprInner::Between { column, low, high } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let col = col.unwrap();
            let idx = col.index();

            let row_val = match row.get(idx) {
                Some(v) => v,
                None => return false,
            };

            let low_val = match js_to_value(low, col.data_type()) {
                Ok(v) => v,
                Err(_) => return false,
            };
            let high_val = match js_to_value(high, col.data_type()) {
                Ok(v) => v,
                Err(_) => return false,
            };

            row_val >= &low_val && row_val <= &high_val
        }
        ExprInner::NotBetween { column, low, high } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let col = col.unwrap();
            let idx = col.index();

            let row_val = match row.get(idx) {
                Some(v) => v,
                None => return false,
            };

            let low_val = match js_to_value(low, col.data_type()) {
                Ok(v) => v,
                Err(_) => return false,
            };
            let high_val = match js_to_value(high, col.data_type()) {
                Ok(v) => v,
                Err(_) => return false,
            };

            row_val < &low_val || row_val > &high_val
        }
        ExprInner::InList { column, values } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let col = col.unwrap();
            let idx = col.index();

            let row_val = match row.get(idx) {
                Some(v) => v,
                None => return false,
            };

            let arr = js_sys::Array::from(values);
            arr.iter().any(|v| {
                if let Ok(cmp_val) = js_to_value(&v, col.data_type()) {
                    row_val == &cmp_val
                } else {
                    false
                }
            })
        }
        ExprInner::NotInList { column, values } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let col = col.unwrap();
            let idx = col.index();

            let row_val = match row.get(idx) {
                Some(v) => v,
                None => return false,
            };

            let arr = js_sys::Array::from(values);
            !arr.iter().any(|v| {
                if let Ok(cmp_val) = js_to_value(&v, col.data_type()) {
                    row_val == &cmp_val
                } else {
                    false
                }
            })
        }
        ExprInner::Like { column, pattern } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::String(s)) => cynos_core::pattern_match::like(s, pattern),
                _ => false,
            }
        }
        ExprInner::NotLike { column, pattern } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::String(s)) => !cynos_core::pattern_match::like(s, pattern),
                _ => false,
            }
        }
        ExprInner::Match { column, pattern } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::String(s)) => cynos_core::pattern_match::regex(s, pattern),
                _ => false,
            }
        }
        ExprInner::NotMatch { column, pattern } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::String(s)) => !cynos_core::pattern_match::regex(s, pattern),
                _ => false,
            }
        }
        ExprInner::IsNull { column } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::Null) | None => true,
                _ => false,
            }
        }
        ExprInner::IsNotNull { column } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            match row.get(idx) {
                Some(Value::Null) | None => false,
                _ => true,
            }
        }
        ExprInner::JsonbEq {
            column,
            path,
            value,
        } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            let jsonb_val = match row.get(idx) {
                Some(Value::Jsonb(j)) => j,
                _ => return false,
            };

            // Parse JSON text bytes → cynos_jsonb::JsonbValue, then query path
            let json_str = match core::str::from_utf8(&jsonb_val.0) {
                Ok(s) => s,
                Err(_) => return false,
            };
            let parsed = match parse_json_text(json_str) {
                Some(v) => v,
                None => return false,
            };
            let json_path = match cynos_jsonb::JsonPath::parse(path) {
                Ok(p) => p,
                Err(_) => return false,
            };
            let results = parsed.query(&json_path);
            if results.is_empty() {
                return false;
            }

            // Compare first result with expected value
            if let Ok(cmp_val) = js_to_value(value, DataType::String) {
                compare_jsonb_with_value(results[0], &cmp_val)
            } else {
                false
            }
        }
        ExprInner::JsonbContains {
            column,
            path,
            value,
        } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            let jsonb_val = match row.get(idx) {
                Some(Value::Jsonb(j)) => j,
                _ => return false,
            };

            let json_str = match core::str::from_utf8(&jsonb_val.0) {
                Ok(s) => s,
                Err(_) => return false,
            };
            let parsed = match parse_json_text(json_str) {
                Some(v) => v,
                None => return false,
            };
            let json_path = match cynos_jsonb::JsonPath::parse(path) {
                Ok(p) => p,
                Err(_) => return false,
            };
            let results = parsed.query(&json_path);
            if results.is_empty() {
                return false;
            }

            if let Ok(cmp_val) = js_to_value(value, DataType::String) {
                if let Value::String(s) = &cmp_val {
                    // Check if the extracted value's string representation contains the search string
                    let extracted_str = jsonb_value_to_string(results[0]);
                    extracted_str.contains(s.as_str())
                } else {
                    false
                }
            } else {
                false
            }
        }
        ExprInner::JsonbExists { column, path } => {
            let col = schema.get_column(&column.name());
            if col.is_none() {
                return false;
            }
            let idx = col.unwrap().index();

            let jsonb_val = match row.get(idx) {
                Some(Value::Jsonb(j)) => j,
                _ => return false,
            };

            let json_str = match core::str::from_utf8(&jsonb_val.0) {
                Ok(s) => s,
                Err(_) => return false,
            };
            let parsed = match parse_json_text(json_str) {
                Some(v) => v,
                None => return false,
            };
            let json_path = match cynos_jsonb::JsonPath::parse(path) {
                Ok(p) => p,
                Err(_) => return false,
            };
            !parsed.query(&json_path).is_empty()
        }
        ExprInner::And { left, right } => {
            evaluate_predicate(left, row, schema) && evaluate_predicate(right, row, schema)
        }
        ExprInner::Or { left, right } => {
            evaluate_predicate(left, row, schema) || evaluate_predicate(right, row, schema)
        }
        ExprInner::Not { inner } => !evaluate_predicate(inner, row, schema),
        ExprInner::True => true,
        // ColumnRef / Literal are value expressions, not predicates.
        // Treating them as `true` preserves backward compatibility.
        ExprInner::ColumnRef { .. } | ExprInner::Literal { .. } => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binary_protocol::SchemaLayoutCache;
    use crate::dataflow_compiler::compile_to_dataflow;
    use crate::query_engine::compile_plan;
    use alloc::rc::Rc;
    use core::cell::RefCell;
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, Row, Value};
    use cynos_query::plan_cache::PlanCache;
    use cynos_reactive::TableId;
    use cynos_storage::TableCache;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    use cynos_core::pattern_match;

    struct TestSelectContext {
        cache: Rc<RefCell<TableCache>>,
        query_registry: Rc<RefCell<LiveRegistry>>,
        table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
        schema_layout_cache: Rc<RefCell<SchemaLayoutCache>>,
        plan_cache: Rc<RefCell<PlanCache>>,
    }

    impl TestSelectContext {
        fn builder(&self) -> SelectBuilder {
            self.builder_with_columns(JsValue::UNDEFINED)
        }

        fn builder_with_columns(&self, columns: JsValue) -> SelectBuilder {
            SelectBuilder::new(
                self.cache.clone(),
                self.query_registry.clone(),
                self.table_id_map.clone(),
                self.schema_layout_cache.clone(),
                self.plan_cache.clone(),
                columns,
            )
        }
    }

    fn build_union_test_context() -> TestSelectContext {
        let users = TableBuilder::new("users")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();
        let orders = TableBuilder::new("orders")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("amount", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut cache = TableCache::new();
        cache.create_table(users).unwrap();
        cache.create_table(orders).unwrap();

        {
            let store = cache.get_table_mut("users").unwrap();
            store
                .insert(Row::new(
                    1,
                    vec![Value::Int64(1), Value::String("Alice".into())],
                ))
                .unwrap();
            store
                .insert(Row::new(
                    2,
                    vec![Value::Int64(2), Value::String("Bob".into())],
                ))
                .unwrap();
            store
                .insert(Row::new(
                    3,
                    vec![Value::Int64(3), Value::String("Charlie".into())],
                ))
                .unwrap();
        }

        {
            let store = cache.get_table_mut("orders").unwrap();
            store
                .insert(Row::new(11, vec![Value::Int64(11), Value::Int64(150)]))
                .unwrap();
        }

        let cache = Rc::new(RefCell::new(cache));
        let query_registry = Rc::new(RefCell::new(LiveRegistry::new()));
        query_registry
            .borrow_mut()
            .set_self_ref(query_registry.clone());

        let table_id_map = Rc::new(RefCell::new(hashbrown::HashMap::new()));
        table_id_map.borrow_mut().insert("users".into(), 1);
        table_id_map.borrow_mut().insert("orders".into(), 2);

        TestSelectContext {
            cache,
            query_registry,
            table_id_map,
            schema_layout_cache: Rc::new(RefCell::new(SchemaLayoutCache::new())),
            plan_cache: Rc::new(RefCell::new(PlanCache::default_size())),
        }
    }

    fn build_self_join_test_context() -> TestSelectContext {
        let employees = TableBuilder::new("employees")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_column("manager_id", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut cache = TableCache::new();
        cache.create_table(employees).unwrap();

        {
            let store = cache.get_table_mut("employees").unwrap();
            store
                .insert(Row::new(
                    1,
                    vec![Value::Int64(1), Value::String("CEO".into()), Value::Null],
                ))
                .unwrap();
            store
                .insert(Row::new(
                    2,
                    vec![
                        Value::Int64(2),
                        Value::String("Manager".into()),
                        Value::Int64(1),
                    ],
                ))
                .unwrap();
            store
                .insert(Row::new(
                    3,
                    vec![
                        Value::Int64(3),
                        Value::String("Engineer".into()),
                        Value::Int64(2),
                    ],
                ))
                .unwrap();
        }

        let cache = Rc::new(RefCell::new(cache));
        let query_registry = Rc::new(RefCell::new(LiveRegistry::new()));
        query_registry
            .borrow_mut()
            .set_self_ref(query_registry.clone());

        let table_id_map = Rc::new(RefCell::new(hashbrown::HashMap::new()));
        table_id_map.borrow_mut().insert("employees".into(), 1);

        TestSelectContext {
            cache,
            query_registry,
            table_id_map,
            schema_layout_cache: Rc::new(RefCell::new(SchemaLayoutCache::new())),
            plan_cache: Rc::new(RefCell::new(PlanCache::default_size())),
        }
    }

    #[wasm_bindgen_test]
    fn test_select_builder_union_executes_distinct() {
        let ctx = build_union_test_context();
        let left = ctx.builder().from("users");
        let right = ctx.builder().from("users");

        let union = left.union(&right).unwrap();
        let plan = union.build_logical_plan("users");
        assert!(matches!(plan, LogicalPlan::Union { all: false, .. }));

        let cache = ctx.cache.borrow();
        let rows = execute_plan(&cache, "users", plan).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[wasm_bindgen_test]
    fn test_select_builder_union_all_executes_with_duplicates() {
        let ctx = build_union_test_context();
        let left = ctx.builder().from("users");
        let right = ctx.builder().from("users");

        let union = left.union_all(&right).unwrap();
        let plan = union.build_logical_plan("users");
        assert!(matches!(plan, LogicalPlan::Union { all: true, .. }));

        let cache = ctx.cache.borrow();
        let rows = execute_plan(&cache, "users", plan).unwrap();
        assert_eq!(rows.len(), 6);
    }

    #[wasm_bindgen_test]
    fn test_select_builder_union_where_resolves_against_union_output() {
        let ctx = build_union_test_context();
        let columns = js_sys::Array::new();
        columns.push(&JsValue::from_str("name"));

        let left = ctx
            .builder_with_columns(columns.clone().into())
            .from("users");
        let right = ctx.builder_with_columns(columns.into()).from("users");

        let filtered = left
            .union_all(&right)
            .unwrap()
            .where_(&crate::expr::Column::new_simple("name").eq(&JsValue::from_str("Bob")));

        let cache = ctx.cache.borrow();
        let rows = execute_plan(&cache, "users", filtered.build_logical_plan("users")).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .all(|row| row.get(0) == Some(&Value::String("Bob".into()))));
    }

    #[wasm_bindgen_test]
    fn test_select_builder_union_order_by_resolves_against_union_output() {
        let ctx = build_union_test_context();
        let columns = js_sys::Array::new();
        columns.push(&JsValue::from_str("name"));

        let left = ctx
            .builder_with_columns(columns.clone().into())
            .from("users");
        let right = ctx.builder_with_columns(columns.into()).from("users");

        let ordered = left
            .union(&right)
            .unwrap()
            .order_by("name", JsSortOrder::Desc)
            .limit(1);

        let cache = ctx.cache.borrow();
        let rows = execute_plan(&cache, "users", ordered.build_logical_plan("users")).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::String("Charlie".into())));
    }

    #[wasm_bindgen_test]
    fn test_select_builder_union_rejects_incompatible_outputs() {
        let ctx = build_union_test_context();
        let left = ctx.builder().from("users");
        let right = ctx.builder().from("orders");

        let error = match left.union(&right) {
            Ok(_) => panic!("union should reject incompatible outputs"),
            Err(error) => error,
        };
        assert_eq!(
            error.as_string().as_deref(),
            Some("UNION operands must produce the same number of columns with matching types")
        );
    }

    #[wasm_bindgen_test]
    fn test_select_builder_self_join_projection_uses_joined_row_offsets() {
        let ctx = build_self_join_test_context();
        let columns = js_sys::Array::new();
        columns.push(&JsValue::from_str("employees.name"));
        columns.push(&JsValue::from_str("managers.name"));

        let query = ctx
            .builder_with_columns(columns.into())
            .from("employees")
            .left_join(
                "employees as managers",
                &crate::expr::Column::new_simple("employees.manager_id")
                    .eq(&JsValue::from_str("managers.id")),
            );

        let plan = query.build_logical_plan("employees");
        match &plan {
            LogicalPlan::Project { columns, .. } => {
                assert_eq!(columns.len(), 2);
                match &columns[0] {
                    cynos_query::ast::Expr::Column(col) => {
                        assert_eq!(col.table, "");
                        assert_eq!(col.index, 1);
                    }
                    other => panic!("expected projected column, got {:?}", other),
                }
                match &columns[1] {
                    cynos_query::ast::Expr::Column(col) => {
                        assert_eq!(col.table, "");
                        assert_eq!(col.index, 4);
                    }
                    other => panic!("expected projected column, got {:?}", other),
                }

                let cache = ctx.cache.borrow();
                let rows = execute_plan(&cache, "employees", plan.clone()).unwrap();
                let values: Vec<Vec<Value>> =
                    rows.iter().map(|row| row.values().to_vec()).collect();
                assert_eq!(
                    values,
                    vec![
                        vec![Value::String("CEO".into()), Value::Null],
                        vec![Value::String("Manager".into()), Value::String("CEO".into()),],
                        vec![
                            Value::String("Engineer".into()),
                            Value::String("Manager".into()),
                        ],
                    ]
                );
            }
            other => panic!("expected project plan, got {:?}", other),
        }
    }

    #[wasm_bindgen_test]
    fn test_select_builder_non_self_join_projection_preserves_source_tables_for_optimizer() {
        let ctx = build_union_test_context();
        let columns = js_sys::Array::new();
        columns.push(&JsValue::from_str("users.name"));
        columns.push(&JsValue::from_str("orders.amount"));

        let query = ctx
            .builder_with_columns(columns.into())
            .from("users")
            .left_join(
                "orders",
                &crate::expr::Column::new_simple("users.id").eq(&JsValue::from_str("orders.id")),
            );

        let plan = query.build_logical_plan("users");
        match &plan {
            LogicalPlan::Project { columns, .. } => {
                assert_eq!(columns.len(), 2);
                match &columns[0] {
                    cynos_query::ast::Expr::Column(col) => {
                        assert_eq!(col.table, "users");
                        assert_eq!(col.index, 1);
                    }
                    other => panic!("expected projected column, got {:?}", other),
                }
                match &columns[1] {
                    cynos_query::ast::Expr::Column(col) => {
                        assert_eq!(col.table, "orders");
                        assert_eq!(col.index, 1);
                    }
                    other => panic!("expected projected column, got {:?}", other),
                }
            }
            other => panic!("expected project plan, got {:?}", other),
        }
    }

    #[wasm_bindgen_test]
    fn test_trace_compile_omits_redundant_outer_join_dependency() {
        let ctx = build_union_test_context();
        let columns = js_sys::Array::new();
        columns.push(&JsValue::from_str("users.name"));

        let query = ctx
            .builder_with_columns(columns.into())
            .from("users")
            .left_join(
                "orders",
                &crate::expr::Column::new_simple("users.id").eq(&JsValue::from_str("orders.id")),
            );

        let logical_plan = query.build_logical_plan("users");
        let cache = ctx.cache.borrow();
        let physical_plan = compile_plan(&cache, "users", logical_plan);
        let physical_text = alloc::format!("{:#?}", physical_plan);

        assert!(!physical_text.contains("table: \"orders\""));

        let mut table_schemas = hashbrown::HashMap::new();
        table_schemas.insert(
            "users".to_string(),
            cache.get_table("users").unwrap().schema().clone(),
        );
        table_schemas.insert(
            "orders".to_string(),
            cache.get_table("orders").unwrap().schema().clone(),
        );

        let table_id_map = ctx.table_id_map.borrow();
        let compile_result = compile_to_dataflow(&physical_plan, &table_id_map, &table_schemas)
            .expect("redundant join-free projection should stay incrementalizable");

        assert!(compile_result.table_ids.contains_key("users"));
        assert!(!compile_result.table_ids.contains_key("orders"));
    }

    #[wasm_bindgen_test]
    fn test_join_filters_and_order_by_preserve_source_tables_for_optimizer() {
        let ctx = build_union_test_context();

        let query = ctx
            .builder()
            .from("users")
            .left_join(
                "orders",
                &crate::expr::Column::new_simple("users.id").eq(&JsValue::from_str("orders.id")),
            )
            .where_(&crate::expr::Column::new_simple("orders.amount").eq(&JsValue::from_f64(150.0)))
            .order_by("users.id", JsSortOrder::Asc);

        let plan = query.build_logical_plan("users");
        match &plan {
            LogicalPlan::Sort { input, order_by } => {
                assert_eq!(order_by.len(), 1);
                match &order_by[0].0 {
                    cynos_query::ast::Expr::Column(col) => {
                        assert_eq!(col.table, "users");
                        assert_eq!(col.index, 0);
                    }
                    other => panic!("expected ORDER BY column, got {:?}", other),
                }

                match input.as_ref() {
                    LogicalPlan::Filter { predicate, .. } => match predicate {
                        cynos_query::ast::Expr::BinaryOp { left, .. } => match left.as_ref() {
                            cynos_query::ast::Expr::Column(col) => {
                                assert_eq!(col.table, "orders");
                                assert_eq!(col.index, 1);
                            }
                            other => panic!("expected filter column, got {:?}", other),
                        },
                        other => panic!("expected binary predicate, got {:?}", other),
                    },
                    other => panic!("expected filter under sort, got {:?}", other),
                }
            }
            other => panic!("expected sort plan, got {:?}", other),
        }
    }

    #[wasm_bindgen_test]
    fn test_observe_reuses_cached_default_and_root_subset_plans() {
        let ctx = build_union_test_context();
        let columns = js_sys::Array::new();
        columns.push(&JsValue::from_str("users.id"));
        columns.push(&JsValue::from_str("users.name"));
        columns.push(&JsValue::from_str("orders.amount"));

        let query = ctx
            .builder_with_columns(columns.into())
            .from("users")
            .left_join(
                "orders",
                &crate::expr::Column::new_simple("users.id").eq(&JsValue::from_str("orders.id")),
            );

        ctx.plan_cache.borrow_mut().clear();

        let _first = query.observe().expect("first observe should succeed");
        {
            let plan_cache = ctx.plan_cache.borrow();
            assert_eq!(plan_cache.misses(), 2);
            assert_eq!(plan_cache.hits(), 0);
            assert_eq!(plan_cache.len(), 2);
        }

        let _second = query.observe().expect("second observe should reuse cache");
        let plan_cache = ctx.plan_cache.borrow();
        assert_eq!(plan_cache.misses(), 2);
        assert_eq!(plan_cache.hits(), 2);
        assert_eq!(plan_cache.len(), 2);
    }

    #[wasm_bindgen_test]
    fn test_like_match_exact() {
        assert!(pattern_match::like("hello", "hello"));
        assert!(!pattern_match::like("hello", "world"));
    }

    #[wasm_bindgen_test]
    fn test_like_match_percent() {
        assert!(pattern_match::like("hello", "%"));
        assert!(pattern_match::like("hello", "h%"));
        assert!(pattern_match::like("hello", "%o"));
        assert!(pattern_match::like("hello", "h%o"));
        assert!(pattern_match::like("hello", "%ell%"));
        assert!(!pattern_match::like("hello", "x%"));
    }

    #[wasm_bindgen_test]
    fn test_like_match_underscore() {
        assert!(pattern_match::like("hello", "_ello"));
        assert!(pattern_match::like("hello", "h_llo"));
        assert!(pattern_match::like("hello", "hell_"));
        assert!(pattern_match::like("hello", "_____"));
        assert!(!pattern_match::like("hello", "______"));
    }

    #[wasm_bindgen_test]
    fn test_like_match_combined() {
        assert!(pattern_match::like("hello", "h%_o"));
        assert!(pattern_match::like("hello world", "hello%"));
        assert!(pattern_match::like("hello world", "%world"));
    }
}
