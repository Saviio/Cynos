//! Query engine integration for API layer.
//!
//! This module bridges the storage layer with the query engine,
//! providing optimized query execution using indexes.

#[allow(unused_imports)]
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;
use cynos_core::{Row, Value, DUMMY_ROW_ID};
use cynos_index::KeyRange;
use cynos_query::context::{ExecutionContext, IndexInfo, QueryIndexType, TableStats};
use cynos_query::executor::{DataSource, ExecutionError, ExecutionResult, PhysicalPlanRunner};
pub use cynos_query::plan_cache::CompiledPhysicalPlan;
use cynos_query::planner::{LogicalPlan, PhysicalPlan, QueryPlanner};
use cynos_storage::TableCache;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

/// DataSource implementation for TableCache.
///
/// This allows the query engine to access table data and indexes.
pub struct TableCacheDataSource<'a> {
    cache: &'a TableCache,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub struct QueryResultSummary {
    pub len: usize,
    pub fingerprint: u64,
}

#[derive(Default)]
struct QueryResultSummaryBuilder {
    len: usize,
    fingerprint: u64,
}

impl QueryResultSummary {
    pub(crate) fn from_rows(rows: &[Rc<Row>]) -> Self {
        let mut builder = QueryResultSummaryBuilder::default();
        for row in rows {
            builder.push(row.as_ref());
        }
        builder.finish()
    }
}

impl QueryResultSummaryBuilder {
    fn push(&mut self, row: &Row) {
        self.len += 1;
        self.mix_u64(row.id());
        self.mix_u64(row.version());

        if row.id() == DUMMY_ROW_ID {
            self.mix_u64(row.values().len() as u64);
            for value in row.values() {
                self.mix_value(value);
            }
        }
    }

    fn finish(self) -> QueryResultSummary {
        QueryResultSummary {
            len: self.len,
            fingerprint: self.fingerprint,
        }
    }

    fn mix_u64(&mut self, value: u64) {
        const PRIME: u64 = 0x9E37_79B1_85EB_CA87;
        self.fingerprint ^= value.wrapping_add(PRIME).rotate_left(27);
        self.fingerprint = self.fingerprint.wrapping_mul(PRIME);
    }

    fn mix_bytes(&mut self, bytes: &[u8]) {
        self.mix_u64(bytes.len() as u64);
        for &byte in bytes {
            self.mix_u64(byte as u64);
        }
    }

    fn mix_value(&mut self, value: &Value) {
        match value {
            Value::Null => self.mix_u64(0),
            Value::Boolean(value) => {
                self.mix_u64(1);
                self.mix_u64(*value as u64);
            }
            Value::Int32(value) => {
                self.mix_u64(2);
                self.mix_u64(*value as u32 as u64);
            }
            Value::Int64(value) => {
                self.mix_u64(3);
                self.mix_u64(*value as u64);
            }
            Value::Float64(value) => {
                self.mix_u64(4);
                self.mix_u64(value.to_bits());
            }
            Value::String(value) => {
                self.mix_u64(5);
                self.mix_bytes(value.as_bytes());
            }
            Value::DateTime(value) => {
                self.mix_u64(6);
                self.mix_u64(*value as u64);
            }
            Value::Bytes(value) => {
                self.mix_u64(7);
                self.mix_bytes(value);
            }
            Value::Jsonb(value) => {
                self.mix_u64(8);
                self.mix_bytes(&value.0);
            }
        }
    }
}

#[doc(hidden)]
pub struct QueryExecutionOutput {
    pub rows: Vec<Rc<Row>>,
    pub summary: QueryResultSummary,
}

impl<'a> TableCacheDataSource<'a> {
    /// Creates a new data source from a TableCache reference.
    pub fn new(cache: &'a TableCache) -> Self {
        Self { cache }
    }
}

impl<'a> DataSource for TableCacheDataSource<'a> {
    fn get_table_rows(&self, table: &str) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        // Rc::clone is cheap (just increment ref count)
        Ok(store.scan().collect())
    }

    fn visit_table_rows<F>(&self, table: &str, mut visitor: F) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        store.visit_rows(|row| visitor(row));
        Ok(())
    }

    fn get_index_range(
        &self,
        table: &str,
        index: &str,
        range_start: Option<&Value>,
        range_end: Option<&Value>,
        include_start: bool,
        include_end: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        self.get_index_range_with_limit(
            table,
            index,
            range_start,
            range_end,
            include_start,
            include_end,
            None,
            0,
            false,
        )
    }

    fn get_index_range_with_limit(
        &self,
        table: &str,
        index: &str,
        range_start: Option<&Value>,
        range_end: Option<&Value>,
        include_start: bool,
        include_end: bool,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        // Build KeyRange from bounds
        let range = match (range_start, range_end) {
            (Some(start), Some(end)) => Some(KeyRange::bound(
                start.clone(),
                end.clone(),
                !include_start,
                !include_end,
            )),
            (Some(start), None) => Some(KeyRange::lower_bound(start.clone(), !include_start)),
            (None, Some(end)) => Some(KeyRange::upper_bound(end.clone(), !include_end)),
            (None, None) => None,
        };

        // Push limit, offset, and reverse down to storage layer for early termination
        Ok(store.index_scan_with_options(index, range.as_ref(), limit, offset, reverse))
    }

    fn get_index_range_composite_with_limit(
        &self,
        table: &str,
        index: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        Ok(store.index_scan_composite_with_options(index, range, limit, offset, reverse))
    }

    fn visit_index_range_with_limit<F>(
        &self,
        table: &str,
        index: &str,
        range_start: Option<&Value>,
        range_end: Option<&Value>,
        include_start: bool,
        include_end: bool,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        let range = match (range_start, range_end) {
            (Some(start), Some(end)) => Some(KeyRange::bound(
                start.clone(),
                end.clone(),
                !include_start,
                !include_end,
            )),
            (Some(start), None) => Some(KeyRange::lower_bound(start.clone(), !include_start)),
            (None, Some(end)) => Some(KeyRange::upper_bound(end.clone(), !include_end)),
            (None, None) => None,
        };

        store.visit_index_scan_with_options(index, range.as_ref(), limit, offset, reverse, |row| {
            visitor(row)
        });
        Ok(())
    }

    fn visit_index_range_composite_with_limit<F>(
        &self,
        table: &str,
        index: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        store.visit_index_scan_composite_with_options(
            index,
            range,
            limit,
            offset,
            reverse,
            |row| visitor(row),
        );
        Ok(())
    }

    fn get_index_point(
        &self,
        table: &str,
        index: &str,
        key: &Value,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        // Use index_scan with a point range (key == key)
        let range = KeyRange::only(key.clone());

        Ok(store.index_scan(index, Some(&range)))
    }

    fn get_index_point_with_limit(
        &self,
        table: &str,
        index: &str,
        key: &Value,
        limit: Option<usize>,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        // Use index_scan_with_limit for early termination
        let range = KeyRange::only(key.clone());

        Ok(store.index_scan_with_limit(index, Some(&range), limit))
    }

    fn visit_index_point_with_limit<F>(
        &self,
        table: &str,
        index: &str,
        key: &Value,
        limit: Option<usize>,
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        let range = KeyRange::only(key.clone());
        store.visit_index_scan_with_options(index, Some(&range), limit, 0, false, |row| {
            visitor(row)
        });
        Ok(())
    }

    fn get_column_count(&self, table: &str) -> ExecutionResult<usize> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        Ok(store.schema().columns().len())
    }

    fn get_table_row_count(&self, table: &str) -> ExecutionResult<usize> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        Ok(store.len())
    }

    fn get_gin_index_rows(
        &self,
        table: &str,
        index: &str,
        key: &str,
        value: &str,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        Ok(store.gin_index_get_by_key_value(index, key, value))
    }

    fn visit_gin_index_rows<F>(
        &self,
        table: &str,
        index: &str,
        key: &str,
        value: &str,
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        store.visit_gin_index_by_key_value(index, key, value, |row| visitor(row));
        Ok(())
    }

    fn get_gin_index_rows_by_key(
        &self,
        table: &str,
        index: &str,
        key: &str,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        Ok(store.gin_index_get_by_key(index, key))
    }

    fn visit_gin_index_rows_by_key<F>(
        &self,
        table: &str,
        index: &str,
        key: &str,
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        store.visit_gin_index_by_key(index, key, |row| visitor(row));
        Ok(())
    }

    fn get_gin_index_rows_multi(
        &self,
        table: &str,
        index: &str,
        pairs: &[(&str, &str)],
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        Ok(store.gin_index_get_by_key_values_all(index, pairs))
    }

    fn visit_gin_index_rows_multi<F>(
        &self,
        table: &str,
        index: &str,
        pairs: &[(&str, &str)],
        mut visitor: F,
    ) -> ExecutionResult<()>
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        store.visit_gin_index_by_key_values_all(index, pairs, |row| visitor(row));
        Ok(())
    }
}

fn register_table_context(cache: &TableCache, ctx: &mut ExecutionContext, table_name: &str) {
    if let Some(store) = cache.get_table(table_name) {
        let schema = store.schema();

        let mut indexes = Vec::new();
        for idx in schema.indices() {
            let index_type = match idx.get_index_type() {
                cynos_core::schema::IndexType::Hash => QueryIndexType::Hash,
                cynos_core::schema::IndexType::BTree => QueryIndexType::BTree,
                cynos_core::schema::IndexType::Gin => QueryIndexType::Gin,
            };
            let mut index_info = IndexInfo::new(
                idx.name(),
                idx.columns().iter().map(|c| c.name.clone()).collect(),
                idx.is_unique(),
            )
            .with_type(index_type);
            if let Some(paths) = idx.gin_paths() {
                index_info = index_info.with_gin_paths(paths.to_vec());
            }
            indexes.push(index_info);
        }

        ctx.register_table(
            table_name,
            TableStats {
                row_count: store.len(),
                is_sorted: false,
                indexes,
            },
        );
    }
}

/// Builds ExecutionContext from TableCache for optimizer.
pub fn build_execution_context(cache: &TableCache, table_name: &str) -> ExecutionContext {
    let mut ctx = ExecutionContext::new();
    register_table_context(cache, &mut ctx, table_name);
    ctx
}

/// Builds an ExecutionContext for every table referenced by the plan.
///
/// This keeps the optimizer context small for single-table queries while still
/// exposing row counts and indexes for all join inputs.
pub fn build_execution_context_for_plan(
    cache: &TableCache,
    table_name: &str,
    plan: &LogicalPlan,
) -> ExecutionContext {
    let mut ctx = ExecutionContext::new();
    let mut tables = plan.collect_tables();
    if !tables.iter().any(|table| table == table_name) {
        tables.push(table_name.into());
    }

    for table in tables {
        register_table_context(cache, &mut ctx, &table);
    }

    ctx
}

/// Executes a logical plan using the query engine.
///
/// This function:
/// 1. Builds execution context with index information
/// 2. Creates QueryPlanner with unified optimization pipeline
/// 3. Plans and optimizes the query (logical + physical)
/// 4. Executes using PhysicalPlanRunner
pub fn execute_plan(
    cache: &TableCache,
    table_name: &str,
    plan: LogicalPlan,
) -> ExecutionResult<Vec<Rc<Row>>> {
    execute_plan_internal(cache, table_name, plan, false)
}

/// Executes a logical plan with optional debug output.
pub fn execute_plan_debug(
    cache: &TableCache,
    table_name: &str,
    plan: LogicalPlan,
) -> ExecutionResult<Vec<Rc<Row>>> {
    execute_plan_internal(cache, table_name, plan, true)
}

fn execute_plan_internal(
    cache: &TableCache,
    table_name: &str,
    plan: LogicalPlan,
    _debug: bool,
) -> ExecutionResult<Vec<Rc<Row>>> {
    // Build execution context with index info
    let ctx = build_execution_context_for_plan(cache, table_name, &plan);

    // Use unified QueryPlanner for complete optimization pipeline
    let planner = QueryPlanner::new(ctx);

    // Plan: logical optimization + physical conversion + physical optimization
    let physical_plan = planner.plan(plan);

    let data_source = TableCacheDataSource::new(cache);
    let runner = PhysicalPlanRunner::new(&data_source);
    let artifact = runner.compile_execution_artifact_with_data_source(&physical_plan);
    runner.execute_with_artifact_row_vec(&physical_plan, &artifact)
}

/// Compiles a logical plan to a physical plan.
/// The physical plan can be cached and reused for repeated executions.
pub fn compile_plan(cache: &TableCache, table_name: &str, plan: LogicalPlan) -> PhysicalPlan {
    // Build execution context with index info
    let ctx = build_execution_context_for_plan(cache, table_name, &plan);

    // Use unified QueryPlanner for complete optimization pipeline
    let planner = QueryPlanner::new(ctx);
    planner.plan(plan)
}

/// Compiles a logical plan and caches execution-time lowering artifacts for repeated execution.
pub fn compile_cached_plan(
    cache: &TableCache,
    table_name: &str,
    plan: LogicalPlan,
) -> CompiledPhysicalPlan {
    let physical_plan = compile_plan(cache, table_name, plan);
    let data_source = TableCacheDataSource::new(cache);
    CompiledPhysicalPlan::new_with_data_source(physical_plan, &data_source)
}

/// Query plan explanation result.
#[derive(Debug)]
pub struct ExplainResult {
    pub logical_plan: String,
    pub optimized_plan: String,
    pub physical_plan: String,
}

/// Explains a logical plan by showing the optimization stages.
///
/// Returns the logical plan, optimized plan, and physical plan as strings.
pub fn explain_plan(cache: &TableCache, table_name: &str, plan: LogicalPlan) -> ExplainResult {
    let logical_plan = alloc::format!("{:#?}", plan);

    // Build execution context with index info
    let ctx = build_execution_context_for_plan(cache, table_name, &plan);

    // Use unified QueryPlanner
    let planner = QueryPlanner::new(ctx);

    // Get optimized logical plan
    let optimized_plan_node = planner.optimize_logical(plan.clone());
    let optimized_plan = alloc::format!("{:#?}", optimized_plan_node);

    // Get physical plan (includes all physical optimizations)
    let physical_plan_node = planner.plan(plan);
    let physical_plan = alloc::format!("{:#?}", physical_plan_node);

    ExplainResult {
        logical_plan,
        optimized_plan,
        physical_plan,
    }
}

/// Executes a pre-compiled physical plan.
/// This is faster than execute_plan because it skips optimization.
/// The plan is still lowered to the fused execution kernel on each call;
/// callers that want to reuse that lowering should use `CompiledPhysicalPlan`.
pub fn execute_physical_plan(
    cache: &TableCache,
    physical_plan: &PhysicalPlan,
) -> ExecutionResult<Vec<Rc<Row>>> {
    let data_source = TableCacheDataSource::new(cache);
    let runner = PhysicalPlanRunner::new(&data_source);
    let artifact = runner.compile_execution_artifact_with_data_source(physical_plan);
    runner.execute_with_artifact_row_vec(physical_plan, &artifact)
}

pub fn execute_compiled_physical_plan(
    cache: &TableCache,
    compiled_plan: &CompiledPhysicalPlan,
) -> ExecutionResult<Vec<Rc<Row>>> {
    let data_source = TableCacheDataSource::new(cache);
    let runner = PhysicalPlanRunner::new(&data_source);
    runner.execute_with_artifact_row_vec(compiled_plan.physical_plan(), compiled_plan.artifact())
}

#[doc(hidden)]
pub fn execute_physical_plan_with_summary(
    cache: &TableCache,
    physical_plan: &PhysicalPlan,
) -> ExecutionResult<QueryExecutionOutput> {
    let data_source = TableCacheDataSource::new(cache);
    let runner = PhysicalPlanRunner::new(&data_source);
    let artifact = runner.compile_execution_artifact_with_data_source(physical_plan);
    let mut rows = Vec::new();
    let mut summary = QueryResultSummaryBuilder::default();
    runner.execute_with_artifact_rows(physical_plan, &artifact, |row| {
        summary.push(row.as_ref());
        rows.push(row);
        Ok(true)
    })?;

    Ok(QueryExecutionOutput {
        rows,
        summary: summary.finish(),
    })
}

#[doc(hidden)]
pub fn execute_compiled_physical_plan_with_summary(
    cache: &TableCache,
    compiled_plan: &CompiledPhysicalPlan,
) -> ExecutionResult<QueryExecutionOutput> {
    let data_source = TableCacheDataSource::new(cache);
    let runner = PhysicalPlanRunner::new(&data_source);
    let mut rows = Vec::new();
    let mut summary = QueryResultSummaryBuilder::default();
    runner.execute_with_artifact_rows(
        compiled_plan.physical_plan(),
        compiled_plan.artifact(),
        |row| {
            summary.push(row.as_ref());
            rows.push(row);
            Ok(true)
        },
    )?;

    Ok(QueryExecutionOutput {
        rows,
        summary: summary.finish(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, Row, Value};
    use cynos_query::ast::Expr as AstExpr;
    use cynos_query::optimizer::{IndexSelection, OptimizerPass};

    fn create_join_test_cache() -> TableCache {
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
            .add_column("user_id", DataType::Int64)
            .unwrap()
            .add_column("amount", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_user_id", &["user_id"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut cache = TableCache::new();
        cache.create_table(users).unwrap();
        cache.create_table(orders).unwrap();

        {
            let users_store = cache.get_table_mut("users").unwrap();
            for id in 0..32 {
                users_store
                    .insert(Row::new(
                        id as u64,
                        alloc::vec![
                            Value::Int64(id),
                            Value::String(alloc::format!("user_{}", id)),
                        ],
                    ))
                    .unwrap();
            }
        }

        {
            let orders_store = cache.get_table_mut("orders").unwrap();
            for id in 0..4096 {
                orders_store
                    .insert(Row::new(
                        (10_000 + id) as u64,
                        alloc::vec![
                            Value::Int64(id),
                            Value::Int64((id % 32) as i64),
                            Value::Int64((id % 100) as i64),
                        ],
                    ))
                    .unwrap();
            }
        }

        cache
    }

    fn create_flipped_index_join_cache() -> TableCache {
        let employees = TableBuilder::new("employees")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_column("dept_id", DataType::Int64)
            .unwrap()
            .add_column("salary", DataType::Int64)
            .unwrap()
            .add_index("idx_dept_id", &["dept_id"], false)
            .unwrap()
            .build()
            .unwrap();

        let departments = TableBuilder::new("departments")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_column("budget", DataType::Int64)
            .unwrap()
            .build()
            .unwrap();

        let mut cache = TableCache::new();
        cache.create_table(employees).unwrap();
        cache.create_table(departments).unwrap();

        {
            let departments_store = cache.get_table_mut("departments").unwrap();
            departments_store
                .insert(Row::new(
                    1,
                    alloc::vec![
                        Value::Int64(1),
                        Value::String("Engineering".into()),
                        Value::Int64(1_000_000),
                    ],
                ))
                .unwrap();
            departments_store
                .insert(Row::new(
                    2,
                    alloc::vec![
                        Value::Int64(2),
                        Value::String("Sales".into()),
                        Value::Int64(500_000),
                    ],
                ))
                .unwrap();
        }

        {
            let employees_store = cache.get_table_mut("employees").unwrap();
            for (row_id, row) in [
                alloc::vec![
                    Value::Int64(1),
                    Value::String("Alice".into()),
                    Value::Int64(1),
                    Value::Int64(80_000),
                ],
                alloc::vec![
                    Value::Int64(2),
                    Value::String("Bob".into()),
                    Value::Int64(1),
                    Value::Int64(90_000),
                ],
                alloc::vec![
                    Value::Int64(3),
                    Value::String("Charlie".into()),
                    Value::Int64(2),
                    Value::Int64(70_000),
                ],
                alloc::vec![
                    Value::Int64(4),
                    Value::String("David".into()),
                    Value::Int64(2),
                    Value::Int64(75_000),
                ],
            ]
            .into_iter()
            .enumerate()
            {
                employees_store
                    .insert(Row::new((10 + row_id) as u64, row))
                    .unwrap();
            }
        }

        cache
    }

    fn create_jsonb_test_cache() -> TableCache {
        let documents = TableBuilder::new("documents")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("metadata", DataType::Jsonb)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_metadata_gin", &["metadata"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut cache = TableCache::new();
        cache.create_table(documents).unwrap();

        let store = cache.get_table_mut("documents").unwrap();
        for (row_id, metadata) in [
            (
                1u64,
                br#"{"tags":["portable","featured"],"category":"tech"}"#.as_slice(),
            ),
            (
                2u64,
                br#"{"tags":["desktop","standard"],"category":"ops"}"#.as_slice(),
            ),
            (
                3u64,
                br#"{"tags":["portable","review"],"category":"tech"}"#.as_slice(),
            ),
        ] {
            store
                .insert(Row::new(
                    row_id,
                    alloc::vec![
                        Value::Int64(row_id as i64),
                        Value::Jsonb(cynos_core::JsonbValue(metadata.to_vec())),
                    ],
                ))
                .unwrap();
        }

        cache
    }

    #[test]
    fn test_table_cache_data_source() {
        // Basic test to ensure the module compiles
        let cache = TableCache::new();
        let _data_source = TableCacheDataSource::new(&cache);
    }

    #[test]
    fn test_build_execution_context_for_plan_includes_join_tables() {
        let cache = create_join_test_cache();
        let plan = LogicalPlan::inner_join(
            LogicalPlan::scan("users"),
            LogicalPlan::scan("orders"),
            AstExpr::eq(
                AstExpr::column("users", "id", 0),
                AstExpr::column("orders", "user_id", 1),
            ),
        );

        let ctx = build_execution_context_for_plan(&cache, "users", &plan);

        assert_eq!(ctx.row_count("users"), 32);
        assert_eq!(ctx.row_count("orders"), 4096);
        assert!(ctx.find_index("orders", &["user_id"]).is_some());
    }

    #[test]
    fn test_compile_plan_prefers_index_join_for_small_outer_fk_join() {
        let cache = create_join_test_cache();
        let plan = LogicalPlan::inner_join(
            LogicalPlan::scan("users"),
            LogicalPlan::scan("orders"),
            AstExpr::eq(
                AstExpr::column("users", "id", 0),
                AstExpr::column("orders", "user_id", 1),
            ),
        );

        let physical = compile_plan(&cache, "users", plan);

        assert!(matches!(physical, PhysicalPlan::IndexNestedLoopJoin { .. }));
    }

    #[test]
    fn test_compiled_plan_matches_physical_plan_execution() {
        let cache = create_join_test_cache();
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            AstExpr::and(
                AstExpr::gte(
                    AstExpr::column("users", "id", 0),
                    AstExpr::literal(Value::Int64(8)),
                ),
                AstExpr::not(AstExpr::eq(
                    AstExpr::column("users", "name", 1),
                    AstExpr::literal(Value::String("user_9".into())),
                )),
            ),
        );

        let physical = compile_plan(&cache, "users", plan.clone());
        let compiled = compile_cached_plan(&cache, "users", plan);

        let expected = execute_physical_plan(&cache, &physical).unwrap();
        let actual = execute_compiled_physical_plan(&cache, &compiled).unwrap();

        let expected_snapshot: Vec<(u64, u64, Vec<Value>)> = expected
            .into_iter()
            .map(|row| (row.id(), row.version(), row.values().to_vec()))
            .collect();
        let actual_snapshot: Vec<(u64, u64, Vec<Value>)> = actual
            .into_iter()
            .map(|row| (row.id(), row.version(), row.values().to_vec()))
            .collect();

        assert_eq!(actual_snapshot, expected_snapshot);
    }

    #[test]
    fn test_compiled_plan_matches_jsonb_gin_contains_execution() {
        let cache = create_jsonb_test_cache();
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("documents"),
            AstExpr::jsonb_contains(
                AstExpr::column("documents", "metadata", 1),
                "$.tags",
                Value::String("portable".into()),
            ),
        );

        let physical = compile_plan(&cache, "documents", plan.clone());
        let compiled = compile_cached_plan(&cache, "documents", plan);

        let expected = execute_physical_plan(&cache, &physical).unwrap();
        let actual = execute_compiled_physical_plan(&cache, &compiled).unwrap();

        let expected_ids: Vec<u64> = expected.into_iter().map(|row| row.id()).collect();
        let actual_ids: Vec<u64> = actual.into_iter().map(|row| row.id()).collect();

        assert_eq!(actual_ids, expected_ids);
        assert_eq!(actual_ids, alloc::vec![1, 3]);
    }

    #[test]
    fn test_execute_physical_plan_matches_legacy_runner_for_join_project_limit() {
        let cache = create_join_test_cache();
        let plan = LogicalPlan::limit(
            LogicalPlan::project(
                LogicalPlan::inner_join(
                    LogicalPlan::scan("users"),
                    LogicalPlan::scan("orders"),
                    AstExpr::eq(
                        AstExpr::column("users", "id", 0),
                        AstExpr::column("orders", "user_id", 1),
                    ),
                ),
                alloc::vec![
                    AstExpr::column("users", "name", 1),
                    AstExpr::column("orders", "amount", 2),
                ],
            ),
            5,
            0,
        );

        let physical = compile_plan(&cache, "users", plan);
        let actual = execute_physical_plan(&cache, &physical).unwrap();

        let data_source = TableCacheDataSource::new(&cache);
        let runner = PhysicalPlanRunner::new(&data_source);
        let expected = runner.execute(&physical).unwrap();

        let actual_snapshot: Vec<(u64, u64, Vec<Value>)> = actual
            .into_iter()
            .map(|row| (row.id(), row.version(), row.values().to_vec()))
            .collect();
        let expected_snapshot: Vec<(u64, u64, Vec<Value>)> = expected
            .entries
            .into_iter()
            .map(|entry| {
                (
                    entry.row.id(),
                    entry.row.version(),
                    entry.row.values().to_vec(),
                )
            })
            .collect();

        assert_eq!(actual_snapshot, expected_snapshot);
    }

    #[test]
    fn test_compiled_plan_summary_matches_materialized_rows() {
        let cache = create_join_test_cache();
        let compiled = compile_cached_plan(
            &cache,
            "users",
            LogicalPlan::limit(
                LogicalPlan::project(
                    LogicalPlan::filter(
                        LogicalPlan::scan("users"),
                        AstExpr::gte(
                            AstExpr::column("users", "id", 0),
                            AstExpr::literal(Value::Int64(8)),
                        ),
                    ),
                    vec![AstExpr::column("users", "name", 1)],
                ),
                2,
                0,
            ),
        );

        let output = execute_compiled_physical_plan_with_summary(&cache, &compiled).unwrap();

        assert_eq!(output.summary, QueryResultSummary::from_rows(&output.rows));
    }

    #[test]
    fn test_physical_plan_summary_matches_materialized_rows() {
        let cache = create_join_test_cache();
        let physical = compile_plan(
            &cache,
            "users",
            LogicalPlan::limit(
                LogicalPlan::project(
                    LogicalPlan::inner_join(
                        LogicalPlan::scan("users"),
                        LogicalPlan::scan("orders"),
                        AstExpr::eq(
                            AstExpr::column("users", "id", 0),
                            AstExpr::column("orders", "user_id", 1),
                        ),
                    ),
                    alloc::vec![
                        AstExpr::column("users", "name", 1),
                        AstExpr::column("orders", "amount", 2),
                    ],
                ),
                8,
                0,
            ),
        );

        let output = execute_physical_plan_with_summary(&cache, &physical).unwrap();

        assert_eq!(output.summary, QueryResultSummary::from_rows(&output.rows));
    }

    #[test]
    fn test_flipped_index_join_preserves_logical_left_row_layout() {
        use cynos_query::ast::SortOrder;

        let cache = create_flipped_index_join_cache();
        let plan = LogicalPlan::sort(
            LogicalPlan::inner_join(
                LogicalPlan::scan("employees"),
                LogicalPlan::scan("departments"),
                AstExpr::eq(
                    AstExpr::column("employees", "dept_id", 2),
                    AstExpr::column("departments", "id", 0),
                ),
            ),
            alloc::vec![(AstExpr::column("employees", "salary", 3), SortOrder::Desc)],
        );

        let physical = compile_plan(&cache, "employees", plan.clone());
        assert!(matches!(physical, PhysicalPlan::Sort { .. }));

        let expected = execute_physical_plan(&cache, &physical).unwrap();
        assert_eq!(expected.len(), 4);
        assert_eq!(expected[0].get(0), Some(&Value::Int64(2)));
        assert_eq!(expected[0].get(3), Some(&Value::Int64(90_000)));
        assert_eq!(expected[0].get(4), Some(&Value::Int64(1)));
        assert_eq!(
            expected[0].values(),
            &[
                Value::Int64(2),
                Value::String("Bob".into()),
                Value::Int64(1),
                Value::Int64(90_000),
                Value::Int64(1),
                Value::String("Engineering".into()),
                Value::Int64(1_000_000),
            ]
        );

        let compiled = compile_cached_plan(&cache, "employees", plan);
        let actual = execute_compiled_physical_plan(&cache, &compiled).unwrap();

        let expected_snapshot: Vec<Vec<Value>> =
            expected.iter().map(|row| row.values().to_vec()).collect();
        let actual_snapshot: Vec<Vec<Value>> =
            actual.iter().map(|row| row.values().to_vec()).collect();
        assert_eq!(actual_snapshot, expected_snapshot);
    }

    #[test]
    fn test_index_selection_with_empty_table_name() {
        // Simulate what happens when col('status').eq('todo') is used
        // The column has empty table name
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "tasks",
            TableStats {
                row_count: 100000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_status", alloc::vec!["status".into()], false),
                    IndexInfo::new("idx_priority", alloc::vec!["priority".into()], false),
                ],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Create plan with empty table name in column (simulating col('status'))
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "tasks".into(),
            }),
            predicate: AstExpr::eq(
                AstExpr::column("", "status", 2), // Empty table name!
                AstExpr::literal(cynos_core::Value::String("todo".into())),
            ),
        };

        let optimized = pass.optimize(plan.clone());

        // Print for debugging
        println!("Input plan: {:?}", plan);
        println!("Optimized plan: {:?}", optimized);

        // Should convert to IndexGet since we have idx_status
        assert!(
            matches!(optimized, LogicalPlan::IndexGet { .. }),
            "Expected IndexGet but got {:?}",
            optimized
        );
    }

    #[test]
    fn test_full_optimizer_pipeline() {
        // Test the full optimizer pipeline using QueryPlanner
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "tasks",
            TableStats {
                row_count: 100000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_status", alloc::vec!["status".into()], false),
                    IndexInfo::new("idx_priority", alloc::vec!["priority".into()], false),
                ],
            },
        );

        // Create QueryPlanner with context
        let planner = QueryPlanner::new(ctx);

        // Create plan with empty table name in column
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "tasks".into(),
            }),
            predicate: AstExpr::eq(
                AstExpr::column("", "status", 2),
                AstExpr::literal(cynos_core::Value::String("todo".into())),
            ),
        };

        println!("Input plan: {:?}", plan);

        // Run full optimization using QueryPlanner
        let optimized = planner.optimize_logical(plan.clone());
        println!("After optimize_logical(): {:?}", optimized);

        // Convert to physical
        let physical = planner.plan(plan);
        println!("Physical plan: {:?}", physical);

        // Should be IndexGet
        assert!(
            matches!(optimized, LogicalPlan::IndexGet { .. }),
            "Expected IndexGet but got {:?}",
            optimized
        );
    }

    #[test]
    fn test_end_to_end_with_real_table() {
        // Create a table with indexes
        let table = TableBuilder::new("tasks")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("status", DataType::String)
            .unwrap()
            .add_column("priority", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_status", &["status"], false)
            .unwrap()
            .add_index("idx_priority", &["priority"], false)
            .unwrap()
            .build()
            .unwrap();

        // Create cache and add table
        let mut cache = TableCache::new();
        cache.create_table(table).unwrap();

        // Insert some test data
        let store = cache.get_table_mut("tasks").unwrap();
        for i in 0..1000 {
            let status = if i % 5 == 0 { "todo" } else { "done" };
            let priority = if i % 4 == 0 { "high" } else { "low" };
            store
                .insert(Row::new(
                    i as u64,
                    alloc::vec![
                        Value::Int64(i),
                        Value::String(status.into()),
                        Value::String(priority.into()),
                    ],
                ))
                .unwrap();
        }

        // Create a filter plan: WHERE status = 'todo'
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: "tasks".into(),
            }),
            predicate: AstExpr::eq(
                AstExpr::column("", "status", 1),
                AstExpr::literal(Value::String("todo".into())),
            ),
        };

        println!("Input plan: {:?}", plan);

        // Build context and use QueryPlanner
        let ctx = build_execution_context(&cache, "tasks");
        println!(
            "Context indexes: {:?}",
            ctx.get_stats("tasks").map(|s| &s.indexes)
        );

        let planner = QueryPlanner::new(ctx);
        let optimized = planner.optimize_logical(plan.clone());
        println!("Optimized plan: {:?}", optimized);

        let physical = planner.plan(plan.clone());
        println!("Physical plan: {:?}", physical);

        // Execute
        let result = execute_plan(&cache, "tasks", plan).unwrap();

        println!("Result count: {}", result.len());

        // Should return 200 rows (1000 / 5 = 200 with status = 'todo')
        assert_eq!(result.len(), 200, "Expected 200 rows with status='todo'");

        // Verify all results have status = 'todo'
        for row in &result {
            assert_eq!(
                row.get(1),
                Some(&Value::String("todo".into())),
                "All rows should have status='todo'"
            );
        }
    }

    #[test]
    fn test_execute_plan_with_limit() {
        use cynos_core::schema::TableBuilder;
        use cynos_core::{DataType, Row, Value};

        // Create a table with indexes
        let table = TableBuilder::new("tasks")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("status", DataType::String)
            .unwrap()
            .add_column("priority", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_status", &["status"], false)
            .unwrap()
            .build()
            .unwrap();

        // Create cache and add table
        let mut cache = TableCache::new();
        cache.create_table(table).unwrap();

        // Insert 1000 rows, 200 with status='todo'
        let store = cache.get_table_mut("tasks").unwrap();
        for i in 0..1000 {
            let status = if i % 5 == 0 { "todo" } else { "done" };
            store
                .insert(Row::new(
                    i as u64,
                    alloc::vec![
                        Value::Int64(i),
                        Value::String(status.into()),
                        Value::String("low".into()),
                    ],
                ))
                .unwrap();
        }

        // Create a filter + limit plan: WHERE status = 'todo' LIMIT 10
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: "tasks".into(),
                }),
                predicate: AstExpr::eq(
                    AstExpr::column("", "status", 1),
                    AstExpr::literal(Value::String("todo".into())),
                ),
            }),
            limit: 10,
            offset: 0,
        };

        println!("Input plan with LIMIT: {:?}", plan);

        // Execute
        let result = execute_plan(&cache, "tasks", plan).unwrap();

        println!("Result count: {} (expected 10)", result.len());

        // Should return exactly 10 rows due to LIMIT
        assert_eq!(result.len(), 10, "Expected 10 rows due to LIMIT");

        // Verify all results have status = 'todo'
        for row in &result {
            assert_eq!(
                row.get(1),
                Some(&Value::String("todo".into())),
                "All rows should have status='todo'"
            );
        }
    }

    #[test]
    fn test_order_by_desc_with_index() {
        use cynos_core::schema::TableBuilder;
        use cynos_core::{DataType, Row, Value};
        use cynos_query::ast::SortOrder;
        use cynos_query::planner::PhysicalPlan;

        // Create a table with an index on 'score'
        let table = TableBuilder::new("scores")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("score", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_score", &["score"], false)
            .unwrap()
            .build()
            .unwrap();

        // Create cache and add table
        let mut cache = TableCache::new();
        cache.create_table(table).unwrap();

        // Insert rows with scores: 10, 20, 30, 40, 50
        let store = cache.get_table_mut("scores").unwrap();
        for i in 1..=5 {
            store
                .insert(Row::new(
                    i as u64,
                    alloc::vec![Value::Int64(i), Value::Int64(i * 10),],
                ))
                .unwrap();
        }

        // Create a plan: SELECT * FROM scores ORDER BY score DESC LIMIT 3
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Scan {
                    table: "scores".into(),
                }),
                order_by: alloc::vec![(AstExpr::column("scores", "score", 1), SortOrder::Desc)],
            }),
            limit: 3,
            offset: 0,
        };

        println!("Input plan: {:?}", plan);

        // Build context and use QueryPlanner
        let ctx = build_execution_context(&cache, "scores");
        println!(
            "Context indexes: {:?}",
            ctx.get_stats("scores").map(|s| &s.indexes)
        );

        let planner = QueryPlanner::new(ctx.clone());
        let physical = planner.plan(plan.clone());
        println!("Physical plan (single line): {:?}", physical);
        println!("Physical plan (pretty): {:#?}", physical);
        println!(
            "Context indexes: {:?}",
            ctx.get_stats("scores").map(|s| &s.indexes)
        );

        // Verify the physical plan is an IndexScan with reverse=true
        match &physical {
            PhysicalPlan::IndexScan { reverse, limit, .. } => {
                assert!(
                    reverse,
                    "IndexScan should have reverse=true for DESC ordering"
                );
                assert_eq!(*limit, Some(3), "IndexScan should have limit=3");
            }
            _ => panic!("Expected IndexScan, got {:?}", physical),
        }

        // Execute and verify results are in DESC order
        let result = execute_plan(&cache, "scores", plan).unwrap();
        println!(
            "Result: {:?}",
            result.iter().map(|r| r.get(1)).collect::<Vec<_>>()
        );

        assert_eq!(result.len(), 3, "Expected 3 rows");
        assert_eq!(
            result[0].get(1),
            Some(&Value::Int64(50)),
            "First row should have score=50"
        );
        assert_eq!(
            result[1].get(1),
            Some(&Value::Int64(40)),
            "Second row should have score=40"
        );
        assert_eq!(
            result[2].get(1),
            Some(&Value::Int64(30)),
            "Third row should have score=30"
        );
    }

    #[test]
    fn test_order_by_asc_with_index() {
        use cynos_core::schema::TableBuilder;
        use cynos_core::{DataType, Row, Value};
        use cynos_query::ast::SortOrder;
        use cynos_query::planner::PhysicalPlan;

        // Create a table with an index on 'score'
        let table = TableBuilder::new("scores_asc")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("score", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_score", &["score"], false)
            .unwrap()
            .build()
            .unwrap();

        // Create cache and add table
        let mut cache = TableCache::new();
        cache.create_table(table).unwrap();

        // Insert rows with scores: 10, 20, 30, 40, 50
        let store = cache.get_table_mut("scores_asc").unwrap();
        for i in 1..=5 {
            store
                .insert(Row::new(
                    i as u64,
                    alloc::vec![Value::Int64(i), Value::Int64(i * 10),],
                ))
                .unwrap();
        }

        // Create a plan: SELECT * FROM scores_asc ORDER BY score ASC LIMIT 3
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Scan {
                    table: "scores_asc".into(),
                }),
                order_by: alloc::vec![(AstExpr::column("scores_asc", "score", 1), SortOrder::Asc)],
            }),
            limit: 3,
            offset: 0,
        };

        println!("Input plan: {:?}", plan);

        // Build context and use QueryPlanner
        let ctx = build_execution_context(&cache, "scores_asc");
        println!(
            "Context indexes: {:?}",
            ctx.get_stats("scores_asc").map(|s| &s.indexes)
        );

        let planner = QueryPlanner::new(ctx);
        let physical = planner.plan(plan.clone());
        println!("Physical plan (single line): {:?}", physical);
        println!("Physical plan (pretty): {:#?}", physical);

        // Verify the physical plan is an IndexScan with reverse=false
        match &physical {
            PhysicalPlan::IndexScan { reverse, limit, .. } => {
                assert!(
                    !reverse,
                    "IndexScan should have reverse=false for ASC ordering"
                );
                assert_eq!(*limit, Some(3), "IndexScan should have limit=3");
            }
            _ => panic!("Expected IndexScan, got {:?}", physical),
        }

        // Execute and verify results are in ASC order
        let result = execute_plan(&cache, "scores_asc", plan).unwrap();
        println!(
            "Result: {:?}",
            result.iter().map(|r| r.get(1)).collect::<Vec<_>>()
        );

        assert_eq!(result.len(), 3, "Expected 3 rows");
        assert_eq!(
            result[0].get(1),
            Some(&Value::Int64(10)),
            "First row should have score=10"
        );
        assert_eq!(
            result[1].get(1),
            Some(&Value::Int64(20)),
            "Second row should have score=20"
        );
        assert_eq!(
            result[2].get(1),
            Some(&Value::Int64(30)),
            "Third row should have score=30"
        );
    }

    /// Test: composite index used for ORDER BY must preserve real tuple order.
    /// Bug: storage currently serializes multi-column keys as debug strings,
    /// so lexicographic string order can diverge from `(col1, col2, ...)` order.
    #[test]
    fn test_order_by_composite_index_preserves_tuple_order() {
        use cynos_core::schema::TableBuilder;
        use cynos_core::{DataType, Row, Value};
        use cynos_query::ast::SortOrder;
        use cynos_query::planner::PhysicalPlan;

        let table = TableBuilder::new("composite_scores")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("a", DataType::Int64)
            .unwrap()
            .add_column("b", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_a_b", &["a", "b"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut cache = TableCache::new();
        cache.create_table(table).unwrap();

        let store = cache.get_table_mut("composite_scores").unwrap();
        for (id, a, b) in [(1_i64, 1_i64, 2_i64), (2, 1, 10), (3, 2, 1)] {
            store
                .insert(Row::new(
                    id as u64,
                    alloc::vec![Value::Int64(id), Value::Int64(a), Value::Int64(b),],
                ))
                .unwrap();
        }

        let plan = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::Scan {
                table: "composite_scores".into(),
            }),
            order_by: alloc::vec![
                (AstExpr::column("composite_scores", "a", 1), SortOrder::Asc,),
                (AstExpr::column("composite_scores", "b", 2), SortOrder::Asc,),
            ],
        };

        let ctx = build_execution_context(&cache, "composite_scores");
        let planner = QueryPlanner::new(ctx);
        let physical = planner.plan(plan.clone());
        println!("Physical plan: {:?}", physical);

        match &physical {
            PhysicalPlan::IndexScan { index, reverse, .. } => {
                assert_eq!(index, "idx_a_b", "ORDER BY should use the composite index");
                assert!(
                    !reverse,
                    "Ascending ORDER BY should use the natural composite-index order"
                );
            }
            _ => panic!(
                "Expected composite ORDER BY to optimize to IndexScan, got {:?}",
                physical
            ),
        }

        let result = execute_plan(&cache, "composite_scores", plan).unwrap();
        let actual: Vec<(i64, i64, i64)> = result
            .iter()
            .map(|row| {
                let id = match row.get(0) {
                    Some(&Value::Int64(v)) => v,
                    other => panic!("Expected Int64 id, got {:?}", other),
                };
                let a = match row.get(1) {
                    Some(&Value::Int64(v)) => v,
                    other => panic!("Expected Int64 a, got {:?}", other),
                };
                let b = match row.get(2) {
                    Some(&Value::Int64(v)) => v,
                    other => panic!("Expected Int64 b, got {:?}", other),
                };
                (id, a, b)
            })
            .collect();

        println!("Composite ORDER BY result: {:?}", actual);

        assert_eq!(
            actual,
            alloc::vec![(1, 1, 2), (2, 1, 10), (3, 2, 1)],
            "Composite index scan must follow tuple order `(a, b)`, not serialized-string order",
        );
    }

    #[test]
    fn test_order_by_composite_index_limit_offset_preserves_pagination_order() {
        use cynos_core::schema::TableBuilder;
        use cynos_core::{DataType, Row, Value};
        use cynos_query::ast::SortOrder;
        use cynos_query::planner::PhysicalPlan;

        let table = TableBuilder::new("composite_scores_paged")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("a", DataType::Int64)
            .unwrap()
            .add_column("b", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_a_b", &["a", "b"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut cache = TableCache::new();
        cache.create_table(table).unwrap();

        let store = cache.get_table_mut("composite_scores_paged").unwrap();
        for (id, a, b) in [(1_i64, 1_i64, 2_i64), (2, 1, 10), (3, 2, 1)] {
            store
                .insert(Row::new(
                    id as u64,
                    alloc::vec![Value::Int64(id), Value::Int64(a), Value::Int64(b),],
                ))
                .unwrap();
        }

        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Scan {
                    table: "composite_scores_paged".into(),
                }),
                order_by: alloc::vec![
                    (
                        AstExpr::column("composite_scores_paged", "a", 1),
                        SortOrder::Asc,
                    ),
                    (
                        AstExpr::column("composite_scores_paged", "b", 2),
                        SortOrder::Asc,
                    ),
                ],
            }),
            limit: 1,
            offset: 1,
        };

        let ctx = build_execution_context(&cache, "composite_scores_paged");
        let planner = QueryPlanner::new(ctx);
        let physical = planner.plan(plan.clone());
        println!("Paged physical plan: {:?}", physical);

        match &physical {
            PhysicalPlan::IndexScan {
                index,
                limit,
                offset,
                reverse,
                ..
            } => {
                assert_eq!(index, "idx_a_b");
                assert_eq!(*limit, Some(1));
                assert_eq!(*offset, Some(1));
                assert!(!reverse);
            }
            _ => panic!(
                "Expected paged composite ORDER BY to optimize to IndexScan, got {:?}",
                physical
            ),
        }

        let result = execute_plan(&cache, "composite_scores_paged", plan).unwrap();
        assert_eq!(
            result.len(),
            1,
            "LIMIT 1 OFFSET 1 should return exactly one row"
        );
        assert_eq!(result[0].get(0), Some(&Value::Int64(2)));
        assert_eq!(result[0].get(1), Some(&Value::Int64(1)));
        assert_eq!(result[0].get(2), Some(&Value::Int64(10)));
    }

    /// Test that index lookup via execute_plan is much faster than full table scan.
    /// This validates that the query engine properly uses indexes.
    #[test]
    fn test_index_lookup_vs_full_scan_performance() {
        use cynos_core::schema::TableBuilder;
        use cynos_core::{DataType, Row, Value};
        use std::time::Instant;

        // Create a table with primary key index
        let table = TableBuilder::new("perf_test")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("value", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut cache = TableCache::new();
        cache.create_table(table).unwrap();

        // Insert 100K rows
        let row_count = 100_000;
        let store = cache.get_table_mut("perf_test").unwrap();
        for i in 0..row_count {
            store
                .insert(Row::new(
                    i as u64,
                    alloc::vec![Value::Int64(i as i64), Value::Int64(i as i64 * 10),],
                ))
                .unwrap();
        }

        let iterations = 100;
        let target_id = 50; // Look for id = 50

        // Method 1: Full table scan (old UpdateBuilder approach)
        let start = Instant::now();
        for _ in 0..iterations {
            let store = cache.get_table("perf_test").unwrap();
            let _found: Vec<_> = store
                .scan()
                .filter(|row| {
                    row.get(0)
                        .map(|v| matches!(v, Value::Int64(id) if *id == target_id))
                        .unwrap_or(false)
                })
                .collect();
        }
        let full_scan_time = start.elapsed();

        // Method 2: Index lookup via query engine (new UpdateBuilder approach)
        let start = Instant::now();
        for _ in 0..iterations {
            let plan = LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table: "perf_test".into(),
                }),
                predicate: AstExpr::eq(
                    AstExpr::column("perf_test", "id", 0),
                    AstExpr::literal(Value::Int64(target_id)),
                ),
            };
            let _result = execute_plan(&cache, "perf_test", plan).unwrap();
        }
        let index_lookup_time = start.elapsed();

        let full_scan_avg_us = full_scan_time.as_micros() as f64 / iterations as f64;
        let index_lookup_avg_us = index_lookup_time.as_micros() as f64 / iterations as f64;
        let speedup = full_scan_avg_us / index_lookup_avg_us;

        println!("\n=== Index Lookup vs Full Scan Performance ===");
        println!("Row count: {}", row_count);
        println!("Iterations: {}", iterations);
        println!("Full scan avg: {:.2} µs", full_scan_avg_us);
        println!("Index lookup avg: {:.2} µs", index_lookup_avg_us);
        println!("Speedup: {:.1}x", speedup);

        // Index lookup should be significantly faster (at least 10x for 100K rows)
        assert!(
            speedup > 10.0,
            "Index lookup should be at least 10x faster than full scan, but was only {:.1}x faster",
            speedup
        );
    }

    /// Test: WHERE on non-indexed column + ORDER BY on indexed column should still filter correctly
    /// Bug: When WHERE name = 'xxx' ORDER BY price DESC is used, the optimizer may choose
    /// idx_price for ORDER BY but ignore the WHERE filter, returning wrong results.
    #[test]
    fn test_where_filter_with_order_by_on_different_index() {
        use cynos_core::schema::TableBuilder;
        use cynos_core::{DataType, Row, Value};

        // Create a table with price index but no name index
        let table = TableBuilder::new("stocks")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_column("price", DataType::Float64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_price", &["price"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut cache = TableCache::new();
        cache.create_table(table).unwrap();

        // Insert test data
        let store = cache.get_table_mut("stocks").unwrap();
        let test_data = [
            (1, "Apple Inc", 150.0),
            (2, "E82 Group", 200.0), // Target row
            (3, "Microsoft", 300.0),
            (4, "Google", 250.0),
            (5, "Amazon", 180.0),
        ];
        for (id, name, price) in test_data {
            store
                .insert(Row::new(
                    id as u64,
                    alloc::vec![
                        Value::Int64(id),
                        Value::String(name.into()),
                        Value::Float64(price),
                    ],
                ))
                .unwrap();
        }

        // Query: WHERE name = 'E82 Group' ORDER BY price DESC LIMIT 100
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::Scan {
                        table: "stocks".into(),
                    }),
                    predicate: AstExpr::eq(
                        AstExpr::column("stocks", "name", 1),
                        AstExpr::literal(Value::String("E82 Group".into())),
                    ),
                }),
                order_by: alloc::vec![(
                    AstExpr::column("stocks", "price", 2),
                    cynos_query::ast::SortOrder::Desc,
                )],
            }),
            limit: 100,
            offset: 0,
        };

        println!("Input plan: {:?}", plan);

        // Build context and execute
        let ctx = build_execution_context(&cache, "stocks");
        let planner = QueryPlanner::new(ctx);
        let physical = planner.plan(plan.clone());
        println!("Physical plan: {:?}", physical);

        let result = execute_plan(&cache, "stocks", plan).unwrap();
        println!("Result count: {}", result.len());
        for row in &result {
            println!("Row: {:?}", row);
        }

        // Should return exactly 1 row with name = 'E82 Group'
        assert_eq!(
            result.len(),
            1,
            "Expected exactly 1 row with name='E82 Group'"
        );
        assert_eq!(
            result[0].get(1),
            Some(&Value::String("E82 Group".into())),
            "The row should have name='E82 Group'"
        );
    }
}
