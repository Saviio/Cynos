//! Database - Main entry point for Cynos database operations.
//!
//! This module provides the `Database` struct which is the primary interface
//! for creating tables, executing queries, and managing data.

use crate::binary_protocol::SchemaLayoutCache;
use crate::convert::{gql_response_to_js, js_to_gql_variables};
use crate::dataflow_compiler::compile_to_dataflow;
use crate::live_runtime::{
    collect_trace_bootstrap_source_bindings, LiveDependencySet, LivePlan, LiveRegistry,
};
#[cfg(feature = "benchmark")]
use crate::profiling::SnapshotInitProfile;
use crate::profiling::{
    DeltaFlushProfile, IvmBridgeProfile, SnapshotFlushProfile, TraceInitProfile,
};
use crate::query_builder::{DeleteBuilder, InsertBuilder, SelectBuilder, UpdateBuilder};
use crate::reactive_bridge::JsGraphqlSubscription;
use crate::table::{JsTable, JsTableBuilder};
use crate::transaction::{CommitProfile, JsTransaction};
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::Row;
use cynos_gql::{PreparedQuery as GqlPreparedQuery, SchemaCache as GraphqlSchemaCache};
use cynos_incremental::{CompiledBootstrapPlan, CompiledIvmPlan, Delta};
use cynos_query::plan_cache::PlanCache;
use cynos_reactive::TableId;
#[cfg(feature = "benchmark")]
use cynos_storage::StorageInsertProfile;
use cynos_storage::TableCache;
use wasm_bindgen::prelude::*;

/// The main database interface.
///
/// Provides methods for:
/// - Creating and dropping tables
/// - CRUD operations (insert, select, update, delete)
/// - Transaction management
/// - Observable queries
#[wasm_bindgen]
pub struct Database {
    name: String,
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<LiveRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    next_table_id: Rc<RefCell<TableId>>,
    schema_layout_cache: Rc<RefCell<SchemaLayoutCache>>,
    plan_cache: Rc<RefCell<PlanCache>>,
    graphql_schema_cache: Rc<RefCell<GraphqlSchemaCache>>,
    schema_epoch: Rc<RefCell<u64>>,
    last_commit_profile: Rc<RefCell<Option<CommitProfile>>>,
    #[cfg(feature = "benchmark")]
    last_insert_profile: Rc<RefCell<Option<StorageInsertProfile>>>,
}

/// A prepared GraphQL query that reuses the parsed document across executions.
#[wasm_bindgen]
pub struct PreparedGraphqlQuery {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<LiveRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    graphql_schema_cache: Rc<RefCell<GraphqlSchemaCache>>,
    schema_epoch: Rc<RefCell<u64>>,
    prepared: GqlPreparedQuery,
}

#[wasm_bindgen]
impl Database {
    /// Creates a new database instance.
    #[wasm_bindgen(constructor)]
    pub fn new(name: &str) -> Self {
        let query_registry = Rc::new(RefCell::new(LiveRegistry::new()));
        // Set self reference for microtask scheduling
        query_registry
            .borrow_mut()
            .set_self_ref(query_registry.clone());

        Self {
            name: name.to_string(),
            cache: Rc::new(RefCell::new(TableCache::new())),
            query_registry,
            table_id_map: Rc::new(RefCell::new(hashbrown::HashMap::new())),
            next_table_id: Rc::new(RefCell::new(1)),
            schema_layout_cache: Rc::new(RefCell::new(SchemaLayoutCache::new())),
            plan_cache: Rc::new(RefCell::new(PlanCache::default_size())),
            graphql_schema_cache: Rc::new(RefCell::new(GraphqlSchemaCache::new())),
            schema_epoch: Rc::new(RefCell::new(0)),
            last_commit_profile: Rc::new(RefCell::new(None)),
            #[cfg(feature = "benchmark")]
            last_insert_profile: Rc::new(RefCell::new(None)),
        }
    }

    /// Async factory method for creating a database (for WASM compatibility).
    #[wasm_bindgen(js_name = create)]
    pub async fn create(name: &str) -> Result<Database, JsValue> {
        Ok(Self::new(name))
    }

    /// Returns the database name.
    #[wasm_bindgen(getter)]
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Creates a new table builder.
    #[wasm_bindgen(js_name = createTable)]
    pub fn create_table(&self, name: &str) -> JsTableBuilder {
        JsTableBuilder::new(name)
    }

    /// Registers a table schema with the database.
    #[wasm_bindgen(js_name = registerTable)]
    pub fn register_table(&self, builder: &JsTableBuilder) -> Result<(), JsValue> {
        let schema = builder.build_internal()?;
        let table_name = schema.name().to_string();

        self.cache
            .borrow_mut()
            .create_table(schema)
            .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

        // Assign table ID
        let table_id = *self.next_table_id.borrow();
        *self.next_table_id.borrow_mut() += 1;
        self.table_id_map.borrow_mut().insert(table_name, table_id);
        *self.schema_epoch.borrow_mut() += 1;
        self.graphql_schema_cache.borrow_mut().clear();

        Ok(())
    }

    /// Gets a table reference by name.
    pub fn table(&self, name: &str) -> Option<JsTable> {
        self.cache
            .borrow()
            .get_table(name)
            .map(|store| JsTable::new(store.schema().clone()))
    }

    /// Drops a table from the database.
    #[wasm_bindgen(js_name = dropTable)]
    pub fn drop_table(&self, name: &str) -> Result<(), JsValue> {
        self.cache
            .borrow_mut()
            .drop_table(name)
            .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

        self.table_id_map.borrow_mut().remove(name);
        *self.schema_epoch.borrow_mut() += 1;
        self.graphql_schema_cache.borrow_mut().clear();
        Ok(())
    }

    /// Returns all table names.
    #[wasm_bindgen(js_name = tableNames)]
    pub fn table_names(&self) -> js_sys::Array {
        let arr = js_sys::Array::new();
        for name in self.cache.borrow().table_names() {
            arr.push(&JsValue::from_str(name));
        }
        arr
    }

    /// Returns the number of tables.
    #[wasm_bindgen(js_name = tableCount)]
    pub fn table_count(&self) -> usize {
        self.cache.borrow().table_count()
    }

    /// Starts a SELECT query.
    /// Accepts either:
    /// - A single string: select('*') or select('name')
    /// - Multiple strings: select('name', 'score') - passed as variadic args
    #[wasm_bindgen(variadic)]
    pub fn select(&self, columns: &JsValue) -> SelectBuilder {
        SelectBuilder::new(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            self.schema_layout_cache.clone(),
            self.plan_cache.clone(),
            columns.clone(),
        )
    }

    /// Starts an INSERT operation.
    pub fn insert(&self, table: &str) -> InsertBuilder {
        InsertBuilder::new(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            #[cfg(feature = "benchmark")]
            self.last_insert_profile.clone(),
            table,
        )
    }

    /// Starts an UPDATE operation.
    pub fn update(&self, table: &str) -> UpdateBuilder {
        UpdateBuilder::new(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            table,
        )
    }

    /// Starts a DELETE operation.
    pub fn delete(&self, table: &str) -> DeleteBuilder {
        DeleteBuilder::new(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            table,
        )
    }

    /// Begins a new transaction.
    pub fn transaction(&self) -> JsTransaction {
        JsTransaction::new(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            self.last_commit_profile.clone(),
        )
    }

    #[wasm_bindgen(js_name = takeLastCommitProfile)]
    pub fn take_last_commit_profile(&self) -> JsValue {
        let Some(profile) = self.last_commit_profile.borrow_mut().take() else {
            return JsValue::NULL;
        };

        commit_profile_to_js_value(profile)
    }

    #[wasm_bindgen(js_name = takeLastDeltaFlushProfile)]
    pub fn take_last_delta_flush_profile(&self) -> JsValue {
        let Some(profile) = self.query_registry.borrow().take_last_delta_flush_profile() else {
            return JsValue::NULL;
        };

        delta_flush_profile_to_js_value(profile)
    }

    #[wasm_bindgen(js_name = takeLastIvmBridgeProfile)]
    pub fn take_last_ivm_bridge_profile(&self) -> JsValue {
        let Some(profile) = self.query_registry.borrow().take_last_ivm_bridge_profile() else {
            return JsValue::NULL;
        };

        ivm_bridge_profile_to_js_value(profile)
    }

    #[wasm_bindgen(js_name = takeLastTraceInitProfile)]
    pub fn take_last_trace_init_profile(&self) -> JsValue {
        let Some(profile) = self.query_registry.borrow().take_last_trace_init_profile() else {
            return JsValue::NULL;
        };

        trace_init_profile_to_js_value(profile)
    }

    #[cfg(feature = "benchmark")]
    #[wasm_bindgen(js_name = takeLastSnapshotInitProfile)]
    pub fn take_last_snapshot_init_profile(&self) -> JsValue {
        let Some(profile) = self
            .query_registry
            .borrow()
            .take_last_snapshot_init_profile()
        else {
            return JsValue::NULL;
        };

        snapshot_init_profile_to_js_value(profile)
    }

    #[cfg(feature = "benchmark")]
    #[wasm_bindgen(js_name = takeLastInsertProfile)]
    pub fn take_last_insert_profile(&self) -> JsValue {
        let Some(profile) = self.last_insert_profile.borrow_mut().take() else {
            return JsValue::NULL;
        };

        storage_insert_profile_to_js_value(profile)
    }

    #[wasm_bindgen(js_name = takeLastSnapshotFlushProfile)]
    pub fn take_last_snapshot_flush_profile(&self) -> JsValue {
        let Some(profile) = self
            .query_registry
            .borrow()
            .take_last_snapshot_flush_profile()
        else {
            return JsValue::NULL;
        };

        snapshot_flush_profile_to_js_value(profile)
    }

    /// Clears all data from all tables.
    pub fn clear(&self) {
        self.cache.borrow_mut().clear();
    }

    /// Clears data from a specific table.
    #[wasm_bindgen(js_name = clearTable)]
    pub fn clear_table(&self, name: &str) -> Result<(), JsValue> {
        self.cache
            .borrow_mut()
            .clear_table(name)
            .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))
    }

    /// Returns the total row count across all tables.
    #[wasm_bindgen(js_name = totalRowCount)]
    pub fn total_row_count(&self) -> usize {
        self.cache.borrow().total_row_count()
    }

    /// Checks if a table exists.
    #[wasm_bindgen(js_name = hasTable)]
    pub fn has_table(&self, name: &str) -> bool {
        self.cache.borrow().has_table(name)
    }

    /// Renders the current GraphQL schema as SDL.
    #[wasm_bindgen(js_name = graphqlSchema)]
    pub fn graphql_schema(&self) -> String {
        let cache = self.cache.borrow();
        let epoch = *self.schema_epoch.borrow();
        self.graphql_schema_cache.borrow_mut().sdl(epoch, &cache)
    }

    /// Executes a GraphQL query against the current database snapshot.
    ///
    /// Returns a standard GraphQL payload object with a single `data` property.
    #[wasm_bindgen(js_name = graphql)]
    pub fn graphql(
        &self,
        query: &str,
        variables: Option<JsValue>,
        operation_name: Option<String>,
    ) -> Result<JsValue, JsValue> {
        let variables = js_to_gql_variables(variables.as_ref())?;
        let prepared = GqlPreparedQuery::parse_with_operation(query, operation_name.as_deref())
            .map_err(|error| JsValue::from_str(error.message()))?;

        let cache = self.cache.borrow();
        let (catalog, bound) = bind_graphql_operation(
            &prepared,
            &cache,
            &self.graphql_schema_cache,
            &self.schema_epoch,
            &variables,
        )?;
        drop(cache);

        execute_graphql_bound_operation(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            catalog,
            bound,
        )
    }

    /// Creates a live GraphQL subscription backed by the root query planner path.
    #[wasm_bindgen(js_name = subscribeGraphql)]
    pub fn subscribe_graphql(
        &self,
        query: &str,
        variables: Option<JsValue>,
        operation_name: Option<String>,
    ) -> Result<JsGraphqlSubscription, JsValue> {
        let variables = js_to_gql_variables(variables.as_ref())?;
        let prepared = GqlPreparedQuery::parse_with_operation(query, operation_name.as_deref())
            .map_err(|error| JsValue::from_str(error.message()))?;

        let cache = self.cache.borrow();
        let (catalog, bound) = bind_graphql_operation(
            &prepared,
            &cache,
            &self.graphql_schema_cache,
            &self.schema_epoch,
            &variables,
        )?;
        drop(cache);

        create_graphql_subscription(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            catalog,
            bound,
        )
    }

    /// Parses and prepares a GraphQL query for repeated execution.
    #[wasm_bindgen(js_name = prepareGraphql)]
    pub fn prepare_graphql(
        &self,
        query: &str,
        operation_name: Option<String>,
    ) -> Result<PreparedGraphqlQuery, JsValue> {
        let prepared = GqlPreparedQuery::parse_with_operation(query, operation_name.as_deref())
            .map_err(|error| JsValue::from_str(error.message()))?;
        Ok(PreparedGraphqlQuery {
            cache: self.cache.clone(),
            query_registry: self.query_registry.clone(),
            table_id_map: self.table_id_map.clone(),
            graphql_schema_cache: self.graphql_schema_cache.clone(),
            schema_epoch: self.schema_epoch.clone(),
            prepared,
        })
    }

    /// Benchmarks pure Rust insert performance without JS serialization overhead.
    ///
    /// This method generates and inserts `count` rows directly in Rust,
    /// measuring only the storage layer performance.
    ///
    /// Returns an object with:
    /// - `duration_ms`: Total time in milliseconds
    /// - `rows_per_sec`: Throughput in rows per second
    #[cfg(feature = "benchmark")]
    #[wasm_bindgen(js_name = benchmarkInsert)]
    pub fn benchmark_insert(&self, table: &str, count: u32) -> Result<JsValue, JsValue> {
        use cynos_core::Value;

        let mut cache = self.cache.borrow_mut();
        let store = cache
            .get_table_mut(table)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table)))?;

        let schema = store.schema().clone();
        let columns = schema.columns();

        // Generate rows in Rust (no JS serialization)
        let start = js_sys::Date::now();

        for i in 0..count {
            let row_id = cynos_core::next_row_id();
            let mut values = Vec::with_capacity(columns.len());

            for (col_idx, col) in columns.iter().enumerate() {
                let value = match col.data_type() {
                    cynos_core::DataType::Int64 => {
                        if col_idx == 0 {
                            // Primary key - use sequential ID
                            Value::Int64(i as i64 + 1)
                        } else {
                            Value::Int64((i % 1000) as i64)
                        }
                    }
                    cynos_core::DataType::Int32 => Value::Int32((i % 100) as i32),
                    cynos_core::DataType::String => Value::String(alloc::format!("value_{}", i)),
                    cynos_core::DataType::Boolean => Value::Boolean(i % 2 == 0),
                    cynos_core::DataType::Float64 => Value::Float64(i as f64 * 0.1),
                    cynos_core::DataType::DateTime => {
                        Value::DateTime(1700000000000 + i as i64 * 1000)
                    }
                    _ => Value::Null,
                };
                values.push(value);
            }

            let row = Row::new(row_id, values);
            store
                .insert(row)
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
        }

        let end = js_sys::Date::now();
        let duration_ms = end - start;
        let rows_per_sec = if duration_ms > 0.0 {
            (count as f64 / duration_ms) * 1000.0
        } else {
            f64::INFINITY
        };

        // Return result object
        let result = js_sys::Object::new();
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("duration_ms"),
            &JsValue::from_f64(duration_ms),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("rows_per_sec"),
            &JsValue::from_f64(rows_per_sec),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("count"),
            &JsValue::from_f64(count as f64),
        )?;

        Ok(result.into())
    }

    /// Benchmarks pure Rust range query performance without JS serialization overhead.
    ///
    /// This method executes a range query (column > threshold) directly in Rust,
    /// measuring only the query execution time without serialization to JS.
    ///
    /// Parameters:
    /// - `table`: Table name to query
    /// - `column`: Column name for the range condition
    /// - `threshold`: The threshold value (column > threshold)
    ///
    /// Returns an object with:
    /// - `query_ms`: Time for query execution only (no serialization)
    /// - `serialize_ms`: Time for serialization to JS
    /// - `total_ms`: Total time including serialization
    /// - `row_count`: Number of rows returned
    /// - `serialization_overhead_pct`: Percentage of time spent on serialization
    #[cfg(feature = "benchmark")]
    #[wasm_bindgen(js_name = benchmarkRangeQuery)]
    pub fn benchmark_range_query(
        &self,
        table: &str,
        column: &str,
        threshold: f64,
    ) -> Result<JsValue, JsValue> {
        use crate::query_engine::execute_plan;
        use cynos_query::ast::{BinaryOp, Expr as AstExpr};
        use cynos_query::planner::LogicalPlan;

        let cache = self.cache.borrow();
        let store = cache
            .get_table(table)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table)))?;

        let schema = store.schema().clone();
        let col = schema
            .get_column(column)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Column not found: {}", column)))?;
        let col_idx = col.index();

        // Build logical plan: SELECT * FROM table WHERE column > threshold
        let scan = LogicalPlan::Scan {
            table: table.to_string(),
        };

        let predicate = AstExpr::BinaryOp {
            left: Box::new(AstExpr::column(table, column, col_idx)),
            op: BinaryOp::Gt,
            right: Box::new(AstExpr::Literal(cynos_core::Value::Int64(threshold as i64))),
        };

        let plan = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate,
        };

        // Measure query execution time (no serialization)
        let query_start = js_sys::Date::now();
        let rows = execute_plan(&cache, table, plan)
            .map_err(|e| JsValue::from_str(&alloc::format!("Query error: {:?}", e)))?;
        let query_end = js_sys::Date::now();
        let query_ms = query_end - query_start;

        let row_count = rows.len();

        // Measure serialization time
        let serialize_start = js_sys::Date::now();
        let _js_result = crate::convert::rows_to_js_array(&rows, &schema);
        let serialize_end = js_sys::Date::now();
        let serialize_ms = serialize_end - serialize_start;

        let total_ms = query_ms + serialize_ms;
        let serialization_overhead_pct = if total_ms > 0.0 {
            (serialize_ms / total_ms) * 100.0
        } else {
            0.0
        };

        // Return result object
        let result = js_sys::Object::new();
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("query_ms"),
            &JsValue::from_f64(query_ms),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("serialize_ms"),
            &JsValue::from_f64(serialize_ms),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("total_ms"),
            &JsValue::from_f64(total_ms),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("row_count"),
            &JsValue::from_f64(row_count as f64),
        )?;
        js_sys::Reflect::set(
            &result,
            &JsValue::from_str("serialization_overhead_pct"),
            &JsValue::from_f64(serialization_overhead_pct),
        )?;

        Ok(result.into())
    }
}

#[wasm_bindgen]
impl PreparedGraphqlQuery {
    /// Executes the prepared GraphQL query with an optional variables object.
    pub fn exec(&self, variables: Option<JsValue>) -> Result<JsValue, JsValue> {
        let variables = js_to_gql_variables(variables.as_ref())?;
        let cache = self.cache.borrow();
        let (catalog, bound) = bind_graphql_operation(
            &self.prepared,
            &cache,
            &self.graphql_schema_cache,
            &self.schema_epoch,
            &variables,
        )?;
        drop(cache);

        execute_graphql_bound_operation(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            catalog,
            bound,
        )
    }

    /// Creates a live subscription from a prepared GraphQL subscription document.
    pub fn subscribe(&self, variables: Option<JsValue>) -> Result<JsGraphqlSubscription, JsValue> {
        let variables = js_to_gql_variables(variables.as_ref())?;
        let cache = self.cache.borrow();
        let (catalog, bound) = bind_graphql_operation(
            &self.prepared,
            &cache,
            &self.graphql_schema_cache,
            &self.schema_epoch,
            &variables,
        )?;
        drop(cache);

        create_graphql_subscription(
            self.cache.clone(),
            self.query_registry.clone(),
            self.table_id_map.clone(),
            catalog,
            bound,
        )
    }
}

fn bind_graphql_operation(
    prepared: &GqlPreparedQuery,
    cache: &TableCache,
    graphql_schema_cache: &Rc<RefCell<GraphqlSchemaCache>>,
    schema_epoch: &Rc<RefCell<u64>>,
    variables: &cynos_gql::VariableValues,
) -> Result<(cynos_gql::GraphqlCatalog, cynos_gql::BoundOperation), JsValue> {
    let epoch = *schema_epoch.borrow();
    let catalog = graphql_schema_cache.borrow_mut().catalog(epoch, cache);
    let bound = prepared
        .bind(&catalog, Some(variables))
        .map_err(|error| JsValue::from_str(error.message()))?;
    Ok((catalog, bound))
}

fn commit_profile_to_js_value(profile: CommitProfile) -> JsValue {
    let object = js_sys::Object::new();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("storageCommitMs"),
        &JsValue::from_f64(profile.storage_commit_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("registryFlushMs"),
        &JsValue::from_f64(profile.registry_flush_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("totalCommitMs"),
        &JsValue::from_f64(profile.total_commit_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("changedTableCount"),
        &JsValue::from_f64(profile.changed_table_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("changedRowCount"),
        &JsValue::from_f64(profile.changed_row_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("deltaRowCount"),
        &JsValue::from_f64(profile.delta_row_count as f64),
    )
    .ok();
    object.into()
}

fn delta_flush_profile_to_js_value(profile: DeltaFlushProfile) -> JsValue {
    let object = js_sys::Object::new();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("deltaTableCount"),
        &JsValue::from_f64(profile.delta_table_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("deltaQueryCount"),
        &JsValue::from_f64(profile.delta_query_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rowsQueryCount"),
        &JsValue::from_f64(profile.rows_query_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("graphqlQueryCount"),
        &JsValue::from_f64(profile.graphql_query_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("deltaRowCount"),
        &JsValue::from_f64(profile.delta_row_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("cloneMs"),
        &JsValue::from_f64(profile.clone_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("queryOnTableChangeMs"),
        &JsValue::from_f64(profile.query_on_table_change_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("sourceDispatchMs"),
        &JsValue::from_f64(profile.source_dispatch_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("unaryExecuteMs"),
        &JsValue::from_f64(profile.unary_execute_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("joinExecuteMs"),
        &JsValue::from_f64(profile.join_execute_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("aggregateExecuteMs"),
        &JsValue::from_f64(profile.aggregate_execute_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("resultApplyMs"),
        &JsValue::from_f64(profile.result_apply_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("totalMs"),
        &JsValue::from_f64(profile.total_ms),
    )
    .ok();
    object.into()
}

fn trace_init_profile_to_js_value(profile: TraceInitProfile) -> JsValue {
    let object = js_sys::Object::new();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("compilePlanMs"),
        &JsValue::from_f64(profile.compile_plan_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("compileToDataflowMs"),
        &JsValue::from_f64(profile.compile_to_dataflow_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("compileIvmPlanMs"),
        &JsValue::from_f64(profile.compile_ivm_plan_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("compileTraceProgramMs"),
        &JsValue::from_f64(profile.compile_trace_program_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("initialQueryMs"),
        &JsValue::from_f64(profile.initial_query_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("sourceBootstrapMs"),
        &JsValue::from_f64(profile.source_bootstrap_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("sourceAccessMs"),
        &JsValue::from_f64(profile.source_access_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("sourceEmitMs"),
        &JsValue::from_f64(profile.source_emit_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("bootstrapScanMs"),
        &JsValue::from_f64(profile.bootstrap_scan_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("bootstrapExecuteMs"),
        &JsValue::from_f64(profile.bootstrap_execute_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("filterBootstrapMs"),
        &JsValue::from_f64(profile.filter_bootstrap_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("projectBootstrapMs"),
        &JsValue::from_f64(profile.project_bootstrap_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("mapBootstrapMs"),
        &JsValue::from_f64(profile.map_bootstrap_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("joinBootstrapMs"),
        &JsValue::from_f64(profile.join_bootstrap_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("aggregateBootstrapMs"),
        &JsValue::from_f64(profile.aggregate_bootstrap_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rootSinkMs"),
        &JsValue::from_f64(profile.root_sink_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("materializedViewInitMs"),
        &JsValue::from_f64(profile.materialized_view_init_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("totalMs"),
        &JsValue::from_f64(profile.total_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("sourceTableCount"),
        &JsValue::from_f64(profile.source_table_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("initialRowCount"),
        &JsValue::from_f64(profile.initial_row_count as f64),
    )
    .ok();
    object.into()
}

#[cfg(feature = "benchmark")]
fn snapshot_init_profile_to_js_value(profile: SnapshotInitProfile) -> JsValue {
    let object = js_sys::Object::new();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("logicalPlanMs"),
        &JsValue::from_f64(profile.logical_plan_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("describeOutputMs"),
        &JsValue::from_f64(profile.describe_output_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("binaryLayoutMs"),
        &JsValue::from_f64(profile.binary_layout_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("partialRefreshPlanMs"),
        &JsValue::from_f64(profile.partial_refresh_plan_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("compileMainPlanMs"),
        &JsValue::from_f64(profile.compile_main_plan_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rootSubsetPlanMs"),
        &JsValue::from_f64(profile.root_subset_plan_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("initialQueryMs"),
        &JsValue::from_f64(profile.initial_query_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("dependencyBindingsMs"),
        &JsValue::from_f64(profile.dependency_bindings_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("initialResultAdaptMs"),
        &JsValue::from_f64(profile.initial_result_adapt_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("observableInitMs"),
        &JsValue::from_f64(profile.observable_init_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("totalMs"),
        &JsValue::from_f64(profile.total_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("initialRowCount"),
        &JsValue::from_f64(profile.initial_row_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("visibleRowCount"),
        &JsValue::from_f64(profile.visible_row_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("partialRefreshEnabled"),
        &JsValue::from_bool(profile.partial_refresh_enabled),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rootSubsetEnabled"),
        &JsValue::from_bool(profile.root_subset_enabled),
    )
    .ok();
    object.into()
}

fn snapshot_flush_profile_to_js_value(profile: SnapshotFlushProfile) -> JsValue {
    let object = js_sys::Object::new();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("changedTableCount"),
        &JsValue::from_f64(profile.changed_table_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rowsQueryCount"),
        &JsValue::from_f64(profile.rows_query_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("graphqlQueryCount"),
        &JsValue::from_f64(profile.graphql_query_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("invalidationMergeMs"),
        &JsValue::from_f64(profile.invalidation_merge_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rowsQueryOnChangeMs"),
        &JsValue::from_f64(profile.rows_query_on_change_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("graphqlQueryOnChangeMs"),
        &JsValue::from_f64(profile.graphql_query_on_change_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("reactivePatchAttemptCount"),
        &JsValue::from_f64(profile.reactive_patch_attempt_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("reactivePatchHitCount"),
        &JsValue::from_f64(profile.reactive_patch_hit_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("reactivePatchMs"),
        &JsValue::from_f64(profile.reactive_patch_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("partialRefreshAttemptCount"),
        &JsValue::from_f64(profile.partial_refresh_attempt_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("partialRefreshHitCount"),
        &JsValue::from_f64(profile.partial_refresh_hit_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("partialRefreshCollectMs"),
        &JsValue::from_f64(profile.partial_refresh_collect_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("partialRefreshRequeryMs"),
        &JsValue::from_f64(profile.partial_refresh_requery_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("partialRefreshApplyMs"),
        &JsValue::from_f64(profile.partial_refresh_apply_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("partialRefreshCompareMs"),
        &JsValue::from_f64(profile.partial_refresh_compare_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rootSubsetAttemptCount"),
        &JsValue::from_f64(profile.root_subset_attempt_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rootSubsetHitCount"),
        &JsValue::from_f64(profile.root_subset_hit_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rootSubsetCollectMs"),
        &JsValue::from_f64(profile.root_subset_collect_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rootSubsetRequeryMs"),
        &JsValue::from_f64(profile.root_subset_requery_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rootSubsetApplyMs"),
        &JsValue::from_f64(profile.root_subset_apply_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rootSubsetCompareMs"),
        &JsValue::from_f64(profile.root_subset_compare_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("fullRequeryCount"),
        &JsValue::from_f64(profile.full_requery_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("fullRequeryMs"),
        &JsValue::from_f64(profile.full_requery_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("fullRequeryCompareMs"),
        &JsValue::from_f64(profile.full_requery_compare_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("callbackMs"),
        &JsValue::from_f64(profile.callback_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("totalMs"),
        &JsValue::from_f64(profile.total_ms),
    )
    .ok();
    object.into()
}

fn ivm_bridge_profile_to_js_value(profile: IvmBridgeProfile) -> JsValue {
    let object = js_sys::Object::new();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("callbackCount"),
        &JsValue::from_f64(profile.callback_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("addedRowCount"),
        &JsValue::from_f64(profile.added_row_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("removedRowCount"),
        &JsValue::from_f64(profile.removed_row_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("serializeAddedMs"),
        &JsValue::from_f64(profile.serialize_added_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("serializeRemovedMs"),
        &JsValue::from_f64(profile.serialize_removed_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("assembleDeltaMs"),
        &JsValue::from_f64(profile.assemble_delta_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("callbackCallMs"),
        &JsValue::from_f64(profile.callback_call_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("totalMs"),
        &JsValue::from_f64(profile.total_ms),
    )
    .ok();
    object.into()
}

#[cfg(feature = "benchmark")]
fn storage_insert_profile_to_js_value(profile: StorageInsertProfile) -> JsValue {
    let object = js_sys::Object::new();
    let gin = js_sys::Object::new();

    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rowCount"),
        &JsValue::from_f64(profile.row_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("secondaryIndexCount"),
        &JsValue::from_f64(profile.secondary_index_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("ginIndexCount"),
        &JsValue::from_f64(profile.gin_index_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("validationMs"),
        &JsValue::from_f64(profile.validation_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rowIdIndexMs"),
        &JsValue::from_f64(profile.row_id_index_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("primaryIndexMs"),
        &JsValue::from_f64(profile.primary_index_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("secondaryIndexMs"),
        &JsValue::from_f64(profile.secondary_index_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("ginCollectMs"),
        &JsValue::from_f64(profile.gin_collect_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("ginFlushMs"),
        &JsValue::from_f64(profile.gin_flush_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("rowSlotMs"),
        &JsValue::from_f64(profile.row_slot_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &object,
        &JsValue::from_str("totalMs"),
        &JsValue::from_f64(profile.total_ms),
    )
    .ok();

    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("parseJsonMs"),
        &JsValue::from_f64(profile.gin.parse_json_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("pathLookupMs"),
        &JsValue::from_f64(profile.gin.path_lookup_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("scalarEmitMs"),
        &JsValue::from_f64(profile.gin.scalar_emit_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("containsStringifyMs"),
        &JsValue::from_f64(profile.gin.contains_stringify_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("containsTrigramEmitMs"),
        &JsValue::from_f64(profile.gin.contains_trigram_emit_ms),
    )
    .ok();
    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("parseCallCount"),
        &JsValue::from_f64(profile.gin.parse_call_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("selectedPathEvalCount"),
        &JsValue::from_f64(profile.gin.selected_path_eval_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("selectedPathHitCount"),
        &JsValue::from_f64(profile.gin.selected_path_hit_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("pathKeyEmitCount"),
        &JsValue::from_f64(profile.gin.path_key_emit_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("scalarValueCount"),
        &JsValue::from_f64(profile.gin.scalar_value_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("containsValueCount"),
        &JsValue::from_f64(profile.gin.contains_value_count as f64),
    )
    .ok();
    js_sys::Reflect::set(
        &gin,
        &JsValue::from_str("containsTrigramCount"),
        &JsValue::from_f64(profile.gin.contains_trigram_count as f64),
    )
    .ok();

    js_sys::Reflect::set(&object, &JsValue::from_str("gin"), &gin).ok();
    object.into()
}

fn execute_graphql_bound_operation(
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<LiveRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    catalog: cynos_gql::GraphqlCatalog,
    bound: cynos_gql::BoundOperation,
) -> Result<JsValue, JsValue> {
    if bound.kind == cynos_gql::OperationType::Subscription {
        return Err(JsValue::from_str(
            "subscription operations must use subscribeGraphql() or PreparedGraphqlQuery.subscribe()",
        ));
    }

    let mut cache_ref = cache.borrow_mut();
    let outcome = cynos_gql::execute::execute_bound_operation_mut(&mut cache_ref, &catalog, &bound)
        .map_err(|error| JsValue::from_str(error.message()))?;
    drop(cache_ref);

    notify_graphql_changes(query_registry, table_id_map, &outcome.changes);
    gql_response_to_js(&outcome.response)
}

fn create_graphql_subscription(
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<LiveRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    catalog: cynos_gql::GraphqlCatalog,
    bound: cynos_gql::BoundOperation,
) -> Result<JsGraphqlSubscription, JsValue> {
    let live_plan = compile_graphql_live_plan(cache.clone(), table_id_map, catalog, bound)?;
    Ok(match live_plan.descriptor.engine {
        crate::live_runtime::LiveEngineKind::Snapshot => {
            live_plan.materialize_graphql_snapshot(cache, query_registry)
        }
        crate::live_runtime::LiveEngineKind::Delta => {
            live_plan.materialize_graphql_delta(cache, query_registry)
        }
    })
}

fn compile_graphql_live_plan(
    cache: Rc<RefCell<TableCache>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    catalog: cynos_gql::GraphqlCatalog,
    bound: cynos_gql::BoundOperation,
) -> Result<LivePlan, JsValue> {
    if bound.kind != cynos_gql::OperationType::Subscription {
        return Err(JsValue::from_str(
            "subscribeGraphql() only accepts subscription operations",
        ));
    }
    if bound.fields.len() != 1 {
        return Err(JsValue::from_str(
            "GraphQL subscriptions must select exactly one root field",
        ));
    }

    let field = bound
        .fields
        .into_iter()
        .next()
        .ok_or_else(|| JsValue::from_str("subscription is missing a root field"))?;
    if matches!(field.kind, cynos_gql::bind::BoundRootFieldKind::Typename) {
        return Err(JsValue::from_str(
            "GraphQL subscriptions must select a concrete root field",
        ));
    }

    let root_plan = cynos_gql::build_root_field_plan(&catalog, &field)
        .map_err(|error| JsValue::from_str(error.message()))?;
    let mut root_dependency_tables = root_plan.logical_plan.collect_tables();
    if !root_dependency_tables
        .iter()
        .any(|table| table == &root_plan.table_name)
    {
        root_dependency_tables.push(root_plan.table_name.clone());
    }
    let all_dependency_tables = cynos_gql::bind::collect_dependency_tables(&field);
    let (dependency_set, dependency_table_bindings) = {
        let table_id_map = table_id_map.borrow();
        let dependency_table_bindings =
            build_graphql_dependency_bindings(&table_id_map, &all_dependency_tables)?;
        let dependency_set = build_graphql_dependency_set(
            &table_id_map,
            &dependency_table_bindings,
            &root_dependency_tables,
        )?;
        (dependency_set, dependency_table_bindings)
    };

    {
        let cache_borrow = cache.borrow();
        let table_id_map = table_id_map.borrow();
        if cynos_gql::bind::is_delta_capable_root_field(&field) {
            if let Some(live_plan) = build_graphql_delta_live_plan(
                &cache_borrow,
                &table_id_map,
                dependency_set.clone(),
                catalog.clone(),
                field.clone(),
                dependency_table_bindings.clone(),
                &root_plan,
            )? {
                return Ok(live_plan);
            }
        }
    }

    let cache_borrow = cache.borrow();
    let compiled_plan = crate::query_engine::compile_cached_plan(
        &cache_borrow,
        &root_plan.table_name,
        root_plan.logical_plan,
    );
    let initial_output = crate::query_engine::execute_compiled_physical_plan_with_summary(
        &cache_borrow,
        &compiled_plan,
    )
    .map_err(|error| JsValue::from_str(&alloc::format!("Query execution error: {:?}", error)))?;

    Ok(LivePlan::graphql_snapshot(
        dependency_set,
        compiled_plan,
        initial_output.rows,
        initial_output.summary,
        catalog,
        field,
        dependency_table_bindings,
    ))
}

fn build_graphql_dependency_bindings(
    table_id_map: &hashbrown::HashMap<String, TableId>,
    dependency_tables: &[String],
) -> Result<Vec<(TableId, String)>, JsValue> {
    let mut bindings = dependency_tables
        .iter()
        .map(|table| {
            table_id_map
                .get(table)
                .copied()
                .map(|table_id| (table_id, table.clone()))
                .ok_or_else(|| JsValue::from_str(&alloc::format!("Table ID not found: {}", table)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    bindings.sort_unstable_by(|(left_id, left_name), (right_id, right_name)| {
        left_id
            .cmp(right_id)
            .then_with(|| left_name.cmp(right_name))
    });
    bindings.dedup_by(|left, right| left.0 == right.0);
    Ok(bindings)
}

fn build_graphql_dependency_set(
    table_id_map: &hashbrown::HashMap<String, TableId>,
    dependency_table_bindings: &[(TableId, String)],
    root_tables: &[String],
) -> Result<LiveDependencySet, JsValue> {
    let dependency_table_ids = dependency_table_bindings
        .iter()
        .map(|(table_id, _)| *table_id)
        .collect::<Vec<_>>();
    let root_table_ids = root_tables
        .iter()
        .map(|table| {
            table_id_map
                .get(table)
                .copied()
                .ok_or_else(|| JsValue::from_str(&alloc::format!("Table ID not found: {}", table)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(LiveDependencySet::graphql(
        dependency_table_ids,
        root_table_ids,
    ))
}

fn build_graphql_delta_live_plan(
    cache: &TableCache,
    table_id_map: &hashbrown::HashMap<String, TableId>,
    dependency_set: LiveDependencySet,
    catalog: cynos_gql::GraphqlCatalog,
    field: cynos_gql::bind::BoundRootField,
    dependency_table_bindings: Vec<(TableId, String)>,
    root_plan: &cynos_gql::RootFieldPlan,
) -> Result<Option<LivePlan>, JsValue> {
    let store = cache.get_table(&root_plan.table_name).ok_or_else(|| {
        JsValue::from_str(&alloc::format!("Table not found: {}", root_plan.table_name))
    })?;

    let physical_plan = crate::query_engine::compile_plan(
        cache,
        &root_plan.table_name,
        root_plan.logical_plan.clone(),
    );
    let mut table_schemas = hashbrown::HashMap::new();
    table_schemas.insert(root_plan.table_name.clone(), store.schema().clone());
    let Some(compile_result) = compile_to_dataflow(&physical_plan, table_id_map, &table_schemas)
    else {
        return Ok(None);
    };
    let compiled_ivm_plan = CompiledIvmPlan::compile(&compile_result.dataflow);
    let compiled_bootstrap_plan = CompiledBootstrapPlan::compile(&compile_result.dataflow);

    let initial_rows =
        crate::query_engine::execute_physical_plan(cache, &physical_plan).map_err(|error| {
            JsValue::from_str(&alloc::format!("Query execution error: {:?}", error))
        })?;
    let source_bindings = collect_trace_bootstrap_source_bindings(
        &physical_plan,
        &compile_result.table_ids,
        &table_schemas,
    );

    Ok(Some(LivePlan::graphql_delta(
        dependency_set,
        compile_result.dataflow,
        compiled_ivm_plan,
        compiled_bootstrap_plan,
        initial_rows,
        source_bindings,
        catalog,
        field,
        dependency_table_bindings,
    )))
}

fn notify_graphql_changes(
    query_registry: Rc<RefCell<LiveRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    changes: &[cynos_gql::TableChange],
) {
    let mut aggregated: hashbrown::HashMap<String, (Vec<Delta<Row>>, hashbrown::HashSet<u64>)> =
        hashbrown::HashMap::new();

    for change in changes {
        let entry = aggregated
            .entry(change.table_name.clone())
            .or_insert_with(|| (Vec::new(), hashbrown::HashSet::new()));

        for row_change in &change.row_changes {
            match row_change {
                cynos_gql::RowChange::Insert(row) => {
                    entry.0.push(Delta::insert(row.clone()));
                    entry.1.insert(row.id());
                }
                cynos_gql::RowChange::Update { old, new } => {
                    entry.0.push(Delta::delete(old.clone()));
                    entry.0.push(Delta::insert(new.clone()));
                    entry.1.insert(old.id());
                }
                cynos_gql::RowChange::Delete(row) => {
                    entry.0.push(Delta::delete(row.clone()));
                    entry.1.insert(row.id());
                }
            }
        }
    }

    let table_id_map = table_id_map.borrow();
    let mut registry = query_registry.borrow_mut();
    for (table_name, (deltas, changed_ids)) in aggregated {
        if let Some(table_id) = table_id_map.get(&table_name).copied() {
            registry.on_table_change_delta(table_id, deltas, &changed_ids);
        }
    }
}

#[allow(dead_code)]
impl Database {
    /// Gets the internal cache (for internal use).
    pub(crate) fn cache(&self) -> Rc<RefCell<TableCache>> {
        self.cache.clone()
    }

    /// Gets the query registry (for internal use).
    pub(crate) fn query_registry(&self) -> Rc<RefCell<LiveRegistry>> {
        self.query_registry.clone()
    }

    /// Gets the table ID for a table name.
    pub(crate) fn get_table_id(&self, name: &str) -> Option<TableId> {
        self.table_id_map.borrow().get(name).copied()
    }

    /// Notifies the query registry of table changes.
    pub(crate) fn notify_table_change(
        &self,
        table_id: TableId,
        changed_ids: &hashbrown::HashSet<u64>,
    ) {
        self.query_registry
            .borrow_mut()
            .on_table_change(table_id, changed_ids);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_runtime::LiveEngineKind;
    use crate::table::{ColumnOptions, ForeignKeyOptions};
    use crate::JsDataType;
    use cynos_core::{Row, Value};
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    fn setup_graphql_users_db() -> Database {
        let db = Database::new("graphql");
        let users = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);
        db.register_table(&users).unwrap();
        db
    }

    fn setup_graphql_users_posts_db() -> Database {
        let db = setup_graphql_users_db();
        let posts = db
            .create_table("posts")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("author_id", JsDataType::Int64, None)
            .column("title", JsDataType::String, None)
            .foreign_key(
                "fk_posts_author",
                "author_id",
                "users",
                "id",
                Some(
                    ForeignKeyOptions::new()
                        .set_field_name("author")
                        .set_reverse_field_name("posts"),
                ),
            );
        db.register_table(&posts).unwrap();
        db
    }

    fn setup_graphql_users_profiles_db() -> Database {
        let db = setup_graphql_users_db();
        let profiles = db
            .create_table("profiles")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column(
                "user_id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_unique(true)),
            )
            .column("bio", JsDataType::String, None)
            .foreign_key(
                "fk_profiles_user",
                "user_id",
                "users",
                "id",
                Some(
                    ForeignKeyOptions::new()
                        .set_field_name("user")
                        .set_reverse_field_name("profile"),
                ),
            );
        db.register_table(&profiles).unwrap();
        db
    }

    fn setup_graphql_issue_filter_db() -> Database {
        let db = Database::new("graphql_issue_filter");

        let projects = db
            .create_table("projects")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("healthScore", JsDataType::Int32, None);
        db.register_table(&projects).unwrap();

        let project_counters = db
            .create_table("projectCounters")
            .column(
                "projectId",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("openIssueCount", JsDataType::Int32, None)
            .foreign_key(
                "fk_project_counters_project",
                "projectId",
                "projects",
                "id",
                Some(
                    ForeignKeyOptions::new()
                        .set_field_name("project")
                        .set_reverse_field_name("counter"),
                ),
            );
        db.register_table(&project_counters).unwrap();

        let project_snapshots = db
            .create_table("projectSnapshots")
            .column(
                "projectId",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("velocity", JsDataType::Int32, None)
            .foreign_key(
                "fk_project_snapshots_project",
                "projectId",
                "projects",
                "id",
                Some(
                    ForeignKeyOptions::new()
                        .set_field_name("project")
                        .set_reverse_field_name("snapshot"),
                ),
            );
        db.register_table(&project_snapshots).unwrap();

        let issues = db
            .create_table("issues")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("projectId", JsDataType::Int64, None)
            .column("title", JsDataType::String, None)
            .column("status", JsDataType::String, None)
            .foreign_key(
                "fk_issues_project",
                "projectId",
                "projects",
                "id",
                Some(
                    ForeignKeyOptions::new()
                        .set_field_name("project")
                        .set_reverse_field_name("issues"),
                ),
            );
        db.register_table(&issues).unwrap();

        db
    }

    fn collect_titles(array: &js_sys::Array) -> Vec<String> {
        let mut titles = Vec::with_capacity(array.length() as usize);
        for index in 0..array.length() {
            let item = array.get(index);
            let title = js_sys::Reflect::get(&item, &JsValue::from_str("title"))
                .unwrap()
                .as_string()
                .unwrap();
            titles.push(title);
        }
        titles.sort();
        titles
    }

    fn collect_ids(array: &js_sys::Array, field_name: &str) -> Vec<i64> {
        let mut ids = Vec::with_capacity(array.length() as usize);
        for index in 0..array.length() {
            let item = array.get(index);
            let id = js_sys::Reflect::get(&item, &JsValue::from_str(field_name))
                .unwrap()
                .as_f64()
                .unwrap() as i64;
            ids.push(id);
        }
        ids.sort_unstable();
        ids
    }

    fn compile_subscription_engine(db: &Database, query: &str) -> LiveEngineKind {
        let prepared = GqlPreparedQuery::parse_with_operation(query, None).unwrap();
        let variables = cynos_gql::VariableValues::default();
        let cache = db.cache.borrow();
        let (catalog, bound) = bind_graphql_operation(
            &prepared,
            &cache,
            &db.graphql_schema_cache,
            &db.schema_epoch,
            &variables,
        )
        .unwrap();
        drop(cache);

        compile_graphql_live_plan(db.cache.clone(), db.table_id_map.clone(), catalog, bound)
            .unwrap()
            .descriptor
            .engine
    }

    #[wasm_bindgen_test]
    fn test_database_new() {
        let db = Database::new("test");
        assert_eq!(db.name(), "test");
        assert_eq!(db.table_count(), 0);
    }

    #[wasm_bindgen_test]
    fn test_database_create_table() {
        let db = Database::new("test");

        let builder = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);

        db.register_table(&builder).unwrap();

        assert!(db.has_table("users"));
        assert_eq!(db.table_count(), 1);
    }

    #[wasm_bindgen_test]
    fn test_database_drop_table() {
        let db = Database::new("test");

        let builder = db.create_table("users").column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        );

        db.register_table(&builder).unwrap();
        assert!(db.has_table("users"));

        db.drop_table("users").unwrap();
        assert!(!db.has_table("users"));
    }

    #[wasm_bindgen_test]
    fn test_database_table_names() {
        let db = Database::new("test");

        let builder1 = db.create_table("users").column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        );
        db.register_table(&builder1).unwrap();

        let builder2 = db.create_table("orders").column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        );
        db.register_table(&builder2).unwrap();

        let names = db.table_names();
        assert_eq!(names.length(), 2);
    }

    #[wasm_bindgen_test]
    fn test_database_get_table() {
        let db = Database::new("test");

        let builder = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);

        db.register_table(&builder).unwrap();

        let table = db.table("users").unwrap();
        assert_eq!(table.name(), "users");
        assert_eq!(table.column_count(), 2);
    }

    #[wasm_bindgen_test]
    fn test_database_clear() {
        let db = Database::new("test");

        let builder = db.create_table("users").column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        );

        db.register_table(&builder).unwrap();

        db.clear();
        assert_eq!(db.total_row_count(), 0);
        // Tables still exist after clear
        assert!(db.has_table("users"));
    }

    #[wasm_bindgen_test]
    fn test_database_graphql_executes_queries() {
        let db = Database::new("test");

        let users = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);
        db.register_table(&users).unwrap();

        let orders = db
            .create_table("orders")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("user_id", JsDataType::Int64, None)
            .column("total", JsDataType::Float64, None)
            .foreign_key(
                "fk_orders_user",
                "user_id",
                "users",
                "id",
                Some(
                    ForeignKeyOptions::new()
                        .set_field_name("buyer")
                        .set_reverse_field_name("orders"),
                ),
            );
        db.register_table(&orders).unwrap();

        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                1,
                alloc::vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                2,
                alloc::vec![Value::Int64(2), Value::String("Bob".into())],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(
                10,
                alloc::vec![Value::Int64(10), Value::Int64(1), Value::Float64(120.0)],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(
                11,
                alloc::vec![Value::Int64(11), Value::Int64(2), Value::Float64(80.0)],
            ))
            .unwrap();

        let result = db
            .graphql(
                "{ orders(orderBy: [{ field: TOTAL, direction: DESC }]) { id buyer { name } } }",
                None,
                None,
            )
            .unwrap();
        let data = js_sys::Reflect::get(&result, &JsValue::from_str("data")).unwrap();
        let orders = js_sys::Reflect::get(&data, &JsValue::from_str("orders")).unwrap();
        let orders = js_sys::Array::from(&orders);
        assert_eq!(orders.length(), 2);

        let first = orders.get(0);
        let id = js_sys::Reflect::get(&first, &JsValue::from_str("id"))
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!(id, 10.0);
        let buyer = js_sys::Reflect::get(&first, &JsValue::from_str("buyer")).unwrap();
        let name = js_sys::Reflect::get(&buyer, &JsValue::from_str("name"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(name, "Alice");
    }

    #[wasm_bindgen_test]
    fn test_database_prepared_graphql_executes_with_variables() {
        let db = Database::new("test");

        let users = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);
        db.register_table(&users).unwrap();

        let orders = db
            .create_table("orders")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("user_id", JsDataType::Int64, None)
            .column("total", JsDataType::Float64, None)
            .foreign_key(
                "fk_orders_user",
                "user_id",
                "users",
                "id",
                Some(ForeignKeyOptions::new().set_reverse_field_name("orders")),
            );
        db.register_table(&orders).unwrap();

        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                1,
                alloc::vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(
                10,
                alloc::vec![Value::Int64(10), Value::Int64(1), Value::Float64(25.0)],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(
                11,
                alloc::vec![Value::Int64(11), Value::Int64(1), Value::Float64(99.0)],
            ))
            .unwrap();

        let prepared = db
            .prepare_graphql(
                "query UserOrders($userId: Long!, $min: Float = 0) { usersByPk(pk: { id: $userId }) { __typename orders(where: { total: { gte: $min } }, orderBy: [{ field: TOTAL, direction: DESC }], limit: 1) { id } } }",
                Some("UserOrders".into()),
            )
            .unwrap();

        let variables = js_sys::Object::new();
        js_sys::Reflect::set(
            &variables,
            &JsValue::from_str("userId"),
            &JsValue::from_f64(1.0),
        )
        .unwrap();
        js_sys::Reflect::set(
            &variables,
            &JsValue::from_str("min"),
            &JsValue::from_f64(60.0),
        )
        .unwrap();

        let result = prepared.exec(Some(variables.into())).unwrap();
        let data = js_sys::Reflect::get(&result, &JsValue::from_str("data")).unwrap();
        let user = js_sys::Reflect::get(&data, &JsValue::from_str("usersByPk")).unwrap();
        let typename = js_sys::Reflect::get(&user, &JsValue::from_str("__typename"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(typename, "Users");
        let orders = js_sys::Reflect::get(&user, &JsValue::from_str("orders")).unwrap();
        let orders = js_sys::Array::from(&orders);
        assert_eq!(orders.length(), 1);
        let top_order = orders.get(0);
        let id = js_sys::Reflect::get(&top_order, &JsValue::from_str("id"))
            .unwrap()
            .as_f64()
            .unwrap();
        assert_eq!(id, 11.0);
    }

    #[wasm_bindgen_test]
    fn test_database_graphql_mutation_updates_subscription_result() {
        let db = Database::new("test");

        let users = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);
        db.register_table(&users).unwrap();

        let subscription = db
            .subscribe_graphql(
                "subscription { users(orderBy: [{ field: ID, direction: ASC }]) { id name } }",
                None,
                None,
            )
            .unwrap();

        let initial = subscription.get_result();
        let initial_data = js_sys::Reflect::get(&initial, &JsValue::from_str("data")).unwrap();
        let initial_users = js_sys::Array::from(
            &js_sys::Reflect::get(&initial_data, &JsValue::from_str("users")).unwrap(),
        );
        assert_eq!(initial_users.length(), 0);

        let mutation = db
            .graphql(
                "mutation { insertUsers(input: [{ id: 1, name: \"Alice\" }]) { id name } }",
                None,
                None,
            )
            .unwrap();
        let mutation_data = js_sys::Reflect::get(&mutation, &JsValue::from_str("data")).unwrap();
        let inserted = js_sys::Array::from(
            &js_sys::Reflect::get(&mutation_data, &JsValue::from_str("insertUsers")).unwrap(),
        );
        assert_eq!(inserted.length(), 1);

        db.query_registry.borrow_mut().flush();

        let updated = subscription.get_result();
        let updated_data = js_sys::Reflect::get(&updated, &JsValue::from_str("data")).unwrap();
        let updated_users = js_sys::Array::from(
            &js_sys::Reflect::get(&updated_data, &JsValue::from_str("users")).unwrap(),
        );
        assert_eq!(updated_users.length(), 1);
        let first = updated_users.get(0);
        let name = js_sys::Reflect::get(&first, &JsValue::from_str("name"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(name, "Alice");
    }

    #[wasm_bindgen_test]
    fn test_graphql_live_selector_chooses_delta_for_scalar_root_subscription() {
        let db = setup_graphql_users_db();
        let engine = compile_subscription_engine(
            &db,
            "subscription UserCard { usersByPk(pk: { id: 1 }) { id name } }",
        );
        assert_eq!(engine, LiveEngineKind::Delta);
    }

    #[wasm_bindgen_test]
    fn test_graphql_live_selector_chooses_delta_for_nested_relations_without_sorting() {
        let db = setup_graphql_users_posts_db();
        let engine = compile_subscription_engine(
            &db,
            "subscription PostAuthorGraph { postsByPk(pk: { id: 10 }) { id title author { id name posts { id title } } } }",
        );
        assert_eq!(engine, LiveEngineKind::Delta);
    }

    #[wasm_bindgen_test]
    fn test_graphql_live_selector_falls_back_to_snapshot_for_nested_relation_sorting() {
        let db = setup_graphql_users_posts_db();
        let engine = compile_subscription_engine(
            &db,
            "subscription UserCard { usersByPk(pk: { id: 1 }) { id name posts(orderBy: [{ field: ID, direction: ASC }]) { id title } } }",
        );
        assert_eq!(engine, LiveEngineKind::Snapshot);
    }

    #[wasm_bindgen_test]
    fn test_graphql_live_selector_chooses_delta_for_unique_reverse_limit_one() {
        let db = setup_graphql_users_profiles_db();
        let engine = compile_subscription_engine(
            &db,
            "subscription UserCard { usersByPk(pk: { id: 1 }) { id name profile(limit: 1) { id bio } } }",
        );
        assert_eq!(engine, LiveEngineKind::Delta);
    }

    #[wasm_bindgen_test]
    fn test_graphql_live_selector_chooses_delta_for_relation_filtered_root_subscription() {
        let db = setup_graphql_issue_filter_db();
        let engine = compile_subscription_engine(
            &db,
            "subscription IssueFeed { issues(where: { AND: [{ status: { eq: \"open\" } }, { project: { AND: [{ healthScore: { gte: 45 } }, { counter: { openIssueCount: { gte: 5 } } }, { snapshot: { velocity: { gte: 18 } } }] } }] }) { id title project { id counter(limit: 1) { openIssueCount } snapshot(limit: 1) { velocity } } } }",
        );
        assert_eq!(engine, LiveEngineKind::Delta);
    }

    #[wasm_bindgen_test]
    fn test_graphql_live_selector_falls_back_to_snapshot_for_non_unique_reverse_limit_one() {
        let db = setup_graphql_users_posts_db();
        let engine = compile_subscription_engine(
            &db,
            "subscription UserCard { usersByPk(pk: { id: 1 }) { id name posts(limit: 1) { id title } } }",
        );
        assert_eq!(engine, LiveEngineKind::Snapshot);
    }

    #[wasm_bindgen_test]
    fn test_database_graphql_delta_subscription_tracks_scalar_root_updates() {
        let db = setup_graphql_users_db();

        let subscription = db
            .subscribe_graphql(
                "subscription UserCard { usersByPk(pk: { id: 1 }) { id name } }",
                None,
                None,
            )
            .unwrap();

        assert_eq!(
            compile_subscription_engine(
                &db,
                "subscription UserCard { usersByPk(pk: { id: 1 }) { id name } }"
            ),
            LiveEngineKind::Delta
        );

        let initial = subscription.get_result();
        let initial_data = js_sys::Reflect::get(&initial, &JsValue::from_str("data")).unwrap();
        let initial_user =
            js_sys::Reflect::get(&initial_data, &JsValue::from_str("usersByPk")).unwrap();
        assert!(initial_user.is_null());

        db.graphql(
            "mutation { insertUsers(input: [{ id: 1, name: \"Alice\" }]) { id name } }",
            None,
            None,
        )
        .unwrap();
        db.query_registry.borrow_mut().flush();

        let inserted = subscription.get_result();
        let inserted_data = js_sys::Reflect::get(&inserted, &JsValue::from_str("data")).unwrap();
        let inserted_user =
            js_sys::Reflect::get(&inserted_data, &JsValue::from_str("usersByPk")).unwrap();
        let inserted_name = js_sys::Reflect::get(&inserted_user, &JsValue::from_str("name"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(inserted_name, "Alice");

        db.graphql(
            "mutation { updateUsers(where: { id: { eq: 1 } }, set: { name: \"Alicia\" }) { id name } }",
            None,
            None,
        )
        .unwrap();
        db.query_registry.borrow_mut().flush();

        let updated = subscription.get_result();
        let updated_data = js_sys::Reflect::get(&updated, &JsValue::from_str("data")).unwrap();
        let updated_user =
            js_sys::Reflect::get(&updated_data, &JsValue::from_str("usersByPk")).unwrap();
        let updated_name = js_sys::Reflect::get(&updated_user, &JsValue::from_str("name"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(updated_name, "Alicia");
    }

    #[wasm_bindgen_test]
    fn test_database_graphql_delta_subscription_tracks_multilevel_nested_relation_changes() {
        let db = setup_graphql_users_posts_db();

        assert_eq!(
            compile_subscription_engine(
                &db,
                "subscription PostAuthorGraph { postsByPk(pk: { id: 10 }) { id title author { id name posts { id title } } } }",
            ),
            LiveEngineKind::Delta
        );

        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                2,
                alloc::vec![Value::Int64(2), Value::String("Bob".into())],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("posts")
            .unwrap()
            .insert(Row::new(
                10,
                alloc::vec![
                    Value::Int64(10),
                    Value::Int64(2),
                    Value::String("First".into()),
                ],
            ))
            .unwrap();

        let subscription = db
            .subscribe_graphql(
                "subscription PostAuthorGraph { postsByPk(pk: { id: 10 }) { id title author { id name posts { id title } } } }",
                None,
                None,
            )
            .unwrap();

        let initial = subscription.get_result();
        let initial_data = js_sys::Reflect::get(&initial, &JsValue::from_str("data")).unwrap();
        let initial_post =
            js_sys::Reflect::get(&initial_data, &JsValue::from_str("postsByPk")).unwrap();
        let initial_author =
            js_sys::Reflect::get(&initial_post, &JsValue::from_str("author")).unwrap();
        let initial_posts = js_sys::Array::from(
            &js_sys::Reflect::get(&initial_author, &JsValue::from_str("posts")).unwrap(),
        );
        assert_eq!(collect_titles(&initial_posts), vec!["First".to_string()]);

        db.graphql(
            "mutation { insertPosts(input: [{ id: 11, author_id: 2, title: \"Second\" }]) { id title } }",
            None,
            None,
        )
        .unwrap();
        db.query_registry.borrow_mut().flush();

        let updated = subscription.get_result();
        let updated_data = js_sys::Reflect::get(&updated, &JsValue::from_str("data")).unwrap();
        let updated_post =
            js_sys::Reflect::get(&updated_data, &JsValue::from_str("postsByPk")).unwrap();
        let updated_author =
            js_sys::Reflect::get(&updated_post, &JsValue::from_str("author")).unwrap();
        let updated_posts = js_sys::Array::from(
            &js_sys::Reflect::get(&updated_author, &JsValue::from_str("posts")).unwrap(),
        );
        assert_eq!(
            collect_titles(&updated_posts),
            vec!["First".to_string(), "Second".to_string()]
        );
    }

    #[wasm_bindgen_test]
    fn test_database_graphql_delta_subscription_reparents_nested_relation_membership() {
        let db = setup_graphql_users_posts_db();

        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                2,
                alloc::vec![Value::Int64(2), Value::String("Bob".into())],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                3,
                alloc::vec![Value::Int64(3), Value::String("Cara".into())],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("posts")
            .unwrap()
            .insert(Row::new(
                10,
                alloc::vec![
                    Value::Int64(10),
                    Value::Int64(2),
                    Value::String("First".into()),
                ],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("posts")
            .unwrap()
            .insert(Row::new(
                11,
                alloc::vec![
                    Value::Int64(11),
                    Value::Int64(2),
                    Value::String("Second".into()),
                ],
            ))
            .unwrap();

        let subscription = db
            .subscribe_graphql(
                "subscription PostAuthorGraph { postsByPk(pk: { id: 10 }) { id title author { id name posts { id title } } } }",
                None,
                None,
            )
            .unwrap();

        let initial = subscription.get_result();
        let initial_data = js_sys::Reflect::get(&initial, &JsValue::from_str("data")).unwrap();
        let initial_post =
            js_sys::Reflect::get(&initial_data, &JsValue::from_str("postsByPk")).unwrap();
        let initial_author =
            js_sys::Reflect::get(&initial_post, &JsValue::from_str("author")).unwrap();
        let initial_posts = js_sys::Array::from(
            &js_sys::Reflect::get(&initial_author, &JsValue::from_str("posts")).unwrap(),
        );
        assert_eq!(
            collect_titles(&initial_posts),
            vec!["First".to_string(), "Second".to_string()]
        );

        db.graphql(
            "mutation { updatePosts(where: { id: { eq: 11 } }, set: { author_id: 3 }) { id title } }",
            None,
            None,
        )
        .unwrap();
        db.query_registry.borrow_mut().flush();

        let updated = subscription.get_result();
        let updated_data = js_sys::Reflect::get(&updated, &JsValue::from_str("data")).unwrap();
        let updated_post =
            js_sys::Reflect::get(&updated_data, &JsValue::from_str("postsByPk")).unwrap();
        let updated_author =
            js_sys::Reflect::get(&updated_post, &JsValue::from_str("author")).unwrap();
        let updated_posts = js_sys::Array::from(
            &js_sys::Reflect::get(&updated_author, &JsValue::from_str("posts")).unwrap(),
        );
        assert_eq!(collect_titles(&updated_posts), vec!["First".to_string()]);
    }

    #[wasm_bindgen_test]
    fn test_database_graphql_subscription_tracks_nested_relation_changes() {
        let db = Database::new("test");

        let users = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);
        db.register_table(&users).unwrap();

        let posts = db
            .create_table("posts")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("author_id", JsDataType::Int64, None)
            .column("title", JsDataType::String, None)
            .foreign_key(
                "fk_posts_author",
                "author_id",
                "users",
                "id",
                Some(
                    ForeignKeyOptions::new()
                        .set_field_name("author")
                        .set_reverse_field_name("posts"),
                ),
            );
        db.register_table(&posts).unwrap();

        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                1,
                alloc::vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();

        let subscription = db
            .subscribe_graphql(
                "subscription WatchUsersWithPosts { users(orderBy: [{ field: ID, direction: ASC }]) { id name posts(orderBy: [{ field: ID, direction: ASC }]) { id title } } }",
                None,
                None,
            )
            .unwrap();

        let initial = subscription.get_result();
        let initial_data = js_sys::Reflect::get(&initial, &JsValue::from_str("data")).unwrap();
        let initial_users = js_sys::Array::from(
            &js_sys::Reflect::get(&initial_data, &JsValue::from_str("users")).unwrap(),
        );
        assert_eq!(initial_users.length(), 1);
        let initial_user = initial_users.get(0);
        let initial_posts = js_sys::Array::from(
            &js_sys::Reflect::get(&initial_user, &JsValue::from_str("posts")).unwrap(),
        );
        assert_eq!(initial_posts.length(), 0);

        db.graphql(
            "mutation { insertPosts(input: [{ id: 10, author_id: 1, title: \"Hello\" }]) { id title } }",
            None,
            None,
        )
        .unwrap();
        db.query_registry.borrow_mut().flush();

        let inserted = subscription.get_result();
        let inserted_data = js_sys::Reflect::get(&inserted, &JsValue::from_str("data")).unwrap();
        let inserted_users = js_sys::Array::from(
            &js_sys::Reflect::get(&inserted_data, &JsValue::from_str("users")).unwrap(),
        );
        let inserted_user = inserted_users.get(0);
        let inserted_posts = js_sys::Array::from(
            &js_sys::Reflect::get(&inserted_user, &JsValue::from_str("posts")).unwrap(),
        );
        assert_eq!(inserted_posts.length(), 1);
        let inserted_post = inserted_posts.get(0);
        let title = js_sys::Reflect::get(&inserted_post, &JsValue::from_str("title"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(title, "Hello");

        db.graphql(
            "mutation { updatePosts(where: { id: { eq: 10 } }, set: { title: \"Updated\" }) { id title } }",
            None,
            None,
        )
        .unwrap();
        db.query_registry.borrow_mut().flush();

        let updated = subscription.get_result();
        let updated_data = js_sys::Reflect::get(&updated, &JsValue::from_str("data")).unwrap();
        let updated_users = js_sys::Array::from(
            &js_sys::Reflect::get(&updated_data, &JsValue::from_str("users")).unwrap(),
        );
        let updated_user = updated_users.get(0);
        let updated_posts = js_sys::Array::from(
            &js_sys::Reflect::get(&updated_user, &JsValue::from_str("posts")).unwrap(),
        );
        assert_eq!(updated_posts.length(), 1);
        let updated_post = updated_posts.get(0);
        let title = js_sys::Reflect::get(&updated_post, &JsValue::from_str("title"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(title, "Updated");
    }

    #[wasm_bindgen_test]
    fn test_database_graphql_delta_subscription_tracks_unique_reverse_limit_one_updates() {
        let db = setup_graphql_users_profiles_db();

        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                1,
                alloc::vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();

        let subscription = db
            .subscribe_graphql(
                "subscription UserCard { usersByPk(pk: { id: 1 }) { id name profile(limit: 1) { id bio } } }",
                None,
                None,
            )
            .unwrap();

        assert_eq!(
            compile_subscription_engine(
                &db,
                "subscription UserCard { usersByPk(pk: { id: 1 }) { id name profile(limit: 1) { id bio } } }",
            ),
            LiveEngineKind::Delta
        );

        let initial = subscription.get_result();
        let initial_data = js_sys::Reflect::get(&initial, &JsValue::from_str("data")).unwrap();
        let initial_user = js_sys::Reflect::get(&initial_data, &JsValue::from_str("usersByPk"))
            .unwrap();
        let initial_profile = js_sys::Array::from(
            &js_sys::Reflect::get(&initial_user, &JsValue::from_str("profile")).unwrap(),
        );
        assert_eq!(initial_profile.length(), 0);

        db.graphql(
            "mutation { insertProfiles(input: [{ id: 10, user_id: 1, bio: \"First\" }]) { id bio } }",
            None,
            None,
        )
        .unwrap();
        db.query_registry.borrow_mut().flush();

        let inserted = subscription.get_result();
        let inserted_data = js_sys::Reflect::get(&inserted, &JsValue::from_str("data")).unwrap();
        let inserted_user =
            js_sys::Reflect::get(&inserted_data, &JsValue::from_str("usersByPk")).unwrap();
        let inserted_profile = js_sys::Array::from(
            &js_sys::Reflect::get(&inserted_user, &JsValue::from_str("profile")).unwrap(),
        );
        assert_eq!(inserted_profile.length(), 1);
        let inserted_bio = js_sys::Reflect::get(&inserted_profile.get(0), &JsValue::from_str("bio"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(inserted_bio, "First");

        db.graphql(
            "mutation { updateProfiles(where: { id: { eq: 10 } }, set: { bio: \"Updated\" }) { id bio } }",
            None,
            None,
        )
        .unwrap();
        db.query_registry.borrow_mut().flush();

        let updated = subscription.get_result();
        let updated_data = js_sys::Reflect::get(&updated, &JsValue::from_str("data")).unwrap();
        let updated_user =
            js_sys::Reflect::get(&updated_data, &JsValue::from_str("usersByPk")).unwrap();
        let updated_profile = js_sys::Array::from(
            &js_sys::Reflect::get(&updated_user, &JsValue::from_str("profile")).unwrap(),
        );
        assert_eq!(updated_profile.length(), 1);
        let updated_bio = js_sys::Reflect::get(&updated_profile.get(0), &JsValue::from_str("bio"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(updated_bio, "Updated");
    }

    #[wasm_bindgen_test]
    fn test_database_graphql_delta_subscription_tracks_relation_filtered_root_membership() {
        let db = setup_graphql_issue_filter_db();

        db.cache
            .borrow_mut()
            .get_table_mut("projects")
            .unwrap()
            .insert(Row::new(1, alloc::vec![Value::Int64(1), Value::Int32(30)]))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("projectCounters")
            .unwrap()
            .insert(Row::new(10, alloc::vec![Value::Int64(1), Value::Int32(6)]))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("projectSnapshots")
            .unwrap()
            .insert(Row::new(11, alloc::vec![Value::Int64(1), Value::Int32(20)]))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("issues")
            .unwrap()
            .insert(Row::new(
                100,
                alloc::vec![
                    Value::Int64(100),
                    Value::Int64(1),
                    Value::String("Issue".into()),
                    Value::String("open".into()),
                ],
            ))
            .unwrap();

        let query = "subscription IssueFeed { issues(where: { AND: [{ status: { eq: \"open\" } }, { project: { AND: [{ healthScore: { gte: 45 } }, { counter: { openIssueCount: { gte: 5 } } }, { snapshot: { velocity: { gte: 18 } } }] } }] }) { id title project { id counter(limit: 1) { openIssueCount } snapshot(limit: 1) { velocity } } } }";
        assert_eq!(compile_subscription_engine(&db, query), LiveEngineKind::Delta);

        let subscription = db.subscribe_graphql(query, None, None).unwrap();

        let initial = subscription.get_result();
        let initial_data = js_sys::Reflect::get(&initial, &JsValue::from_str("data")).unwrap();
        let initial_issues = js_sys::Array::from(
            &js_sys::Reflect::get(&initial_data, &JsValue::from_str("issues")).unwrap(),
        );
        assert_eq!(initial_issues.length(), 0);

        db.graphql(
            "mutation { updateProjects(where: { id: { eq: 1 } }, set: { healthScore: 50 }) { id } }",
            None,
            None,
        )
        .unwrap();
        db.query_registry.borrow_mut().flush();

        let inserted = subscription.get_result();
        let inserted_data = js_sys::Reflect::get(&inserted, &JsValue::from_str("data")).unwrap();
        let inserted_issues = js_sys::Array::from(
            &js_sys::Reflect::get(&inserted_data, &JsValue::from_str("issues")).unwrap(),
        );
        assert_eq!(collect_ids(&inserted_issues, "id"), vec![100]);

        db.graphql(
            "mutation { updateProjectSnapshots(where: { projectId: { eq: 1 } }, set: { velocity: 5 }) { projectId } }",
            None,
            None,
        )
        .unwrap();
        db.query_registry.borrow_mut().flush();

        let removed = subscription.get_result();
        let removed_data = js_sys::Reflect::get(&removed, &JsValue::from_str("data")).unwrap();
        let removed_issues = js_sys::Array::from(
            &js_sys::Reflect::get(&removed_data, &JsValue::from_str("issues")).unwrap(),
        );
        assert_eq!(removed_issues.length(), 0);
    }

    #[wasm_bindgen_test]
    fn test_database_graphql_snapshot_subscription_reparents_nested_relation_membership() {
        let db = setup_graphql_users_posts_db();

        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                1,
                alloc::vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                2,
                alloc::vec![Value::Int64(2), Value::String("Bob".into())],
            ))
            .unwrap();
        db.cache
            .borrow_mut()
            .get_table_mut("posts")
            .unwrap()
            .insert(Row::new(
                10,
                alloc::vec![
                    Value::Int64(10),
                    Value::Int64(1),
                    Value::String("Hello".into()),
                ],
            ))
            .unwrap();

        let subscription = db
            .subscribe_graphql(
                "subscription WatchUsersWithPosts { users(orderBy: [{ field: ID, direction: ASC }]) { id name posts(orderBy: [{ field: ID, direction: ASC }]) { id title } } }",
                None,
                None,
            )
            .unwrap();

        assert_eq!(
            compile_subscription_engine(
                &db,
                "subscription WatchUsersWithPosts { users(orderBy: [{ field: ID, direction: ASC }]) { id name posts(orderBy: [{ field: ID, direction: ASC }]) { id title } } }"
            ),
            LiveEngineKind::Snapshot
        );

        let initial = subscription.get_result();
        let initial_data = js_sys::Reflect::get(&initial, &JsValue::from_str("data")).unwrap();
        let initial_users = js_sys::Array::from(
            &js_sys::Reflect::get(&initial_data, &JsValue::from_str("users")).unwrap(),
        );
        let user_one_posts = js_sys::Array::from(
            &js_sys::Reflect::get(&initial_users.get(0), &JsValue::from_str("posts")).unwrap(),
        );
        let user_two_posts = js_sys::Array::from(
            &js_sys::Reflect::get(&initial_users.get(1), &JsValue::from_str("posts")).unwrap(),
        );
        assert_eq!(collect_titles(&user_one_posts), vec!["Hello".to_string()]);
        assert!(collect_titles(&user_two_posts).is_empty());

        db.graphql(
            "mutation { updatePosts(where: { id: { eq: 10 } }, set: { author_id: 2 }) { id title } }",
            None,
            None,
        )
        .unwrap();
        db.query_registry.borrow_mut().flush();

        let updated = subscription.get_result();
        let updated_data = js_sys::Reflect::get(&updated, &JsValue::from_str("data")).unwrap();
        let updated_users = js_sys::Array::from(
            &js_sys::Reflect::get(&updated_data, &JsValue::from_str("users")).unwrap(),
        );
        let updated_user_one_posts = js_sys::Array::from(
            &js_sys::Reflect::get(&updated_users.get(0), &JsValue::from_str("posts")).unwrap(),
        );
        let updated_user_two_posts = js_sys::Array::from(
            &js_sys::Reflect::get(&updated_users.get(1), &JsValue::from_str("posts")).unwrap(),
        );
        assert!(collect_titles(&updated_user_one_posts).is_empty());
        assert_eq!(
            collect_titles(&updated_user_two_posts),
            vec!["Hello".to_string()]
        );
    }

    #[wasm_bindgen_test]
    fn test_prepared_graphql_supports_mutation_and_subscription() {
        let db = Database::new("test");

        let users = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);
        db.register_table(&users).unwrap();

        let prepared_subscription = db
            .prepare_graphql(
                "subscription UserFeed { users(orderBy: [{ field: ID, direction: ASC }]) { id name } }",
                Some("UserFeed".into()),
            )
            .unwrap();
        let subscription = prepared_subscription.subscribe(None).unwrap();

        let prepared_mutation = db
            .prepare_graphql(
                "mutation AddUser($id: Long!, $name: String!) { insertUsers(input: [{ id: $id, name: $name }]) { id name } }",
                Some("AddUser".into()),
            )
            .unwrap();

        let variables = js_sys::Object::new();
        js_sys::Reflect::set(
            &variables,
            &JsValue::from_str("id"),
            &JsValue::from_f64(2.0),
        )
        .unwrap();
        js_sys::Reflect::set(
            &variables,
            &JsValue::from_str("name"),
            &JsValue::from_str("Bob"),
        )
        .unwrap();

        let mutation_result = prepared_mutation.exec(Some(variables.into())).unwrap();
        let mutation_data =
            js_sys::Reflect::get(&mutation_result, &JsValue::from_str("data")).unwrap();
        let inserted = js_sys::Array::from(
            &js_sys::Reflect::get(&mutation_data, &JsValue::from_str("insertUsers")).unwrap(),
        );
        assert_eq!(inserted.length(), 1);

        db.query_registry.borrow_mut().flush();

        let payload = subscription.get_result();
        let data = js_sys::Reflect::get(&payload, &JsValue::from_str("data")).unwrap();
        let users =
            js_sys::Array::from(&js_sys::Reflect::get(&data, &JsValue::from_str("users")).unwrap());
        assert_eq!(users.length(), 1);
        let first = users.get(0);
        let id = js_sys::Reflect::get(&first, &JsValue::from_str("id"))
            .unwrap()
            .as_f64()
            .unwrap();
        let name = js_sys::Reflect::get(&first, &JsValue::from_str("name"))
            .unwrap()
            .as_string()
            .unwrap();
        assert_eq!(id, 2.0);
        assert_eq!(name, "Bob");
    }
}
