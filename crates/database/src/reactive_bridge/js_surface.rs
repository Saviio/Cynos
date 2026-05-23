use super::graphql_observable::{GraphqlDeltaObservable, GraphqlSubscriptionObservable};
use super::{
    binary_result_to_js_value, encode_rc_rows_to_binary, encode_rows_iter_to_binary,
    ReQueryObservable,
};
use crate::binary_protocol::{BinaryResult, SchemaLayout};
use crate::convert::{prune_jsonb_js_cache, value_to_js, GraphqlJsCachePolicy};
#[cfg(not(feature = "benchmark"))]
use crate::profiling::IvmBridgeProfiler;
#[cfg(feature = "benchmark")]
use crate::profiling::{now_ms, IvmBridgeProfile, IvmBridgeProfiler};
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::schema::Table;
use cynos_core::{Row, Value};
use cynos_incremental::{TraceDeltaBatch, TraceTupleArena, TraceTupleHandle};
use cynos_reactive::ObservableQuery;
use hashbrown::HashMap;
use wasm_bindgen::prelude::*;

#[derive(Clone)]
enum GraphqlSubscriptionInner {
    Snapshot(Rc<RefCell<GraphqlSubscriptionObservable>>),
    Delta(Rc<RefCell<GraphqlDeltaObservable>>),
}

impl GraphqlSubscriptionInner {
    fn attach_keepalive(&self) -> usize {
        match self {
            Self::Snapshot(inner) => inner.borrow_mut().attach_keepalive(),
            Self::Delta(inner) => inner.borrow_mut().attach_keepalive(),
        }
    }

    fn response_js_value(&self) -> JsValue {
        match self {
            Self::Snapshot(inner) => inner.borrow_mut().response_js_value(),
            Self::Delta(inner) => inner.borrow_mut().response_js_value(),
        }
    }

    fn subscribe<F: Fn(&JsValue) + 'static>(&self, callback: F) -> usize {
        match self {
            Self::Snapshot(inner) => inner.borrow_mut().subscribe(callback),
            Self::Delta(inner) => inner.borrow_mut().subscribe(callback),
        }
    }

    fn unsubscribe(&self, id: usize) -> bool {
        match self {
            Self::Snapshot(inner) => inner.borrow_mut().unsubscribe(id),
            Self::Delta(inner) => inner.borrow_mut().unsubscribe(id),
        }
    }

    fn listener_count(&self) -> usize {
        match self {
            Self::Snapshot(inner) => inner.borrow().listener_count(),
            Self::Delta(inner) => inner.borrow().listener_count(),
        }
    }
}

/// JavaScript-friendly observable query wrapper.
/// Uses re-query strategy for optimal performance with indexes.
#[wasm_bindgen]
pub struct JsObservableQuery {
    inner: Rc<RefCell<ReQueryObservable>>,
    schema: Table,
    /// Optional projected column names. If Some, only these columns are returned.
    projected_columns: Option<Vec<String>>,
    /// Pre-computed binary layout for getResultBinary().
    binary_layout: SchemaLayout,
    /// Optional aggregate column names. If Some, this is an aggregate query.
    aggregate_columns: Option<Vec<String>>,
}

impl JsObservableQuery {
    pub(crate) fn new(
        inner: Rc<RefCell<ReQueryObservable>>,
        schema: Table,
        binary_layout: SchemaLayout,
    ) -> Self {
        Self {
            inner,
            schema,
            projected_columns: None,
            binary_layout,
            aggregate_columns: None,
        }
    }

    pub(crate) fn new_with_projection(
        inner: Rc<RefCell<ReQueryObservable>>,
        schema: Table,
        projected_columns: Vec<String>,
        binary_layout: SchemaLayout,
    ) -> Self {
        Self {
            inner,
            schema,
            projected_columns: Some(projected_columns),
            binary_layout,
            aggregate_columns: None,
        }
    }

    /// Get the inner observable for creating JsChangesStream.
    pub(crate) fn inner(&self) -> Rc<RefCell<ReQueryObservable>> {
        self.inner.clone()
    }

    /// Get the schema.
    pub(crate) fn schema(&self) -> &Table {
        &self.schema
    }

    /// Get the projected columns.
    pub(crate) fn projected_columns(&self) -> Option<&Vec<String>> {
        self.projected_columns.as_ref()
    }

    /// Get the aggregate columns.
    #[allow(dead_code)]
    pub(crate) fn aggregate_columns(&self) -> Option<&Vec<String>> {
        self.aggregate_columns.as_ref()
    }
}

#[wasm_bindgen]
impl JsObservableQuery {
    /// Subscribes to query changes.
    ///
    /// The callback receives the complete current result set as a JavaScript array.
    /// It is called whenever data changes (not immediately - use getResult for initial data).
    /// Returns an unsubscribe function.
    pub fn subscribe(&mut self, callback: js_sys::Function) -> js_sys::Function {
        let schema = self.schema.clone();
        let projected_columns = self.projected_columns.clone();
        let aggregate_columns = self.aggregate_columns.clone();
        let materializer = if let Some(ref cols) = aggregate_columns {
            JsSnapshotRowsMaterializer::projected(cols.clone())
        } else if let Some(ref cols) = projected_columns {
            JsSnapshotRowsMaterializer::projected(cols.clone())
        } else {
            JsSnapshotRowsMaterializer::full(schema.clone())
        };
        let materialization_cache = Rc::new(RefCell::new(JsSnapshotRowsCache::default()));

        let sub_id = self.inner.borrow_mut().subscribe(move |rows| {
            let current_data =
                materializer.materialize(rows, &mut materialization_cache.borrow_mut());
            callback.call1(&JsValue::NULL, &current_data).ok();
        });

        // Create unsubscribe function
        let inner_unsub = self.inner.clone();
        let called = Rc::new(RefCell::new(false));
        let called_c = called.clone();
        let unsubscribe = Closure::wrap(Box::new(move || {
            let mut c = called_c.borrow_mut();
            if !*c {
                *c = true;
                inner_unsub.borrow_mut().unsubscribe(sub_id);
            }
        }) as Box<dyn FnMut()>);
        unsubscribe.into_js_value().unchecked_into()
    }

    /// Subscribes to query changes using binary snapshots.
    ///
    /// The callback receives a `BinaryResult` for the complete current result set.
    /// This avoids per-update JS object materialization inside the WASM bridge.
    /// Call `getSchemaLayout()` once and decode with `ResultSet` on the JS side.
    ///
    /// It is called whenever data changes (not immediately - use getResultBinary for initial data).
    /// Returns an unsubscribe function.
    #[wasm_bindgen(js_name = subscribeBinary)]
    pub fn subscribe_binary(&mut self, callback: js_sys::Function) -> js_sys::Function {
        let binary_layout = self.binary_layout.clone();

        let sub_id = self.inner.borrow_mut().subscribe(move |rows| {
            let current_data =
                binary_result_to_js_value(encode_rc_rows_to_binary(rows, &binary_layout));
            callback.call1(&JsValue::NULL, &current_data).ok();
        });

        let inner_unsub = self.inner.clone();
        let called = Rc::new(RefCell::new(false));
        let called_c = called.clone();
        let unsubscribe = Closure::wrap(Box::new(move || {
            let mut c = called_c.borrow_mut();
            if !*c {
                *c = true;
                inner_unsub.borrow_mut().unsubscribe(sub_id);
            }
        }) as Box<dyn FnMut()>);
        unsubscribe.into_js_value().unchecked_into()
    }

    /// Returns the current result as a JavaScript array.
    #[wasm_bindgen(js_name = getResult)]
    pub fn get_result(&self) -> JsValue {
        let inner = self.inner.borrow();
        if let Some(ref cols) = self.aggregate_columns {
            projected_rows_to_js_array(inner.result(), cols)
        } else if let Some(ref cols) = self.projected_columns {
            projected_rows_to_js_array(inner.result(), cols)
        } else {
            rows_to_js_array(inner.result(), &self.schema)
        }
    }

    /// Returns the current result as a binary buffer for zero-copy access.
    #[wasm_bindgen(js_name = getResultBinary)]
    pub fn get_result_binary(&self) -> BinaryResult {
        let inner = self.inner.borrow();
        encode_rc_rows_to_binary(inner.result(), &self.binary_layout)
    }

    /// Returns the schema layout for decoding binary results.
    #[wasm_bindgen(js_name = getSchemaLayout)]
    pub fn get_schema_layout(&self) -> SchemaLayout {
        self.binary_layout.clone()
    }

    /// Returns the number of rows in the result.
    #[wasm_bindgen(getter)]
    pub fn length(&self) -> usize {
        self.inner.borrow().len()
    }

    /// Returns whether the result is empty.
    #[wasm_bindgen(js_name = isEmpty)]
    pub fn is_empty(&self) -> bool {
        self.inner.borrow().is_empty()
    }

    /// Returns the number of active subscriptions.
    #[wasm_bindgen(js_name = subscriptionCount)]
    pub fn subscription_count(&self) -> usize {
        self.inner.borrow().subscription_count()
    }
}

/// JavaScript-friendly IVM observable query wrapper.
/// Uses DBSP-based incremental view maintenance for O(delta) updates.
#[wasm_bindgen]
pub struct JsIvmObservableQuery {
    inner: Rc<RefCell<ObservableQuery>>,
    /// Pre-computed binary layout for getResultBinary().
    binary_layout: SchemaLayout,
    /// Pre-built row materializer with cached property keys.
    materializer: JsSnapshotRowsMaterializer,
    /// Snapshot cache reused across getResult() calls.
    materialization_cache: Rc<RefCell<JsSnapshotRowsCache>>,
    /// Shared sink for per-flush bridge profiling.
    #[allow(dead_code)]
    bridge_profiler: Rc<RefCell<IvmBridgeProfiler>>,
}

impl JsIvmObservableQuery {
    pub(crate) fn new(
        inner: Rc<RefCell<ObservableQuery>>,
        schema: Table,
        binary_layout: SchemaLayout,
        bridge_profiler: Rc<RefCell<IvmBridgeProfiler>>,
    ) -> Self {
        let materializer = JsSnapshotRowsMaterializer::full(schema.clone());
        Self {
            inner,
            binary_layout,
            materializer,
            materialization_cache: Rc::new(RefCell::new(JsSnapshotRowsCache::default())),
            bridge_profiler,
        }
    }

    pub(crate) fn new_with_projection(
        inner: Rc<RefCell<ObservableQuery>>,
        _schema: Table,
        projected_columns: Vec<String>,
        binary_layout: SchemaLayout,
        bridge_profiler: Rc<RefCell<IvmBridgeProfiler>>,
    ) -> Self {
        let materializer = JsSnapshotRowsMaterializer::projected(projected_columns.clone());
        Self {
            inner,
            binary_layout,
            materializer,
            materialization_cache: Rc::new(RefCell::new(JsSnapshotRowsCache::default())),
            bridge_profiler,
        }
    }
}

#[wasm_bindgen]
impl JsIvmObservableQuery {
    /// Subscribes to IVM query changes.
    ///
    /// The callback receives a delta object `{ added: Row[], removed: Row[] }`
    /// instead of the full result set. This is the true O(delta) path —
    /// the UI side should apply the delta to its own state.
    ///
    /// Use `getResult()` to get the initial full result before subscribing.
    /// Returns an unsubscribe function.
    pub fn subscribe(&mut self, callback: js_sys::Function) -> js_sys::Function {
        let materializer = self.materializer.clone();
        #[cfg(feature = "benchmark")]
        let bridge_profiler = self.bridge_profiler.clone();
        let added_key = JsValue::from_str("added");
        let removed_key = JsValue::from_str("removed");
        let materialization_cache = self.materialization_cache.clone();

        let sub_id = self
            .inner
            .borrow_mut()
            .subscribe_trace_batches(move |batch| {
                #[cfg(feature = "benchmark")]
                let total_started_at = now_ms();
                #[cfg(feature = "benchmark")]
                let (delta_obj, serialize_added_ms, serialize_removed_ms, assemble_delta_ms) =
                    trace_delta_batch_to_js_delta_profiled(
                        batch,
                        &materializer,
                        &mut materialization_cache.borrow_mut(),
                        &added_key,
                        &removed_key,
                    );
                #[cfg(not(feature = "benchmark"))]
                let delta_obj = trace_delta_batch_to_js_delta(
                    batch,
                    &materializer,
                    &mut materialization_cache.borrow_mut(),
                    &added_key,
                    &removed_key,
                );
                #[cfg(feature = "benchmark")]
                let added_row_count = batch.insert_count();
                #[cfg(feature = "benchmark")]
                let removed_row_count = batch.delete_count();

                #[cfg(feature = "benchmark")]
                let callback_started_at = now_ms();
                callback.call1(&JsValue::NULL, &delta_obj).ok();
                #[cfg(feature = "benchmark")]
                let callback_call_ms = now_ms() - callback_started_at;

                #[cfg(feature = "benchmark")]
                bridge_profiler
                    .borrow_mut()
                    .record_sample(&IvmBridgeProfile {
                        callback_count: 1,
                        added_row_count,
                        removed_row_count,
                        serialize_added_ms,
                        serialize_removed_ms,
                        assemble_delta_ms,
                        callback_call_ms,
                        total_ms: now_ms() - total_started_at,
                    });
            });

        let inner_unsub = self.inner.clone();
        let called = Rc::new(RefCell::new(false));
        let called_c = called.clone();
        let unsubscribe = Closure::wrap(Box::new(move || {
            let mut c = called_c.borrow_mut();
            if !*c {
                *c = true;
                inner_unsub.borrow_mut().unsubscribe(sub_id);
            }
        }) as Box<dyn FnMut()>);
        unsubscribe.into_js_value().unchecked_into()
    }

    /// Returns the current result as a JavaScript array.
    #[wasm_bindgen(js_name = getResult)]
    pub fn get_result(&self) -> JsValue {
        let inner = self.inner.borrow();
        self.materializer.materialize_from_iter(
            inner.result_rows(),
            inner.len(),
            &mut self.materialization_cache.borrow_mut(),
        )
    }

    /// Returns the current result as a binary buffer for zero-copy access.
    #[wasm_bindgen(js_name = getResultBinary)]
    pub fn get_result_binary(&self) -> BinaryResult {
        let inner = self.inner.borrow();
        encode_rows_iter_to_binary(inner.result_rows(), inner.len(), &self.binary_layout)
    }

    /// Returns the schema layout for decoding binary results.
    #[wasm_bindgen(js_name = getSchemaLayout)]
    pub fn get_schema_layout(&self) -> SchemaLayout {
        self.binary_layout.clone()
    }

    /// Returns the number of rows in the result.
    #[wasm_bindgen(getter)]
    pub fn length(&self) -> usize {
        self.inner.borrow().len()
    }

    /// Returns whether the result is empty.
    #[wasm_bindgen(js_name = isEmpty)]
    pub fn is_empty(&self) -> bool {
        self.inner.borrow().is_empty()
    }

    /// Returns the number of active subscriptions.
    #[wasm_bindgen(js_name = subscriptionCount)]
    pub fn subscription_count(&self) -> usize {
        self.inner.borrow().subscription_count()
    }
}

#[cfg_attr(feature = "benchmark", allow(dead_code))]
fn trace_delta_batch_to_js_delta(
    batch: &TraceDeltaBatch,
    materializer: &JsSnapshotRowsMaterializer,
    cache: &mut JsSnapshotRowsCache,
    added_key: &JsValue,
    removed_key: &JsValue,
) -> JsValue {
    let added = js_sys::Array::new_with_length(batch.insert_count() as u32);
    let removed = js_sys::Array::new_with_length(batch.delete_count() as u32);
    let mut added_index = 0u32;
    let mut removed_index = 0u32;

    for delta in batch.deltas() {
        let js_row = cache.materialize_trace_handle(materializer, batch.arena(), &delta.data);
        if delta.is_insert() {
            added.set(added_index, js_row);
            added_index += 1;
        } else if delta.is_delete() {
            removed.set(removed_index, js_row);
            removed_index += 1;
            cache.remove_row(batch.arena().row_id(&delta.data));
        }
    }

    let delta_obj = js_sys::Object::new();
    js_sys::Reflect::set(&delta_obj, added_key, &added).ok();
    js_sys::Reflect::set(&delta_obj, removed_key, &removed).ok();

    delta_obj.into()
}

#[cfg(feature = "benchmark")]
fn trace_delta_batch_to_js_delta_profiled(
    batch: &TraceDeltaBatch,
    materializer: &JsSnapshotRowsMaterializer,
    cache: &mut JsSnapshotRowsCache,
    added_key: &JsValue,
    removed_key: &JsValue,
) -> (JsValue, f64, f64, f64) {
    let serialize_started_at = now_ms();
    let added = js_sys::Array::new_with_length(batch.insert_count() as u32);
    let removed = js_sys::Array::new_with_length(batch.delete_count() as u32);
    let mut added_index = 0u32;
    let mut removed_index = 0u32;

    for delta in batch.deltas() {
        let js_row = cache.materialize_trace_handle(materializer, batch.arena(), &delta.data);
        if delta.is_insert() {
            added.set(added_index, js_row);
            added_index += 1;
        } else if delta.is_delete() {
            removed.set(removed_index, js_row);
            removed_index += 1;
            cache.remove_row(batch.arena().row_id(&delta.data));
        }
    }

    let serialize_delta_ms = now_ms() - serialize_started_at;
    let assemble_started_at = now_ms();
    let delta_obj = js_sys::Object::new();
    js_sys::Reflect::set(&delta_obj, added_key, &added).ok();
    js_sys::Reflect::set(&delta_obj, removed_key, &removed).ok();
    let assemble_delta_ms = now_ms() - assemble_started_at;

    (delta_obj.into(), serialize_delta_ms, 0.0, assemble_delta_ms)
}

/// JavaScript-friendly GraphQL subscription wrapper.
///
/// The callback receives a standard GraphQL payload object with a single `data`
/// property. The payload is emitted immediately on subscribe and again whenever
/// the rendered GraphQL response changes.
#[wasm_bindgen]
pub struct JsGraphqlSubscription {
    inner: GraphqlSubscriptionInner,
    keepalive_sub_id: usize,
}

impl JsGraphqlSubscription {
    pub(crate) fn new_snapshot(inner: Rc<RefCell<GraphqlSubscriptionObservable>>) -> Self {
        Self::new(GraphqlSubscriptionInner::Snapshot(inner))
    }

    pub(crate) fn new_delta(inner: Rc<RefCell<GraphqlDeltaObservable>>) -> Self {
        Self::new(GraphqlSubscriptionInner::Delta(inner))
    }

    fn new(inner: GraphqlSubscriptionInner) -> Self {
        let keepalive_sub_id = inner.attach_keepalive();
        Self {
            inner,
            keepalive_sub_id,
        }
    }
}

impl Drop for JsGraphqlSubscription {
    fn drop(&mut self) {
        self.inner.unsubscribe(self.keepalive_sub_id);
    }
}

#[wasm_bindgen]
impl JsGraphqlSubscription {
    /// Returns the current GraphQL payload.
    #[wasm_bindgen(js_name = getResult)]
    pub fn get_result(&self) -> JsValue {
        self.inner.response_js_value()
    }

    /// Subscribes to GraphQL payload changes and emits the initial value immediately.
    pub fn subscribe(&self, callback: js_sys::Function) -> js_sys::Function {
        let inner = self.inner.clone();
        let initial_callback = callback.clone();
        let sub_id = inner.subscribe(move |payload| {
            callback.call1(&JsValue::NULL, payload).ok();
        });
        let initial = inner.response_js_value();
        initial_callback.call1(&JsValue::NULL, &initial).ok();

        let called = Rc::new(RefCell::new(false));
        let called_c = called.clone();
        let unsubscribe = Closure::wrap(Box::new(move || {
            let mut c = called_c.borrow_mut();
            if !*c {
                *c = true;
                inner.unsubscribe(sub_id);
            }
        }) as Box<dyn FnMut()>);
        unsubscribe.into_js_value().unchecked_into()
    }

    /// Returns the number of active subscriptions.
    #[wasm_bindgen(js_name = subscriptionCount)]
    pub fn subscription_count(&self) -> usize {
        self.inner.listener_count()
    }
}

/// JavaScript-friendly changes stream.
///
/// This provides the `changes()` API that yields the complete result set
/// whenever data changes. The callback receives the full current data,
/// not incremental changes - perfect for React's setState pattern.
#[wasm_bindgen]
pub struct JsChangesStream {
    inner: Rc<RefCell<ReQueryObservable>>,
    schema: Table,
    /// Optional projected column names. If Some, only these columns are returned.
    projected_columns: Option<Vec<String>>,
    /// Pre-computed binary layout for getResultBinary().
    binary_layout: SchemaLayout,
}

impl JsChangesStream {
    pub(crate) fn from_observable(observable: JsObservableQuery) -> Self {
        Self {
            inner: observable.inner(),
            schema: observable.schema().clone(),
            projected_columns: observable.projected_columns().cloned(),
            binary_layout: observable.binary_layout.clone(),
        }
    }
}

#[wasm_bindgen]
impl JsChangesStream {
    /// Subscribes to the changes stream.
    ///
    /// The callback receives the complete current result set as a JavaScript array.
    /// It is called immediately with the initial data, and again whenever data changes.
    /// Perfect for React: `stream.subscribe(data => setUsers(data))`
    ///
    /// Returns an unsubscribe function.
    pub fn subscribe(&self, callback: js_sys::Function) -> js_sys::Function {
        let schema = self.schema.clone();
        let inner = self.inner.clone();
        let projected_columns = self.projected_columns.clone();
        let materializer = if let Some(ref cols) = projected_columns {
            JsSnapshotRowsMaterializer::projected(cols.clone())
        } else {
            JsSnapshotRowsMaterializer::full(schema.clone())
        };
        let materialization_cache = Rc::new(RefCell::new(JsSnapshotRowsCache::default()));

        // Emit initial value immediately
        let initial_data = materializer.materialize(
            inner.borrow().result(),
            &mut materialization_cache.borrow_mut(),
        );
        callback.call1(&JsValue::NULL, &initial_data).ok();

        // Subscribe to subsequent changes
        let sub_id = inner.borrow_mut().subscribe(move |rows| {
            let current_data =
                materializer.materialize(rows, &mut materialization_cache.borrow_mut());
            callback.call1(&JsValue::NULL, &current_data).ok();
        });

        // Create unsubscribe function
        let called = Rc::new(RefCell::new(false));
        let called_c = called.clone();
        let unsubscribe = Closure::wrap(Box::new(move || {
            let mut c = called_c.borrow_mut();
            if !*c {
                *c = true;
                inner.borrow_mut().unsubscribe(sub_id);
            }
        }) as Box<dyn FnMut()>);
        unsubscribe.into_js_value().unchecked_into()
    }

    /// Subscribes to the changes stream using binary snapshots.
    ///
    /// The callback receives a `BinaryResult` for the full current result set.
    /// It is called immediately with the initial data, and again whenever data changes.
    /// Use `getSchemaLayout()` once and decode with `ResultSet` on the JS side.
    ///
    /// Returns an unsubscribe function.
    #[wasm_bindgen(js_name = subscribeBinary)]
    pub fn subscribe_binary(&self, callback: js_sys::Function) -> js_sys::Function {
        let inner = self.inner.clone();
        let binary_layout = self.binary_layout.clone();

        let initial_data = binary_result_to_js_value(encode_rc_rows_to_binary(
            inner.borrow().result(),
            &binary_layout,
        ));
        callback.call1(&JsValue::NULL, &initial_data).ok();

        let binary_layout_clone = binary_layout.clone();
        let sub_id = inner.borrow_mut().subscribe(move |rows| {
            let current_data =
                binary_result_to_js_value(encode_rc_rows_to_binary(rows, &binary_layout_clone));
            callback.call1(&JsValue::NULL, &current_data).ok();
        });

        let called = Rc::new(RefCell::new(false));
        let called_c = called.clone();
        let unsubscribe = Closure::wrap(Box::new(move || {
            let mut c = called_c.borrow_mut();
            if !*c {
                *c = true;
                inner.borrow_mut().unsubscribe(sub_id);
            }
        }) as Box<dyn FnMut()>);
        unsubscribe.into_js_value().unchecked_into()
    }

    /// Returns the current result.
    #[wasm_bindgen(js_name = getResult)]
    pub fn get_result(&self) -> JsValue {
        let inner = self.inner.borrow();
        if let Some(ref cols) = self.projected_columns {
            projected_rows_to_js_array(inner.result(), cols)
        } else {
            rows_to_js_array(inner.result(), &self.schema)
        }
    }

    /// Returns the current result as a binary buffer for zero-copy access.
    #[wasm_bindgen(js_name = getResultBinary)]
    pub fn get_result_binary(&self) -> BinaryResult {
        let inner = self.inner.borrow();
        encode_rc_rows_to_binary(inner.result(), &self.binary_layout)
    }

    /// Returns the schema layout for decoding binary results.
    #[wasm_bindgen(js_name = getSchemaLayout)]
    pub fn get_schema_layout(&self) -> SchemaLayout {
        self.binary_layout.clone()
    }
}

pub(super) struct CachedSnapshotRowJs {
    pub(super) version: u64,
    pub(super) epoch: u64,
    pub(super) value: JsValue,
}

#[derive(Default)]
pub(super) struct JsSnapshotRowsCache {
    pub(super) rows: HashMap<u64, CachedSnapshotRowJs>,
    pub(super) jsonb_cache: HashMap<Vec<u8>, JsValue>,
    pub(super) jsonb_cache_bytes: usize,
    pub(super) epoch: u64,
}

#[derive(Clone)]
enum JsSnapshotRowsMaterializer {
    Full { property_keys: Vec<JsValue> },
    Projected { property_keys: Vec<JsValue> },
}

impl JsSnapshotRowsMaterializer {
    fn full(schema: Table) -> Self {
        let property_keys = schema
            .columns()
            .iter()
            .map(|column| JsValue::from_str(column.name()))
            .collect();
        Self::Full { property_keys }
    }

    fn projected(column_names: Vec<String>) -> Self {
        let property_keys = column_names
            .iter()
            .map(|column_name| JsValue::from_str(column_name))
            .collect();
        Self::Projected { property_keys }
    }

    fn materialize(&self, rows: &[Rc<Row>], cache: &mut JsSnapshotRowsCache) -> JsValue {
        self.materialize_from_iter(rows.iter().map(|row| row.as_ref()), rows.len(), cache)
    }

    fn materialize_from_iter<'a, I>(
        &self,
        rows: I,
        row_count: usize,
        cache: &mut JsSnapshotRowsCache,
    ) -> JsValue
    where
        I: IntoIterator<Item = &'a Row>,
    {
        if cache.epoch == u64::MAX {
            cache.rows.clear();
            cache.epoch = 1;
        } else {
            cache.epoch += 1;
            if cache.epoch == 0 {
                cache.epoch = 1;
            }
        }
        let epoch = cache.epoch;

        let arr = js_sys::Array::new_with_length(row_count as u32);
        for (index, row) in rows.into_iter().enumerate() {
            let js_row = self.materialize_row(row, cache, epoch);
            arr.set(index as u32, js_row);
        }

        cache.rows.retain(|_, entry| entry.epoch == epoch);
        cache.prune_rows_if_needed();
        arr.into()
    }

    fn materialize_row(&self, row: &Row, cache: &mut JsSnapshotRowsCache, epoch: u64) -> JsValue {
        if let Some(entry) = cache.rows.get_mut(&row.id()) {
            if entry.version == row.version() {
                entry.epoch = epoch;
                return entry.value.clone();
            }
        }

        let value = self.build_row_object(row, cache);
        cache.rows.insert(
            row.id(),
            CachedSnapshotRowJs {
                version: row.version(),
                epoch,
                value: value.clone(),
            },
        );
        value
    }

    fn build_row_object(&self, row: &Row, cache: &mut JsSnapshotRowsCache) -> JsValue {
        match self {
            Self::Full { property_keys } | Self::Projected { property_keys } => {
                row_to_js_with_keys(row, property_keys, cache)
            }
        }
    }

    fn build_trace_handle_object(
        &self,
        arena: &TraceTupleArena,
        handle: &TraceTupleHandle,
        cache: &mut JsSnapshotRowsCache,
    ) -> JsValue {
        match self {
            Self::Full { property_keys } | Self::Projected { property_keys } => {
                trace_handle_to_js_with_keys(arena, handle, property_keys, cache)
            }
        }
    }
}

impl JsSnapshotRowsCache {
    fn materialize_trace_handle(
        &mut self,
        materializer: &JsSnapshotRowsMaterializer,
        arena: &TraceTupleArena,
        handle: &TraceTupleHandle,
    ) -> JsValue {
        let row_id = arena.row_id(handle);
        let version = arena.version(handle);

        if let Some(entry) = self.rows.get_mut(&row_id) {
            if entry.version == version {
                return entry.value.clone();
            }
        }

        let value = materializer.build_trace_handle_object(arena, handle, self);
        self.rows.insert(
            row_id,
            CachedSnapshotRowJs {
                version,
                epoch: self.epoch,
                value: value.clone(),
            },
        );
        self.prune_rows_if_needed();
        value
    }

    fn remove_row(&mut self, row_id: u64) {
        self.rows.remove(&row_id);
    }

    fn value_to_js_cached(&mut self, value: &Value) -> JsValue {
        match value {
            Value::Jsonb(jsonb) => {
                if let Some(cached) = self.jsonb_cache.get(jsonb.0.as_slice()) {
                    return cached.clone();
                }

                let parsed = value_to_js(value);
                self.jsonb_cache_bytes = self.jsonb_cache_bytes.saturating_add(jsonb.0.len());
                self.jsonb_cache.insert(jsonb.0.clone(), parsed.clone());
                self.prune_jsonb_cache_if_needed();
                parsed
            }
            _ => value_to_js(value),
        }
    }

    pub(super) fn prune_jsonb_cache_if_needed(&mut self) {
        prune_jsonb_js_cache(
            &mut self.jsonb_cache,
            &mut self.jsonb_cache_bytes,
            GraphqlJsCachePolicy::DEFAULT.jsonb(),
        );
    }

    fn prune_rows_if_needed(&mut self) {
        let (max_entries, target_entries) = GraphqlJsCachePolicy::DEFAULT.snapshot_row_limits();
        self.prune_rows_with_limits(max_entries, target_entries);
    }

    pub(super) fn prune_rows_with_limits(&mut self, max_entries: usize, target_entries: usize) {
        if self.rows.len() <= max_entries {
            return;
        }

        let target_entries = target_entries.min(max_entries);
        let current_epoch = self.epoch;
        let mut projected_len = self.rows.len();
        let mut keys_to_remove = Vec::new();

        for (&row_id, entry) in self.rows.iter() {
            if projected_len <= target_entries {
                break;
            }
            if entry.epoch == current_epoch {
                continue;
            }
            projected_len = projected_len.saturating_sub(1);
            keys_to_remove.push(row_id);
        }

        for (&row_id, entry) in self.rows.iter() {
            if projected_len <= target_entries {
                break;
            }
            if entry.epoch != current_epoch {
                continue;
            }
            projected_len = projected_len.saturating_sub(1);
            keys_to_remove.push(row_id);
        }

        for row_id in keys_to_remove {
            self.rows.remove(&row_id);
        }
    }
}

/// Converts rows to a JavaScript array.
pub(super) fn rows_to_js_array(rows: &[Rc<Row>], schema: &Table) -> JsValue {
    JsSnapshotRowsMaterializer::full(schema.clone())
        .materialize(rows, &mut JsSnapshotRowsCache::default())
}

/// Converts projected rows to a JavaScript array.
/// Only includes the specified columns in the output.
fn projected_rows_to_js_array(rows: &[Rc<Row>], column_names: &[String]) -> JsValue {
    JsSnapshotRowsMaterializer::projected(column_names.to_vec())
        .materialize(rows, &mut JsSnapshotRowsCache::default())
}

fn row_to_js_with_keys(
    row: &Row,
    property_keys: &[JsValue],
    cache: &mut JsSnapshotRowsCache,
) -> JsValue {
    let obj = js_sys::Object::new();

    for (i, property_key) in property_keys.iter().enumerate() {
        if let Some(value) = row.get(i) {
            let js_val = cache.value_to_js_cached(value);
            js_sys::Reflect::set(&obj, property_key, &js_val).ok();
        }
    }

    obj.into()
}

fn trace_handle_to_js_with_keys(
    arena: &TraceTupleArena,
    handle: &TraceTupleHandle,
    property_keys: &[JsValue],
    cache: &mut JsSnapshotRowsCache,
) -> JsValue {
    let obj = js_sys::Object::new();

    for (i, property_key) in property_keys.iter().enumerate() {
        if let Some(value) = arena.value_at(handle, i) {
            let js_val = cache.value_to_js_cached(&value);
            js_sys::Reflect::set(&obj, property_key, &js_val).ok();
        }
    }

    obj.into()
}
