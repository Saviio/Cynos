//! Reactive bridge - Integration between Query and Reactive modules.
//!
//! This module provides the bridge between the query system and the reactive
//! system, enabling observable queries that update automatically when
//! underlying data changes.
//!
//! Two strategies are supported:
//! 1. Re-query: re-execute the cached physical plan on each change (original)
//! 2. IVM (DBSP): propagate deltas through a compiled dataflow graph (new)
//!
//! The IVM path is used when the query is incrementalizable (no Sort/Limit/TopN).
//! Otherwise, falls back to re-query.

use crate::binary_protocol::{BinaryEncoder, BinaryResult, SchemaLayout};
use crate::convert::{gql_response_to_js, row_to_js, value_to_js};
use crate::query_engine::{
    execute_compiled_physical_plan_with_summary, CompiledPhysicalPlan, QueryResultSummary,
};
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::schema::Table;
use cynos_core::{Row, Value};
use cynos_incremental::{DataflowNode, Delta, MaterializedView, TableId};
use cynos_reactive::ObservableQuery;
use cynos_storage::TableCache;
use hashbrown::{HashMap, HashSet};
use wasm_bindgen::prelude::*;

fn collect_changed_rows(
    cache: &Rc<RefCell<TableCache>>,
    compiled_plan: &CompiledPhysicalPlan,
    changed_ids: &HashSet<u64>,
) -> Option<Vec<(u64, Option<Rc<Row>>)>> {
    let table_name = compiled_plan.reactive_patch_table()?;
    let cache = cache.borrow();
    let store = cache.get_table(table_name)?;
    let mut changed_rows = Vec::with_capacity(changed_ids.len());
    for &row_id in changed_ids {
        changed_rows.push((row_id, store.get(row_id)));
    }
    Some(changed_rows)
}

fn query_results_equal(
    old_summary: &QueryResultSummary,
    new_summary: &QueryResultSummary,
    old: &[Rc<Row>],
    new: &[Rc<Row>],
) -> bool {
    if old_summary != new_summary || old.len() != new.len() {
        return false;
    }

    old.iter().zip(new.iter()).all(|(old_row, new_row)| {
        Rc::ptr_eq(old_row, new_row)
            || (old_row.id() == new_row.id()
                && old_row.version() == new_row.version()
                && old_row.values() == new_row.values())
    })
}

#[derive(Default)]
struct GraphqlSubscribers {
    callbacks: Vec<(usize, Box<dyn Fn(&cynos_gql::GraphqlResponse) + 'static>)>,
    keepalive_ids: HashSet<usize>,
    next_sub_id: usize,
}

impl GraphqlSubscribers {
    fn add_keepalive(&mut self) -> usize {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        self.keepalive_ids.insert(id);
        id
    }

    fn add_callback<F>(&mut self, callback: F) -> usize
    where
        F: Fn(&cynos_gql::GraphqlResponse) + 'static,
    {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        self.callbacks.push((id, Box::new(callback)));
        id
    }

    fn remove(&mut self, id: usize) -> bool {
        if self.keepalive_ids.remove(&id) {
            return true;
        }

        let len_before = self.callbacks.len();
        self.callbacks.retain(|(sub_id, _)| *sub_id != id);
        self.callbacks.len() < len_before
    }

    fn total_count(&self) -> usize {
        self.keepalive_ids.len() + self.callbacks.len()
    }

    fn callback_count(&self) -> usize {
        self.callbacks.len()
    }

    fn emit(&self, response: &cynos_gql::GraphqlResponse) {
        for (_, callback) in &self.callbacks {
            callback(response);
        }
    }
}

fn build_graphql_response_batched(
    cache: &TableCache,
    catalog: &cynos_gql::GraphqlCatalog,
    field: &cynos_gql::bind::BoundRootField,
    plan: &cynos_gql::GraphqlBatchPlan,
    state: &mut cynos_gql::GraphqlBatchState,
    rows: &[Rc<Row>],
) -> Result<cynos_gql::GraphqlResponse, cynos_gql::GqlError> {
    cynos_gql::batch_render::render_graphql_response(cache, catalog, field, plan, state, rows)
}

fn build_graphql_response_from_owned_rows_batched(
    cache: &TableCache,
    catalog: &cynos_gql::GraphqlCatalog,
    field: &cynos_gql::bind::BoundRootField,
    plan: &cynos_gql::GraphqlBatchPlan,
    state: &mut cynos_gql::GraphqlBatchState,
    rows: &[Row],
) -> Result<cynos_gql::GraphqlResponse, cynos_gql::GqlError> {
    let rows: Vec<Rc<Row>> = rows.iter().cloned().map(Rc::new).collect();
    build_graphql_response_batched(cache, catalog, field, plan, state, &rows)
}

fn root_field_has_relations(field: &cynos_gql::bind::BoundRootField) -> bool {
    match &field.kind {
        cynos_gql::bind::BoundRootFieldKind::Typename => false,
        cynos_gql::bind::BoundRootFieldKind::Collection { selection, .. }
        | cynos_gql::bind::BoundRootFieldKind::ByPk { selection, .. }
        | cynos_gql::bind::BoundRootFieldKind::Insert { selection, .. }
        | cynos_gql::bind::BoundRootFieldKind::Update { selection, .. }
        | cynos_gql::bind::BoundRootFieldKind::Delete { selection, .. } => {
            selection_has_relations(selection)
        }
    }
}

fn selection_has_relations(selection: &cynos_gql::bind::BoundSelectionSet) -> bool {
    selection.fields.iter().any(field_has_relations)
}

fn field_has_relations(field: &cynos_gql::bind::BoundField) -> bool {
    matches!(
        field,
        cynos_gql::bind::BoundField::ForwardRelation { .. }
            | cynos_gql::bind::BoundField::ReverseRelation { .. }
    )
}

fn build_snapshot_batch_invalidation(
    table_names: &HashMap<TableId, String>,
    changes: &HashMap<TableId, HashSet<u64>>,
    root_changed: bool,
) -> Result<cynos_gql::GraphqlInvalidation, ()> {
    let mut changed_tables = Vec::with_capacity(changes.len());
    let mut dirty_table_rows = HashMap::new();
    for table_id in changes.keys() {
        let Some(table_name) = table_names.get(table_id) else {
            return Err(());
        };
        changed_tables.push(table_name.clone());
        if let Some(changed_ids) = changes.get(table_id) {
            dirty_table_rows.insert(table_name.clone(), changed_ids.clone());
        }
    }

    Ok(cynos_gql::GraphqlInvalidation {
        root_changed,
        changed_tables,
        dirty_edge_keys: HashMap::new(),
        dirty_table_rows,
    })
}

fn build_delta_batch_invalidation(
    plan: &cynos_gql::GraphqlBatchPlan,
    table_names: &HashMap<TableId, String>,
    table_id: TableId,
    deltas: &[Delta<Row>],
    root_changed: bool,
) -> Result<cynos_gql::GraphqlInvalidation, ()> {
    let Some(table_name) = table_names.get(&table_id) else {
        return Err(());
    };
    let dirty_row_ids: HashSet<u64> = deltas.iter().map(|delta| delta.data.id()).collect();

    let mut invalidation = cynos_gql::GraphqlInvalidation {
        root_changed,
        changed_tables: alloc::vec![table_name.clone()],
        dirty_edge_keys: HashMap::new(),
        dirty_table_rows: HashMap::from([(table_name.clone(), dirty_row_ids)]),
    };

    for edge_id in plan.edges_for_table(table_name) {
        let edge = plan.edge(*edge_id);
        let key_column_index = match edge.kind {
            cynos_gql::render_plan::RelationEdgeKind::Forward => edge.relation.parent_column_index,
            cynos_gql::render_plan::RelationEdgeKind::Reverse => edge.relation.child_column_index,
        };

        let mut dirty_keys = HashSet::<Value>::new();
        for delta in deltas {
            let Some(value) = delta.data.get(key_column_index).cloned() else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            dirty_keys.insert(value);
        }

        if !dirty_keys.is_empty() {
            invalidation.dirty_edge_keys.insert(*edge_id, dirty_keys);
        }
    }

    Ok(invalidation)
}

fn graphql_response_to_js_value(response: &cynos_gql::GraphqlResponse) -> JsValue {
    gql_response_to_js(response).unwrap_or(JsValue::NULL)
}

/// A re-query based observable that re-executes the query on each change.
/// This leverages the query optimizer and indexes for optimal performance.
/// The physical plan and lowered execution artifact are cached to avoid repeated
/// optimization and predicate lowering overhead. Simple single-table pipelines
/// also use a row-local patch path to avoid full re-execution when possible.
pub struct ReQueryObservable {
    /// The cached compiled plan to execute
    compiled_plan: CompiledPhysicalPlan,
    /// Reference to the table cache
    cache: Rc<RefCell<TableCache>>,
    /// Current result set
    result: Vec<Rc<Row>>,
    /// Summary of the current result set for fast equality checks
    result_summary: QueryResultSummary,
    /// Subscription callbacks
    subscriptions: Vec<(usize, Box<dyn Fn(&[Rc<Row>]) + 'static>)>,
    /// Next subscription ID
    next_sub_id: usize,
}

impl ReQueryObservable {
    /// Creates a new re-query observable with a pre-compiled physical plan.
    pub fn new(
        compiled_plan: CompiledPhysicalPlan,
        cache: Rc<RefCell<TableCache>>,
        initial_result: Vec<Rc<Row>>,
    ) -> Self {
        let result_summary = QueryResultSummary::from_rows(&initial_result);
        Self::new_with_summary(compiled_plan, cache, initial_result, result_summary)
    }

    #[doc(hidden)]
    pub fn new_with_summary(
        compiled_plan: CompiledPhysicalPlan,
        cache: Rc<RefCell<TableCache>>,
        initial_result: Vec<Rc<Row>>,
        result_summary: QueryResultSummary,
    ) -> Self {
        Self {
            compiled_plan,
            cache,
            result: initial_result,
            result_summary,
            subscriptions: Vec::new(),
            next_sub_id: 0,
        }
    }

    /// Returns the current result.
    pub fn result(&self) -> &[Rc<Row>] {
        &self.result
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.result.len()
    }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool {
        self.result.is_empty()
    }

    /// Subscribes to changes.
    pub fn subscribe<F: Fn(&[Rc<Row>]) + 'static>(&mut self, callback: F) -> usize {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        self.subscriptions.push((id, Box::new(callback)));
        id
    }

    /// Unsubscribes by ID.
    pub fn unsubscribe(&mut self, id: usize) -> bool {
        let len_before = self.subscriptions.len();
        self.subscriptions.retain(|(sub_id, _)| *sub_id != id);
        self.subscriptions.len() < len_before
    }

    /// Returns subscription count.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Called when the table changes - re-executes the cached physical plan.
    /// Only notifies subscribers if the result actually changed.
    /// Skips re-query entirely if there are no subscribers.
    ///
    /// `changed_ids` contains the row IDs that were modified.
    /// For simple single-table pipelines this enables a row-local fast path;
    /// all other plans fall back to deterministic full-result comparison.
    pub fn on_change(&mut self, changed_ids: &HashSet<u64>) {
        // Skip re-query if no subscribers - major optimization for unused observables
        if self.subscriptions.is_empty() {
            return;
        }

        if let Some(changed_rows) =
            collect_changed_rows(&self.cache, &self.compiled_plan, changed_ids)
        {
            match self
                .compiled_plan
                .apply_reactive_patch(&mut self.result, &changed_rows)
            {
                Some(true) => {
                    self.result_summary = QueryResultSummary::from_rows(&self.result);
                    for (_, callback) in &self.subscriptions {
                        callback(&self.result);
                    }
                    return;
                }
                Some(false) => return,
                None => {}
            }
        }

        // Re-execute the cached compiled plan (no optimization or lowering overhead)
        let cache = self.cache.borrow();

        match execute_compiled_physical_plan_with_summary(&cache, &self.compiled_plan) {
            Ok(output) => {
                // Only notify if result changed
                if !query_results_equal(
                    &self.result_summary,
                    &output.summary,
                    &self.result,
                    &output.rows,
                ) {
                    self.result = output.rows;
                    self.result_summary = output.summary;
                    // Notify all subscribers
                    for (_, callback) in &self.subscriptions {
                        callback(&self.result);
                    }
                }
            }
            Err(_) => {
                // Query execution failed, keep old result
            }
        }
    }
}

pub struct GraphqlSubscriptionObservable {
    compiled_plan: CompiledPhysicalPlan,
    cache: Rc<RefCell<TableCache>>,
    catalog: cynos_gql::GraphqlCatalog,
    field: cynos_gql::bind::BoundRootField,
    batch_plan: cynos_gql::GraphqlBatchPlan,
    batch_state: cynos_gql::GraphqlBatchState,
    dependency_table_names: HashMap<TableId, String>,
    root_table_ids: HashSet<TableId>,
    root_rows: Vec<Rc<Row>>,
    root_summary: QueryResultSummary,
    response: Option<cynos_gql::GraphqlResponse>,
    response_dirty: bool,
    subscribers: GraphqlSubscribers,
}

impl GraphqlSubscriptionObservable {
    pub fn new(
        compiled_plan: CompiledPhysicalPlan,
        cache: Rc<RefCell<TableCache>>,
        catalog: cynos_gql::GraphqlCatalog,
        field: cynos_gql::bind::BoundRootField,
        batch_plan: cynos_gql::GraphqlBatchPlan,
        dependency_table_bindings: Vec<(TableId, String)>,
        root_table_ids: HashSet<TableId>,
        initial_rows: Vec<Rc<Row>>,
        initial_summary: QueryResultSummary,
    ) -> Self {
        Self {
            compiled_plan,
            cache,
            batch_plan,
            batch_state: cynos_gql::GraphqlBatchState::default(),
            dependency_table_names: dependency_table_bindings.into_iter().collect(),
            catalog,
            field,
            root_table_ids,
            root_rows: initial_rows,
            root_summary: initial_summary,
            response: None,
            response_dirty: true,
            subscribers: GraphqlSubscribers::default(),
        }
    }

    pub fn attach_keepalive(&mut self) -> usize {
        self.subscribers.add_keepalive()
    }

    pub fn response_js_value(&mut self) -> JsValue {
        if self.response.is_some() && !self.response_dirty {
            return graphql_response_to_js_value(self.response.as_ref().unwrap());
        }

        if self.subscribers.callback_count() == 0 {
            return self.render_response_js_value();
        }

        match self.current_response() {
            Some(response) => graphql_response_to_js_value(response),
            None => JsValue::NULL,
        }
    }

    pub fn subscribe<F: Fn(&cynos_gql::GraphqlResponse) + 'static>(
        &mut self,
        callback: F,
    ) -> usize {
        self.subscribers.add_callback(callback)
    }

    pub fn unsubscribe(&mut self, id: usize) -> bool {
        self.subscribers.remove(id)
    }

    pub fn subscription_count(&self) -> usize {
        self.subscribers.total_count()
    }

    pub fn listener_count(&self) -> usize {
        self.subscribers.callback_count()
    }

    pub fn on_change(&mut self, changes: &HashMap<TableId, HashSet<u64>>) {
        if self.subscribers.total_count() == 0 {
            return;
        }

        let mut root_changed_ids = HashSet::new();
        let mut saw_nested_change = false;
        for (table_id, changed_ids) in changes {
            if self.root_table_ids.contains(table_id) {
                root_changed_ids.extend(changed_ids.iter().copied());
            } else {
                saw_nested_change = true;
            }
        }

        let mut root_changed = false;
        if !root_changed_ids.is_empty() {
            root_changed = match self.refresh_root_rows(&root_changed_ids) {
                Some(changed) => changed,
                None => return,
            };
        }

        if !root_changed && !saw_nested_change {
            return;
        }

        match build_snapshot_batch_invalidation(&self.dependency_table_names, changes, root_changed)
        {
            Ok(invalidation) => self
                .batch_state
                .apply_invalidation(&self.batch_plan, &invalidation),
            Err(()) => {
                self.batch_state = cynos_gql::GraphqlBatchState::default();
            }
        }
        self.response_dirty = true;
        if self.subscribers.callback_count() == 0 {
            return;
        }

        if let Some(changed) = self.materialize_response_if_dirty() {
            if changed {
                if let Some(response) = self.response.as_ref() {
                    self.subscribers.emit(response);
                }
            }
        }
    }

    fn refresh_root_rows(&mut self, changed_ids: &HashSet<u64>) -> Option<bool> {
        if let Some(changed_rows) =
            collect_changed_rows(&self.cache, &self.compiled_plan, changed_ids)
        {
            match self
                .compiled_plan
                .apply_reactive_patch(&mut self.root_rows, &changed_rows)
            {
                Some(true) => {
                    self.root_summary = QueryResultSummary::from_rows(&self.root_rows);
                    return Some(true);
                }
                Some(false) => return Some(false),
                None => {}
            }
        }

        let cache = self.cache.borrow();
        let output =
            execute_compiled_physical_plan_with_summary(&cache, &self.compiled_plan).ok()?;
        if query_results_equal(
            &self.root_summary,
            &output.summary,
            &self.root_rows,
            &output.rows,
        ) {
            return Some(false);
        }

        self.root_rows = output.rows;
        self.root_summary = output.summary;
        Some(true)
    }

    fn materialize_response_if_dirty(&mut self) -> Option<bool> {
        if !self.response_dirty && self.response.is_some() {
            return Some(false);
        }

        let cache = self.cache.borrow();
        let response = build_graphql_response_batched(
            &cache,
            &self.catalog,
            &self.field,
            &self.batch_plan,
            &mut self.batch_state,
            &self.root_rows,
        )
        .ok()?;
        let changed = self
            .response
            .as_ref()
            .map_or(true, |current| *current != response);
        if changed {
            self.response = Some(response);
        }
        self.response_dirty = false;
        Some(changed)
    }

    fn current_response(&mut self) -> Option<&cynos_gql::GraphqlResponse> {
        self.materialize_response_if_dirty()?;
        self.response.as_ref()
    }

    fn render_response_js_value(&mut self) -> JsValue {
        let cache = self.cache.borrow();
        let response = build_graphql_response_batched(
            &cache,
            &self.catalog,
            &self.field,
            &self.batch_plan,
            &mut self.batch_state,
            &self.root_rows,
        );
        match response {
            Ok(response) => graphql_response_to_js_value(&response),
            Err(_) => JsValue::NULL,
        }
    }
}

pub struct GraphqlDeltaObservable {
    view: MaterializedView,
    cache: Rc<RefCell<TableCache>>,
    catalog: cynos_gql::GraphqlCatalog,
    field: cynos_gql::bind::BoundRootField,
    batch_plan: cynos_gql::GraphqlBatchPlan,
    batch_state: cynos_gql::GraphqlBatchState,
    dependency_table_names: HashMap<TableId, String>,
    has_nested_relations: bool,
    response: Option<cynos_gql::GraphqlResponse>,
    response_dirty: bool,
    subscribers: GraphqlSubscribers,
}

impl GraphqlDeltaObservable {
    pub fn new(
        dataflow: DataflowNode,
        cache: Rc<RefCell<TableCache>>,
        catalog: cynos_gql::GraphqlCatalog,
        field: cynos_gql::bind::BoundRootField,
        batch_plan: cynos_gql::GraphqlBatchPlan,
        dependency_table_bindings: Vec<(TableId, String)>,
        initial_rows: Vec<Row>,
    ) -> Self {
        Self {
            view: MaterializedView::with_initial(dataflow, initial_rows),
            cache,
            batch_plan,
            batch_state: cynos_gql::GraphqlBatchState::default(),
            dependency_table_names: dependency_table_bindings.into_iter().collect(),
            catalog,
            has_nested_relations: root_field_has_relations(&field),
            field,
            response: None,
            response_dirty: true,
            subscribers: GraphqlSubscribers::default(),
        }
    }

    pub fn attach_keepalive(&mut self) -> usize {
        self.subscribers.add_keepalive()
    }

    pub fn response_js_value(&mut self) -> JsValue {
        if self.response.is_some() && !self.response_dirty {
            return graphql_response_to_js_value(self.response.as_ref().unwrap());
        }

        if self.subscribers.callback_count() == 0 {
            return self.render_response_js_value();
        }

        match self.current_response() {
            Some(response) => graphql_response_to_js_value(response),
            None => JsValue::NULL,
        }
    }

    pub fn dependencies(&self) -> &[TableId] {
        self.view.dependencies()
    }

    pub fn subscribe<F: Fn(&cynos_gql::GraphqlResponse) + 'static>(
        &mut self,
        callback: F,
    ) -> usize {
        self.subscribers.add_callback(callback)
    }

    pub fn unsubscribe(&mut self, id: usize) -> bool {
        self.subscribers.remove(id)
    }

    pub fn subscription_count(&self) -> usize {
        self.subscribers.total_count()
    }

    pub fn listener_count(&self) -> usize {
        self.subscribers.callback_count()
    }

    pub fn on_table_change(&mut self, table_id: TableId, deltas: Vec<Delta<Row>>) {
        if self.subscribers.total_count() == 0 {
            return;
        }

        let batch_invalidation = build_delta_batch_invalidation(
            &self.batch_plan,
            &self.dependency_table_names,
            table_id,
            &deltas,
            false,
        );
        let output_deltas = self.view.on_table_change(table_id, deltas);
        if output_deltas.is_empty() && !self.has_nested_relations {
            return;
        }

        match batch_invalidation {
            Ok(mut invalidation) => {
                invalidation.root_changed = !output_deltas.is_empty();
                self.batch_state
                    .apply_invalidation(&self.batch_plan, &invalidation);
            }
            Err(()) => {
                self.batch_state = cynos_gql::GraphqlBatchState::default();
            }
        }
        self.response_dirty = true;
        if self.subscribers.callback_count() == 0 {
            return;
        }

        if let Some(changed) = self.materialize_response_if_dirty() {
            if changed {
                if let Some(response) = self.response.as_ref() {
                    self.subscribers.emit(response);
                }
            }
        }
    }

    fn materialize_response_if_dirty(&mut self) -> Option<bool> {
        if !self.response_dirty && self.response.is_some() {
            return Some(false);
        }

        let rows = self.view.result();
        let cache = self.cache.borrow();
        let response = build_graphql_response_from_owned_rows_batched(
            &cache,
            &self.catalog,
            &self.field,
            &self.batch_plan,
            &mut self.batch_state,
            &rows,
        )
        .ok()?;
        let changed = self
            .response
            .as_ref()
            .map_or(true, |current| *current != response);
        if changed {
            self.response = Some(response);
        }
        self.response_dirty = false;
        Some(changed)
    }

    fn current_response(&mut self) -> Option<&cynos_gql::GraphqlResponse> {
        self.materialize_response_if_dirty()?;
        self.response.as_ref()
    }

    fn render_response_js_value(&mut self) -> JsValue {
        let rows = self.view.result();
        let cache = self.cache.borrow();
        let response = build_graphql_response_from_owned_rows_batched(
            &cache,
            &self.catalog,
            &self.field,
            &self.batch_plan,
            &mut self.batch_state,
            &rows,
        );
        match response {
            Ok(response) => graphql_response_to_js_value(&response),
            Err(_) => JsValue::NULL,
        }
    }
}

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

    fn subscribe<F: Fn(&cynos_gql::GraphqlResponse) + 'static>(&self, callback: F) -> usize {
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

        let sub_id = self.inner.borrow_mut().subscribe(move |rows| {
            let current_data = if let Some(ref cols) = aggregate_columns {
                projected_rows_to_js_array(rows, cols)
            } else if let Some(ref cols) = projected_columns {
                projected_rows_to_js_array(rows, cols)
            } else {
                rows_to_js_array(rows, &schema)
            };
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
        let rows = inner.result();
        let mut encoder = BinaryEncoder::new(self.binary_layout.clone(), rows.len());
        encoder.encode_rows(rows);
        BinaryResult::new(encoder.finish())
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
    schema: Table,
    /// Optional projected column names.
    projected_columns: Option<Vec<String>>,
    /// Pre-computed binary layout for getResultBinary().
    binary_layout: SchemaLayout,
    /// Optional aggregate column names.
    aggregate_columns: Option<Vec<String>>,
}

impl JsIvmObservableQuery {
    pub(crate) fn new(
        inner: Rc<RefCell<ObservableQuery>>,
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
        inner: Rc<RefCell<ObservableQuery>>,
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
        let schema = self.schema.clone();
        let projected_columns = self.projected_columns.clone();
        let aggregate_columns = self.aggregate_columns.clone();

        let sub_id = self.inner.borrow_mut().subscribe(move |change_set| {
            let delta_obj = js_sys::Object::new();

            // Serialize only added rows
            let added = if let Some(ref cols) = aggregate_columns {
                ivm_rows_to_js_array(&change_set.added, cols)
            } else if let Some(ref cols) = projected_columns {
                ivm_rows_to_js_array(&change_set.added, cols)
            } else {
                ivm_full_rows_to_js_array(&change_set.added, &schema)
            };

            // Serialize only removed rows
            let removed = if let Some(ref cols) = aggregate_columns {
                ivm_rows_to_js_array(&change_set.removed, cols)
            } else if let Some(ref cols) = projected_columns {
                ivm_rows_to_js_array(&change_set.removed, cols)
            } else {
                ivm_full_rows_to_js_array(&change_set.removed, &schema)
            };

            js_sys::Reflect::set(&delta_obj, &JsValue::from_str("added"), &added).ok();
            js_sys::Reflect::set(&delta_obj, &JsValue::from_str("removed"), &removed).ok();

            callback.call1(&JsValue::NULL, &delta_obj).ok();
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
        let rows = inner.result();
        if let Some(ref cols) = self.aggregate_columns {
            ivm_rows_to_js_array(&rows, cols)
        } else if let Some(ref cols) = self.projected_columns {
            ivm_rows_to_js_array(&rows, cols)
        } else {
            ivm_full_rows_to_js_array(&rows, &self.schema)
        }
    }

    /// Returns the current result as a binary buffer for zero-copy access.
    #[wasm_bindgen(js_name = getResultBinary)]
    pub fn get_result_binary(&self) -> BinaryResult {
        let inner = self.inner.borrow();
        let rows = inner.result();
        let rc_rows: Vec<Rc<Row>> = rows.into_iter().map(Rc::new).collect();
        let mut encoder = BinaryEncoder::new(self.binary_layout.clone(), rc_rows.len());
        encoder.encode_rows(&rc_rows);
        BinaryResult::new(encoder.finish())
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

/// Converts IVM rows (owned Row, not Rc<Row>) to a JavaScript array using projected columns.
fn ivm_rows_to_js_array(rows: &[Row], column_names: &[String]) -> JsValue {
    let arr = js_sys::Array::new_with_length(rows.len() as u32);
    for (i, row) in rows.iter().enumerate() {
        let obj = js_sys::Object::new();
        for (col_idx, col_name) in column_names.iter().enumerate() {
            if let Some(value) = row.get(col_idx) {
                let js_val = value_to_js(value);
                js_sys::Reflect::set(&obj, &JsValue::from_str(col_name), &js_val).ok();
            }
        }
        arr.set(i as u32, obj.into());
    }
    arr.into()
}

/// Converts IVM rows (owned Row, not Rc<Row>) to a JavaScript array using full schema.
fn ivm_full_rows_to_js_array(rows: &[Row], schema: &Table) -> JsValue {
    let arr = js_sys::Array::new_with_length(rows.len() as u32);
    for (i, row) in rows.iter().enumerate() {
        let obj = js_sys::Object::new();
        for col in schema.columns() {
            if let Some(value) = row.get(col.index()) {
                let js_val = value_to_js(value);
                js_sys::Reflect::set(&obj, &JsValue::from_str(col.name()), &js_val).ok();
            }
        }
        arr.set(i as u32, obj.into());
    }
    arr.into()
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
        let sub_id = inner.subscribe(move |response| {
            let payload = graphql_response_to_js_value(response);
            callback.call1(&JsValue::NULL, &payload).ok();
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

        // Emit initial value immediately
        let initial_data = if let Some(ref cols) = projected_columns {
            projected_rows_to_js_array(inner.borrow().result(), cols)
        } else {
            rows_to_js_array(inner.borrow().result(), &schema)
        };
        callback.call1(&JsValue::NULL, &initial_data).ok();

        // Subscribe to subsequent changes
        let schema_clone = schema.clone();
        let projected_columns_clone = projected_columns.clone();
        let sub_id = inner.borrow_mut().subscribe(move |rows| {
            let current_data = if let Some(ref cols) = projected_columns_clone {
                projected_rows_to_js_array(rows, cols)
            } else {
                rows_to_js_array(rows, &schema_clone)
            };
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
        let rows = inner.result();
        let mut encoder = BinaryEncoder::new(self.binary_layout.clone(), rows.len());
        encoder.encode_rows(rows);
        BinaryResult::new(encoder.finish())
    }

    /// Returns the schema layout for decoding binary results.
    #[wasm_bindgen(js_name = getSchemaLayout)]
    pub fn get_schema_layout(&self) -> SchemaLayout {
        self.binary_layout.clone()
    }
}

/// Converts rows to a JavaScript array.
fn rows_to_js_array(rows: &[Rc<Row>], schema: &Table) -> JsValue {
    let arr = js_sys::Array::new_with_length(rows.len() as u32);
    for (i, row) in rows.iter().enumerate() {
        arr.set(i as u32, row_to_js(row, schema));
    }
    arr.into()
}

/// Converts projected rows to a JavaScript array.
/// Only includes the specified columns in the output.
fn projected_rows_to_js_array(rows: &[Rc<Row>], column_names: &[String]) -> JsValue {
    let arr = js_sys::Array::new_with_length(rows.len() as u32);
    for (i, row) in rows.iter().enumerate() {
        let obj = js_sys::Object::new();
        for (col_idx, col_name) in column_names.iter().enumerate() {
            if let Some(value) = row.get(col_idx) {
                let js_val = value_to_js(value);
                js_sys::Reflect::set(&obj, &JsValue::from_str(col_name), &js_val).ok();
            }
        }
        arr.set(i as u32, obj.into());
    }
    arr.into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_runtime::LiveRegistry;
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, Value};
    use cynos_query::ast::Expr;
    use cynos_query::executor::{InMemoryDataSource, PhysicalPlanRunner};
    use cynos_query::planner::PhysicalPlan;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    fn test_schema() -> Table {
        TableBuilder::new("users")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_column("age", DataType::Int32)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    fn make_row(id: u64, name: &str, age: i32) -> Row {
        Row::new(
            id,
            alloc::vec![
                Value::Int64(id as i64),
                Value::String(name.into()),
                Value::Int32(age),
            ],
        )
    }

    #[wasm_bindgen_test]
    fn test_live_registry_new() {
        let registry = LiveRegistry::new();
        assert_eq!(registry.query_count(), 0);
    }

    #[wasm_bindgen_test]
    fn test_rows_to_js_array() {
        let schema = test_schema();
        let rows: Vec<Rc<Row>> = alloc::vec![
            Rc::new(make_row(1, "Alice", 25)),
            Rc::new(make_row(2, "Bob", 30)),
        ];

        let js = rows_to_js_array(&rows, &schema);
        let arr = js_sys::Array::from(&js);
        assert_eq!(arr.length(), 2);
    }

    #[test]
    fn test_projection_query_preserves_version_and_live_query_detects_update() {
        let plan = PhysicalPlan::project(
            PhysicalPlan::table_scan("users"),
            alloc::vec![Expr::column("users", "name", 1)],
        );

        let mut old_ds = InMemoryDataSource::new();
        old_ds.add_table(
            "users",
            alloc::vec![Row::new_with_version(
                1,
                5,
                alloc::vec![
                    Value::Int64(1),
                    Value::String("Alice".into()),
                    Value::Int32(25),
                ],
            )],
            3,
        );
        let old_result = PhysicalPlanRunner::new(&old_ds).execute(&plan).unwrap();
        let old_rows: Vec<Rc<Row>> = old_result
            .entries
            .iter()
            .map(|entry| entry.row.clone())
            .collect();

        let mut new_ds = InMemoryDataSource::new();
        new_ds.add_table(
            "users",
            alloc::vec![Row::new_with_version(
                1,
                6,
                alloc::vec![
                    Value::Int64(1),
                    Value::String("Alicia".into()),
                    Value::Int32(25),
                ],
            )],
            3,
        );
        let new_result = PhysicalPlanRunner::new(&new_ds).execute(&plan).unwrap();
        let new_rows: Vec<Rc<Row>> = new_result
            .entries
            .iter()
            .map(|entry| entry.row.clone())
            .collect();

        assert_eq!(
            old_rows[0].version(),
            5,
            "Projection should preserve the source row version for reactive diffing",
        );
        assert_eq!(
            new_rows[0].version(),
            6,
            "Projection should preserve the source row version for reactive diffing",
        );

        let old_summary = QueryResultSummary::from_rows(&old_rows);
        let new_summary = QueryResultSummary::from_rows(&new_rows);
        assert!(
            !query_results_equal(&old_summary, &new_summary, &old_rows, &new_rows),
            "Projected value changed from Alice to Alicia, so live query comparison should detect a change",
        );
    }

    #[test]
    fn test_result_comparison_falls_back_to_exact_rows_when_summary_matches() {
        let old_rows: Vec<Rc<Row>> = alloc::vec![Rc::new(Row::new_with_version(
            1,
            5,
            alloc::vec![Value::String("Alice".into())],
        ))];
        let new_rows: Vec<Rc<Row>> = alloc::vec![Rc::new(Row::new_with_version(
            1,
            5,
            alloc::vec![Value::String("Alicia".into())],
        ))];

        let colliding_summary = QueryResultSummary {
            len: 1,
            fingerprint: 42,
        };

        assert!(
            !query_results_equal(&colliding_summary, &colliding_summary, &old_rows, &new_rows,),
            "Row comparison must remain deterministic even if two summaries collide",
        );
    }
}
