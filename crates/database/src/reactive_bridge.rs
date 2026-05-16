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
use crate::convert::{
    gql_response_to_js_with_cache, gql_response_to_js_with_root_list_patch, value_to_js,
    GraphqlJsEncodeCache, GraphqlRootListJsCache,
};
use crate::live_runtime::{
    RowsSnapshotDependencyGraph, RowsSnapshotLookupPrimitive, RowsSnapshotOrderKey,
    RowsSnapshotPartialRefreshMetadata, RowsSnapshotPartialRefreshState,
    RowsSnapshotRootSubsetMetadata, RowsSnapshotRootSubsetPlan,
};
use crate::profiling::{
    now_ms, IvmBridgeProfile, IvmBridgeProfiler, SnapshotQueryProfile, SnapshotRefreshMode,
};
#[cfg(feature = "benchmark")]
use crate::profiling::{GraphqlDeltaProfile, GraphqlSnapshotQueryProfile};
use crate::query_engine::{
    choose_root_subset_plan_variant, execute_compiled_physical_plan_on_table_subset,
    execute_compiled_physical_plan_with_summary, CompiledPhysicalPlan, QueryResultSummary,
    RootSubsetPlanVariant,
};
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;
use core::cmp::Ordering;
use cynos_core::schema::Table;
use cynos_core::{Row, Value};
use cynos_incremental::{
    DataflowNode, Delta, MaterializedView, TableId, TraceDeltaBatch, TraceTupleArena,
    TraceTupleHandle,
};
use cynos_reactive::ObservableQuery;
use cynos_storage::{RowStore, TableCache};
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

fn encode_rc_rows_to_binary(rows: &[Rc<Row>], binary_layout: &SchemaLayout) -> BinaryResult {
    encode_rc_rows_iter_to_binary(rows.iter(), rows.len(), binary_layout)
}

fn encode_rc_rows_iter_to_binary<'a, I>(
    rows: I,
    row_count: usize,
    binary_layout: &SchemaLayout,
) -> BinaryResult
where
    I: IntoIterator<Item = &'a Rc<Row>>,
{
    let mut encoder = BinaryEncoder::new(binary_layout.clone(), row_count);
    encoder.encode_rows_iter(rows);
    BinaryResult::new(encoder.finish())
}

fn binary_result_to_js_value(result: BinaryResult) -> JsValue {
    result.into()
}

#[derive(Clone)]
struct PartialRefreshCandidateRow {
    row: Rc<Row>,
    root_row_id: u64,
}

struct SnapshotPartialRefreshRuntime {
    metadata: RowsSnapshotPartialRefreshMetadata,
    candidate_rows: Vec<PartialRefreshCandidateRow>,
}

impl SnapshotPartialRefreshRuntime {
    fn new(cache: &TableCache, state: RowsSnapshotPartialRefreshState) -> Self {
        let candidate_rows = build_partial_refresh_candidate_rows(
            cache,
            &state.metadata,
            &state.initial_candidate_rows,
        )
        .expect("partial refresh candidate rows should be resolvable");
        Self {
            metadata: state.metadata,
            candidate_rows,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum SnapshotRootKey {
    Empty,
    One(Value),
    Many(Vec<Value>),
}

impl SnapshotRootKey {
    fn from_values(mut values: Vec<Value>) -> Self {
        match values.len() {
            0 => Self::Empty,
            1 => Self::One(values.pop().unwrap()),
            _ => Self::Many(values),
        }
    }

    fn from_output_row(row: &Row, indices: &[usize]) -> Option<Self> {
        let mut values = Vec::with_capacity(indices.len());
        for &index in indices {
            values.push(row.get(index)?.clone());
        }
        Some(Self::from_values(values))
    }

    fn from_store_row(row: &Row, indices: &[usize]) -> Option<Self> {
        let mut values = Vec::with_capacity(indices.len());
        for &column_index in indices {
            values.push(row.get(column_index)?.clone());
        }
        Some(Self::from_values(values))
    }
}

struct RootSubsetRefreshRuntime {
    compiled_plans: crate::live_runtime::RowsSnapshotRootSubsetVariants,
    metadata: RowsSnapshotRootSubsetMetadata,
    row_id_to_root_key: HashMap<u64, SnapshotRootKey>,
    root_ordinals: HashMap<SnapshotRootKey, usize>,
    next_root_ordinal: usize,
    root_rows: HashMap<SnapshotRootKey, Vec<Rc<Row>>>,
    visible_root_order: Vec<SnapshotRootKey>,
}

struct SnapshotRootRefreshOutcome {
    changed: bool,
    dirty_root_rows: HashSet<u64>,
}

impl RootSubsetRefreshRuntime {
    fn new(
        cache: &TableCache,
        plan: RowsSnapshotRootSubsetPlan,
        initial_rows: &[Rc<Row>],
    ) -> Option<Self> {
        let RowsSnapshotRootSubsetPlan {
            metadata,
            compiled_plans,
        } = plan;
        let store = cache.get_table(&metadata.root_table)?;

        let mut row_id_to_root_key = HashMap::new();
        let mut root_ordinals = HashMap::new();
        let mut next_root_ordinal = 0usize;
        store.visit_rows(|row| {
            if let Some(root_key) =
                SnapshotRootKey::from_store_row(row, &metadata.root_pk_store_indices)
            {
                row_id_to_root_key.insert(row.id(), root_key.clone());
                if !root_ordinals.contains_key(&root_key) {
                    root_ordinals.insert(root_key, next_root_ordinal);
                    next_root_ordinal += 1;
                }
            }
            true
        });

        let mut root_rows = HashMap::new();
        let mut visible_root_order = Vec::new();
        let mut seen_roots = HashSet::new();
        let mut last_root_key: Option<SnapshotRootKey> = None;

        for row in initial_rows {
            let root_key =
                SnapshotRootKey::from_output_row(row.as_ref(), &metadata.root_pk_output_indices)?;
            if last_root_key.as_ref() != Some(&root_key) && seen_roots.contains(&root_key) {
                return None;
            }
            if seen_roots.insert(root_key.clone()) {
                visible_root_order.push(root_key.clone());
            }
            root_rows
                .entry(root_key.clone())
                .or_insert_with(Vec::new)
                .push(row.clone());
            last_root_key = Some(root_key);
        }

        Some(Self {
            compiled_plans,
            metadata,
            row_id_to_root_key,
            root_ordinals,
            next_root_ordinal,
            root_rows,
            visible_root_order,
        })
    }

    fn select_variant(
        &self,
        cache: &TableCache,
        affected_root_ids: &HashSet<u64>,
    ) -> RootSubsetPlanVariant {
        let table_row_count = cache
            .get_table(&self.metadata.root_table)
            .map(|store| store.len())
            .unwrap_or(0);
        choose_root_subset_plan_variant(affected_root_ids.len(), table_row_count)
    }

    fn select_compiled_plan<'a>(
        &'a self,
        cache: &TableCache,
        affected_root_ids: &HashSet<u64>,
    ) -> &'a CompiledPhysicalPlan {
        self.compiled_plans
            .select(self.select_variant(cache, affected_root_ids))
    }

    fn collect_affected_root_keys(
        &mut self,
        cache: &TableCache,
        affected_root_ids: &HashSet<u64>,
        refresh_existing: bool,
    ) -> Option<HashSet<SnapshotRootKey>> {
        let store = cache.get_table(&self.metadata.root_table)?;
        let mut affected_keys = HashSet::new();

        for &row_id in affected_root_ids {
            if let Some(existing) = self.row_id_to_root_key.get(&row_id).cloned() {
                affected_keys.insert(existing);
                if !refresh_existing {
                    continue;
                }
            }

            if let Some(row) = store.get(row_id) {
                let root_key = SnapshotRootKey::from_store_row(
                    row.as_ref(),
                    &self.metadata.root_pk_store_indices,
                )?;
                affected_keys.insert(root_key.clone());
                if !self.root_ordinals.contains_key(&root_key) {
                    self.root_ordinals
                        .insert(root_key.clone(), self.next_root_ordinal);
                    self.next_root_ordinal += 1;
                }
                self.row_id_to_root_key.insert(row_id, root_key);
            } else {
                self.row_id_to_root_key.remove(&row_id);
            }
        }

        Some(affected_keys)
    }

    fn apply_subset_rows(
        &mut self,
        affected_root_keys: &HashSet<SnapshotRootKey>,
        rows: Vec<Rc<Row>>,
    ) -> Option<Vec<Rc<Row>>> {
        let mut recomputed_groups: HashMap<SnapshotRootKey, Vec<Rc<Row>>> = HashMap::new();
        for row in rows {
            let root_key = SnapshotRootKey::from_output_row(
                row.as_ref(),
                &self.metadata.root_pk_output_indices,
            )?;
            recomputed_groups
                .entry(root_key)
                .or_insert_with(Vec::new)
                .push(row);
        }

        let mut next_visible_root_order = Vec::with_capacity(
            self.visible_root_order
                .len()
                .saturating_add(recomputed_groups.len()),
        );
        for root_key in affected_root_keys {
            if !recomputed_groups.contains_key(root_key) {
                self.root_rows.remove(root_key);
            }
        }

        for root_key in self.visible_root_order.drain(..) {
            if affected_root_keys.contains(&root_key) {
                if let Some(group) = recomputed_groups.remove(&root_key) {
                    self.root_rows.insert(root_key.clone(), group);
                    next_visible_root_order.push(root_key);
                }
                continue;
            }

            next_visible_root_order.push(root_key);
        }

        let mut new_root_keys: Vec<SnapshotRootKey> = recomputed_groups.keys().cloned().collect();
        new_root_keys.sort_by_key(|root_key| {
            self.root_ordinals
                .get(root_key)
                .copied()
                .unwrap_or(usize::MAX)
        });

        for root_key in new_root_keys {
            let group = recomputed_groups.remove(&root_key).unwrap_or_default();
            self.root_rows.insert(root_key.clone(), group);
            let ordinal = self
                .root_ordinals
                .get(&root_key)
                .copied()
                .unwrap_or(usize::MAX);
            let insert_at = next_visible_root_order
                .iter()
                .position(|candidate| {
                    self.root_ordinals
                        .get(candidate)
                        .copied()
                        .unwrap_or(usize::MAX)
                        > ordinal
                })
                .unwrap_or(next_visible_root_order.len());
            next_visible_root_order.insert(insert_at, root_key);
        }

        self.visible_root_order = next_visible_root_order;

        let total_rows = self
            .visible_root_order
            .iter()
            .filter_map(|root_key| self.root_rows.get(root_key).map(Vec::len))
            .sum();
        let mut result = Vec::with_capacity(total_rows);
        for root_key in &self.visible_root_order {
            if let Some(group) = self.root_rows.get(root_key) {
                result.extend(group.iter().cloned());
            }
        }
        Some(result)
    }
}

fn slice_partial_visible_rows(
    candidate_rows: &[PartialRefreshCandidateRow],
    metadata: &RowsSnapshotPartialRefreshMetadata,
) -> Vec<Rc<Row>> {
    candidate_rows
        .iter()
        .skip(metadata.visible_offset)
        .take(metadata.visible_limit)
        .map(|entry| entry.row.clone())
        .collect()
}

fn compare_partial_refresh_values(
    left: Option<&Value>,
    right: Option<&Value>,
    order: cynos_query::ast::SortOrder,
) -> Ordering {
    let cmp = match (left, right) {
        (Some(left), Some(right)) => left.cmp(right),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    };

    match order {
        cynos_query::ast::SortOrder::Asc => cmp,
        cynos_query::ast::SortOrder::Desc => cmp.reverse(),
    }
}

fn compare_partial_refresh_rows(
    left: &PartialRefreshCandidateRow,
    right: &PartialRefreshCandidateRow,
    order_keys: &[RowsSnapshotOrderKey],
) -> Ordering {
    for order_key in order_keys {
        let cmp = compare_partial_refresh_values(
            left.row.get(order_key.output_index),
            right.row.get(order_key.output_index),
            order_key.order,
        );
        if cmp != Ordering::Equal {
            return cmp;
        }
    }

    left.root_row_id
        .cmp(&right.root_row_id)
        .then_with(|| left.row.values().cmp(right.row.values()))
        .then_with(|| left.row.version().cmp(&right.row.version()))
}

fn insert_row_ids_by_column_values(
    store: &RowStore,
    column_index: usize,
    lookup: &RowsSnapshotLookupPrimitive,
    values: &HashSet<Value>,
    row_ids: &mut HashSet<u64>,
) -> bool {
    if values.is_empty() {
        return false;
    }

    let mut added = false;
    match lookup {
        RowsSnapshotLookupPrimitive::PrimaryKey => {
            for value in values {
                store.visit_row_ids_by_pk_values(core::slice::from_ref(value), |row_id| {
                    added |= row_ids.insert(row_id);
                    true
                });
            }
        }
        RowsSnapshotLookupPrimitive::SingleColumnIndex { index_name } => {
            for value in values {
                store.visit_index_row_ids_by_value(index_name, value, |row_id| {
                    added |= row_ids.insert(row_id);
                    true
                });
            }
        }
        RowsSnapshotLookupPrimitive::ScanFallback => {
            store.visit_rows(|row| {
                if row
                    .get(column_index)
                    .map(|candidate| values.iter().any(|value| candidate.sql_eq(value)))
                    .unwrap_or(false)
                {
                    added |= row_ids.insert(row.id());
                }
                true
            });
        }
    }

    added
}

fn collect_join_values_by_row_ids(
    store: &RowStore,
    row_ids: &[u64],
    column_index: usize,
    join_values: &mut HashSet<Value>,
) {
    join_values.clear();
    store.visit_rows_by_ids(row_ids, |row| {
        if let Some(value) = row.get(column_index) {
            if !value.is_null() {
                join_values.insert(value.clone());
            }
        }
        true
    });
}

fn collect_join_values_by_row_ids_and_deltas(
    store: &RowStore,
    row_ids: &[u64],
    deltas: Option<&Vec<Delta<Row>>>,
    column_index: usize,
    join_values: &mut HashSet<Value>,
) {
    collect_join_values_by_row_ids(store, row_ids, column_index, join_values);
    let Some(deltas) = deltas else {
        return;
    };

    for delta in deltas {
        if let Some(value) = delta.data().get(column_index) {
            if !value.is_null() {
                join_values.insert(value.clone());
            }
        }
    }
}

fn resolve_root_row_id_for_output_row(
    cache: &TableCache,
    metadata: &RowsSnapshotPartialRefreshMetadata,
    row: &Rc<Row>,
) -> Option<u64> {
    let store = cache.get_table(&metadata.root_table)?;
    let pk_values: Vec<Value> = metadata
        .root_pk_output_indices
        .iter()
        .map(|&output_index| row.get(output_index).cloned())
        .collect::<Option<Vec<_>>>()?;
    if pk_values.iter().any(Value::is_null) {
        return None;
    }

    store
        .get_by_pk_values(&pk_values)
        .first()
        .map(|root_row| root_row.id())
}

fn build_partial_refresh_candidate_rows(
    cache: &TableCache,
    metadata: &RowsSnapshotPartialRefreshMetadata,
    rows: &[Rc<Row>],
) -> Option<Vec<PartialRefreshCandidateRow>> {
    let mut candidate_rows = Vec::with_capacity(rows.len());
    for row in rows {
        candidate_rows.push(PartialRefreshCandidateRow {
            row: row.clone(),
            root_row_id: resolve_root_row_id_for_output_row(cache, metadata, row)?,
        });
    }
    Some(candidate_rows)
}

#[derive(Default)]
struct GraphqlSubscribers {
    callbacks: Vec<(usize, Box<dyn Fn(&JsValue) + 'static>)>,
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
        F: Fn(&JsValue) + 'static,
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

    fn emit(&self, payload: &JsValue) {
        for (_, callback) in &self.callbacks {
            callback(payload);
        }
    }
}

fn build_graphql_response(
    cache: &TableCache,
    catalog: &cynos_gql::GraphqlCatalog,
    field: &cynos_gql::bind::BoundRootField,
    rows: &[Rc<Row>],
) -> Result<cynos_gql::GraphqlResponse, cynos_gql::GqlError> {
    let root_field = cynos_gql::execute::render_root_field_rows(cache, catalog, field, rows)?;
    Ok(cynos_gql::GraphqlResponse::new(
        cynos_gql::ResponseValue::object(alloc::vec![root_field]),
    ))
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

fn build_graphql_response_batched_refs(
    cache: &TableCache,
    catalog: &cynos_gql::GraphqlCatalog,
    field: &cynos_gql::bind::BoundRootField,
    plan: &cynos_gql::GraphqlBatchPlan,
    state: &mut cynos_gql::GraphqlBatchState,
    rows: &[&Rc<Row>],
) -> Result<cynos_gql::GraphqlResponse, cynos_gql::GqlError> {
    cynos_gql::batch_render::render_graphql_response_refs(cache, catalog, field, plan, state, rows)
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
    dirty_root_rows: &HashSet<u64>,
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
        dirty_root_rows: dirty_root_rows.clone(),
        stable_root_positions: false,
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
        dirty_root_rows: HashSet::new(),
        stable_root_positions: false,
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

fn output_deltas_preserve_root_positions(output_deltas: &[Delta<Row>]) -> bool {
    if output_deltas.is_empty() {
        return true;
    }

    let mut insert_ids = HashSet::new();
    let mut delete_ids = HashSet::new();
    for delta in output_deltas {
        if delta.is_insert() {
            insert_ids.insert(delta.data.id());
        } else {
            delete_ids.insert(delta.data.id());
        }
    }

    !insert_ids.is_empty()
        && insert_ids.len() == delete_ids.len()
        && insert_ids == delete_ids
        && output_deltas.len() == insert_ids.len().saturating_mul(2)
}

fn graphql_response_to_js_value(
    response: &cynos_gql::GraphqlResponse,
    encode_cache: &mut GraphqlJsEncodeCache,
) -> JsValue {
    gql_response_to_js_with_cache(response, encode_cache).unwrap_or(JsValue::NULL)
}

fn graphql_response_to_js_value_batched(
    response: &cynos_gql::GraphqlResponse,
    encode_cache: &mut GraphqlJsEncodeCache,
    root_list_cache: &mut GraphqlRootListJsCache,
    patch: Option<&cynos_gql::GraphqlRootListPatch>,
) -> JsValue {
    gql_response_to_js_with_root_list_patch(response, encode_cache, root_list_cache, patch)
        .unwrap_or(JsValue::NULL)
}

fn batch_response_changed(state: &cynos_gql::GraphqlBatchState) -> Option<bool> {
    match state.last_root_patch() {
        Some(cynos_gql::GraphqlRootListPatch::StablePositions(positions)) => {
            Some(!positions.is_empty())
        }
        Some(cynos_gql::GraphqlRootListPatch::Splice {
            removed_positions,
            inserted_positions,
            updated_positions,
        }) => Some(
            !removed_positions.is_empty()
                || !inserted_positions.is_empty()
                || !updated_positions.is_empty(),
        ),
        None => None,
    }
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
    /// Table ID -> table name bindings for dependency-aware invalidation.
    dependency_table_names: HashMap<TableId, String>,
    /// Current result set
    result: Vec<Rc<Row>>,
    /// Summary of the current result set for fast equality checks
    result_summary: QueryResultSummary,
    /// Optional runtime state for candidate-window partial requery.
    partial_refresh: Option<SnapshotPartialRefreshRuntime>,
    /// Optional runtime state for root-subset requery on no-blocking snapshot queries.
    root_subset_refresh: Option<RootSubsetRefreshRuntime>,
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
        dependency_table_bindings: Vec<(TableId, String)>,
    ) -> Self {
        let result_summary = QueryResultSummary::from_rows(&initial_result);
        Self::new_with_summary(
            compiled_plan,
            cache,
            initial_result,
            result_summary,
            dependency_table_bindings,
            None,
            None,
        )
    }

    #[doc(hidden)]
    pub(crate) fn new_with_summary(
        compiled_plan: CompiledPhysicalPlan,
        cache: Rc<RefCell<TableCache>>,
        initial_result: Vec<Rc<Row>>,
        result_summary: QueryResultSummary,
        dependency_table_bindings: Vec<(TableId, String)>,
        partial_refresh: Option<RowsSnapshotPartialRefreshState>,
        root_subset_refresh: Option<RowsSnapshotRootSubsetPlan>,
    ) -> Self {
        let (partial_refresh, root_subset_refresh) = {
            let cache_ref = cache.borrow();
            let partial_refresh =
                partial_refresh.map(|state| SnapshotPartialRefreshRuntime::new(&cache_ref, state));
            let root_subset_refresh = root_subset_refresh.and_then(|metadata| {
                RootSubsetRefreshRuntime::new(&cache_ref, metadata, &initial_result)
            });
            (partial_refresh, root_subset_refresh)
        };
        Self {
            compiled_plan,
            cache,
            dependency_table_names: dependency_table_bindings.into_iter().collect(),
            result: initial_result,
            result_summary,
            partial_refresh,
            root_subset_refresh,
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
    /// `changes` contains row IDs grouped by the table that changed.
    /// For simple single-table pipelines this enables a row-local fast path
    /// only when the underlying patch table changed; all other plans fall back
    /// to deterministic full-result comparison.
    pub fn on_change(&mut self, changes: &HashMap<TableId, HashSet<u64>>) {
        let _ = self.on_change_profiled_inner(changes, None);
    }

    #[allow(dead_code)]
    pub(crate) fn on_change_profiled(
        &mut self,
        changes: &HashMap<TableId, HashSet<u64>>,
    ) -> SnapshotQueryProfile {
        self.on_change_profiled_inner(changes, None)
    }

    pub(crate) fn on_change_with_deltas_profiled(
        &mut self,
        changes: &HashMap<TableId, HashSet<u64>>,
        delta_changes: &HashMap<TableId, Vec<Delta<Row>>>,
    ) -> SnapshotQueryProfile {
        self.on_change_profiled_inner(changes, Some(delta_changes))
    }

    fn on_change_profiled_inner(
        &mut self,
        changes: &HashMap<TableId, HashSet<u64>>,
        delta_changes: Option<&HashMap<TableId, Vec<Delta<Row>>>>,
    ) -> SnapshotQueryProfile {
        let started_at = now_ms();
        let mut profile = SnapshotQueryProfile::default();

        // Skip re-query if no subscribers - major optimization for unused observables
        if self.subscriptions.is_empty() {
            return profile;
        }

        if let Some(changed_ids) = self.patch_table_changed_ids(changes) {
            if let Some(changed_rows) =
                collect_changed_rows(&self.cache, &self.compiled_plan, changed_ids)
            {
                profile.reactive_patch_attempted = true;
                let patch_started_at = now_ms();
                match self
                    .compiled_plan
                    .apply_reactive_patch(&mut self.result, &changed_rows)
                {
                    Some(true) => {
                        profile.reactive_patch_hit = true;
                        profile.reactive_patch_ms = now_ms() - patch_started_at;
                        self.result_summary = QueryResultSummary::from_rows(&self.result);
                        let callback_started_at = now_ms();
                        for (_, callback) in &self.subscriptions {
                            callback(&self.result);
                        }
                        profile.callback_ms += now_ms() - callback_started_at;
                        profile.total_ms = now_ms() - started_at;
                        return profile;
                    }
                    Some(false) => {
                        profile.reactive_patch_ms = now_ms() - patch_started_at;
                        profile.total_ms = now_ms() - started_at;
                        return profile;
                    }
                    None => {}
                }
                profile.reactive_patch_ms = now_ms() - patch_started_at;
            }
        }

        if let Some(changed) =
            self.try_partial_refresh_profiled(changes, delta_changes, &mut profile)
        {
            if changed {
                let callback_started_at = now_ms();
                for (_, callback) in &self.subscriptions {
                    callback(&self.result);
                }
                profile.callback_ms += now_ms() - callback_started_at;
            }
            profile.total_ms = now_ms() - started_at;
            return profile;
        }

        if let Some(changed) =
            self.try_root_subset_refresh_profiled(changes, delta_changes, &mut profile)
        {
            if changed {
                let callback_started_at = now_ms();
                for (_, callback) in &self.subscriptions {
                    callback(&self.result);
                }
                profile.callback_ms += now_ms() - callback_started_at;
            }
            profile.total_ms = now_ms() - started_at;
            return profile;
        }

        // Re-execute the cached compiled plan (no optimization or lowering overhead)
        profile.refresh_mode = SnapshotRefreshMode::FullRequery;
        let full_requery_started_at = now_ms();
        let output = {
            let cache = self.cache.borrow();
            execute_compiled_physical_plan_with_summary(&cache, &self.compiled_plan)
        };
        profile.full_requery_ms = now_ms() - full_requery_started_at;

        match output {
            Ok(output) => {
                if self.apply_full_requery_output_profiled(
                    output.rows,
                    output.summary,
                    &mut profile.full_requery_compare_ms,
                ) {
                    // Notify all subscribers
                    let callback_started_at = now_ms();
                    for (_, callback) in &self.subscriptions {
                        callback(&self.result);
                    }
                    profile.callback_ms += now_ms() - callback_started_at;
                }
            }
            Err(_) => {
                // Query execution failed, keep old result
            }
        }

        profile.total_ms = now_ms() - started_at;
        profile
    }

    fn patch_table_changed_ids<'a>(
        &self,
        changes: &'a HashMap<TableId, HashSet<u64>>,
    ) -> Option<&'a HashSet<u64>> {
        let patch_table = self.compiled_plan.reactive_patch_table()?;
        if changes.len() != 1 {
            return None;
        }

        let (&table_id, changed_ids) = changes.iter().next()?;
        let table_name = self.dependency_table_names.get(&table_id)?;
        if table_name == patch_table {
            Some(changed_ids)
        } else {
            None
        }
    }

    fn apply_full_requery_output_profiled(
        &mut self,
        rows: Vec<Rc<Row>>,
        summary: QueryResultSummary,
        compare_ms: &mut f64,
    ) -> bool {
        if let Some(metadata) = self
            .partial_refresh
            .as_ref()
            .map(|partial_refresh| partial_refresh.metadata.clone())
        {
            let candidate_rows = {
                let cache = self.cache.borrow();
                build_partial_refresh_candidate_rows(&cache, &metadata, &rows)
            };
            let Some(candidate_rows) = candidate_rows else {
                return false;
            };
            let visible_rows = slice_partial_visible_rows(&candidate_rows, &metadata);
            let visible_summary = QueryResultSummary::from_rows(&visible_rows);
            let compare_started_at = now_ms();
            let changed = !query_results_equal(
                &self.result_summary,
                &visible_summary,
                &self.result,
                &visible_rows,
            );
            *compare_ms += now_ms() - compare_started_at;

            if let Some(partial_refresh) = &mut self.partial_refresh {
                partial_refresh.candidate_rows = candidate_rows;
            }
            self.result = visible_rows;
            self.result_summary = visible_summary;
            return changed;
        }

        let compare_started_at = now_ms();
        if query_results_equal(&self.result_summary, &summary, &self.result, &rows) {
            *compare_ms += now_ms() - compare_started_at;
            return false;
        }
        *compare_ms += now_ms() - compare_started_at;

        self.result = rows;
        self.result_summary = summary;
        true
    }

    fn try_partial_refresh_profiled(
        &mut self,
        changes: &HashMap<TableId, HashSet<u64>>,
        delta_changes: Option<&HashMap<TableId, Vec<Delta<Row>>>>,
        profile: &mut SnapshotQueryProfile,
    ) -> Option<bool> {
        profile.partial_refresh_attempted = self.partial_refresh.is_some();
        let collect_started_at = now_ms();
        let (affected_root_ids, affected_candidate_rows, partial_metadata, current_candidate_rows) = {
            let partial_refresh = self.partial_refresh.as_ref()?;
            let affected_root_ids = Self::collect_affected_root_row_ids(
                &self.cache,
                changes,
                delta_changes,
                &partial_refresh.metadata.dependency_graph,
            )?;
            if affected_root_ids.is_empty() {
                return Some(false);
            }

            let affected_candidate_rows = partial_refresh
                .candidate_rows
                .iter()
                .filter(|entry| affected_root_ids.contains(&entry.root_row_id))
                .count();
            (
                affected_root_ids,
                affected_candidate_rows,
                partial_refresh.metadata.clone(),
                partial_refresh.candidate_rows.clone(),
            )
        };
        profile.partial_refresh_collect_ms += now_ms() - collect_started_at;
        if affected_candidate_rows > partial_metadata.overscan {
            return None;
        }

        let requery_started_at = now_ms();
        let recomputed_rows = {
            let cache = self.cache.borrow();
            let rows = execute_compiled_physical_plan_on_table_subset(
                &cache,
                &self.compiled_plan,
                &partial_metadata.root_table,
                &affected_root_ids,
            )
            .ok()?;
            build_partial_refresh_candidate_rows(&cache, &partial_metadata, &rows)?
        };
        profile.partial_refresh_requery_ms += now_ms() - requery_started_at;

        let apply_started_at = now_ms();
        let unaffected = current_candidate_rows
            .iter()
            .filter(|entry| !affected_root_ids.contains(&entry.root_row_id))
            .cloned();
        let mut merged_rows: Vec<PartialRefreshCandidateRow> =
            unaffected.chain(recomputed_rows.into_iter()).collect();
        merged_rows.sort_by(|left, right| {
            compare_partial_refresh_rows(left, right, &partial_metadata.order_keys)
        });
        merged_rows.truncate(partial_metadata.candidate_limit);
        let min_shadow_window = partial_metadata
            .visible_offset
            .saturating_add(partial_metadata.visible_limit)
            .saturating_add(partial_metadata.overscan);
        if merged_rows.len() < min_shadow_window
            && current_candidate_rows.len() >= min_shadow_window
        {
            return None;
        }

        let visible_rows = slice_partial_visible_rows(&merged_rows, &partial_metadata);
        let visible_summary = QueryResultSummary::from_rows(&visible_rows);
        let compare_started_at = now_ms();
        let changed = !query_results_equal(
            &self.result_summary,
            &visible_summary,
            &self.result,
            &visible_rows,
        );
        profile.partial_refresh_compare_ms += now_ms() - compare_started_at;

        if let Some(partial_refresh) = &mut self.partial_refresh {
            partial_refresh.candidate_rows = merged_rows;
        }
        self.result = visible_rows;
        self.result_summary = visible_summary;
        profile.partial_refresh_hit = true;
        profile.refresh_mode = SnapshotRefreshMode::CandidateWindowPartialRefresh;
        profile.partial_refresh_apply_ms += now_ms() - apply_started_at;
        Some(changed)
    }

    fn try_root_subset_refresh_profiled(
        &mut self,
        changes: &HashMap<TableId, HashSet<u64>>,
        delta_changes: Option<&HashMap<TableId, Vec<Delta<Row>>>>,
        profile: &mut SnapshotQueryProfile,
    ) -> Option<bool> {
        profile.root_subset_attempted = self.root_subset_refresh.is_some();
        let collect_started_at = now_ms();
        let (affected_root_ids, affected_root_keys) = {
            let root_subset_refresh = self.root_subset_refresh.as_mut()?;
            let affected_root_ids = Self::collect_affected_root_row_ids(
                &self.cache,
                changes,
                delta_changes,
                &root_subset_refresh.metadata.dependency_graph,
            )?;
            if affected_root_ids.is_empty() {
                return Some(false);
            }

            let cache = self.cache.borrow();
            let refresh_existing_root_keys =
                changes.contains_key(&root_subset_refresh.metadata.dependency_graph.root_table_id);
            let affected_root_keys = root_subset_refresh.collect_affected_root_keys(
                &cache,
                &affected_root_ids,
                refresh_existing_root_keys,
            )?;
            (affected_root_ids, affected_root_keys)
        };
        profile.root_subset_collect_ms += now_ms() - collect_started_at;

        if affected_root_keys.is_empty() {
            return Some(false);
        }

        let root_table = self
            .root_subset_refresh
            .as_ref()
            .map(|runtime| runtime.metadata.root_table.clone())?;
        let requery_started_at = now_ms();
        let rows = {
            let cache = self.cache.borrow();
            let subset_plan = self
                .root_subset_refresh
                .as_ref()
                .map(|runtime| runtime.select_compiled_plan(&cache, &affected_root_ids))?;
            execute_compiled_physical_plan_on_table_subset(
                &cache,
                subset_plan,
                &root_table,
                &affected_root_ids,
            )
            .ok()?
        };
        profile.root_subset_requery_ms += now_ms() - requery_started_at;

        let apply_started_at = now_ms();
        let rows = {
            let root_subset_refresh = self.root_subset_refresh.as_mut()?;
            root_subset_refresh.apply_subset_rows(&affected_root_keys, rows)?
        };
        let summary = QueryResultSummary::from_rows(&rows);
        let compare_started_at = now_ms();
        let changed = !query_results_equal(&self.result_summary, &summary, &self.result, &rows);
        profile.root_subset_compare_ms += now_ms() - compare_started_at;
        self.result = rows;
        self.result_summary = summary;
        profile.root_subset_apply_ms += now_ms() - apply_started_at;
        profile.root_subset_hit = true;
        profile.refresh_mode = SnapshotRefreshMode::RootSubsetRefresh;
        Some(changed)
    }

    fn collect_affected_root_row_ids(
        cache_ref: &Rc<RefCell<TableCache>>,
        changes: &HashMap<TableId, HashSet<u64>>,
        delta_changes: Option<&HashMap<TableId, Vec<Delta<Row>>>>,
        dependency_graph: &RowsSnapshotDependencyGraph,
    ) -> Option<HashSet<u64>> {
        let cache = cache_ref.borrow();
        let mut dirty_rows_by_table: HashMap<TableId, HashSet<u64>> = HashMap::new();
        for (table_id, row_ids) in changes {
            if dependency_graph.table_name(*table_id).is_none() {
                continue;
            }
            dirty_rows_by_table
                .entry(*table_id)
                .or_insert_with(HashSet::new)
                .extend(row_ids.iter().copied());
        }

        let mut queue: Vec<TableId> = dirty_rows_by_table.keys().copied().collect();
        let mut queued_tables: HashSet<TableId> = queue.iter().copied().collect();
        let mut dirty_row_ids = Vec::new();
        let mut join_values = HashSet::new();

        while let Some(table_id) = queue.pop() {
            queued_tables.remove(&table_id);
            let dirty_row_set = dirty_rows_by_table.get(&table_id)?;
            if dirty_row_set.is_empty() {
                continue;
            }

            dirty_row_ids.clear();
            dirty_row_ids.extend(dirty_row_set.iter().copied());

            let Some(table_name) = dependency_graph.table_name(table_id) else {
                continue;
            };
            let store = cache.get_table(table_name)?;
            let table_deltas = delta_changes.and_then(|changes| changes.get(&table_id));
            if table_id != dependency_graph.root_table_id
                && store.count_existing_rows_by_ids(&dirty_row_ids) != dirty_row_ids.len()
                && table_deltas.is_none_or(Vec::is_empty)
            {
                return None;
            }

            for edge in dependency_graph.edges_from(table_id) {
                collect_join_values_by_row_ids_and_deltas(
                    store,
                    &dirty_row_ids,
                    table_deltas,
                    edge.source_column_index,
                    &mut join_values,
                );
                if join_values.is_empty() {
                    continue;
                }

                let target_store = cache.get_table(&edge.target_table)?;
                let target_entry = dirty_rows_by_table
                    .entry(edge.target_table_id)
                    .or_insert_with(HashSet::new);
                let added = insert_row_ids_by_column_values(
                    target_store,
                    edge.target_column_index,
                    &edge.lookup,
                    &join_values,
                    target_entry,
                );
                if added && !queued_tables.contains(&edge.target_table_id) {
                    queued_tables.insert(edge.target_table_id);
                    queue.push(edge.target_table_id);
                }
            }
        }

        Some(
            dirty_rows_by_table
                .remove(&dependency_graph.root_table_id)
                .unwrap_or_default(),
        )
    }
}

pub struct GraphqlSubscriptionObservable {
    compiled_plan: CompiledPhysicalPlan,
    cache: Rc<RefCell<TableCache>>,
    catalog: cynos_gql::GraphqlCatalog,
    field: cynos_gql::bind::BoundRootField,
    batch_plan: Option<cynos_gql::GraphqlBatchPlan>,
    batch_state: cynos_gql::GraphqlBatchState,
    dependency_table_names: HashMap<TableId, String>,
    root_table_ids: HashSet<TableId>,
    root_rows: Vec<Rc<Row>>,
    root_summary: QueryResultSummary,
    root_subset_refresh: Option<RootSubsetRefreshRuntime>,
    response: Option<cynos_gql::GraphqlResponse>,
    response_js: Option<JsValue>,
    response_encode_cache: GraphqlJsEncodeCache,
    response_root_list_js_cache: GraphqlRootListJsCache,
    response_dirty: bool,
    subscribers: GraphqlSubscribers,
}

impl GraphqlSubscriptionObservable {
    pub(crate) fn new(
        compiled_plan: CompiledPhysicalPlan,
        cache: Rc<RefCell<TableCache>>,
        catalog: cynos_gql::GraphqlCatalog,
        field: cynos_gql::bind::BoundRootField,
        dependency_table_bindings: Vec<(TableId, String)>,
        root_subset_refresh: Option<RowsSnapshotRootSubsetPlan>,
        root_table_ids: HashSet<TableId>,
        initial_rows: Vec<Rc<Row>>,
        initial_summary: QueryResultSummary,
    ) -> Self {
        let root_subset_refresh = {
            let cache_ref = cache.borrow();
            root_subset_refresh
                .and_then(|plan| RootSubsetRefreshRuntime::new(&cache_ref, plan, &initial_rows))
        };
        Self {
            compiled_plan,
            cache,
            batch_plan: cynos_gql::compile_batch_plan(&catalog, &field)
                .ok()
                .filter(|plan| plan.has_relations()),
            batch_state: cynos_gql::GraphqlBatchState::default(),
            dependency_table_names: dependency_table_bindings.into_iter().collect(),
            catalog,
            field,
            root_table_ids,
            root_rows: initial_rows,
            root_summary: initial_summary,
            root_subset_refresh,
            response: None,
            response_js: None,
            response_encode_cache: GraphqlJsEncodeCache::default(),
            response_root_list_js_cache: GraphqlRootListJsCache::default(),
            response_dirty: true,
            subscribers: GraphqlSubscribers::default(),
        }
    }

    pub fn attach_keepalive(&mut self) -> usize {
        self.subscribers.add_keepalive()
    }

    pub fn response_js_value(&mut self) -> JsValue {
        if self.response.is_some() && !self.response_dirty {
            if let Some(payload) = &self.response_js {
                return payload.clone();
            }

            let payload = match self.batch_plan.as_ref() {
                Some(_) => graphql_response_to_js_value_batched(
                    self.response.as_ref().unwrap(),
                    &mut self.response_encode_cache,
                    &mut self.response_root_list_js_cache,
                    self.batch_state.last_root_patch(),
                ),
                None => graphql_response_to_js_value(
                    self.response.as_ref().unwrap(),
                    &mut self.response_encode_cache,
                ),
            };
            self.response_js = Some(payload.clone());
            return payload;
        }

        if self.subscribers.callback_count() == 0 {
            return self.render_response_js_value();
        }

        if self.current_response().is_none() {
            return JsValue::NULL;
        }

        if let Some(payload) = &self.response_js {
            payload.clone()
        } else {
            let payload = match self.batch_plan.as_ref() {
                Some(_) => graphql_response_to_js_value_batched(
                    self.response.as_ref().unwrap(),
                    &mut self.response_encode_cache,
                    &mut self.response_root_list_js_cache,
                    self.batch_state.last_root_patch(),
                ),
                None => graphql_response_to_js_value(
                    self.response.as_ref().unwrap(),
                    &mut self.response_encode_cache,
                ),
            };
            self.response_js = Some(payload.clone());
            payload
        }
    }

    pub fn subscribe<F: Fn(&JsValue) + 'static>(&mut self, callback: F) -> usize {
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
        self.on_change_inner(
            changes,
            None,
            #[cfg(feature = "benchmark")]
            None,
        );
    }

    #[cfg(feature = "benchmark")]
    pub(crate) fn on_change_profiled(
        &mut self,
        changes: &HashMap<TableId, HashSet<u64>>,
    ) -> GraphqlSnapshotQueryProfile {
        let mut profile = GraphqlSnapshotQueryProfile::default();
        self.on_change_inner(changes, None, Some(&mut profile));
        profile
    }

    pub(crate) fn on_change_with_deltas(
        &mut self,
        changes: &HashMap<TableId, HashSet<u64>>,
        delta_changes: &HashMap<TableId, Vec<Delta<Row>>>,
    ) {
        self.on_change_inner(
            changes,
            Some(delta_changes),
            #[cfg(feature = "benchmark")]
            None,
        );
    }

    #[cfg(feature = "benchmark")]
    pub(crate) fn on_change_with_deltas_profiled(
        &mut self,
        changes: &HashMap<TableId, HashSet<u64>>,
        delta_changes: &HashMap<TableId, Vec<Delta<Row>>>,
    ) -> GraphqlSnapshotQueryProfile {
        let mut profile = GraphqlSnapshotQueryProfile::default();
        self.on_change_inner(changes, Some(delta_changes), Some(&mut profile));
        profile
    }

    fn on_change_inner(
        &mut self,
        changes: &HashMap<TableId, HashSet<u64>>,
        delta_changes: Option<&HashMap<TableId, Vec<Delta<Row>>>>,
        #[cfg(feature = "benchmark")] mut profile: Option<&mut GraphqlSnapshotQueryProfile>,
    ) {
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

        let should_refresh_roots =
            !root_changed_ids.is_empty() || self.root_subset_refresh.is_some();
        let mut root_changed = false;
        let mut dirty_root_rows = HashSet::new();
        if should_refresh_roots {
            #[cfg(feature = "benchmark")]
            let refresh_started_at = now_ms();
            let refresh_outcome =
                match self.refresh_root_rows(changes, delta_changes, &root_changed_ids) {
                    Some(outcome) => outcome,
                    None => return,
                };
            root_changed = refresh_outcome.changed;
            dirty_root_rows = refresh_outcome.dirty_root_rows;
            #[cfg(feature = "benchmark")]
            if let Some(profile) = profile.as_deref_mut() {
                profile.root_refresh_ms += now_ms() - refresh_started_at;
            }
        }

        if !root_changed && !saw_nested_change {
            return;
        }

        if let Some(plan) = self.batch_plan.as_ref() {
            #[cfg(feature = "benchmark")]
            let invalidation_started_at = now_ms();
            match build_snapshot_batch_invalidation(
                &self.dependency_table_names,
                changes,
                root_changed,
                &dirty_root_rows,
            ) {
                Ok(invalidation) => self.batch_state.apply_invalidation(plan, &invalidation),
                Err(()) => {
                    self.batch_state = cynos_gql::GraphqlBatchState::default();
                }
            }
            #[cfg(feature = "benchmark")]
            if let Some(profile) = profile.as_deref_mut() {
                profile.batch_invalidation_ms += now_ms() - invalidation_started_at;
            }
        }
        self.response_dirty = true;
        if self.subscribers.callback_count() == 0 {
            return;
        }

        #[cfg(feature = "benchmark")]
        let render_started_at = now_ms();
        if let Some(changed) = self.materialize_response_if_dirty() {
            #[cfg(feature = "benchmark")]
            if let Some(profile) = profile.as_deref_mut() {
                profile.render_ms += now_ms() - render_started_at;
            }
            if changed {
                if self.response.is_some() {
                    #[cfg(feature = "benchmark")]
                    let encode_started_at = now_ms();
                    let payload = self.response_js_value();
                    #[cfg(feature = "benchmark")]
                    if let Some(profile) = profile.as_deref_mut() {
                        profile.encode_ms += now_ms() - encode_started_at;
                    }
                    #[cfg(feature = "benchmark")]
                    let emit_started_at = now_ms();
                    self.subscribers.emit(&payload);
                    #[cfg(feature = "benchmark")]
                    if let Some(profile) = profile.as_deref_mut() {
                        profile.emit_ms += now_ms() - emit_started_at;
                    }
                }
            }
        } else {
            #[cfg(feature = "benchmark")]
            if let Some(profile) = profile.as_deref_mut() {
                profile.render_ms += now_ms() - render_started_at;
            }
        }
    }

    fn refresh_root_rows(
        &mut self,
        changes: &HashMap<TableId, HashSet<u64>>,
        delta_changes: Option<&HashMap<TableId, Vec<Delta<Row>>>>,
        changed_ids: &HashSet<u64>,
    ) -> Option<SnapshotRootRefreshOutcome> {
        let only_root_table_changes = !changes.is_empty()
            && changes
                .keys()
                .all(|table_id| self.root_table_ids.contains(table_id));
        if !changed_ids.is_empty() && only_root_table_changes {
            if let Some(changed_rows) =
                collect_changed_rows(&self.cache, &self.compiled_plan, changed_ids)
            {
                match self
                    .compiled_plan
                    .apply_reactive_patch(&mut self.root_rows, &changed_rows)
                {
                    Some(true) => {
                        self.root_summary = QueryResultSummary::from_rows(&self.root_rows);
                        return Some(SnapshotRootRefreshOutcome {
                            changed: true,
                            dirty_root_rows: changed_ids.clone(),
                        });
                    }
                    Some(false) => {
                        return Some(SnapshotRootRefreshOutcome {
                            changed: false,
                            dirty_root_rows: HashSet::new(),
                        });
                    }
                    None => {}
                }
            }
        }

        if let Some(outcome) = self.try_root_subset_refresh(changes, delta_changes) {
            return Some(outcome);
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
            return Some(SnapshotRootRefreshOutcome {
                changed: false,
                dirty_root_rows: HashSet::new(),
            });
        }

        self.root_rows = output.rows;
        self.root_summary = output.summary;
        Some(SnapshotRootRefreshOutcome {
            changed: true,
            dirty_root_rows: changed_ids.clone(),
        })
    }

    fn try_root_subset_refresh(
        &mut self,
        changes: &HashMap<TableId, HashSet<u64>>,
        delta_changes: Option<&HashMap<TableId, Vec<Delta<Row>>>>,
    ) -> Option<SnapshotRootRefreshOutcome> {
        let (affected_root_ids, affected_root_keys) = {
            let root_subset_refresh = self.root_subset_refresh.as_mut()?;
            let affected_root_ids = ReQueryObservable::collect_affected_root_row_ids(
                &self.cache,
                changes,
                delta_changes,
                &root_subset_refresh.metadata.dependency_graph,
            )?;
            if affected_root_ids.is_empty() {
                return Some(SnapshotRootRefreshOutcome {
                    changed: false,
                    dirty_root_rows: HashSet::new(),
                });
            }

            let cache = self.cache.borrow();
            let refresh_existing_root_keys =
                changes.contains_key(&root_subset_refresh.metadata.dependency_graph.root_table_id);
            let affected_root_keys = root_subset_refresh.collect_affected_root_keys(
                &cache,
                &affected_root_ids,
                refresh_existing_root_keys,
            )?;
            (affected_root_ids, affected_root_keys)
        };

        if affected_root_keys.is_empty() {
            return Some(SnapshotRootRefreshOutcome {
                changed: false,
                dirty_root_rows: HashSet::new(),
            });
        }

        let root_table = self
            .root_subset_refresh
            .as_ref()
            .map(|runtime| runtime.metadata.root_table.clone())?;
        let rows = {
            let cache = self.cache.borrow();
            let subset_plan = self
                .root_subset_refresh
                .as_ref()
                .map(|runtime| runtime.select_compiled_plan(&cache, &affected_root_ids))?;
            execute_compiled_physical_plan_on_table_subset(
                &cache,
                subset_plan,
                &root_table,
                &affected_root_ids,
            )
            .ok()?
        };

        let rows = {
            let root_subset_refresh = self.root_subset_refresh.as_mut()?;
            root_subset_refresh.apply_subset_rows(&affected_root_keys, rows)?
        };
        let summary = QueryResultSummary::from_rows(&rows);
        let changed = !query_results_equal(&self.root_summary, &summary, &self.root_rows, &rows);
        self.root_rows = rows;
        self.root_summary = summary;
        Some(SnapshotRootRefreshOutcome {
            changed,
            dirty_root_rows: affected_root_ids,
        })
    }

    fn materialize_response_if_dirty(&mut self) -> Option<bool> {
        if !self.response_dirty && self.response.is_some() {
            return Some(false);
        }

        let cache = self.cache.borrow();
        let response = match self.batch_plan.as_ref() {
            Some(plan) => build_graphql_response_batched(
                &cache,
                &self.catalog,
                &self.field,
                plan,
                &mut self.batch_state,
                &self.root_rows,
            )
            .ok()?,
            None => {
                build_graphql_response(&cache, &self.catalog, &self.field, &self.root_rows).ok()?
            }
        };
        let changed = match self.batch_plan.as_ref() {
            Some(_) => batch_response_changed(&self.batch_state).unwrap_or_else(|| {
                self.response
                    .as_ref()
                    .map_or(true, |current| *current != response)
            }),
            None => self
                .response
                .as_ref()
                .map_or(true, |current| *current != response),
        };
        if changed {
            self.response = Some(response);
            self.response_js = None;
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
        let response = match self.batch_plan.as_ref() {
            Some(plan) => build_graphql_response_batched(
                &cache,
                &self.catalog,
                &self.field,
                plan,
                &mut self.batch_state,
                &self.root_rows,
            ),
            None => build_graphql_response(&cache, &self.catalog, &self.field, &self.root_rows),
        };
        match response {
            Ok(response) => match self.batch_plan.as_ref() {
                Some(_) => graphql_response_to_js_value_batched(
                    &response,
                    &mut self.response_encode_cache,
                    &mut self.response_root_list_js_cache,
                    self.batch_state.last_root_patch(),
                ),
                None => graphql_response_to_js_value(&response, &mut self.response_encode_cache),
            },
            Err(_) => JsValue::NULL,
        }
    }
}

pub struct GraphqlDeltaObservable {
    view: MaterializedView,
    cache: Rc<RefCell<TableCache>>,
    catalog: cynos_gql::GraphqlCatalog,
    field: cynos_gql::bind::BoundRootField,
    batch_plan: Option<cynos_gql::GraphqlBatchPlan>,
    batch_state: cynos_gql::GraphqlBatchState,
    dependency_table_names: HashMap<TableId, String>,
    has_nested_relations: bool,
    response: Option<cynos_gql::GraphqlResponse>,
    response_js: Option<JsValue>,
    response_encode_cache: GraphqlJsEncodeCache,
    response_root_list_js_cache: GraphqlRootListJsCache,
    response_dirty: bool,
    subscribers: GraphqlSubscribers,
}

impl GraphqlDeltaObservable {
    pub fn new(
        dataflow: DataflowNode,
        cache: Rc<RefCell<TableCache>>,
        catalog: cynos_gql::GraphqlCatalog,
        field: cynos_gql::bind::BoundRootField,
        dependency_table_bindings: Vec<(TableId, String)>,
        initial_rows: Vec<Row>,
    ) -> Self {
        Self {
            view: MaterializedView::with_initial(dataflow, initial_rows),
            cache,
            batch_plan: cynos_gql::compile_batch_plan(&catalog, &field)
                .ok()
                .filter(|plan| plan.has_relations()),
            batch_state: cynos_gql::GraphqlBatchState::default(),
            dependency_table_names: dependency_table_bindings.into_iter().collect(),
            catalog,
            has_nested_relations: root_field_has_relations(&field),
            field,
            response: None,
            response_js: None,
            response_encode_cache: GraphqlJsEncodeCache::default(),
            response_root_list_js_cache: GraphqlRootListJsCache::default(),
            response_dirty: true,
            subscribers: GraphqlSubscribers::default(),
        }
    }

    pub fn new_with_sources(
        dataflow: DataflowNode,
        cache: Rc<RefCell<TableCache>>,
        catalog: cynos_gql::GraphqlCatalog,
        field: cynos_gql::bind::BoundRootField,
        dependency_table_bindings: Vec<(TableId, String)>,
        initial_rows: Vec<Row>,
        source_rows: &HashMap<TableId, Vec<Row>>,
    ) -> Self {
        Self {
            view: MaterializedView::with_sources(dataflow, initial_rows, source_rows),
            cache,
            batch_plan: cynos_gql::compile_batch_plan(&catalog, &field)
                .ok()
                .filter(|plan| plan.has_relations()),
            batch_state: cynos_gql::GraphqlBatchState::default(),
            dependency_table_names: dependency_table_bindings.into_iter().collect(),
            catalog,
            has_nested_relations: root_field_has_relations(&field),
            field,
            response: None,
            response_js: None,
            response_encode_cache: GraphqlJsEncodeCache::default(),
            response_root_list_js_cache: GraphqlRootListJsCache::default(),
            response_dirty: true,
            subscribers: GraphqlSubscribers::default(),
        }
    }

    pub fn new_with_compiled_loader<F>(
        dataflow: DataflowNode,
        compiled_ivm_plan: cynos_incremental::CompiledIvmPlan,
        compiled_bootstrap_plan: cynos_incremental::CompiledBootstrapPlan,
        cache: Rc<RefCell<TableCache>>,
        catalog: cynos_gql::GraphqlCatalog,
        field: cynos_gql::bind::BoundRootField,
        dependency_table_bindings: Vec<(TableId, String)>,
        initial_rows: Vec<Rc<Row>>,
        load_source_rows: F,
    ) -> Self
    where
        F: FnMut(TableId) -> Vec<Rc<Row>>,
    {
        Self {
            view: MaterializedView::with_compiled_loader_and_bootstrap(
                dataflow,
                compiled_ivm_plan,
                compiled_bootstrap_plan,
                initial_rows,
                load_source_rows,
            ),
            cache,
            batch_plan: cynos_gql::compile_batch_plan(&catalog, &field)
                .ok()
                .filter(|plan| plan.has_relations()),
            batch_state: cynos_gql::GraphqlBatchState::default(),
            dependency_table_names: dependency_table_bindings.into_iter().collect(),
            catalog,
            has_nested_relations: root_field_has_relations(&field),
            field,
            response: None,
            response_js: None,
            response_encode_cache: GraphqlJsEncodeCache::default(),
            response_root_list_js_cache: GraphqlRootListJsCache::default(),
            response_dirty: true,
            subscribers: GraphqlSubscribers::default(),
        }
    }

    pub fn new_with_compiled_source_visitor<F>(
        dataflow: DataflowNode,
        compiled_ivm_plan: cynos_incremental::CompiledIvmPlan,
        compiled_bootstrap_plan: cynos_incremental::CompiledBootstrapPlan,
        cache: Rc<RefCell<TableCache>>,
        catalog: cynos_gql::GraphqlCatalog,
        field: cynos_gql::bind::BoundRootField,
        dependency_table_bindings: Vec<(TableId, String)>,
        initial_rows: Vec<Rc<Row>>,
        visit_source_rows: F,
    ) -> Self
    where
        F: FnMut(TableId, usize, &mut dyn FnMut(Rc<Row>)),
    {
        Self {
            view: MaterializedView::with_compiled_source_visitor_and_bootstrap(
                dataflow,
                compiled_ivm_plan,
                compiled_bootstrap_plan,
                initial_rows,
                visit_source_rows,
            ),
            cache,
            batch_plan: cynos_gql::compile_batch_plan(&catalog, &field)
                .ok()
                .filter(|plan| plan.has_relations()),
            batch_state: cynos_gql::GraphqlBatchState::default(),
            dependency_table_names: dependency_table_bindings.into_iter().collect(),
            catalog,
            has_nested_relations: root_field_has_relations(&field),
            field,
            response: None,
            response_js: None,
            response_encode_cache: GraphqlJsEncodeCache::default(),
            response_root_list_js_cache: GraphqlRootListJsCache::default(),
            response_dirty: true,
            subscribers: GraphqlSubscribers::default(),
        }
    }

    pub fn attach_keepalive(&mut self) -> usize {
        self.subscribers.add_keepalive()
    }

    pub fn response_js_value(&mut self) -> JsValue {
        if self.response.is_some() && !self.response_dirty {
            if let Some(payload) = &self.response_js {
                return payload.clone();
            }

            let payload = match self.batch_plan.as_ref() {
                Some(_) => graphql_response_to_js_value_batched(
                    self.response.as_ref().unwrap(),
                    &mut self.response_encode_cache,
                    &mut self.response_root_list_js_cache,
                    self.batch_state.last_root_patch(),
                ),
                None => graphql_response_to_js_value(
                    self.response.as_ref().unwrap(),
                    &mut self.response_encode_cache,
                ),
            };
            self.response_js = Some(payload.clone());
            return payload;
        }

        if self.subscribers.callback_count() == 0 {
            return self.render_response_js_value();
        }

        if self.current_response().is_none() {
            return JsValue::NULL;
        }

        if let Some(payload) = &self.response_js {
            payload.clone()
        } else {
            let payload = match self.batch_plan.as_ref() {
                Some(_) => graphql_response_to_js_value_batched(
                    self.response.as_ref().unwrap(),
                    &mut self.response_encode_cache,
                    &mut self.response_root_list_js_cache,
                    self.batch_state.last_root_patch(),
                ),
                None => graphql_response_to_js_value(
                    self.response.as_ref().unwrap(),
                    &mut self.response_encode_cache,
                ),
            };
            self.response_js = Some(payload.clone());
            payload
        }
    }

    pub fn dependencies(&self) -> &[TableId] {
        self.view.dependencies()
    }

    pub fn subscribe<F: Fn(&JsValue) + 'static>(&mut self, callback: F) -> usize {
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
        self.on_table_change_inner(
            table_id,
            deltas,
            #[cfg(feature = "benchmark")]
            None,
        );
    }

    #[cfg(feature = "benchmark")]
    pub(crate) fn on_table_change_profiled(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
    ) -> GraphqlDeltaProfile {
        let mut profile = GraphqlDeltaProfile::default();
        self.on_table_change_inner(table_id, deltas, Some(&mut profile));
        profile
    }

    fn on_table_change_inner(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
        #[cfg(feature = "benchmark")] mut profile: Option<&mut GraphqlDeltaProfile>,
    ) {
        if self.subscribers.total_count() == 0 {
            return;
        }

        let batch_invalidation = self.batch_plan.as_ref().map(|plan| {
            build_delta_batch_invalidation(
                plan,
                &self.dependency_table_names,
                table_id,
                &deltas,
                false,
            )
        });
        #[cfg(feature = "benchmark")]
        let view_started_at = now_ms();
        let output_deltas = self.view.on_table_change(table_id, deltas);
        #[cfg(feature = "benchmark")]
        if let Some(profile) = profile.as_deref_mut() {
            profile.view_update_ms += now_ms() - view_started_at;
        }
        if output_deltas.is_empty() && !self.has_nested_relations {
            return;
        }

        if let Some(plan) = self.batch_plan.as_ref() {
            #[cfg(feature = "benchmark")]
            let invalidation_started_at = now_ms();
            match batch_invalidation {
                Some(Ok(mut invalidation)) => {
                    invalidation.root_changed = !output_deltas.is_empty();
                    if invalidation.root_changed {
                        invalidation.dirty_root_rows =
                            output_deltas.iter().map(|delta| delta.data.id()).collect();
                        invalidation.stable_root_positions =
                            output_deltas_preserve_root_positions(&output_deltas);
                    }
                    self.batch_state.apply_invalidation(plan, &invalidation);
                }
                Some(Err(())) => {
                    self.batch_state = cynos_gql::GraphqlBatchState::default();
                }
                None => {}
            }
            #[cfg(feature = "benchmark")]
            if let Some(profile) = profile.as_deref_mut() {
                profile.invalidation_ms += now_ms() - invalidation_started_at;
            }
        }
        self.response_dirty = true;
        if self.subscribers.callback_count() == 0 {
            return;
        }

        #[cfg(feature = "benchmark")]
        let render_started_at = now_ms();
        if let Some(changed) = self.materialize_response_if_dirty() {
            #[cfg(feature = "benchmark")]
            if let Some(profile) = profile.as_deref_mut() {
                profile.render_ms += now_ms() - render_started_at;
            }
            if changed {
                if self.response.is_some() {
                    #[cfg(feature = "benchmark")]
                    let encode_started_at = now_ms();
                    let payload = self.response_js_value();
                    #[cfg(feature = "benchmark")]
                    if let Some(profile) = profile.as_deref_mut() {
                        profile.encode_ms += now_ms() - encode_started_at;
                    }
                    #[cfg(feature = "benchmark")]
                    let emit_started_at = now_ms();
                    self.subscribers.emit(&payload);
                    #[cfg(feature = "benchmark")]
                    if let Some(profile) = profile.as_deref_mut() {
                        profile.emit_ms += now_ms() - emit_started_at;
                    }
                }
            }
        } else {
            #[cfg(feature = "benchmark")]
            if let Some(profile) = profile.as_deref_mut() {
                profile.render_ms += now_ms() - render_started_at;
            }
        }
    }

    fn materialize_response_if_dirty(&mut self) -> Option<bool> {
        if !self.response_dirty && self.response.is_some() {
            return Some(false);
        }

        let cache = self.cache.borrow();
        let response = match self.batch_plan.as_ref() {
            Some(plan) => {
                let rows = self.view.result_row_refs().collect::<Vec<_>>();
                build_graphql_response_batched_refs(
                    &cache,
                    &self.catalog,
                    &self.field,
                    plan,
                    &mut self.batch_state,
                    &rows,
                )
                .ok()?
            }
            None => {
                let rows = self.view.result_rc();
                build_graphql_response(&cache, &self.catalog, &self.field, &rows).ok()?
            }
        };
        let changed = match self.batch_plan.as_ref() {
            Some(_) => batch_response_changed(&self.batch_state).unwrap_or_else(|| {
                self.response
                    .as_ref()
                    .map_or(true, |current| *current != response)
            }),
            None => self
                .response
                .as_ref()
                .map_or(true, |current| *current != response),
        };
        if changed {
            self.response = Some(response);
            self.response_js = None;
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
        let response = match self.batch_plan.as_ref() {
            Some(plan) => {
                let rows = self.view.result_row_refs().collect::<Vec<_>>();
                build_graphql_response_batched_refs(
                    &cache,
                    &self.catalog,
                    &self.field,
                    plan,
                    &mut self.batch_state,
                    &rows,
                )
            }
            None => {
                let rows = self.view.result_rc();
                build_graphql_response(&cache, &self.catalog, &self.field, &rows)
            }
        };
        match response {
            Ok(response) => match self.batch_plan.as_ref() {
                Some(_) => graphql_response_to_js_value_batched(
                    &response,
                    &mut self.response_encode_cache,
                    &mut self.response_root_list_js_cache,
                    self.batch_state.last_root_patch(),
                ),
                None => graphql_response_to_js_value(&response, &mut self.response_encode_cache),
            },
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
        let bridge_profiler = self.bridge_profiler.clone();
        let added_key = JsValue::from_str("added");
        let removed_key = JsValue::from_str("removed");
        let materialization_cache = self.materialization_cache.clone();

        let sub_id = self
            .inner
            .borrow_mut()
            .subscribe_trace_batches(move |batch| {
                let total_started_at = now_ms();
                let (delta_obj, serialize_added_ms, serialize_removed_ms, assemble_delta_ms) =
                    trace_delta_batch_to_js_delta(
                        batch,
                        &materializer,
                        &mut materialization_cache.borrow_mut(),
                        &added_key,
                        &removed_key,
                    );
                let added_row_count = batch.insert_count();
                let removed_row_count = batch.delete_count();

                let callback_started_at = now_ms();
                callback.call1(&JsValue::NULL, &delta_obj).ok();
                let callback_call_ms = now_ms() - callback_started_at;

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
            inner.result_row_refs(),
            inner.len(),
            &mut self.materialization_cache.borrow_mut(),
        )
    }

    /// Returns the current result as a binary buffer for zero-copy access.
    #[wasm_bindgen(js_name = getResultBinary)]
    pub fn get_result_binary(&self) -> BinaryResult {
        let inner = self.inner.borrow();
        encode_rc_rows_iter_to_binary(inner.result_row_refs(), inner.len(), &self.binary_layout)
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

fn trace_delta_batch_to_js_delta(
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

struct CachedSnapshotRowJs {
    version: u64,
    epoch: u64,
    value: JsValue,
}

const SNAPSHOT_JSONB_CACHE_MAX_ENTRIES: usize = 8_192;
const SNAPSHOT_JSONB_CACHE_TARGET_ENTRIES: usize = 6_144;
const SNAPSHOT_JSONB_CACHE_MAX_BYTES: usize = 4 * 1024 * 1024;
const SNAPSHOT_JSONB_CACHE_TARGET_BYTES: usize = 3 * 1024 * 1024;

#[derive(Default)]
struct JsSnapshotRowsCache {
    rows: HashMap<u64, CachedSnapshotRowJs>,
    jsonb_cache: HashMap<Vec<u8>, JsValue>,
    jsonb_cache_bytes: usize,
    epoch: u64,
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
        self.materialize_from_iter(rows.iter(), rows.len(), cache)
    }

    fn materialize_from_iter<'a, I>(
        &self,
        rows: I,
        row_count: usize,
        cache: &mut JsSnapshotRowsCache,
    ) -> JsValue
    where
        I: IntoIterator<Item = &'a Rc<Row>>,
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
            let js_row = self.materialize_row(row.as_ref(), cache, epoch);
            arr.set(index as u32, js_row);
        }

        cache.rows.retain(|_, entry| entry.epoch == epoch);
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

    fn prune_jsonb_cache_if_needed(&mut self) {
        if self.jsonb_cache.len() <= SNAPSHOT_JSONB_CACHE_MAX_ENTRIES
            && self.jsonb_cache_bytes <= SNAPSHOT_JSONB_CACHE_MAX_BYTES
        {
            return;
        }

        let mut projected_len = self.jsonb_cache.len();
        let mut projected_bytes = self.jsonb_cache_bytes;
        let mut keys_to_remove = Vec::new();

        for key in self.jsonb_cache.keys() {
            if projected_len <= SNAPSHOT_JSONB_CACHE_TARGET_ENTRIES
                && projected_bytes <= SNAPSHOT_JSONB_CACHE_TARGET_BYTES
            {
                break;
            }

            projected_len = projected_len.saturating_sub(1);
            projected_bytes = projected_bytes.saturating_sub(key.len());
            keys_to_remove.push(key.clone());
        }

        for key in keys_to_remove {
            if self.jsonb_cache.remove(&key).is_some() {
                self.jsonb_cache_bytes = self.jsonb_cache_bytes.saturating_sub(key.len());
            }
        }
    }
}

/// Converts rows to a JavaScript array.
fn rows_to_js_array(rows: &[Rc<Row>], schema: &Table) -> JsValue {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_runtime::{
        LiveRegistry, RowsSnapshotDependencyGraph, RowsSnapshotDirectedJoinEdge,
        RowsSnapshotLookupPrimitive, RowsSnapshotPartialRefreshMetadata,
        RowsSnapshotRootSubsetMetadata, RowsSnapshotRootSubsetPlan,
    };
    use crate::query_engine::QueryResultSummary;
    use crate::query_engine::{compile_cached_plan, execute_compiled_physical_plan_with_summary};
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, Value};
    use cynos_query::ast::{Expr, SortOrder};
    use cynos_query::executor::{InMemoryDataSource, PhysicalPlanRunner};
    use cynos_query::planner::{LogicalPlan, PhysicalPlan};
    use cynos_storage::TableCache;
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

    fn partial_refresh_dependency_graph() -> RowsSnapshotDependencyGraph {
        RowsSnapshotDependencyGraph {
            root_table_id: 1,
            table_names: HashMap::from([
                (1, "issues".into()),
                (2, "projects".into()),
                (3, "counters".into()),
            ]),
            edges_by_source: HashMap::from([
                (
                    1,
                    alloc::vec![RowsSnapshotDirectedJoinEdge {
                        source_column_index: 1,
                        target_table_id: 2,
                        target_table: "projects".into(),
                        target_column_index: 0,
                        lookup: RowsSnapshotLookupPrimitive::PrimaryKey,
                    }],
                ),
                (
                    2,
                    alloc::vec![
                        RowsSnapshotDirectedJoinEdge {
                            source_column_index: 0,
                            target_table_id: 1,
                            target_table: "issues".into(),
                            target_column_index: 1,
                            lookup: RowsSnapshotLookupPrimitive::SingleColumnIndex {
                                index_name: "idx_issues_project_id".into(),
                            },
                        },
                        RowsSnapshotDirectedJoinEdge {
                            source_column_index: 0,
                            target_table_id: 3,
                            target_table: "counters".into(),
                            target_column_index: 0,
                            lookup: RowsSnapshotLookupPrimitive::PrimaryKey,
                        },
                    ],
                ),
                (
                    3,
                    alloc::vec![RowsSnapshotDirectedJoinEdge {
                        source_column_index: 0,
                        target_table_id: 2,
                        target_table: "projects".into(),
                        target_column_index: 0,
                        lookup: RowsSnapshotLookupPrimitive::PrimaryKey,
                    }],
                ),
            ]),
        }
    }

    fn root_subset_dependency_graph() -> RowsSnapshotDependencyGraph {
        RowsSnapshotDependencyGraph {
            root_table_id: 1,
            table_names: HashMap::from([
                (1, "issues".into()),
                (2, "projects".into()),
                (3, "counters".into()),
            ]),
            edges_by_source: HashMap::from([
                (
                    1,
                    alloc::vec![RowsSnapshotDirectedJoinEdge {
                        source_column_index: 1,
                        target_table_id: 2,
                        target_table: "projects".into(),
                        target_column_index: 0,
                        lookup: RowsSnapshotLookupPrimitive::PrimaryKey,
                    }],
                ),
                (
                    2,
                    alloc::vec![
                        RowsSnapshotDirectedJoinEdge {
                            source_column_index: 0,
                            target_table_id: 1,
                            target_table: "issues".into(),
                            target_column_index: 1,
                            lookup: RowsSnapshotLookupPrimitive::ScanFallback,
                        },
                        RowsSnapshotDirectedJoinEdge {
                            source_column_index: 0,
                            target_table_id: 3,
                            target_table: "counters".into(),
                            target_column_index: 0,
                            lookup: RowsSnapshotLookupPrimitive::PrimaryKey,
                        },
                    ],
                ),
                (
                    3,
                    alloc::vec![RowsSnapshotDirectedJoinEdge {
                        source_column_index: 0,
                        target_table_id: 2,
                        target_table: "projects".into(),
                        target_column_index: 0,
                        lookup: RowsSnapshotLookupPrimitive::PrimaryKey,
                    }],
                ),
            ]),
        }
    }

    fn patch_test_observable() -> ReQueryObservable {
        let users = TableBuilder::new("users")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("age", DataType::Int32)
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
        cache
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(1, alloc::vec![Value::Int64(1), Value::Int32(42)]))
            .unwrap();
        cache
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(1, alloc::vec![Value::Int64(1), Value::Int64(100)]))
            .unwrap();

        let cache = Rc::new(RefCell::new(cache));
        let compiled_plan = CompiledPhysicalPlan::new(PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::gt(
                Expr::column("users", "age", 1),
                Expr::literal(Value::Int32(30)),
            ),
        ));
        let initial_output = {
            let cache_ref = cache.borrow();
            execute_compiled_physical_plan_with_summary(&cache_ref, &compiled_plan).unwrap()
        };

        ReQueryObservable::new_with_summary(
            compiled_plan,
            cache,
            initial_output.rows,
            initial_output.summary,
            alloc::vec![(1, "users".into()), (2, "orders".into())],
            None,
            None,
        )
    }

    fn partial_refresh_test_observable() -> (Rc<RefCell<TableCache>>, ReQueryObservable) {
        let issues = TableBuilder::new("issues")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("project_id", DataType::Int64)
            .unwrap()
            .add_column("updated_at", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_issues_project_id", &["project_id"], false)
            .unwrap()
            .build()
            .unwrap();
        let projects = TableBuilder::new("projects")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("state", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();
        let counters = TableBuilder::new("counters")
            .unwrap()
            .add_column("project_id", DataType::Int64)
            .unwrap()
            .add_column("open_count", DataType::Int64)
            .unwrap()
            .add_primary_key(&["project_id"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut cache = TableCache::new();
        cache.create_table(issues).unwrap();
        cache.create_table(projects).unwrap();
        cache.create_table(counters).unwrap();

        {
            let store = cache.get_table_mut("issues").unwrap();
            for (id, updated_at) in [(1u64, 500i64), (2, 400), (3, 300), (4, 200), (5, 100)] {
                store
                    .insert(Row::new(
                        id,
                        alloc::vec![
                            Value::Int64(id as i64),
                            Value::Int64(id as i64),
                            Value::Int64(updated_at),
                        ],
                    ))
                    .unwrap();
            }
        }

        {
            let store = cache.get_table_mut("projects").unwrap();
            for id in 1..=5u64 {
                store
                    .insert(Row::new(
                        id,
                        alloc::vec![Value::Int64(id as i64), Value::String("active".into()),],
                    ))
                    .unwrap();
            }
        }

        {
            let store = cache.get_table_mut("counters").unwrap();
            for (project_id, open_count) in [(1u64, 10i64), (2, 20), (3, 30), (4, 40), (5, 50)] {
                store
                    .insert(Row::new(
                        project_id,
                        alloc::vec![Value::Int64(project_id as i64), Value::Int64(open_count)],
                    ))
                    .unwrap();
            }
        }

        let cache = Rc::new(RefCell::new(cache));
        let logical_plan = LogicalPlan::project(
            LogicalPlan::limit(
                LogicalPlan::sort(
                    LogicalPlan::filter(
                        LogicalPlan::left_join(
                            LogicalPlan::left_join(
                                LogicalPlan::scan("issues"),
                                LogicalPlan::scan("projects"),
                                Expr::eq(
                                    Expr::column("issues", "project_id", 1),
                                    Expr::column("projects", "id", 0),
                                ),
                            ),
                            LogicalPlan::scan("counters"),
                            Expr::eq(
                                Expr::column("projects", "id", 0),
                                Expr::column("counters", "project_id", 0),
                            ),
                        ),
                        Expr::eq(
                            Expr::column("projects", "state", 1),
                            Expr::literal(Value::String("active".into())),
                        ),
                    ),
                    alloc::vec![(Expr::column("issues", "updated_at", 2), SortOrder::Desc)],
                ),
                4,
                0,
            ),
            alloc::vec![
                Expr::column("issues", "id", 0),
                Expr::column("issues", "updated_at", 2),
                Expr::column("projects", "state", 1),
                Expr::column("counters", "open_count", 1),
            ],
        );
        let compiled_plan = {
            let cache_ref = cache.borrow();
            compile_cached_plan(&cache_ref, "issues", logical_plan.clone())
        };
        let initial_output = {
            let cache_ref = cache.borrow();
            execute_compiled_physical_plan_with_summary(&cache_ref, &compiled_plan).unwrap()
        };
        let visible_rows: Vec<Rc<Row>> = initial_output.rows.iter().take(2).cloned().collect();
        let visible_summary = QueryResultSummary::from_rows(&visible_rows);
        let observable = ReQueryObservable::new_with_summary(
            compiled_plan,
            cache.clone(),
            visible_rows,
            visible_summary,
            alloc::vec![
                (1, "issues".into()),
                (2, "projects".into()),
                (3, "counters".into()),
            ],
            Some(RowsSnapshotPartialRefreshState {
                metadata: RowsSnapshotPartialRefreshMetadata {
                    root_table: "issues".into(),
                    root_pk_output_indices: alloc::vec![0],
                    order_keys: alloc::vec![RowsSnapshotOrderKey {
                        output_index: 1,
                        order: SortOrder::Desc,
                    }],
                    visible_offset: 0,
                    visible_limit: 2,
                    overscan: 1,
                    candidate_limit: 4,
                    dependency_graph: partial_refresh_dependency_graph(),
                },
                initial_candidate_rows: initial_output.rows,
            }),
            None,
        );
        (cache, observable)
    }

    fn root_subset_test_observable() -> (Rc<RefCell<TableCache>>, ReQueryObservable) {
        let issues = TableBuilder::new("issues")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("project_id", DataType::Int64)
            .unwrap()
            .add_column("updated_at", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();
        let projects = TableBuilder::new("projects")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("state", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();
        let counters = TableBuilder::new("counters")
            .unwrap()
            .add_column("project_id", DataType::Int64)
            .unwrap()
            .add_column("open_count", DataType::Int64)
            .unwrap()
            .add_primary_key(&["project_id"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut cache = TableCache::new();
        cache.create_table(issues).unwrap();
        cache.create_table(projects).unwrap();
        cache.create_table(counters).unwrap();

        {
            let store = cache.get_table_mut("issues").unwrap();
            for (id, updated_at) in [(1u64, 500i64), (2, 400), (3, 300), (4, 200), (5, 100)] {
                store
                    .insert(Row::new(
                        id,
                        alloc::vec![
                            Value::Int64(id as i64),
                            Value::Int64(id as i64),
                            Value::Int64(updated_at),
                        ],
                    ))
                    .unwrap();
            }
        }

        {
            let store = cache.get_table_mut("projects").unwrap();
            for id in 1..=5u64 {
                store
                    .insert(Row::new(
                        id,
                        alloc::vec![Value::Int64(id as i64), Value::String("active".into())],
                    ))
                    .unwrap();
            }
        }

        {
            let store = cache.get_table_mut("counters").unwrap();
            for (project_id, open_count) in [(1u64, 10i64), (2, 20), (3, 30), (4, 40), (5, 50)] {
                store
                    .insert(Row::new(
                        project_id,
                        alloc::vec![Value::Int64(project_id as i64), Value::Int64(open_count)],
                    ))
                    .unwrap();
            }
        }

        let cache = Rc::new(RefCell::new(cache));
        let logical_plan = LogicalPlan::project(
            LogicalPlan::filter(
                LogicalPlan::left_join(
                    LogicalPlan::left_join(
                        LogicalPlan::scan("issues"),
                        LogicalPlan::scan("projects"),
                        Expr::eq(
                            Expr::column("issues", "project_id", 1),
                            Expr::column("projects", "id", 0),
                        ),
                    ),
                    LogicalPlan::scan("counters"),
                    Expr::eq(
                        Expr::column("projects", "id", 0),
                        Expr::column("counters", "project_id", 0),
                    ),
                ),
                Expr::eq(
                    Expr::column("projects", "state", 1),
                    Expr::literal(Value::String("active".into())),
                ),
            ),
            alloc::vec![
                Expr::column("issues", "id", 0),
                Expr::column("issues", "updated_at", 2),
                Expr::column("projects", "state", 1),
                Expr::column("counters", "open_count", 1),
            ],
        );
        let compiled_plan = {
            let cache_ref = cache.borrow();
            compile_cached_plan(&cache_ref, "issues", logical_plan.clone())
        };
        let root_subset_plan = {
            let cache_ref = cache.borrow();
            compile_cached_plan(&cache_ref, "issues", logical_plan)
        };
        let initial_output = {
            let cache_ref = cache.borrow();
            execute_compiled_physical_plan_with_summary(&cache_ref, &compiled_plan).unwrap()
        };

        let observable = ReQueryObservable::new_with_summary(
            compiled_plan,
            cache.clone(),
            initial_output.rows,
            initial_output.summary,
            alloc::vec![
                (1, "issues".into()),
                (2, "projects".into()),
                (3, "counters".into()),
            ],
            None,
            Some(RowsSnapshotRootSubsetPlan {
                metadata: RowsSnapshotRootSubsetMetadata {
                    root_table: "issues".into(),
                    root_pk_store_indices: alloc::vec![0],
                    root_pk_output_indices: alloc::vec![0],
                    dependency_graph: root_subset_dependency_graph(),
                },
                compiled_plans: crate::live_runtime::RowsSnapshotRootSubsetVariants {
                    small: root_subset_plan.clone(),
                    large: root_subset_plan,
                },
            }),
        );
        (cache, observable)
    }

    fn visible_issue_ids(observable: &ReQueryObservable) -> Vec<i64> {
        observable
            .result()
            .iter()
            .filter_map(|row| row.get(0))
            .filter_map(Value::as_i64)
            .collect()
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
    fn test_snapshot_rows_cache_prunes_jsonb_entries_incrementally() {
        let mut cache = JsSnapshotRowsCache::default();
        for index in 0..SNAPSHOT_JSONB_CACHE_MAX_ENTRIES + 64 {
            let mut key = alloc::vec![(index % 251) as u8; 1024];
            key.extend_from_slice(&(index as u64).to_le_bytes());
            cache.jsonb_cache_bytes += key.len();
            cache.jsonb_cache.insert(key, JsValue::NULL);
        }

        cache.prune_jsonb_cache_if_needed();

        assert!(cache.jsonb_cache.len() <= SNAPSHOT_JSONB_CACHE_TARGET_ENTRIES);
        assert!(cache.jsonb_cache_bytes <= SNAPSHOT_JSONB_CACHE_TARGET_BYTES);
    }

    #[test]
    fn test_patch_table_changed_ids_accepts_exact_patch_table_changes() {
        let observable = patch_test_observable();
        let changes = HashMap::from([(1, HashSet::from([1u64]))]);

        let changed_ids = observable
            .patch_table_changed_ids(&changes)
            .expect("patch table change should be eligible for reactive patching");
        assert!(changed_ids.contains(&1));
    }

    #[test]
    fn test_patch_table_changed_ids_rejects_unrelated_or_mixed_table_changes() {
        let observable = patch_test_observable();

        let unrelated = HashMap::from([(2, HashSet::from([1u64]))]);
        assert!(
            observable.patch_table_changed_ids(&unrelated).is_none(),
            "unrelated table changes must not be routed into the patch fast-path",
        );

        let mixed = HashMap::from([(1, HashSet::from([1u64])), (2, HashSet::from([1u64]))]);
        assert!(
            observable.patch_table_changed_ids(&mixed).is_none(),
            "mixed-table changes must fall back to full requery so we keep table provenance intact",
        );
    }

    #[test]
    fn test_partial_refresh_propagates_multi_hop_join_changes() {
        let (cache, mut observable) = partial_refresh_test_observable();
        observable.subscribe(|_| {});

        {
            let mut cache_ref = cache.borrow_mut();
            let store = cache_ref.get_table_mut("counters").unwrap();
            let old_row = store.get(1).unwrap();
            store
                .update(
                    1,
                    Row::new_with_version(
                        1,
                        old_row.version().wrapping_add(1),
                        alloc::vec![Value::Int64(1), Value::Int64(99)],
                    ),
                )
                .unwrap();
        }

        observable.on_change(&HashMap::from([(3, HashSet::from([1u64]))]));

        let first_row = observable.result().first().expect("visible row");
        assert_eq!(first_row.get(0), Some(&Value::Int64(1)));
        assert_eq!(first_row.get(3), Some(&Value::Int64(99)));
    }

    #[test]
    fn test_partial_refresh_falls_back_when_shadow_window_is_depleted() {
        let (cache, mut observable) = partial_refresh_test_observable();
        observable.subscribe(|_| {});

        {
            let mut cache_ref = cache.borrow_mut();
            let store = cache_ref.get_table_mut("projects").unwrap();
            let old_row = store.get(1).unwrap();
            store
                .update(
                    1,
                    Row::new_with_version(
                        1,
                        old_row.version().wrapping_add(1),
                        alloc::vec![Value::Int64(1), Value::String("paused".into())],
                    ),
                )
                .unwrap();
        }
        observable.on_change(&HashMap::from([(2, HashSet::from([1u64]))]));
        assert_eq!(visible_issue_ids(&observable), alloc::vec![2, 3]);

        {
            let mut cache_ref = cache.borrow_mut();
            let store = cache_ref.get_table_mut("projects").unwrap();
            let old_row = store.get(2).unwrap();
            store
                .update(
                    2,
                    Row::new_with_version(
                        2,
                        old_row.version().wrapping_add(1),
                        alloc::vec![Value::Int64(2), Value::String("paused".into())],
                    ),
                )
                .unwrap();
        }
        observable.on_change(&HashMap::from([(2, HashSet::from([2u64]))]));

        assert_eq!(visible_issue_ids(&observable), alloc::vec![3, 4]);
        let partial_refresh = observable
            .partial_refresh
            .as_ref()
            .expect("partial refresh runtime");
        let candidate_issue_ids: Vec<i64> = partial_refresh
            .candidate_rows
            .iter()
            .filter_map(|entry| entry.row.get(0))
            .filter_map(Value::as_i64)
            .collect();
        assert_eq!(candidate_issue_ids, alloc::vec![3, 4, 5]);
    }

    #[test]
    fn test_root_subset_refresh_updates_multi_hop_join_without_full_requery() {
        let (cache, mut observable) = root_subset_test_observable();
        observable.subscribe(|_| {});

        {
            let mut cache_ref = cache.borrow_mut();
            let store = cache_ref.get_table_mut("counters").unwrap();
            let old_row = store.get(3).unwrap();
            store
                .update(
                    3,
                    Row::new_with_version(
                        3,
                        old_row.version().wrapping_add(1),
                        alloc::vec![Value::Int64(3), Value::Int64(300)],
                    ),
                )
                .unwrap();
        }

        let profile = observable.on_change_profiled(&HashMap::from([(3, HashSet::from([3u64]))]));

        assert_eq!(profile.refresh_mode, SnapshotRefreshMode::RootSubsetRefresh);
        assert!(profile.root_subset_hit);
        let updated_row = observable
            .result()
            .iter()
            .find(|row| row.get(0) == Some(&Value::Int64(3)))
            .expect("issue 3 should still be visible");
        assert_eq!(updated_row.get(3), Some(&Value::Int64(300)));
    }

    #[test]
    fn test_root_subset_refresh_keeps_fast_path_for_deleted_intermediate_rows() {
        let (cache, mut observable) = root_subset_test_observable();
        observable.subscribe(|_| {});

        let project_delete = {
            let mut cache_ref = cache.borrow_mut();
            let store = cache_ref.get_table_mut("projects").unwrap();
            store.delete_with_delta(3).unwrap()
        };

        let changes = HashMap::from([(2, HashSet::from([3u64]))]);
        let deltas = HashMap::from([(2, alloc::vec![project_delete])]);
        let profile = observable.on_change_with_deltas_profiled(&changes, &deltas);

        assert_eq!(profile.refresh_mode, SnapshotRefreshMode::RootSubsetRefresh);
        assert!(profile.root_subset_hit);
        assert_eq!(visible_issue_ids(&observable), alloc::vec![1, 2, 4, 5]);
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
