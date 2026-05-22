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
use crate::live_runtime::{
    RowsSnapshotDependencyGraph, RowsSnapshotPartialRefreshState, RowsSnapshotRootSubsetPlan,
};
use crate::profiling::{now_ms, SnapshotQueryProfile, SnapshotRefreshMode};
use crate::query_engine::{
    execute_compiled_physical_plan_on_table_subset, execute_compiled_physical_plan_with_summary,
    CompiledPhysicalPlan, QueryResultSummary,
};
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::Row;
use cynos_incremental::{Delta, TableId};
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

fn encode_rows_iter_to_binary<'a, I>(
    rows: I,
    row_count: usize,
    binary_layout: &SchemaLayout,
) -> BinaryResult
where
    I: IntoIterator<Item = &'a Row>,
{
    let mut encoder = BinaryEncoder::new(binary_layout.clone(), row_count);
    encoder.encode_row_refs_iter(rows);
    BinaryResult::new(encoder.finish())
}

fn binary_result_to_js_value(result: BinaryResult) -> JsValue {
    result.into()
}

mod snapshot_refresh;

use snapshot_refresh::{
    build_partial_refresh_candidate_rows, collect_join_values_by_row_ids_and_deltas,
    compare_partial_refresh_rows, insert_row_ids_by_column_values, slice_partial_visible_rows,
    PartialRefreshCandidateRow, RootSubsetRefreshRuntime, SnapshotPartialRefreshRuntime,
};

mod graphql_payload;

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
        let (affected_root_ids, affected_candidate_rows, partial_metadata) = {
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
        let mut merged_rows: Vec<PartialRefreshCandidateRow> = {
            let current_candidate_rows = &self.partial_refresh.as_ref()?.candidate_rows;
            current_candidate_rows
                .iter()
                .filter(|entry| !affected_root_ids.contains(&entry.root_row_id))
                .cloned()
                .chain(recomputed_rows.into_iter())
                .collect()
        };
        merged_rows.sort_by(|left, right| {
            compare_partial_refresh_rows(left, right, &partial_metadata.order_keys)
        });
        merged_rows.truncate(partial_metadata.candidate_limit);
        let min_shadow_window = partial_metadata
            .visible_offset
            .saturating_add(partial_metadata.visible_limit)
            .saturating_add(partial_metadata.overscan);
        let current_candidate_len = self.partial_refresh.as_ref()?.candidate_rows.len();
        if merged_rows.len() < min_shadow_window && current_candidate_len >= min_shadow_window {
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

mod graphql_observable;

pub use graphql_observable::{GraphqlDeltaObservable, GraphqlSubscriptionObservable};

mod js_surface;

pub use js_surface::{
    JsChangesStream, JsGraphqlSubscription, JsIvmObservableQuery, JsObservableQuery,
};

#[cfg(test)]
mod tests {
    use super::graphql_payload::{build_snapshot_batch_invalidation, GraphqlPayloadChange};
    use super::js_surface::{rows_to_js_array, CachedSnapshotRowJs, JsSnapshotRowsCache};
    use super::*;
    use crate::live_runtime::{
        LiveRegistry, RowsSnapshotDependencyGraph, RowsSnapshotDirectedJoinEdge,
        RowsSnapshotLookupPrimitive, RowsSnapshotOrderKey, RowsSnapshotPartialRefreshMetadata,
        RowsSnapshotRootSubsetMetadata, RowsSnapshotRootSubsetPlan,
    };
    use crate::query_engine::QueryResultSummary;
    use crate::query_engine::{compile_cached_plan, execute_compiled_physical_plan_with_summary};
    use cynos_core::schema::{Table, TableBuilder};
    use cynos_core::{DataType, Value};
    use cynos_gql::render_plan::compile_batch_plan;
    use cynos_gql::{GraphqlCatalog, PreparedQuery};
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

    fn graphql_users_posts_batch_plan() -> (cynos_gql::GraphqlBatchPlan, HashMap<TableId, String>) {
        let mut cache = TableCache::new();
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
        let posts = TableBuilder::new("posts")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("author_id", DataType::Int64)
            .unwrap()
            .add_column("title", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_foreign_key_with_graphql_names(
                "fk_posts_author",
                "author_id",
                "users",
                "id",
                Some("author"),
                Some("posts"),
            )
            .unwrap()
            .build()
            .unwrap();
        cache.create_table(users).unwrap();
        cache.create_table(posts).unwrap();

        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let prepared =
            PreparedQuery::parse("subscription { users { id posts { id title } } }").unwrap();
        let bound = prepared.bind(&catalog, None).unwrap();
        let field = bound.fields.into_iter().next().unwrap();
        let plan = compile_batch_plan(&catalog, &field).unwrap();
        let table_names = HashMap::from([(1, "users".into()), (2, "posts".into())]);
        (plan, table_names)
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
        let policy = crate::convert::JsonbJsCachePolicy::DEFAULT;
        for index in 0..policy.target_entries() + 2_112 {
            let mut key = alloc::vec![(index % 251) as u8; 1024];
            key.extend_from_slice(&(index as u64).to_le_bytes());
            cache.jsonb_cache_bytes += key.len();
            cache.jsonb_cache.insert(key, JsValue::NULL);
        }

        cache.prune_jsonb_cache_if_needed();

        assert!(cache.jsonb_cache.len() <= policy.target_entries());
        assert!(cache.jsonb_cache_bytes <= policy.target_bytes());
    }

    #[test]
    fn test_snapshot_rows_cache_prunes_stale_rows_before_current_epoch() {
        let mut cache = JsSnapshotRowsCache {
            epoch: 7,
            ..JsSnapshotRowsCache::default()
        };
        for row_id in 0..4u64 {
            cache.rows.insert(
                row_id,
                CachedSnapshotRowJs {
                    version: 1,
                    epoch: if row_id == 0 { 7 } else { 6 },
                    value: JsValue::NULL,
                },
            );
        }

        cache.prune_rows_with_limits(3, 2);

        assert!(cache.rows.len() <= 2);
        assert!(cache.rows.contains_key(&0));
    }

    #[test]
    fn test_graphql_payload_change_describes_root_list_deltas() {
        let unknown = GraphqlPayloadChange::from_root_list_patch(None);
        assert_eq!(unknown.changed_hint(), None);
        assert!(unknown.as_root_list_patch(None).is_none());

        let stable_empty = cynos_gql::GraphqlRootListPatch::StablePositions(Vec::new());
        let stable_change = GraphqlPayloadChange::from_root_list_patch(Some(&stable_empty));
        assert_eq!(stable_change.changed_hint(), Some(false));
        assert_eq!(
            stable_change.as_root_list_patch(Some(&stable_empty)),
            Some(&stable_empty)
        );

        let stable_updated = cynos_gql::GraphqlRootListPatch::StablePositions(alloc::vec![2]);
        assert_eq!(
            GraphqlPayloadChange::from_root_list_patch(Some(&stable_updated)).changed_hint(),
            Some(true)
        );

        let splice_empty = cynos_gql::GraphqlRootListPatch::Splice {
            removed_positions: Vec::new(),
            inserted_positions: Vec::new(),
            updated_positions: Vec::new(),
        };
        assert_eq!(
            GraphqlPayloadChange::from_root_list_patch(Some(&splice_empty)).changed_hint(),
            Some(false)
        );

        let splice_changed = cynos_gql::GraphqlRootListPatch::Splice {
            removed_positions: alloc::vec![1],
            inserted_positions: Vec::new(),
            updated_positions: Vec::new(),
        };
        assert_eq!(
            GraphqlPayloadChange::from_root_list_patch(Some(&splice_changed)).changed_hint(),
            Some(true)
        );
    }

    #[test]
    fn test_snapshot_batch_invalidation_uses_delta_relation_keys() {
        let (plan, table_names) = graphql_users_posts_batch_plan();
        let old_post = Row::new(
            10,
            alloc::vec![
                Value::Int64(10),
                Value::Int64(1),
                Value::String("old".into()),
            ],
        );
        let new_post = Row::new(
            10,
            alloc::vec![
                Value::Int64(10),
                Value::Int64(2),
                Value::String("new".into()),
            ],
        );
        let changes = HashMap::from([(2, HashSet::from([10]))]);
        let deltas = HashMap::from([(
            2,
            alloc::vec![Delta::delete(old_post), Delta::insert(new_post)],
        )]);

        let invalidation = build_snapshot_batch_invalidation(
            &plan,
            &table_names,
            &changes,
            Some(&deltas),
            false,
            &HashSet::new(),
        )
        .unwrap();

        let edge_id = *plan.edges_for_table("posts").first().unwrap();
        assert!(
            invalidation.changed_tables.is_empty(),
            "delta-backed snapshot invalidation should not fall back to coarse edge clearing"
        );
        assert_eq!(
            invalidation.dirty_edge_keys.get(&edge_id),
            Some(&HashSet::from([Value::Int64(1), Value::Int64(2)]))
        );
        assert_eq!(
            invalidation.dirty_table_rows.get("posts"),
            Some(&HashSet::from([10]))
        );
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
