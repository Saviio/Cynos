use super::graphql_payload::{
    build_delta_batch_invalidation, build_graphql_response, build_graphql_response_batched,
    build_graphql_response_batched_refs, build_snapshot_batch_invalidation,
    output_deltas_preserve_root_positions, root_field_has_relations, GraphqlOutputAdapter,
    GraphqlResponsePayloadCache, GraphqlSubscribers,
};
use super::snapshot_refresh::{RootSubsetRefreshRuntime, SnapshotRootRefreshOutcome};
use super::{collect_changed_rows, query_results_equal, ReQueryObservable};
use crate::live_runtime::RowsSnapshotRootSubsetPlan;
#[cfg(feature = "benchmark")]
use crate::profiling::now_ms;
#[cfg(feature = "benchmark")]
use crate::profiling::{GraphqlDeltaProfile, GraphqlSnapshotQueryProfile};
use crate::query_engine::{
    execute_compiled_physical_plan_on_table_subset, execute_compiled_physical_plan_with_summary,
    CompiledPhysicalPlan, QueryResultSummary,
};
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::Row;
use cynos_incremental::{DataflowNode, Delta, MaterializedView, TableId};
use cynos_storage::TableCache;
use hashbrown::{HashMap, HashSet};
use wasm_bindgen::prelude::*;

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
    response_payload: GraphqlResponsePayloadCache,
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
            response_payload: GraphqlResponsePayloadCache {
                dirty: true,
                ..GraphqlResponsePayloadCache::default()
            },
            subscribers: GraphqlSubscribers::default(),
        }
    }

    pub fn attach_keepalive(&mut self) -> usize {
        self.subscribers.add_keepalive()
    }

    pub fn response_js_value(&mut self) -> JsValue {
        if self.response_payload.has_clean_response() {
            let adapter =
                GraphqlOutputAdapter::full_payload_for(self.batch_plan.as_ref(), &self.batch_state);
            return self.response_payload.cached_js_value(adapter);
        }

        if self.subscribers.callback_count() == 0 {
            return self.render_response_js_value();
        }

        if self.current_response().is_none() {
            return JsValue::NULL;
        }

        let adapter =
            GraphqlOutputAdapter::full_payload_for(self.batch_plan.as_ref(), &self.batch_state);
        self.response_payload.cached_js_value(adapter)
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
                plan,
                &self.dependency_table_names,
                changes,
                delta_changes,
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
        self.response_payload.mark_dirty();
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
                if self.response_payload.current_response().is_some() {
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
        if self.response_payload.has_clean_response() {
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
        let adapter =
            GraphqlOutputAdapter::full_payload_for(self.batch_plan.as_ref(), &self.batch_state);
        let changed = adapter.response_changed(self.response_payload.current_response(), &response);
        self.response_payload.finish_materialize(response, changed);
        Some(changed)
    }

    fn current_response(&mut self) -> Option<&cynos_gql::GraphqlResponse> {
        self.materialize_response_if_dirty()?;
        self.response_payload.current_response()
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
            Ok(response) => {
                let adapter = GraphqlOutputAdapter::full_payload_for(
                    self.batch_plan.as_ref(),
                    &self.batch_state,
                );
                self.response_payload.encode_response(&response, adapter)
            }
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
    response_payload: GraphqlResponsePayloadCache,
    subscribers: GraphqlSubscribers,
}

impl GraphqlDeltaObservable {
    fn from_view(
        view: MaterializedView,
        cache: Rc<RefCell<TableCache>>,
        catalog: cynos_gql::GraphqlCatalog,
        field: cynos_gql::bind::BoundRootField,
        dependency_table_bindings: Vec<(TableId, String)>,
    ) -> Self {
        let batch_plan = cynos_gql::compile_batch_plan(&catalog, &field)
            .ok()
            .filter(|plan| plan.has_relations());
        let has_nested_relations = root_field_has_relations(&field);

        Self {
            view,
            cache,
            batch_plan,
            batch_state: cynos_gql::GraphqlBatchState::default(),
            dependency_table_names: dependency_table_bindings.into_iter().collect(),
            catalog,
            has_nested_relations,
            field,
            response_payload: GraphqlResponsePayloadCache {
                dirty: true,
                ..GraphqlResponsePayloadCache::default()
            },
            subscribers: GraphqlSubscribers::default(),
        }
    }

    pub fn new(
        dataflow: DataflowNode,
        cache: Rc<RefCell<TableCache>>,
        catalog: cynos_gql::GraphqlCatalog,
        field: cynos_gql::bind::BoundRootField,
        dependency_table_bindings: Vec<(TableId, String)>,
        initial_rows: Vec<Row>,
    ) -> Self {
        Self::from_view(
            MaterializedView::with_initial(dataflow, initial_rows),
            cache,
            catalog,
            field,
            dependency_table_bindings,
        )
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
        Self::from_view(
            MaterializedView::with_sources(dataflow, initial_rows, source_rows),
            cache,
            catalog,
            field,
            dependency_table_bindings,
        )
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
        Self::from_view(
            MaterializedView::with_compiled_loader_and_bootstrap(
                dataflow,
                compiled_ivm_plan,
                compiled_bootstrap_plan,
                initial_rows,
                load_source_rows,
            ),
            cache,
            catalog,
            field,
            dependency_table_bindings,
        )
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
        Self::from_view(
            MaterializedView::with_compiled_source_visitor_and_bootstrap(
                dataflow,
                compiled_ivm_plan,
                compiled_bootstrap_plan,
                initial_rows,
                visit_source_rows,
            ),
            cache,
            catalog,
            field,
            dependency_table_bindings,
        )
    }

    pub fn attach_keepalive(&mut self) -> usize {
        self.subscribers.add_keepalive()
    }

    pub fn response_js_value(&mut self) -> JsValue {
        if self.response_payload.has_clean_response() {
            let adapter =
                GraphqlOutputAdapter::full_payload_for(self.batch_plan.as_ref(), &self.batch_state);
            return self.response_payload.cached_js_value(adapter);
        }

        if self.subscribers.callback_count() == 0 {
            return self.render_response_js_value();
        }

        if self.current_response().is_none() {
            return JsValue::NULL;
        }

        let adapter =
            GraphqlOutputAdapter::full_payload_for(self.batch_plan.as_ref(), &self.batch_state);
        self.response_payload.cached_js_value(adapter)
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
        self.response_payload.mark_dirty();
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
                if self.response_payload.current_response().is_some() {
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
        if self.response_payload.has_clean_response() {
            return Some(false);
        }

        let cache = self.cache.borrow();
        let response = match self.batch_plan.as_ref() {
            Some(plan) => {
                let rows = self.view.result_rows().collect::<Vec<_>>();
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
        let adapter =
            GraphqlOutputAdapter::full_payload_for(self.batch_plan.as_ref(), &self.batch_state);
        let changed = adapter.response_changed(self.response_payload.current_response(), &response);
        self.response_payload.finish_materialize(response, changed);
        Some(changed)
    }

    fn current_response(&mut self) -> Option<&cynos_gql::GraphqlResponse> {
        self.materialize_response_if_dirty()?;
        self.response_payload.current_response()
    }

    fn render_response_js_value(&mut self) -> JsValue {
        let cache = self.cache.borrow();
        let response = match self.batch_plan.as_ref() {
            Some(plan) => {
                let rows = self.view.result_rows().collect::<Vec<_>>();
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
            Ok(response) => {
                let adapter = GraphqlOutputAdapter::full_payload_for(
                    self.batch_plan.as_ref(),
                    &self.batch_state,
                );
                self.response_payload.encode_response(&response, adapter)
            }
            Err(_) => JsValue::NULL,
        }
    }
}
