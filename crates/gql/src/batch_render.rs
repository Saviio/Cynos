use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;

use cynos_core::{schema::IndexType, Row, Value};
use cynos_index::KeyRange;
use cynos_storage::{RowStore, TableCache};
use hashbrown::{HashMap, HashSet};

use crate::bind::{
    BoundCollectionQuery, BoundFilter, BoundRootField, BoundRootFieldKind, ColumnPredicate,
    PredicateOp,
};
use crate::catalog::{GraphqlCatalog, TableMeta};
use crate::error::{GqlError, GqlErrorKind, GqlResult};
use crate::execute::{apply_collection_query, matches_filter};
use crate::plan::{build_table_query_plan, execute_logical_plan};
use crate::render_plan::{
    EdgeId, GraphqlBatchPlan, NodeId, RelationEdgeKind, RelationEdgePlan, RelationFetchStrategy,
    RenderFieldKind,
};
use crate::response::{GraphqlResponse, ResponseField, ResponseValue};

trait RowRenderRef {
    fn row_rc(&self) -> &Rc<Row>;
}

impl RowRenderRef for Rc<Row> {
    fn row_rc(&self) -> &Rc<Row> {
        self
    }
}

impl RowRenderRef for &Rc<Row> {
    fn row_rc(&self) -> &Rc<Row> {
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct GraphqlInvalidation {
    pub root_changed: bool,
    pub dirty_root_rows: HashSet<u64>,
    pub stable_root_positions: bool,
    pub changed_tables: Vec<String>,
    pub dirty_edge_keys: HashMap<EdgeId, HashSet<Value>>,
    pub dirty_table_rows: HashMap<String, HashSet<u64>>,
}

impl GraphqlInvalidation {
    fn table_changed(&self, table_name: &str) -> bool {
        self.changed_tables
            .iter()
            .any(|changed| changed == table_name)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct RowCacheKey {
    node_id: NodeId,
    row_id: u64,
    row_version: u64,
}

impl RowCacheKey {
    fn new(node_id: NodeId, row: &Rc<Row>) -> Self {
        Self {
            node_id,
            row_id: row.id(),
            row_version: row.version(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GraphqlBatchCachePolicy {
    row_max_entries: usize,
    row_target_entries: usize,
}

impl GraphqlBatchCachePolicy {
    const DEFAULT: Self = Self {
        row_max_entries: 131_072,
        row_target_entries: 98_304,
    };

    fn row_limits(self) -> (usize, usize) {
        (
            self.row_max_entries,
            self.row_target_entries.min(self.row_max_entries),
        )
    }
}

#[derive(Clone, Debug, Default)]
pub struct GraphqlBatchState {
    row_cache: HashMap<RowCacheKey, ResponseValue>,
    row_sources: HashMap<RowCacheKey, Rc<Row>>,
    row_dependencies: HashMap<RowCacheKey, Vec<(EdgeId, Value)>>,
    node_row_index: HashMap<NodeId, HashMap<u64, HashSet<RowCacheKey>>>,
    edge_bucket_cache: HashMap<EdgeId, HashMap<Value, Vec<Rc<Row>>>>,
    edge_parent_membership: HashMap<EdgeId, HashMap<Value, HashSet<RowCacheKey>>>,
    root_list_cache: Option<RootListCacheEntry>,
    dirty_root_rows: HashSet<u64>,
    root_list_requires_full_rebuild: bool,
    last_root_patch: Option<GraphqlRootListPatch>,
}

#[derive(Clone, Debug)]
struct RootListCacheEntry {
    row_keys: Vec<RowCacheKey>,
    row_positions: HashMap<u64, usize>,
    items: Rc<[ResponseValue]>,
    list_value: ResponseValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphqlRootListPatch {
    StablePositions(Vec<usize>),
    Splice {
        removed_positions: Vec<usize>,
        inserted_positions: Vec<usize>,
        updated_positions: Vec<usize>,
    },
}

impl GraphqlBatchState {
    pub fn apply_invalidation(
        &mut self,
        plan: &GraphqlBatchPlan,
        invalidation: &GraphqlInvalidation,
    ) {
        let mut pending = Vec::new();
        let mut seen = HashSet::new();

        if invalidation.root_changed {
            self.dirty_root_rows
                .extend(invalidation.dirty_root_rows.iter().copied());
            if !invalidation.stable_root_positions {
                self.root_list_requires_full_rebuild = true;
            }
            if invalidation.dirty_root_rows.is_empty() {
                self.collect_node_rows(plan.root_node(), &mut pending);
            } else if invalidation.dirty_table_rows.is_empty()
                && invalidation.dirty_edge_keys.is_empty()
            {
                for row_id in &invalidation.dirty_root_rows {
                    self.collect_row_id_entries(plan.root_node(), *row_id, &mut pending);
                }
            }
        }

        for (table_name, row_ids) in &invalidation.dirty_table_rows {
            for &node_id in plan.nodes_for_table(table_name) {
                for row_id in row_ids {
                    self.collect_row_id_entries(node_id, *row_id, &mut pending);
                }
            }
        }

        let mut targeted_edges = HashSet::new();
        for edge in plan.edges() {
            let keys = invalidation.dirty_edge_keys.get(&edge.id);
            let edge_changed = invalidation.table_changed(&edge.direct_table);
            if !edge_changed && keys.is_none() {
                continue;
            }
            targeted_edges.insert(edge.id);
            if let Some(keys) = keys {
                self.collect_edge_parent_rows(edge.id, keys, &mut pending);
                if let Some(edge_cache) = self.edge_bucket_cache.get_mut(&edge.id) {
                    for key in keys {
                        edge_cache.remove(key);
                    }
                }
            } else {
                self.collect_all_edge_parent_rows(edge.id, &mut pending);
                self.edge_bucket_cache.remove(&edge.id);
            }
        }

        for (edge_id, keys) in &invalidation.dirty_edge_keys {
            if targeted_edges.contains(edge_id) {
                continue;
            }
            self.collect_edge_parent_rows(*edge_id, keys, &mut pending);
            if let Some(edge_cache) = self.edge_bucket_cache.get_mut(edge_id) {
                for key in keys {
                    edge_cache.remove(key);
                }
            }
        }

        while let Some(row_key) = pending.pop() {
            if !seen.insert(row_key) {
                continue;
            }
            if row_key.node_id == plan.root_node() {
                self.dirty_root_rows.insert(row_key.row_id);
            }
            self.collect_parent_rows_for_row(plan, row_key, &mut pending);
            self.remove_row_entry(row_key);
        }
    }

    fn remember_row(&mut self, row_key: RowCacheKey, row: &Rc<Row>) {
        self.row_sources.insert(row_key, row.clone());
        self.node_row_index
            .entry(row_key.node_id)
            .or_insert_with(HashMap::new)
            .entry(row_key.row_id)
            .or_insert_with(HashSet::new)
            .insert(row_key);
    }

    fn register_parent_membership(&mut self, row_key: RowCacheKey, edge_id: EdgeId, key: Value) {
        let dependencies = self
            .row_dependencies
            .entry(row_key)
            .or_insert_with(Vec::new);
        if !dependencies
            .iter()
            .any(|(dep_edge_id, dep_key)| *dep_edge_id == edge_id && *dep_key == key)
        {
            dependencies.push((edge_id, key.clone()));
        }
        self.edge_parent_membership
            .entry(edge_id)
            .or_insert_with(HashMap::new)
            .entry(key)
            .or_insert_with(HashSet::new)
            .insert(row_key);
    }

    fn collect_node_rows(&self, node_id: NodeId, pending: &mut Vec<RowCacheKey>) {
        if let Some(node_rows) = self.node_row_index.get(&node_id) {
            for row_keys in node_rows.values() {
                pending.extend(row_keys.iter().copied());
            }
        }
    }

    fn collect_row_id_entries(&self, node_id: NodeId, row_id: u64, pending: &mut Vec<RowCacheKey>) {
        if let Some(row_keys) = self
            .node_row_index
            .get(&node_id)
            .and_then(|rows| rows.get(&row_id))
        {
            pending.extend(row_keys.iter().copied());
        }
    }

    fn collect_edge_parent_rows(
        &self,
        edge_id: EdgeId,
        keys: &HashSet<Value>,
        pending: &mut Vec<RowCacheKey>,
    ) {
        let Some(edge_membership) = self.edge_parent_membership.get(&edge_id) else {
            return;
        };
        for key in keys {
            if let Some(parent_rows) = edge_membership.get(key) {
                pending.extend(parent_rows.iter().copied());
            }
        }
    }

    fn collect_all_edge_parent_rows(&self, edge_id: EdgeId, pending: &mut Vec<RowCacheKey>) {
        let Some(edge_membership) = self.edge_parent_membership.get(&edge_id) else {
            return;
        };
        for parent_rows in edge_membership.values() {
            pending.extend(parent_rows.iter().copied());
        }
    }

    fn collect_parent_rows_for_row(
        &self,
        plan: &GraphqlBatchPlan,
        row_key: RowCacheKey,
        pending: &mut Vec<RowCacheKey>,
    ) {
        let Some(row) = self.row_sources.get(&row_key) else {
            return;
        };

        for &edge_id in plan.incoming_edges(row_key.node_id) {
            let edge = plan.edge(edge_id);
            let Some(key) = row.get(edge_target_column_index(edge)).cloned() else {
                continue;
            };
            if key.is_null() {
                continue;
            }
            if let Some(edge_membership) = self.edge_parent_membership.get(&edge_id) {
                if let Some(parent_rows) = edge_membership.get(&key) {
                    pending.extend(parent_rows.iter().copied());
                }
            }
        }
    }

    fn remove_row_entry(&mut self, row_key: RowCacheKey) {
        self.remove_row_entry_inner(row_key, true);
    }

    fn remove_row_entry_inner(&mut self, row_key: RowCacheKey, mark_root_dirty: bool) {
        self.row_cache.remove(&row_key);

        if let Some(dependencies) = self.row_dependencies.remove(&row_key) {
            for (edge_id, key) in dependencies {
                let mut remove_edge_membership = false;
                if let Some(edge_membership) = self.edge_parent_membership.get_mut(&edge_id) {
                    if let Some(parent_rows) = edge_membership.get_mut(&key) {
                        parent_rows.remove(&row_key);
                        if parent_rows.is_empty() {
                            edge_membership.remove(&key);
                        }
                    }
                    remove_edge_membership = edge_membership.is_empty();
                }
                if remove_edge_membership {
                    self.edge_parent_membership.remove(&edge_id);
                }
            }
        }

        self.row_sources.remove(&row_key);

        if let Some(node_rows) = self.node_row_index.get_mut(&row_key.node_id) {
            if let Some(row_versions) = node_rows.get_mut(&row_key.row_id) {
                row_versions.remove(&row_key);
                if row_versions.is_empty() {
                    node_rows.remove(&row_key.row_id);
                }
            }
            if node_rows.is_empty() {
                self.node_row_index.remove(&row_key.node_id);
            }
        }

        if mark_root_dirty
            && self
                .root_list_cache
                .as_ref()
                .is_some_and(|cache| cache.row_positions.contains_key(&row_key.row_id))
        {
            self.dirty_root_rows.insert(row_key.row_id);
        }
    }

    fn prune_if_needed(&mut self, plan: &GraphqlBatchPlan) {
        let (max_entries, target_entries) = GraphqlBatchCachePolicy::DEFAULT.row_limits();
        self.prune_rows_with_limits(plan, max_entries, target_entries);
    }

    fn prune_rows_with_limits(
        &mut self,
        plan: &GraphqlBatchPlan,
        max_entries: usize,
        target_entries: usize,
    ) {
        if self.row_cache.len() <= max_entries {
            return;
        }
        let Some(_) = self.root_list_cache.as_ref() else {
            return;
        };

        let target_entries = target_entries.min(max_entries);
        let live_rows = self.collect_live_rows_from_root_list(plan);
        let mut projected_len = self.row_cache.len();
        let mut row_keys_to_remove = Vec::new();
        for row_key in self.row_cache.keys().copied() {
            if projected_len <= target_entries {
                break;
            }
            if live_rows.contains(&row_key) {
                continue;
            }
            row_keys_to_remove.push(row_key);
            projected_len = projected_len.saturating_sub(1);
        }
        for row_key in row_keys_to_remove {
            self.remove_row_entry_inner(row_key, false);
        }
        self.prune_unreferenced_edge_buckets();
    }

    fn collect_live_rows_from_root_list(&self, plan: &GraphqlBatchPlan) -> HashSet<RowCacheKey> {
        let mut live_rows = HashSet::new();
        let mut pending = self
            .root_list_cache
            .as_ref()
            .map(|cache| cache.row_keys.clone())
            .unwrap_or_default();

        while let Some(row_key) = pending.pop() {
            if !live_rows.insert(row_key) {
                continue;
            }
            let Some(dependencies) = self.row_dependencies.get(&row_key) else {
                continue;
            };
            for (edge_id, key) in dependencies {
                let edge = plan.edge(*edge_id);
                let Some(child_rows) = self
                    .edge_bucket_cache
                    .get(edge_id)
                    .and_then(|buckets| buckets.get(key))
                else {
                    continue;
                };
                for row in child_rows {
                    pending.push(RowCacheKey::new(edge.child_node, row));
                }
            }
        }

        live_rows
    }

    fn prune_unreferenced_edge_buckets(&mut self) {
        let edge_parent_membership = &self.edge_parent_membership;
        self.edge_bucket_cache.retain(|edge_id, buckets| {
            buckets.retain(|key, _| {
                edge_parent_membership
                    .get(edge_id)
                    .and_then(|membership| membership.get(key))
                    .is_some_and(|parents| !parents.is_empty())
            });
            !buckets.is_empty()
        });
    }

    fn clear_root_list_cache(&mut self) {
        self.root_list_cache = None;
        self.dirty_root_rows.clear();
        self.root_list_requires_full_rebuild = false;
        self.last_root_patch = None;
    }

    fn update_root_list_cache(
        &mut self,
        row_keys: Vec<RowCacheKey>,
        items: Vec<ResponseValue>,
    ) -> ResponseValue {
        let row_positions = row_keys
            .iter()
            .enumerate()
            .map(|(index, row_key)| (row_key.row_id, index))
            .collect();
        let items: Rc<[ResponseValue]> = items.into();
        let list_value = ResponseValue::list_shared(items.clone());
        self.root_list_cache = Some(RootListCacheEntry {
            row_keys,
            row_positions,
            items,
            list_value: list_value.clone(),
        });
        self.dirty_root_rows.clear();
        self.root_list_requires_full_rebuild = false;
        self.last_root_patch = None;
        list_value
    }

    pub fn last_root_patch(&self) -> Option<&GraphqlRootListPatch> {
        self.last_root_patch.as_ref()
    }
}

pub fn render_graphql_response(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    rows: &[Rc<Row>],
) -> GqlResult<GraphqlResponse> {
    render_graphql_response_impl(cache, catalog, field, plan, state, rows)
}

pub fn render_graphql_response_refs(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    rows: &[&Rc<Row>],
) -> GqlResult<GraphqlResponse> {
    render_graphql_response_impl(cache, catalog, field, plan, state, rows)
}

fn render_graphql_response_impl<R: RowRenderRef>(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    rows: &[R],
) -> GqlResult<GraphqlResponse> {
    let field = render_root_field(cache, catalog, field, plan, state, rows)?;
    Ok(GraphqlResponse::new(ResponseValue::object(alloc::vec![
        field
    ])))
}

fn render_root_field<R: RowRenderRef>(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    rows: &[R],
) -> GqlResult<ResponseField> {
    let value = match &field.kind {
        BoundRootFieldKind::Collection { .. }
        | BoundRootFieldKind::Insert { .. }
        | BoundRootFieldKind::Update { .. }
        | BoundRootFieldKind::Delete { .. } => {
            render_root_node_list(cache, catalog, plan, state, plan.root_node(), rows)?
        }
        BoundRootFieldKind::ByPk { .. } => match rows.first() {
            Some(row) => {
                let row = row.row_rc();
                if !row_is_cached(state, plan.root_node(), row) {
                    let singleton = [row];
                    prefetch_node_edges_with_children(
                        cache,
                        catalog,
                        plan,
                        state,
                        plan.root_node(),
                        &singleton,
                    )?;
                }
                render_node_object(cache, catalog, plan, state, plan.root_node(), row)?
            }
            None => ResponseValue::Null,
        },
        BoundRootFieldKind::Typename => {
            return Err(GqlError::new(
                GqlErrorKind::Unsupported,
                "typename root fields do not accept row rendering",
            ));
        }
    };

    Ok(ResponseField::new(field.response_key.clone(), value))
}

fn render_root_node_list<R: RowRenderRef>(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    node_id: NodeId,
    rows: &[R],
) -> GqlResult<ResponseValue> {
    if rows.is_empty() {
        state.clear_root_list_cache();
        return Ok(ResponseValue::list(Vec::new()));
    }

    if let Some(list_value) =
        try_render_root_node_list_cached(cache, catalog, plan, state, node_id, rows)?
    {
        state.prune_if_needed(plan);
        return Ok(list_value);
    }

    let items = render_node_list(cache, catalog, plan, state, node_id, rows)?;
    let row_keys = rows
        .iter()
        .map(|row| RowCacheKey::new(node_id, row.row_rc()))
        .collect();
    let list_value = state.update_root_list_cache(row_keys, items);
    state.prune_if_needed(plan);
    Ok(list_value)
}

fn try_render_root_node_list_cached<R: RowRenderRef>(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    node_id: NodeId,
    rows: &[R],
) -> GqlResult<Option<ResponseValue>> {
    let Some(mut cached) = state.root_list_cache.take() else {
        return Ok(None);
    };

    if !state.root_list_requires_full_rebuild {
        if cached.row_keys.len() == rows.len() {
            if state.dirty_root_rows.is_empty() {
                let list_value = cached.list_value.clone();
                state.root_list_cache = Some(cached);
                return Ok(Some(list_value));
            }

            let dirty_positions = state
                .dirty_root_rows
                .iter()
                .map(|row_id| {
                    cached.row_positions.get(row_id).copied().ok_or_else(|| {
                        GqlError::new(GqlErrorKind::Execution, "missing root row position")
                    })
                })
                .collect::<GqlResult<Vec<_>>>();

            match dirty_positions {
                Ok(dirty_positions) => {
                    let mut row_key_updates = Vec::with_capacity(dirty_positions.len());
                    let mut items = None;
                    let mut applied_positions = Vec::with_capacity(dirty_positions.len());

                    for position in dirty_positions {
                        let row =
                            rows.get(position)
                                .map(RowRenderRef::row_rc)
                                .ok_or_else(|| {
                                    GqlError::new(
                                        GqlErrorKind::Execution,
                                        "root row position out of bounds",
                                    )
                                })?;
                        if row.id() != cached.row_keys[position].row_id {
                            state.root_list_requires_full_rebuild = true;
                            break;
                        }
                        if !row_is_cached(state, node_id, row) {
                            let singleton = [row];
                            prefetch_node_edges_with_children(
                                cache, catalog, plan, state, node_id, &singleton,
                            )?;
                        }
                        let rendered =
                            render_node_object(cache, catalog, plan, state, node_id, row)?;
                        if cached.items[position] != rendered {
                            let items = items.get_or_insert_with(|| cached.items.as_ref().to_vec());
                            items[position] = rendered;
                        }
                        row_key_updates.push((position, RowCacheKey::new(node_id, row)));
                        applied_positions.push(position);
                    }

                    if !state.root_list_requires_full_rebuild {
                        for (position, row_key) in row_key_updates {
                            cached.row_keys[position] = row_key;
                        }

                        if let Some(items) = items {
                            let list_value = state.update_root_list_cache(cached.row_keys, items);
                            state.last_root_patch =
                                Some(GraphqlRootListPatch::StablePositions(applied_positions));
                            return Ok(Some(list_value));
                        }
                        state.dirty_root_rows.clear();
                        state.last_root_patch =
                            Some(GraphqlRootListPatch::StablePositions(Vec::new()));
                        let list_value = cached.list_value.clone();
                        state.root_list_cache = Some(cached);
                        return Ok(Some(list_value));
                    }
                }
                Err(_) => {
                    state.root_list_requires_full_rebuild = true;
                }
            }
        } else {
            state.root_list_requires_full_rebuild = true;
        }
    }

    if let Some((row_keys, items, patch)) =
        try_render_root_node_list_splice(cache, catalog, plan, state, node_id, rows, &cached)?
    {
        let list_value = state.update_root_list_cache(row_keys, items);
        state.last_root_patch = Some(patch);
        return Ok(Some(list_value));
    }

    Ok(None)
}

fn try_render_root_node_list_splice<R: RowRenderRef>(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    node_id: NodeId,
    rows: &[R],
    cached: &RootListCacheEntry,
) -> GqlResult<Option<(Vec<RowCacheKey>, Vec<ResponseValue>, GraphqlRootListPatch)>> {
    let row_keys = rows
        .iter()
        .map(|row| RowCacheKey::new(node_id, row.row_rc()))
        .collect::<Vec<_>>();
    let new_row_positions = row_keys
        .iter()
        .enumerate()
        .map(|(index, row_key)| (row_key.row_id, index))
        .collect::<HashMap<_, _>>();

    let mut removed_positions = cached
        .row_keys
        .iter()
        .enumerate()
        .filter_map(|(index, row_key)| {
            (!new_row_positions.contains_key(&row_key.row_id)).then_some(index)
        })
        .collect::<Vec<_>>();
    removed_positions.sort_unstable_by(|left, right| right.cmp(left));

    let inserted_positions = row_keys
        .iter()
        .enumerate()
        .filter_map(|(index, row_key)| {
            (!cached.row_positions.contains_key(&row_key.row_id)).then_some(index)
        })
        .collect::<Vec<_>>();

    let mut last_old_position = None;
    for row_key in &row_keys {
        let Some(old_position) = cached.row_positions.get(&row_key.row_id).copied() else {
            continue;
        };
        if last_old_position.is_some_and(|previous| old_position < previous) {
            return Ok(None);
        }
        last_old_position = Some(old_position);
    }

    let mut positions_needing_render = inserted_positions.clone();
    let mut updated_positions = Vec::new();
    for (position, row_key) in row_keys.iter().enumerate() {
        let Some(old_position) = cached.row_positions.get(&row_key.row_id).copied() else {
            continue;
        };
        if cached.row_keys[old_position] != *row_key
            || state.dirty_root_rows.contains(&row_key.row_id)
        {
            positions_needing_render.push(position);
        }
    }
    positions_needing_render.sort_unstable();
    positions_needing_render.dedup();

    let uncached_rows = positions_needing_render
        .iter()
        .filter_map(|position| rows.get(*position).map(RowRenderRef::row_rc))
        .filter(|row| !row_is_cached(state, node_id, row))
        .collect::<Vec<_>>();
    if !uncached_rows.is_empty() {
        prefetch_node_edges_with_children(cache, catalog, plan, state, node_id, &uncached_rows)?;
    }

    let positions_needing_render = positions_needing_render.into_iter().collect::<HashSet<_>>();
    let mut items = Vec::with_capacity(rows.len());
    for (position, row_key) in row_keys.iter().enumerate() {
        let old_position = cached.row_positions.get(&row_key.row_id).copied();
        if !positions_needing_render.contains(&position) {
            if let Some(old_position) = old_position {
                items.push(cached.items[old_position].clone());
                continue;
            }
        }

        let row = rows
            .get(position)
            .map(RowRenderRef::row_rc)
            .ok_or_else(|| {
                GqlError::new(GqlErrorKind::Execution, "root row position out of bounds")
            })?;
        let rendered = render_node_object(cache, catalog, plan, state, node_id, row)?;
        if let Some(old_position) = old_position {
            if cached.items[old_position] == rendered {
                items.push(cached.items[old_position].clone());
            } else {
                updated_positions.push(position);
                items.push(rendered);
            }
        } else {
            items.push(rendered);
        }
    }

    Ok(Some((
        row_keys,
        items,
        GraphqlRootListPatch::Splice {
            removed_positions,
            inserted_positions,
            updated_positions,
        },
    )))
}

fn render_node_list<R: RowRenderRef>(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    node_id: NodeId,
    rows: &[R],
) -> GqlResult<Vec<ResponseValue>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let uncached_rows = rows
        .iter()
        .map(RowRenderRef::row_rc)
        .filter(|row| !row_is_cached(state, node_id, row))
        .collect::<Vec<_>>();
    if !uncached_rows.is_empty() {
        prefetch_node_edges_with_children(cache, catalog, plan, state, node_id, &uncached_rows)?;
    }

    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        values.push(render_node_object(
            cache,
            catalog,
            plan,
            state,
            node_id,
            row.row_rc(),
        )?);
    }
    Ok(values)
}

fn render_node_object(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    node_id: NodeId,
    row: &Rc<Row>,
) -> GqlResult<ResponseValue> {
    let row_key = RowCacheKey::new(node_id, row);
    if let Some(cached) = state.row_cache.get(&row_key) {
        return Ok(cached.clone());
    }

    state.remember_row(row_key, row);

    let node = plan.node(node_id);
    let mut fields = Vec::with_capacity(node.fields.len());
    for field in &node.fields {
        let value = match &field.kind {
            RenderFieldKind::Typename { value } => {
                ResponseValue::Scalar(Value::String(value.clone()))
            }
            RenderFieldKind::Column { column_index } => row
                .get(*column_index)
                .cloned()
                .map(ResponseValue::Scalar)
                .unwrap_or(ResponseValue::Null),
            RenderFieldKind::ForwardRelation { edge_id } => {
                render_forward_relation(cache, catalog, plan, state, *edge_id, row_key, row)?
            }
            RenderFieldKind::ReverseRelation { edge_id } => {
                render_reverse_relation(cache, catalog, plan, state, *edge_id, row_key, row)?
            }
        };
        fields.push(ResponseField::new(field.response_key.clone(), value));
    }

    let value = ResponseValue::object(fields);
    state.row_cache.insert(row_key, value.clone());
    Ok(value)
}

fn render_forward_relation(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    edge_id: EdgeId,
    parent_row_key: RowCacheKey,
    row: &Rc<Row>,
) -> GqlResult<ResponseValue> {
    let edge = plan.edge(edge_id);
    let Some(key) = row.get(edge.relation.child_column_index).cloned() else {
        return Ok(ResponseValue::Null);
    };
    if key.is_null() {
        return Ok(ResponseValue::Null);
    }

    state.register_parent_membership(parent_row_key, edge_id, key.clone());

    let child_row = state
        .edge_bucket_cache
        .get(&edge_id)
        .and_then(|buckets| buckets.get(&key))
        .and_then(|rows| rows.first())
        .cloned();

    match child_row {
        Some(child_row) => {
            if !row_is_cached(state, edge.child_node, &child_row) {
                let singleton = [&child_row];
                prefetch_node_edges_with_children(
                    cache,
                    catalog,
                    plan,
                    state,
                    edge.child_node,
                    &singleton,
                )?;
            }
            render_node_object(cache, catalog, plan, state, edge.child_node, &child_row)
        }
        None => Ok(ResponseValue::Null),
    }
}

fn row_is_cached(state: &GraphqlBatchState, node_id: NodeId, row: &Rc<Row>) -> bool {
    state
        .row_cache
        .contains_key(&RowCacheKey::new(node_id, row))
}

fn render_reverse_relation(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    edge_id: EdgeId,
    parent_row_key: RowCacheKey,
    row: &Rc<Row>,
) -> GqlResult<ResponseValue> {
    let edge = plan.edge(edge_id);
    let Some(key) = row.get(edge.relation.parent_column_index).cloned() else {
        return Ok(ResponseValue::list(Vec::new()));
    };
    if key.is_null() {
        return Ok(ResponseValue::list(Vec::new()));
    }

    state.register_parent_membership(parent_row_key, edge_id, key.clone());

    let child_rows = state
        .edge_bucket_cache
        .get(&edge_id)
        .and_then(|buckets| buckets.get(&key))
        .cloned()
        .unwrap_or_default();
    let items = render_node_list(cache, catalog, plan, state, edge.child_node, &child_rows)?;
    Ok(ResponseValue::list(items))
}

fn prefetch_node_edges(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    node_id: NodeId,
    rows: &[&Rc<Row>],
) -> GqlResult<()> {
    if rows.is_empty() {
        return Ok(());
    }

    for field in &plan.node(node_id).fields {
        let edge_id = match field.kind {
            RenderFieldKind::ForwardRelation { edge_id }
            | RenderFieldKind::ReverseRelation { edge_id } => edge_id,
            RenderFieldKind::Typename { .. } | RenderFieldKind::Column { .. } => continue,
        };

        let edge = plan.edge(edge_id);
        let keys = collect_edge_keys(edge, rows);
        if keys.is_empty() {
            continue;
        }

        let missing_keys = {
            let edge_cache = state
                .edge_bucket_cache
                .entry(edge_id)
                .or_insert_with(HashMap::new);
            keys.into_iter()
                .filter(|key| !edge_cache.contains_key(key))
                .collect::<HashSet<_>>()
        };

        if missing_keys.is_empty() {
            continue;
        }

        let fetched = fetch_edge_buckets(cache, catalog, edge, &missing_keys)?;
        let edge_cache = state
            .edge_bucket_cache
            .entry(edge_id)
            .or_insert_with(HashMap::new);
        for key in &missing_keys {
            let rows = fetched.get(key).cloned().unwrap_or_default();
            edge_cache.insert(key.clone(), rows);
        }
    }

    Ok(())
}

fn prefetch_node_edges_with_children(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    node_id: NodeId,
    rows: &[&Rc<Row>],
) -> GqlResult<()> {
    prefetch_node_edges(cache, catalog, plan, state, node_id, rows)?;

    let mut child_rows_by_node = HashMap::<NodeId, Vec<Rc<Row>>>::new();
    let mut seen_child_rows = HashSet::<RowCacheKey>::new();

    for field in &plan.node(node_id).fields {
        let edge_id = match field.kind {
            RenderFieldKind::ForwardRelation { edge_id }
            | RenderFieldKind::ReverseRelation { edge_id } => edge_id,
            RenderFieldKind::Typename { .. } | RenderFieldKind::Column { .. } => continue,
        };

        let edge = plan.edge(edge_id);
        let keys = collect_edge_keys(edge, rows);
        if keys.is_empty() {
            continue;
        }

        let Some(edge_cache) = state.edge_bucket_cache.get(&edge_id) else {
            continue;
        };
        for key in keys {
            let Some(child_rows) = edge_cache.get(&key) else {
                continue;
            };
            for child_row in child_rows {
                if row_is_cached(state, edge.child_node, child_row) {
                    continue;
                }
                let child_row_key = RowCacheKey::new(edge.child_node, child_row);
                if !seen_child_rows.insert(child_row_key) {
                    continue;
                }
                child_rows_by_node
                    .entry(edge.child_node)
                    .or_insert_with(Vec::new)
                    .push(child_row.clone());
            }
        }
    }

    for (child_node, child_rows) in child_rows_by_node {
        let child_row_refs = child_rows.iter().collect::<Vec<_>>();
        prefetch_node_edges(cache, catalog, plan, state, child_node, &child_row_refs)?;
    }

    Ok(())
}

fn collect_edge_keys(edge: &RelationEdgePlan, rows: &[&Rc<Row>]) -> HashSet<Value> {
    let mut keys = HashSet::new();
    for row in rows {
        let value = match edge.kind {
            RelationEdgeKind::Forward => row.get(edge.relation.child_column_index),
            RelationEdgeKind::Reverse => row.get(edge.relation.parent_column_index),
        };
        if let Some(value) = value.cloned() {
            if !value.is_null() {
                keys.insert(value);
            }
        }
    }
    keys
}

fn fetch_edge_buckets(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    edge: &RelationEdgePlan,
    keys: &HashSet<Value>,
) -> GqlResult<HashMap<Value, Vec<Rc<Row>>>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }

    let mut buckets = match edge.strategy {
        RelationFetchStrategy::PlannerBatch => {
            match planner_batch_fetch(cache, catalog, edge, keys) {
                Ok(buckets) => Ok(buckets),
                Err(error) if edge_query_uses_relations(edge) => Err(error),
                Err(_) => scan_and_bucket_fetch(cache, edge, keys),
            }
        }
        RelationFetchStrategy::IndexedProbeBatch => match indexed_probe_fetch(cache, edge, keys) {
            Ok(buckets) => Ok(buckets),
            Err(error) if edge_query_uses_relations(edge) => Err(error),
            Err(_) => match planner_batch_fetch(cache, catalog, edge, keys) {
                Ok(buckets) => Ok(buckets),
                Err(error) if edge_query_uses_relations(edge) => Err(error),
                Err(_) => scan_and_bucket_fetch(cache, edge, keys),
            },
        },
        RelationFetchStrategy::ScanAndBucket => scan_and_bucket_fetch(cache, edge, keys),
    }?;

    for key in keys {
        buckets.entry(key.clone()).or_insert_with(Vec::new);
    }
    Ok(buckets)
}

fn edge_query_uses_relations(edge: &RelationEdgePlan) -> bool {
    edge.query
        .as_ref()
        .and_then(|query| query.filter.as_ref())
        .is_some_and(filter_uses_relations)
}

fn filter_uses_relations(filter: &BoundFilter) -> bool {
    match filter {
        BoundFilter::And(filters) | BoundFilter::Or(filters) => {
            filters.iter().any(filter_uses_relations)
        }
        BoundFilter::Column(_) => false,
        BoundFilter::Relation(_) => true,
    }
}

fn planner_batch_fetch(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    edge: &RelationEdgePlan,
    keys: &HashSet<Value>,
) -> GqlResult<HashMap<Value, Vec<Rc<Row>>>> {
    let table_name = edge_target_table(edge);
    let table = catalog.table(table_name).ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Binding,
            alloc::format!("table `{}` is not available", table_name),
        )
    })?;
    if let Some(buckets) = try_ordered_reverse_window_fetch(cache, edge, keys)? {
        return Ok(buckets);
    }
    if let Some(buckets) = try_indexed_reverse_bounded_fetch(cache, edge, keys)? {
        return Ok(buckets);
    }
    let query = build_batch_query(table, edge, keys)?;
    let plan = build_table_query_plan(catalog, table_name, table, &query)?;
    let rows = execute_logical_plan(cache, table_name, plan)?;

    let buckets = bucket_rows_for_query(rows, edge_target_column_index(edge), edge.query.as_ref());
    Ok(buckets)
}

fn try_ordered_reverse_window_fetch(
    cache: &TableCache,
    edge: &RelationEdgePlan,
    keys: &HashSet<Value>,
) -> GqlResult<Option<HashMap<Value, Vec<Rc<Row>>>>> {
    if edge.kind != RelationEdgeKind::Reverse {
        return Ok(None);
    }

    let Some(query) = edge.query.as_ref() else {
        return Ok(None);
    };
    if query.order_by.is_empty() || query.limit.is_none() {
        return Ok(None);
    }
    if query.filter.as_ref().is_some_and(filter_uses_relations) {
        return Ok(None);
    }

    let Some(reverse) = common_order_direction(query) else {
        return Ok(None);
    };

    let table_name = edge_target_table(edge);
    let store = cache.get_table(table_name).ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Execution,
            alloc::format!("table `{}` was not found", table_name),
        )
    })?;
    let Some(index_name) = find_reverse_order_prefix_index_name(store, edge, query) else {
        return Ok(None);
    };

    let mut buckets = HashMap::with_capacity(keys.len());
    for key in keys {
        buckets.insert(
            key.clone(),
            fetch_ordered_reverse_window_rows(store, index_name, key, query, reverse),
        );
    }
    Ok(Some(buckets))
}

fn try_indexed_reverse_bounded_fetch(
    cache: &TableCache,
    edge: &RelationEdgePlan,
    keys: &HashSet<Value>,
) -> GqlResult<Option<HashMap<Value, Vec<Rc<Row>>>>> {
    if edge.kind != RelationEdgeKind::Reverse {
        return Ok(None);
    }

    let Some(query) = edge.query.as_ref() else {
        return Ok(None);
    };
    if query.limit.is_none() || query.order_by.is_empty() {
        return Ok(None);
    }
    if query.filter.as_ref().is_some_and(filter_uses_relations) {
        return Ok(None);
    }

    match indexed_probe_fetch(cache, edge, keys) {
        Ok(buckets) => Ok(Some(buckets)),
        Err(error) if edge_query_uses_relations(edge) => Err(error),
        Err(_) => Ok(None),
    }
}

fn fetch_ordered_reverse_window_rows(
    store: &RowStore,
    index_name: &str,
    key: &Value,
    query: &BoundCollectionQuery,
    reverse: bool,
) -> Vec<Rc<Row>> {
    let prefix = alloc::vec![key.clone()];
    let Some(limit) = query.limit else {
        return Vec::new();
    };
    if limit == 0 {
        return Vec::new();
    }

    let Some(filter) = query.filter.as_ref() else {
        return store.index_scan_composite_prefix_with_options(
            index_name,
            &prefix,
            query.limit,
            query.offset,
            reverse,
        );
    };

    let mut rows = Vec::new();
    let mut skipped = 0usize;
    let mut emitted = 0usize;
    store.visit_index_scan_composite_prefix_with_options(
        index_name,
        &prefix,
        None,
        0,
        reverse,
        |row| {
            if !matches_filter(row.as_ref(), filter) {
                return true;
            }
            if skipped < query.offset {
                skipped += 1;
                return true;
            }
            if emitted >= limit {
                return false;
            }

            rows.push(row.clone());
            emitted += 1;
            emitted < limit
        },
    );
    rows
}

fn common_order_direction(query: &BoundCollectionQuery) -> Option<bool> {
    let first = query.order_by.first()?.descending;
    query
        .order_by
        .iter()
        .all(|spec| spec.descending == first)
        .then_some(first)
}

fn find_reverse_order_prefix_index_name<'a>(
    store: &'a RowStore,
    edge: &RelationEdgePlan,
    query: &BoundCollectionQuery,
) -> Option<&'a str> {
    if !order_by_columns_form_unique_key(store, &query.order_by) {
        return None;
    }

    store
        .schema()
        .indices()
        .iter()
        .find(|index| {
            index.get_index_type() == IndexType::BTree
                && index.columns().len() == query.order_by.len().saturating_add(1)
                && index.columns()[0].name == edge.relation.child_column
                && query
                    .order_by
                    .iter()
                    .enumerate()
                    .all(|(order_index, spec)| {
                        store
                            .schema()
                            .columns()
                            .get(spec.column_index)
                            .is_some_and(|column| {
                                index.columns()[order_index + 1].name == column.name()
                            })
                    })
        })
        .map(|index| index.name())
}

fn order_by_columns_form_unique_key(store: &RowStore, order_by: &[crate::bind::OrderSpec]) -> bool {
    if order_by.is_empty() {
        return false;
    }

    let matches_order_columns = |columns: &[cynos_core::schema::IndexedColumn]| {
        columns.len() == order_by.len()
            && order_by.iter().enumerate().all(|(index, spec)| {
                store
                    .schema()
                    .columns()
                    .get(spec.column_index)
                    .is_some_and(|column| columns[index].name == column.name())
            })
    };

    store
        .schema()
        .primary_key()
        .is_some_and(|primary_key| matches_order_columns(primary_key.columns()))
        || store
            .schema()
            .indices()
            .iter()
            .any(|index| index.is_unique() && matches_order_columns(index.columns()))
}

fn indexed_probe_fetch(
    cache: &TableCache,
    edge: &RelationEdgePlan,
    keys: &HashSet<Value>,
) -> GqlResult<HashMap<Value, Vec<Rc<Row>>>> {
    let table_name = edge_target_table(edge);
    let store = cache.get_table(table_name).ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Execution,
            alloc::format!("table `{}` was not found", table_name),
        )
    })?;

    let mut buckets = HashMap::new();
    match edge.kind {
        RelationEdgeKind::Forward => {
            let pk_compatible = store.schema().primary_key().is_some_and(|pk| {
                pk.columns().len() == 1 && pk.columns()[0].name == edge.relation.parent_column
            });
            let index_name = find_single_column_index_name(store, &edge.relation.parent_column);
            for key in keys {
                let rows = if pk_compatible {
                    store.get_by_pk_values(core::slice::from_ref(key))
                } else if let Some(index_name) = index_name {
                    fetch_rows_by_known_index_or_scan(
                        store,
                        index_name,
                        &edge.relation.parent_column,
                        key,
                    )
                } else {
                    return Err(GqlError::new(
                        GqlErrorKind::Unsupported,
                        "indexed probe fetch requires a primary-key or single-column index",
                    ));
                };
                buckets.insert(key.clone(), rows);
            }
        }
        RelationEdgeKind::Reverse => {
            let query = edge.query.as_ref().ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Unsupported,
                    "reverse indexed probe fetch requires a bound collection query",
                )
            })?;
            if query.filter.as_ref().is_some_and(filter_uses_relations) {
                return Err(GqlError::new(
                    GqlErrorKind::Unsupported,
                    "reverse indexed probe fetch cannot evaluate relation filters without planner support",
                ));
            }
            let index_name = if store.schema().get_index(&edge.relation.fk_name).is_some() {
                Some(edge.relation.fk_name.as_str())
            } else {
                find_single_column_index_name(store, &edge.relation.child_column)
            };
            let Some(index_name) = index_name else {
                return Err(GqlError::new(
                    GqlErrorKind::Unsupported,
                    "reverse indexed probe fetch requires an index on the relation key",
                ));
            };

            for key in keys {
                let rows = fetch_rows_by_known_index_or_scan_windowed(
                    store,
                    index_name,
                    &edge.relation.child_column,
                    key,
                    query.order_by.is_empty().then_some(query),
                );
                let rows = if query.order_by.is_empty() {
                    rows
                } else {
                    apply_collection_query(rows, query)
                };
                buckets.insert(key.clone(), rows);
            }
        }
    }
    Ok(buckets)
}

fn scan_and_bucket_fetch(
    cache: &TableCache,
    edge: &RelationEdgePlan,
    keys: &HashSet<Value>,
) -> GqlResult<HashMap<Value, Vec<Rc<Row>>>> {
    let table_name = edge_target_table(edge);
    let store = cache.get_table(table_name).ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Execution,
            alloc::format!("table `{}` was not found", table_name),
        )
    })?;
    let key_column_index = edge_target_column_index(edge);

    let mut buckets: HashMap<Value, Vec<Rc<Row>>> = HashMap::new();
    for row in store.scan() {
        let Some(value) = row.get(key_column_index).cloned() else {
            continue;
        };
        if value.is_null() || !keys.contains(&value) {
            continue;
        }
        buckets.entry(value).or_insert_with(Vec::new).push(row);
    }

    if let Some(query) = edge.query.as_ref() {
        if query.filter.as_ref().is_some_and(filter_uses_relations) {
            return Err(GqlError::new(
                GqlErrorKind::Unsupported,
                "scan-and-bucket fetch cannot evaluate relation filters without planner support",
            ));
        }
        for rows in buckets.values_mut() {
            let materialized = apply_collection_query(core::mem::take(rows), query);
            *rows = materialized;
        }
    }

    Ok(buckets)
}

fn build_batch_query(
    table: &TableMeta,
    edge: &RelationEdgePlan,
    keys: &HashSet<Value>,
) -> GqlResult<BoundCollectionQuery> {
    let key_filter = relation_key_filter(table, edge, keys)?;
    match edge.kind {
        RelationEdgeKind::Forward => Ok(BoundCollectionQuery {
            filter: Some(key_filter),
            order_by: Vec::new(),
            limit: None,
            offset: 0,
        }),
        RelationEdgeKind::Reverse => {
            let mut query = edge.query.clone().ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Unsupported,
                    "reverse relation batch query requires a bound collection query",
                )
            })?;
            query.filter = Some(match query.filter.take() {
                Some(existing) => BoundFilter::And(alloc::vec![key_filter, existing]),
                None => key_filter,
            });
            query.limit = None;
            query.offset = 0;
            Ok(query)
        }
    }
}

fn relation_key_filter(
    table: &TableMeta,
    edge: &RelationEdgePlan,
    keys: &HashSet<Value>,
) -> GqlResult<BoundFilter> {
    let column_index = edge_target_column_index(edge);
    let column = table.column_by_index(column_index).ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Binding,
            alloc::format!(
                "column index {} was not found on `{}`",
                column_index,
                table.table_name
            ),
        )
    })?;

    let mut key_values: Vec<_> = keys.iter().cloned().collect();
    key_values.sort();
    Ok(BoundFilter::Column(ColumnPredicate {
        column_index,
        data_type: column.data_type,
        ops: alloc::vec![PredicateOp::In(key_values)],
    }))
}

fn bucket_rows(rows: Vec<Rc<Row>>, key_column_index: usize) -> HashMap<Value, Vec<Rc<Row>>> {
    let mut buckets: HashMap<Value, Vec<Rc<Row>>> = HashMap::new();
    for row in rows {
        let Some(key) = row.get(key_column_index).cloned() else {
            continue;
        };
        if key.is_null() {
            continue;
        }
        buckets.entry(key).or_insert_with(Vec::new).push(row);
    }
    buckets
}

fn bucket_rows_for_query(
    rows: Vec<Rc<Row>>,
    key_column_index: usize,
    query: Option<&BoundCollectionQuery>,
) -> HashMap<Value, Vec<Rc<Row>>> {
    let Some(query) = query else {
        return bucket_rows(rows, key_column_index);
    };
    if query.limit.is_none() && query.offset == 0 {
        return bucket_rows(rows, key_column_index);
    }

    let mut buckets: HashMap<Value, Vec<Rc<Row>>> = HashMap::new();
    let mut seen_per_bucket: HashMap<Value, usize> = HashMap::new();
    let window_end = query.limit.map(|limit| query.offset.saturating_add(limit));

    for row in rows {
        let Some(key) = row.get(key_column_index).cloned() else {
            continue;
        };
        if key.is_null() {
            continue;
        }

        let seen = seen_per_bucket.entry(key.clone()).or_insert(0);
        let keep = *seen >= query.offset && window_end.is_none_or(|end| *seen < end);
        *seen += 1;
        if keep {
            buckets.entry(key).or_insert_with(Vec::new).push(row);
        }
    }

    buckets
}

fn edge_target_table(edge: &RelationEdgePlan) -> &str {
    match edge.kind {
        RelationEdgeKind::Forward => &edge.relation.parent_table,
        RelationEdgeKind::Reverse => &edge.relation.child_table,
    }
}

fn edge_target_column_index(edge: &RelationEdgePlan) -> usize {
    match edge.kind {
        RelationEdgeKind::Forward => edge.relation.parent_column_index,
        RelationEdgeKind::Reverse => edge.relation.child_column_index,
    }
}

fn fetch_rows_by_known_index_or_scan(
    store: &RowStore,
    index_name: &str,
    column_name: &str,
    value: &Value,
) -> Vec<Rc<Row>> {
    if store.schema().get_index(index_name).is_some() {
        return store.index_scan(index_name, Some(&KeyRange::only(value.clone())));
    }

    let Some(column_index) = store.schema().get_column_index(column_name) else {
        return Vec::new();
    };
    store
        .scan()
        .filter(|row| {
            row.get(column_index)
                .map(|candidate| candidate.sql_eq(value))
                .unwrap_or(false)
        })
        .collect()
}

fn fetch_rows_by_known_index_or_scan_windowed(
    store: &RowStore,
    index_name: &str,
    column_name: &str,
    value: &Value,
    windowed_query: Option<&BoundCollectionQuery>,
) -> Vec<Rc<Row>> {
    if let Some(query) = windowed_query {
        if query.limit == Some(0) {
            return Vec::new();
        }

        if store.schema().get_index(index_name).is_some() {
            let range = KeyRange::only(value.clone());
            if query.filter.is_none() {
                return store.index_scan_with_limit_offset(
                    index_name,
                    Some(&range),
                    query.limit,
                    query.offset,
                );
            }

            let mut rows = Vec::new();
            let mut skipped = 0usize;
            let mut emitted = 0usize;
            store.visit_index_scan_with_options(index_name, Some(&range), None, 0, false, |row| {
                if !query
                    .filter
                    .as_ref()
                    .is_none_or(|filter| matches_filter(row.as_ref(), filter))
                {
                    return true;
                }
                if skipped < query.offset {
                    skipped += 1;
                    return true;
                }
                if let Some(limit) = query.limit {
                    if emitted >= limit {
                        return false;
                    }
                }

                rows.push(row.clone());
                emitted += 1;
                query.limit.is_none_or(|limit| emitted < limit)
            });
            return rows;
        }

        let Some(column_index) = store.schema().get_column_index(column_name) else {
            return Vec::new();
        };
        let mut rows = Vec::new();
        let mut skipped = 0usize;
        let mut emitted = 0usize;
        store.visit_rows(|row| {
            let matches = row
                .get(column_index)
                .map(|candidate| candidate.sql_eq(value))
                .unwrap_or(false);
            if !matches {
                return true;
            }
            if !query
                .filter
                .as_ref()
                .is_none_or(|filter| matches_filter(row.as_ref(), filter))
            {
                return true;
            }
            if skipped < query.offset {
                skipped += 1;
                return true;
            }
            if let Some(limit) = query.limit {
                if emitted >= limit {
                    return false;
                }
            }
            rows.push(row.clone());
            emitted += 1;
            query.limit.is_none_or(|limit| emitted < limit)
        });
        return rows;
    }

    fetch_rows_by_known_index_or_scan(store, index_name, column_name, value)
}

fn find_single_column_index_name<'a>(store: &'a RowStore, column_name: &str) -> Option<&'a str> {
    store
        .schema()
        .indices()
        .iter()
        .find(|index| index.columns().len() == 1 && index.columns()[0].name == column_name)
        .map(|index| index.name())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::build_root_field_plan;
    use crate::query::{execute_query, PreparedQuery};
    use cynos_core::schema::TableBuilder;
    use cynos_core::DataType;
    use hashbrown::{HashMap, HashSet};

    fn build_cache() -> TableCache {
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
            .add_index("idx_posts_author_id", &["author_id"], false)
            .unwrap()
            .add_index("idx_posts_author_id_id", &["author_id", "id"], false)
            .unwrap()
            .build()
            .unwrap();
        let comments = TableBuilder::new("comments")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("post_id", DataType::Int64)
            .unwrap()
            .add_column("body", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_foreign_key_with_graphql_names(
                "fk_comments_post",
                "post_id",
                "posts",
                "id",
                Some("post"),
                Some("comments"),
            )
            .unwrap()
            .add_index("idx_comments_post_id", &["post_id"], false)
            .unwrap()
            .build()
            .unwrap();

        cache.create_table(users).unwrap();
        cache.create_table(posts).unwrap();
        cache.create_table(comments).unwrap();

        cache
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                1,
                alloc::vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();
        cache
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                2,
                alloc::vec![Value::Int64(2), Value::String("Bob".into())],
            ))
            .unwrap();

        cache
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
        cache
            .get_table_mut("posts")
            .unwrap()
            .insert(Row::new(
                11,
                alloc::vec![
                    Value::Int64(11),
                    Value::Int64(1),
                    Value::String("Rust".into()),
                ],
            ))
            .unwrap();
        cache
            .get_table_mut("posts")
            .unwrap()
            .insert(Row::new(
                12,
                alloc::vec![
                    Value::Int64(12),
                    Value::Int64(2),
                    Value::String("DB".into())
                ],
            ))
            .unwrap();

        cache
            .get_table_mut("comments")
            .unwrap()
            .insert(Row::new(
                100,
                alloc::vec![
                    Value::Int64(100),
                    Value::Int64(10),
                    Value::String("first".into()),
                ],
            ))
            .unwrap();
        cache
            .get_table_mut("comments")
            .unwrap()
            .insert(Row::new(
                101,
                alloc::vec![
                    Value::Int64(101),
                    Value::Int64(11),
                    Value::String("second".into()),
                ],
            ))
            .unwrap();
        cache
            .get_table_mut("comments")
            .unwrap()
            .insert(Row::new(
                102,
                alloc::vec![
                    Value::Int64(102),
                    Value::Int64(11),
                    Value::String("third".into()),
                ],
            ))
            .unwrap();

        cache
    }

    fn execute_with_batch(
        cache: &TableCache,
        catalog: &GraphqlCatalog,
        query: &str,
    ) -> GraphqlResponse {
        let prepared = PreparedQuery::parse(query).unwrap();
        let bound = prepared.bind(catalog, None).unwrap();
        let field = bound.fields.into_iter().next().unwrap();
        let root_plan = build_root_field_plan(catalog, &field).unwrap();
        let rows =
            execute_logical_plan(cache, &root_plan.table_name, root_plan.logical_plan).unwrap();
        let plan = crate::compile_batch_plan(catalog, &field).unwrap();
        let mut state = GraphqlBatchState::default();
        render_graphql_response(cache, catalog, &field, &plan, &mut state, &rows).unwrap()
    }

    fn prepare_batch_execution(
        cache: &TableCache,
        catalog: &GraphqlCatalog,
        query: &str,
    ) -> (
        crate::bind::BoundRootField,
        GraphqlBatchPlan,
        Vec<Rc<Row>>,
        GraphqlBatchState,
    ) {
        let prepared = PreparedQuery::parse(query).unwrap();
        let bound = prepared.bind(catalog, None).unwrap();
        let field = bound.fields.into_iter().next().unwrap();
        let root_plan = build_root_field_plan(catalog, &field).unwrap();
        let rows =
            execute_logical_plan(cache, &root_plan.table_name, root_plan.logical_plan).unwrap();
        let plan = crate::compile_batch_plan(catalog, &field).unwrap();
        let mut state = GraphqlBatchState::default();
        render_graphql_response(cache, catalog, &field, &plan, &mut state, &rows).unwrap();
        (field, plan, rows, state)
    }

    fn root_list_ptr(value: &ResponseValue) -> *const [ResponseValue] {
        match value {
            ResponseValue::List(items) => Rc::as_ptr(items),
            other => panic!("expected list value, found {other:?}"),
        }
    }

    fn object_ptr(value: &ResponseValue) -> *const [ResponseField] {
        match value {
            ResponseValue::Object(fields) => Rc::as_ptr(fields),
            other => panic!("expected object value, found {other:?}"),
        }
    }

    #[test]
    fn batch_renderer_matches_recursive_execution_for_reverse_relation_order_limit() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query = "{ users(orderBy: [{ field: ID, direction: ASC }]) { id name posts(orderBy: [{ field: ID, direction: DESC }], limit: 1) { id title } } }";

        let expected = execute_query(&cache, &catalog, query, None, None).unwrap();
        let actual = execute_with_batch(&cache, &catalog, query);

        assert_eq!(actual, expected);
    }

    #[test]
    fn ordered_reverse_relation_window_uses_composite_prefix_probe() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let prepared = PreparedQuery::parse(
            "{ users(orderBy: [{ field: ID, direction: ASC }]) { id posts(orderBy: [{ field: ID, direction: DESC }], limit: 1) { id title } } }",
        )
        .unwrap();
        let bound = prepared.bind(&catalog, None).unwrap();
        let field = bound.fields.into_iter().next().unwrap();
        let plan = crate::compile_batch_plan(&catalog, &field).unwrap();
        let posts_edge = plan
            .edges()
            .iter()
            .find(|edge| edge.direct_table == "posts")
            .expect("posts edge");
        let keys = HashSet::from([Value::Int64(1), Value::Int64(2)]);

        let buckets = try_ordered_reverse_window_fetch(&cache, posts_edge, &keys)
            .unwrap()
            .expect("ordered reverse relation should use composite prefix probe");

        let user_1_ids: Vec<_> = buckets[&Value::Int64(1)]
            .iter()
            .map(|row| row.id())
            .collect();
        let user_2_ids: Vec<_> = buckets[&Value::Int64(2)]
            .iter()
            .map(|row| row.id())
            .collect();

        assert_eq!(user_1_ids, alloc::vec![11]);
        assert_eq!(user_2_ids, alloc::vec![12]);
    }

    #[test]
    fn ordered_reverse_relation_prefix_probe_applies_filter_before_window() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query = "{ users(orderBy: [{ field: ID, direction: ASC }]) { id name posts(where: { id: { gte: 11 } }, orderBy: [{ field: ID, direction: ASC }], limit: 1) { id title } } }";

        let expected = execute_query(&cache, &catalog, query, None, None).unwrap();
        let actual = execute_with_batch(&cache, &catalog, query);
        assert_eq!(actual, expected);

        let prepared = PreparedQuery::parse(query).unwrap();
        let bound = prepared.bind(&catalog, None).unwrap();
        let field = bound.fields.into_iter().next().unwrap();
        let plan = crate::compile_batch_plan(&catalog, &field).unwrap();
        let posts_edge = plan
            .edges()
            .iter()
            .find(|edge| edge.direct_table == "posts")
            .expect("posts edge");
        let keys = HashSet::from([Value::Int64(1), Value::Int64(2)]);
        let buckets = try_ordered_reverse_window_fetch(&cache, posts_edge, &keys)
            .unwrap()
            .expect("filtered ordered reverse relation should use composite prefix probe");

        let user_1_ids: Vec<_> = buckets[&Value::Int64(1)]
            .iter()
            .map(|row| row.id())
            .collect();
        let user_2_ids: Vec<_> = buckets[&Value::Int64(2)]
            .iter()
            .map(|row| row.id())
            .collect();

        assert_eq!(user_1_ids, alloc::vec![11]);
        assert_eq!(user_2_ids, alloc::vec![12]);
    }

    #[test]
    fn ordered_reverse_relation_without_prefix_index_uses_bounded_indexed_probe() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query = "{ users(orderBy: [{ field: ID, direction: ASC }]) { id posts(orderBy: [{ field: TITLE, direction: ASC }], limit: 1) { id title } } }";

        let expected = execute_query(&cache, &catalog, query, None, None).unwrap();
        let actual = execute_with_batch(&cache, &catalog, query);
        assert_eq!(actual, expected);

        let prepared = PreparedQuery::parse(query).unwrap();
        let bound = prepared.bind(&catalog, None).unwrap();
        let field = bound.fields.into_iter().next().unwrap();
        let plan = crate::compile_batch_plan(&catalog, &field).unwrap();
        let posts_edge = plan
            .edges()
            .iter()
            .find(|edge| edge.direct_table == "posts")
            .expect("posts edge");
        let keys = HashSet::from([Value::Int64(1), Value::Int64(2)]);

        assert!(
            try_ordered_reverse_window_fetch(&cache, posts_edge, &keys)
                .unwrap()
                .is_none(),
            "title order has no composite reverse-prefix index"
        );
        let buckets = try_indexed_reverse_bounded_fetch(&cache, posts_edge, &keys)
            .unwrap()
            .expect("ordered bounded reverse relation should use indexed FK probe");

        let user_1_ids: Vec<_> = buckets[&Value::Int64(1)]
            .iter()
            .map(|row| row.id())
            .collect();
        let user_2_ids: Vec<_> = buckets[&Value::Int64(2)]
            .iter()
            .map(|row| row.id())
            .collect();

        assert_eq!(user_1_ids, alloc::vec![10]);
        assert_eq!(user_2_ids, alloc::vec![12]);
    }

    #[test]
    fn batch_plan_uses_indexed_probe_for_windowed_reverse_relation_without_order() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let prepared = PreparedQuery::parse(
            "{ users(orderBy: [{ field: ID, direction: ASC }]) { id posts(limit: 1, offset: 1) { id title } } }",
        )
        .unwrap();
        let bound = prepared.bind(&catalog, None).unwrap();
        let field = bound.fields.into_iter().next().unwrap();
        let plan = crate::compile_batch_plan(&catalog, &field).unwrap();
        let posts_edge = plan
            .edges()
            .iter()
            .find(|edge| edge.direct_table == "posts")
            .expect("posts edge");

        assert_eq!(
            posts_edge.strategy,
            RelationFetchStrategy::IndexedProbeBatch
        );
    }

    #[test]
    fn batch_renderer_matches_recursive_execution_for_reverse_relation_limit_offset_without_order()
    {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query = "{ users(orderBy: [{ field: ID, direction: ASC }]) { id name posts(limit: 1, offset: 1) { id title } } }";

        let expected = execute_query(&cache, &catalog, query, None, None).unwrap();
        let actual = execute_with_batch(&cache, &catalog, query);

        assert_eq!(actual, expected);
    }

    #[test]
    fn indexed_reverse_probe_applies_filter_before_window_without_order() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query = "{ users(orderBy: [{ field: ID, direction: ASC }]) { id name posts(where: { id: { gte: 11 } }, limit: 1) { id title } } }";

        let expected = execute_query(&cache, &catalog, query, None, None).unwrap();
        let actual = execute_with_batch(&cache, &catalog, query);
        assert_eq!(actual, expected);

        let prepared = PreparedQuery::parse(query).unwrap();
        let bound = prepared.bind(&catalog, None).unwrap();
        let field = bound.fields.into_iter().next().unwrap();
        let plan = crate::compile_batch_plan(&catalog, &field).unwrap();
        let posts_edge = plan
            .edges()
            .iter()
            .find(|edge| edge.direct_table == "posts")
            .expect("posts edge");
        assert_eq!(
            posts_edge.strategy,
            RelationFetchStrategy::IndexedProbeBatch
        );

        let keys = HashSet::from([Value::Int64(1), Value::Int64(2)]);
        let buckets = indexed_probe_fetch(&cache, posts_edge, &keys).unwrap();
        let user_1_ids: Vec<_> = buckets[&Value::Int64(1)]
            .iter()
            .map(|row| row.id())
            .collect();
        let user_2_ids: Vec<_> = buckets[&Value::Int64(2)]
            .iter()
            .map(|row| row.id())
            .collect();

        assert_eq!(user_1_ids, alloc::vec![11]);
        assert_eq!(user_2_ids, alloc::vec![12]);
    }

    #[test]
    fn batch_renderer_matches_recursive_execution_for_multilevel_relations() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query = "{ posts(orderBy: [{ field: ID, direction: ASC }]) { id title author { id name posts(where: { id: { gte: 11 } }, orderBy: [{ field: ID, direction: ASC }]) { id title comments(orderBy: [{ field: ID, direction: ASC }]) { id body } } } } }";

        let expected = execute_query(&cache, &catalog, query, None, None).unwrap();
        let actual = execute_with_batch(&cache, &catalog, query);

        assert_eq!(actual, expected);
    }

    #[test]
    fn batch_invalidation_keeps_unrelated_roots_for_nested_comment_updates() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query = "{ posts(orderBy: [{ field: ID, direction: ASC }]) { id title author { id name posts(orderBy: [{ field: ID, direction: ASC }]) { id title comments(orderBy: [{ field: ID, direction: ASC }]) { id body } } } } }";

        let (_field, plan, rows, mut state) = prepare_batch_execution(&cache, &catalog, query);
        let comments_edge_id = plan
            .edges()
            .iter()
            .find(|edge| edge.direct_table == "comments")
            .map(|edge| edge.id)
            .unwrap();

        state.apply_invalidation(
            &plan,
            &GraphqlInvalidation {
                root_changed: false,
                dirty_root_rows: HashSet::new(),
                stable_root_positions: false,
                changed_tables: alloc::vec!["comments".into()],
                dirty_edge_keys: HashMap::from([(
                    comments_edge_id,
                    HashSet::from([Value::Int64(11)]),
                )]),
                dirty_table_rows: HashMap::from([("comments".into(), HashSet::from([101_u64]))]),
            },
        );

        assert!(state
            .edge_bucket_cache
            .get(&comments_edge_id)
            .is_some_and(|buckets| buckets.contains_key(&Value::Int64(10))));
        assert!(state
            .edge_bucket_cache
            .get(&comments_edge_id)
            .is_none_or(|buckets| !buckets.contains_key(&Value::Int64(11))));

        let root_node = plan.root_node();
        let root_post_10 = RowCacheKey::new(root_node, &rows[0]);
        let root_post_11 = RowCacheKey::new(root_node, &rows[1]);
        let root_post_12 = RowCacheKey::new(root_node, &rows[2]);
        assert!(!state.row_cache.contains_key(&root_post_10));
        assert!(!state.row_cache.contains_key(&root_post_11));
        assert!(state.row_cache.contains_key(&root_post_12));
    }

    #[test]
    fn batch_invalidation_keeps_unrelated_roots_for_forward_relation_updates() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query = "{ posts(orderBy: [{ field: ID, direction: ASC }]) { id title author { id name posts(orderBy: [{ field: ID, direction: ASC }]) { id title } } } }";

        let (_field, plan, rows, mut state) = prepare_batch_execution(&cache, &catalog, query);
        let author_edge_id = plan
            .edges()
            .iter()
            .find(|edge| edge.kind == RelationEdgeKind::Forward && edge.relation.name == "author")
            .map(|edge| edge.id)
            .unwrap();
        let user_node_id = *plan.nodes_for_table("users").first().unwrap();

        state.apply_invalidation(
            &plan,
            &GraphqlInvalidation {
                root_changed: false,
                dirty_root_rows: HashSet::new(),
                stable_root_positions: false,
                changed_tables: alloc::vec!["users".into()],
                dirty_edge_keys: HashMap::from([(
                    author_edge_id,
                    HashSet::from([Value::Int64(2)]),
                )]),
                dirty_table_rows: HashMap::from([("users".into(), HashSet::from([2_u64]))]),
            },
        );

        let root_node = plan.root_node();
        let root_post_10 = RowCacheKey::new(root_node, &rows[0]);
        let root_post_11 = RowCacheKey::new(root_node, &rows[1]);
        let root_post_12 = RowCacheKey::new(root_node, &rows[2]);
        assert!(state.row_cache.contains_key(&root_post_10));
        assert!(state.row_cache.contains_key(&root_post_11));
        assert!(!state.row_cache.contains_key(&root_post_12));

        let cached_user_1 = state
            .row_cache
            .keys()
            .any(|key| key.node_id == user_node_id && key.row_id == 1);
        let cached_user_2 = state
            .row_cache
            .keys()
            .any(|key| key.node_id == user_node_id && key.row_id == 2);
        assert!(cached_user_1);
        assert!(!cached_user_2);
    }

    #[test]
    fn batch_state_prunes_unreachable_rows_without_dropping_live_graph() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query =
            "{ posts(orderBy: [{ field: ID, direction: ASC }]) { id title author { id name } } }";

        let (_field, plan, rows, mut state) = prepare_batch_execution(&cache, &catalog, query);
        let root_node = plan.root_node();
        let live_root_key = RowCacheKey::new(root_node, &rows[0]);
        let live_rows_before = state.collect_live_rows_from_root_list(&plan);

        let dead_key = RowCacheKey {
            node_id: root_node,
            row_id: 999,
            row_version: 1,
        };
        state.row_cache.insert(dead_key, ResponseValue::Null);
        state
            .edge_bucket_cache
            .insert(999, HashMap::from([(Value::Int64(999), Vec::new())]));

        let target_entries = state.row_cache.len().saturating_sub(1);
        state.prune_rows_with_limits(&plan, target_entries, target_entries);

        assert!(!state.row_cache.contains_key(&dead_key));
        assert!(state.row_cache.contains_key(&live_root_key));
        for row_key in live_rows_before {
            assert!(
                state.row_cache.contains_key(&row_key),
                "live cached row {row_key:?} should survive pruning"
            );
        }
        assert!(!state.edge_bucket_cache.contains_key(&999));
    }

    #[test]
    fn batch_invalidation_targets_changed_root_rows_without_flushing_unrelated_roots() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query =
            "{ posts(orderBy: [{ field: ID, direction: ASC }]) { id title author { id name } } }";

        let (_field, plan, rows, mut state) = prepare_batch_execution(&cache, &catalog, query);
        let root_node = plan.root_node();
        let root_post_10 = RowCacheKey::new(root_node, &rows[0]);
        let root_post_11 = RowCacheKey::new(root_node, &rows[1]);
        let root_post_12 = RowCacheKey::new(root_node, &rows[2]);

        state.apply_invalidation(
            &plan,
            &GraphqlInvalidation {
                root_changed: true,
                dirty_root_rows: HashSet::from([11_u64]),
                stable_root_positions: false,
                changed_tables: Vec::new(),
                dirty_edge_keys: HashMap::new(),
                dirty_table_rows: HashMap::new(),
            },
        );

        assert!(state.row_cache.contains_key(&root_post_10));
        assert!(!state.row_cache.contains_key(&root_post_11));
        assert!(state.row_cache.contains_key(&root_post_12));
    }

    #[test]
    fn batch_renderer_reuses_root_list_when_stable_root_update_keeps_response_equal() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query =
            "{ posts(orderBy: [{ field: ID, direction: ASC }]) { id title author { id name } } }";

        let (field, plan, rows, mut state) = prepare_batch_execution(&cache, &catalog, query);
        let initial_list_ptr = root_list_ptr(&state.root_list_cache.as_ref().unwrap().list_value);

        let mut updated_rows = rows.clone();
        updated_rows[1] = Rc::new(Row::new_with_version(
            rows[1].id(),
            rows[1].version() + 1,
            rows[1].values().to_vec(),
        ));

        state.apply_invalidation(
            &plan,
            &GraphqlInvalidation {
                root_changed: true,
                dirty_root_rows: HashSet::from([rows[1].id()]),
                stable_root_positions: true,
                changed_tables: Vec::new(),
                dirty_edge_keys: HashMap::new(),
                dirty_table_rows: HashMap::new(),
            },
        );

        let response =
            render_graphql_response(&cache, &catalog, &field, &plan, &mut state, &updated_rows)
                .unwrap();
        let rerendered_list_ptr =
            root_list_ptr(&state.root_list_cache.as_ref().unwrap().list_value);

        assert_eq!(response, execute_with_batch(&cache, &catalog, query));
        assert_eq!(rerendered_list_ptr, initial_list_ptr);
    }

    #[test]
    fn batch_renderer_only_rebuilds_changed_root_object_when_positions_stay_stable() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query =
            "{ posts(orderBy: [{ field: ID, direction: ASC }]) { id title author { id name } } }";

        let (field, plan, rows, mut state) = prepare_batch_execution(&cache, &catalog, query);
        let initial_items = state.root_list_cache.as_ref().unwrap().items.clone();
        let initial_first_ptr = object_ptr(&initial_items[0]);
        let initial_second_ptr = object_ptr(&initial_items[1]);
        let initial_third_ptr = object_ptr(&initial_items[2]);

        let mut updated_values = rows[1].values().to_vec();
        updated_values[2] = Value::String("second+".into());
        let mut updated_rows = rows.clone();
        updated_rows[1] = Rc::new(Row::new_with_version(
            rows[1].id(),
            rows[1].version() + 1,
            updated_values,
        ));

        state.apply_invalidation(
            &plan,
            &GraphqlInvalidation {
                root_changed: true,
                dirty_root_rows: HashSet::from([rows[1].id()]),
                stable_root_positions: true,
                changed_tables: Vec::new(),
                dirty_edge_keys: HashMap::new(),
                dirty_table_rows: HashMap::new(),
            },
        );

        render_graphql_response(&cache, &catalog, &field, &plan, &mut state, &updated_rows)
            .unwrap();
        let rerendered_items = &state.root_list_cache.as_ref().unwrap().items;

        assert_eq!(object_ptr(&rerendered_items[0]), initial_first_ptr);
        assert_ne!(object_ptr(&rerendered_items[1]), initial_second_ptr);
        assert_eq!(object_ptr(&rerendered_items[2]), initial_third_ptr);
    }

    #[test]
    fn batch_renderer_reports_splice_patch_for_root_membership_change() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query =
            "{ posts(orderBy: [{ field: ID, direction: ASC }]) { id title author { id name } } }";

        let (field, plan, rows, mut state) = prepare_batch_execution(&cache, &catalog, query);
        let updated_rows = alloc::vec![rows[0].clone(), rows[2].clone()];

        state.apply_invalidation(
            &plan,
            &GraphqlInvalidation {
                root_changed: true,
                dirty_root_rows: HashSet::from([rows[1].id()]),
                stable_root_positions: false,
                changed_tables: Vec::new(),
                dirty_edge_keys: HashMap::new(),
                dirty_table_rows: HashMap::new(),
            },
        );

        render_graphql_response(&cache, &catalog, &field, &plan, &mut state, &updated_rows)
            .unwrap();

        assert_eq!(
            state.last_root_patch(),
            Some(&GraphqlRootListPatch::Splice {
                removed_positions: alloc::vec![1],
                inserted_positions: Vec::new(),
                updated_positions: Vec::new(),
            })
        );
    }
}
