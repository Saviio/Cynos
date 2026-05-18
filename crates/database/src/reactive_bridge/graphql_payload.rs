use crate::convert::{
    gql_response_to_js_with_cache, gql_response_to_js_with_root_list_patch, GraphqlJsEncodeCache,
    GraphqlRootListJsCache,
};
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::{Row, Value};
use cynos_incremental::{Delta, TableId};
use cynos_storage::TableCache;
use hashbrown::{HashMap, HashSet};
use wasm_bindgen::prelude::*;

#[derive(Default)]
pub(super) struct GraphqlSubscribers {
    callbacks: Vec<(usize, Box<dyn Fn(&JsValue) + 'static>)>,
    keepalive_ids: HashSet<usize>,
    next_sub_id: usize,
}

impl GraphqlSubscribers {
    pub(super) fn add_keepalive(&mut self) -> usize {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        self.keepalive_ids.insert(id);
        id
    }

    pub(super) fn add_callback<F>(&mut self, callback: F) -> usize
    where
        F: Fn(&JsValue) + 'static,
    {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        self.callbacks.push((id, Box::new(callback)));
        id
    }

    pub(super) fn remove(&mut self, id: usize) -> bool {
        if self.keepalive_ids.remove(&id) {
            return true;
        }

        let len_before = self.callbacks.len();
        self.callbacks.retain(|(sub_id, _)| *sub_id != id);
        self.callbacks.len() < len_before
    }

    pub(super) fn total_count(&self) -> usize {
        self.keepalive_ids.len() + self.callbacks.len()
    }

    pub(super) fn callback_count(&self) -> usize {
        self.callbacks.len()
    }

    pub(super) fn emit(&self, payload: &JsValue) {
        for (_, callback) in &self.callbacks {
            callback(payload);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GraphqlPayloadMode {
    FullPayload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GraphqlPayloadChange<'a> {
    Unknown,
    RootList(GraphqlRootListDelta<'a>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GraphqlRootListDelta<'a> {
    StablePositions(&'a [usize]),
    Splice {
        removed_positions: &'a [usize],
        inserted_positions: &'a [usize],
        updated_positions: &'a [usize],
    },
}

impl<'a> GraphqlPayloadChange<'a> {
    pub(super) fn from_root_list_patch(patch: Option<&'a cynos_gql::GraphqlRootListPatch>) -> Self {
        match patch {
            Some(cynos_gql::GraphqlRootListPatch::StablePositions(positions)) => {
                Self::RootList(GraphqlRootListDelta::StablePositions(positions.as_slice()))
            }
            Some(cynos_gql::GraphqlRootListPatch::Splice {
                removed_positions,
                inserted_positions,
                updated_positions,
            }) => Self::RootList(GraphqlRootListDelta::Splice {
                removed_positions: removed_positions.as_slice(),
                inserted_positions: inserted_positions.as_slice(),
                updated_positions: updated_positions.as_slice(),
            }),
            None => Self::Unknown,
        }
    }

    pub(super) fn as_root_list_patch(
        self,
        patch: Option<&'a cynos_gql::GraphqlRootListPatch>,
    ) -> Option<&'a cynos_gql::GraphqlRootListPatch> {
        match self {
            Self::RootList(_) => patch,
            Self::Unknown => None,
        }
    }

    pub(super) fn changed_hint(self) -> Option<bool> {
        match self {
            Self::Unknown => None,
            Self::RootList(delta) => Some(delta.is_non_empty()),
        }
    }
}

impl<'a> GraphqlRootListDelta<'a> {
    fn is_non_empty(self) -> bool {
        match self {
            Self::StablePositions(positions) => !positions.is_empty(),
            Self::Splice {
                removed_positions,
                inserted_positions,
                updated_positions,
            } => {
                !removed_positions.is_empty()
                    || !inserted_positions.is_empty()
                    || !updated_positions.is_empty()
            }
        }
    }
}

#[derive(Clone, Copy)]
enum GraphqlResponseEncoding<'a> {
    Plain,
    BatchedRootList {
        patch: Option<&'a cynos_gql::GraphqlRootListPatch>,
        change: GraphqlPayloadChange<'a>,
    },
}

#[derive(Clone, Copy)]
pub(super) struct GraphqlOutputAdapter<'a> {
    mode: GraphqlPayloadMode,
    encoding: GraphqlResponseEncoding<'a>,
}

impl<'a> GraphqlOutputAdapter<'a> {
    pub(super) fn full_payload_for(
        batch_plan: Option<&cynos_gql::GraphqlBatchPlan>,
        batch_state: &'a cynos_gql::GraphqlBatchState,
    ) -> Self {
        let encoding = match batch_plan {
            Some(_) => GraphqlResponseEncoding::BatchedRootList {
                patch: batch_state.last_root_patch(),
                change: GraphqlPayloadChange::from_root_list_patch(batch_state.last_root_patch()),
            },
            None => GraphqlResponseEncoding::Plain,
        };
        Self {
            mode: GraphqlPayloadMode::FullPayload,
            encoding,
        }
    }

    pub(super) fn response_changed(
        self,
        current: Option<&cynos_gql::GraphqlResponse>,
        next: &cynos_gql::GraphqlResponse,
    ) -> bool {
        match self.mode {
            GraphqlPayloadMode::FullPayload => match self.encoding {
                GraphqlResponseEncoding::BatchedRootList { change, .. } => change
                    .changed_hint()
                    .unwrap_or_else(|| current.map_or(true, |current| current != next)),
                GraphqlResponseEncoding::Plain => current.map_or(true, |current| current != next),
            },
        }
    }
}

#[derive(Default)]
pub(super) struct GraphqlResponsePayloadCache {
    pub(super) response: Option<cynos_gql::GraphqlResponse>,
    pub(super) response_js: Option<JsValue>,
    pub(super) encode_cache: GraphqlJsEncodeCache,
    pub(super) root_list_js_cache: GraphqlRootListJsCache,
    pub(super) dirty: bool,
}

impl GraphqlResponsePayloadCache {
    pub(super) fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub(super) fn has_clean_response(&self) -> bool {
        self.response.is_some() && !self.dirty
    }

    pub(super) fn current_response(&self) -> Option<&cynos_gql::GraphqlResponse> {
        self.response.as_ref()
    }

    pub(super) fn cached_js_value(&mut self, adapter: GraphqlOutputAdapter<'_>) -> JsValue {
        if let Some(payload) = &self.response_js {
            return payload.clone();
        }

        let Some(response) = self.response.as_ref() else {
            return JsValue::NULL;
        };
        let payload = encode_graphql_response_js(
            response,
            &mut self.encode_cache,
            &mut self.root_list_js_cache,
            adapter,
        );
        self.response_js = Some(payload.clone());
        payload
    }

    pub(super) fn encode_response(
        &mut self,
        response: &cynos_gql::GraphqlResponse,
        adapter: GraphqlOutputAdapter<'_>,
    ) -> JsValue {
        encode_graphql_response_js(
            response,
            &mut self.encode_cache,
            &mut self.root_list_js_cache,
            adapter,
        )
    }

    pub(super) fn finish_materialize(
        &mut self,
        response: cynos_gql::GraphqlResponse,
        changed: bool,
    ) {
        if changed {
            self.response = Some(response);
            self.response_js = None;
        }
        self.dirty = false;
    }
}

pub(super) fn build_graphql_response(
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

pub(super) fn build_graphql_response_batched(
    cache: &TableCache,
    catalog: &cynos_gql::GraphqlCatalog,
    field: &cynos_gql::bind::BoundRootField,
    plan: &cynos_gql::GraphqlBatchPlan,
    state: &mut cynos_gql::GraphqlBatchState,
    rows: &[Rc<Row>],
) -> Result<cynos_gql::GraphqlResponse, cynos_gql::GqlError> {
    cynos_gql::batch_render::render_graphql_response(cache, catalog, field, plan, state, rows)
}

pub(super) fn build_graphql_response_batched_refs(
    cache: &TableCache,
    catalog: &cynos_gql::GraphqlCatalog,
    field: &cynos_gql::bind::BoundRootField,
    plan: &cynos_gql::GraphqlBatchPlan,
    state: &mut cynos_gql::GraphqlBatchState,
    rows: &[&Rc<Row>],
) -> Result<cynos_gql::GraphqlResponse, cynos_gql::GqlError> {
    cynos_gql::batch_render::render_graphql_response_refs(cache, catalog, field, plan, state, rows)
}

pub(super) fn root_field_has_relations(field: &cynos_gql::bind::BoundRootField) -> bool {
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

pub(super) fn build_snapshot_batch_invalidation(
    plan: &cynos_gql::GraphqlBatchPlan,
    table_names: &HashMap<TableId, String>,
    changes: &HashMap<TableId, HashSet<u64>>,
    delta_changes: Option<&HashMap<TableId, Vec<Delta<Row>>>>,
    root_changed: bool,
    dirty_root_rows: &HashSet<u64>,
) -> Result<cynos_gql::GraphqlInvalidation, ()> {
    let mut changed_tables = Vec::with_capacity(changes.len());
    let mut dirty_table_rows = HashMap::new();
    let mut dirty_edge_keys = HashMap::new();
    for table_id in changes.keys() {
        let Some(table_name) = table_names.get(table_id) else {
            return Err(());
        };
        let table_deltas = delta_changes.and_then(|changes| changes.get(table_id));
        if let Some(deltas) = table_deltas.filter(|deltas| !deltas.is_empty()) {
            collect_dirty_edge_keys_for_table_deltas(
                plan,
                table_name,
                deltas,
                &mut dirty_edge_keys,
            );
        } else {
            // Without row deltas we cannot know which relation keys moved, so
            // keep the coarse table-level invalidation as the safe fallback.
            changed_tables.push(table_name.clone());
        }
        if let Some(changed_ids) = changes.get(table_id) {
            dirty_table_rows.insert(table_name.clone(), changed_ids.clone());
        }
    }

    Ok(cynos_gql::GraphqlInvalidation {
        root_changed,
        dirty_root_rows: dirty_root_rows.clone(),
        stable_root_positions: false,
        changed_tables,
        dirty_edge_keys,
        dirty_table_rows,
    })
}

fn collect_dirty_edge_keys_for_table_deltas(
    plan: &cynos_gql::GraphqlBatchPlan,
    table_name: &str,
    deltas: &[Delta<Row>],
    dirty_edge_keys: &mut HashMap<cynos_gql::render_plan::EdgeId, HashSet<Value>>,
) {
    for edge_id in plan.edges_for_table(table_name) {
        let edge = plan.edge(*edge_id);
        let key_column_index = batch_edge_delta_key_column_index(edge);

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
            dirty_edge_keys
                .entry(*edge_id)
                .or_insert_with(HashSet::new)
                .extend(dirty_keys);
        }
    }
}

fn batch_edge_delta_key_column_index(edge: &cynos_gql::render_plan::RelationEdgePlan) -> usize {
    match edge.kind {
        cynos_gql::render_plan::RelationEdgeKind::Forward => edge.relation.parent_column_index,
        cynos_gql::render_plan::RelationEdgeKind::Reverse => edge.relation.child_column_index,
    }
}

pub(super) fn build_delta_batch_invalidation(
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

    collect_dirty_edge_keys_for_table_deltas(
        plan,
        table_name,
        deltas,
        &mut invalidation.dirty_edge_keys,
    );

    Ok(invalidation)
}

pub(super) fn output_deltas_preserve_root_positions(output_deltas: &[Delta<Row>]) -> bool {
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

fn encode_graphql_response_js(
    response: &cynos_gql::GraphqlResponse,
    encode_cache: &mut GraphqlJsEncodeCache,
    root_list_cache: &mut GraphqlRootListJsCache,
    adapter: GraphqlOutputAdapter<'_>,
) -> JsValue {
    match adapter.mode {
        GraphqlPayloadMode::FullPayload => match adapter.encoding {
            GraphqlResponseEncoding::Plain => {
                gql_response_to_js_with_cache(response, encode_cache).unwrap_or(JsValue::NULL)
            }
            GraphqlResponseEncoding::BatchedRootList { patch, change } => {
                gql_response_to_js_with_root_list_patch(
                    response,
                    encode_cache,
                    root_list_cache,
                    change.as_root_list_patch(patch),
                )
                .unwrap_or(JsValue::NULL)
            }
        },
    }
}
