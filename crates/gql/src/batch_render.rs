use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;

use cynos_core::{Row, Value};
use cynos_index::KeyRange;
use cynos_storage::{RowStore, TableCache};
use hashbrown::{HashMap, HashSet};

use crate::bind::{
    BoundCollectionQuery, BoundFilter, BoundRootField, BoundRootFieldKind, ColumnPredicate,
    PredicateOp,
};
use crate::catalog::{GraphqlCatalog, TableMeta};
use crate::error::{GqlError, GqlErrorKind, GqlResult};
use crate::execute::apply_collection_query;
use crate::plan::{build_table_query_plan, execute_logical_plan};
use crate::render_plan::{
    EdgeId, GraphqlBatchPlan, NodeId, RelationEdgeKind, RelationEdgePlan, RelationFetchStrategy,
    RenderFieldKind,
};
use crate::response::{GraphqlResponse, ResponseField, ResponseValue};

#[derive(Clone, Debug, Default)]
pub struct GraphqlInvalidation {
    pub root_changed: bool,
    pub changed_tables: Vec<String>,
    pub dirty_edge_keys: HashMap<EdgeId, HashSet<Value>>,
    pub dirty_table_rows: HashMap<String, HashSet<u64>>,
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

#[derive(Clone, Debug, Default)]
pub struct GraphqlBatchState {
    row_cache: HashMap<RowCacheKey, ResponseValue>,
    row_sources: HashMap<RowCacheKey, Rc<Row>>,
    row_dependencies: HashMap<RowCacheKey, Vec<(EdgeId, Value)>>,
    node_row_index: HashMap<NodeId, HashMap<u64, HashSet<RowCacheKey>>>,
    edge_bucket_cache: HashMap<EdgeId, HashMap<Value, Vec<Rc<Row>>>>,
    edge_parent_membership: HashMap<EdgeId, HashMap<Value, HashSet<RowCacheKey>>>,
}

impl GraphqlBatchState {
    pub fn apply_invalidation(
        &mut self,
        plan: &GraphqlBatchPlan,
        invalidation: &GraphqlInvalidation,
    ) {
        let changed_tables: HashSet<String> = invalidation.changed_tables.iter().cloned().collect();
        let mut pending = Vec::new();
        let mut seen = HashSet::new();

        if invalidation.root_changed {
            self.collect_node_rows(plan.root_node(), &mut pending);
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
            let edge_changed = changed_tables.contains(&edge.direct_table);
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
            let parents = self.parent_rows_for_row(plan, row_key);
            self.remove_row_entry(row_key);
            pending.extend(parents);
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

    fn parent_rows_for_row(
        &self,
        plan: &GraphqlBatchPlan,
        row_key: RowCacheKey,
    ) -> Vec<RowCacheKey> {
        let Some(row) = self.row_sources.get(&row_key) else {
            return Vec::new();
        };

        let mut parents = HashSet::new();
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
                    parents.extend(parent_rows.iter().copied());
                }
            }
        }

        parents.into_iter().collect()
    }

    fn remove_row_entry(&mut self, row_key: RowCacheKey) {
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
    let field = render_root_field(cache, catalog, field, plan, state, rows)?;
    Ok(GraphqlResponse::new(ResponseValue::object(alloc::vec![
        field
    ])))
}

fn render_root_field(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    rows: &[Rc<Row>],
) -> GqlResult<ResponseField> {
    let value = match &field.kind {
        BoundRootFieldKind::Collection { .. }
        | BoundRootFieldKind::Insert { .. }
        | BoundRootFieldKind::Update { .. }
        | BoundRootFieldKind::Delete { .. } => ResponseValue::list(render_node_list(
            cache,
            catalog,
            plan,
            state,
            plan.root_node(),
            rows,
        )?),
        BoundRootFieldKind::ByPk { .. } => match rows.first() {
            Some(row) => {
                prefetch_node_edges(
                    cache,
                    catalog,
                    plan,
                    state,
                    plan.root_node(),
                    core::slice::from_ref(row),
                )?;
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

fn render_node_list(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    plan: &GraphqlBatchPlan,
    state: &mut GraphqlBatchState,
    node_id: NodeId,
    rows: &[Rc<Row>],
) -> GqlResult<Vec<ResponseValue>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    prefetch_node_edges(cache, catalog, plan, state, node_id, rows)?;

    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        values.push(render_node_object(
            cache, catalog, plan, state, node_id, row,
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
            prefetch_node_edges(
                cache,
                catalog,
                plan,
                state,
                edge.child_node,
                core::slice::from_ref(&child_row),
            )?;
            render_node_object(cache, catalog, plan, state, edge.child_node, &child_row)
        }
        None => Ok(ResponseValue::Null),
    }
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
    rows: &[Rc<Row>],
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

fn collect_edge_keys(edge: &RelationEdgePlan, rows: &[Rc<Row>]) -> HashSet<Value> {
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
        RelationFetchStrategy::PlannerBatch => match planner_batch_fetch(cache, catalog, edge, keys)
        {
            Ok(buckets) => Ok(buckets),
            Err(error) if edge_query_uses_relations(edge) => Err(error),
            Err(_) => scan_and_bucket_fetch(cache, edge, keys),
        },
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
    let query = build_batch_query(table, edge, keys)?;
    let plan = build_table_query_plan(catalog, table_name, table, &query)?;
    let rows = execute_logical_plan(cache, table_name, plan)?;

    let mut buckets = bucket_rows(rows, edge_target_column_index(edge));
    if let Some(query) = edge.query.as_ref() {
        apply_bucket_window(&mut buckets, query);
    }
    Ok(buckets)
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
                let rows = fetch_rows_by_known_index_or_scan(
                    store,
                    index_name,
                    &edge.relation.child_column,
                    key,
                );
                buckets.insert(key.clone(), apply_collection_query(rows, query));
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

fn apply_bucket_window(buckets: &mut HashMap<Value, Vec<Rc<Row>>>, query: &BoundCollectionQuery) {
    if query.limit.is_none() && query.offset == 0 {
        return;
    }

    for rows in buckets.values_mut() {
        let start = core::cmp::min(query.offset, rows.len());
        let end = match query.limit {
            Some(limit) => start.saturating_add(limit).min(rows.len()),
            None => rows.len(),
        };
        *rows = rows[start..end].to_vec();
    }
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
}
