use crate::binary_protocol::SchemaLayout;
use crate::profiling::{
    now_ms, DeltaFlushProfile, IvmBridgeProfile, IvmBridgeProfiler, SnapshotFlushProfile,
    TraceInitProfile,
};
#[cfg(feature = "benchmark")]
use crate::profiling::{GraphqlDeltaProfile, SnapshotInitProfile};
use crate::query_engine::{CompiledPhysicalPlan, QueryResultSummary, RootSubsetPlanVariant};
use crate::reactive_bridge::{
    GraphqlDeltaObservable, GraphqlSubscriptionObservable, JsGraphqlSubscription,
    JsIvmObservableQuery, JsObservableQuery, ReQueryObservable,
};
use alloc::collections::BTreeSet;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::schema::Table;
use cynos_core::{Row, Value};
use cynos_gql::{bind::BoundRootField, GraphqlCatalog};
#[cfg(feature = "benchmark")]
use cynos_incremental::TraceUpdateProfile;
use cynos_incremental::{CompiledBootstrapPlan, CompiledIvmPlan, DataflowNode, Delta, TableId};
use cynos_incremental::{TraceTupleArena, TraceTupleHandle};
use cynos_index::KeyRange;
use cynos_query::ast::SortOrder;
use cynos_query::planner::{IndexBounds, PhysicalPlan};
use cynos_reactive::ObservableQuery;
use cynos_storage::TableCache;
use hashbrown::{HashMap, HashSet};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::{Closure, JsValue};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LiveEngineKind {
    Snapshot,
    Delta,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LiveOutputKind {
    RowsSnapshot,
    RowsDelta,
    GraphqlSnapshot,
    GraphqlDelta,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct LiveDependencySet {
    pub tables: Vec<TableId>,
    pub root_tables: Vec<TableId>,
}

impl LiveDependencySet {
    pub fn new(mut tables: Vec<TableId>, mut root_tables: Vec<TableId>) -> Self {
        tables.sort_unstable();
        tables.dedup();
        root_tables.sort_unstable();
        root_tables.dedup();
        Self {
            tables,
            root_tables,
        }
    }

    pub fn snapshot(tables: Vec<TableId>) -> Self {
        Self::new(tables, Vec::new())
    }

    pub fn graphql(tables: Vec<TableId>, root_tables: Vec<TableId>) -> Self {
        Self::new(tables, root_tables)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum TraceBootstrapAccessPath {
    FullScan,
    IndexScanScalar {
        index: String,
        range: Option<KeyRange<Value>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    },
    IndexScanComposite {
        index: String,
        range: Option<KeyRange<Vec<Value>>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    },
    IndexGet {
        index: String,
        key: Value,
        limit: Option<usize>,
    },
    IndexInGet {
        index: String,
        keys: Vec<Value>,
    },
    GinKeyValue {
        index: String,
        key: String,
        value: String,
    },
    GinKey {
        index: String,
        key: String,
    },
    GinMulti {
        index: String,
        pairs: Vec<(String, String)>,
        match_all: bool,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct TraceBootstrapSourceBinding {
    pub source_index: usize,
    pub table_id: TableId,
    pub table_name: String,
    pub access_path: TraceBootstrapAccessPath,
    pub covers_source_filter: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum RowsProjection {
    Full { schema: Table },
    Projection { schema: Table, columns: Vec<String> },
}

impl RowsProjection {
    fn into_snapshot_js(
        self,
        inner: Rc<RefCell<ReQueryObservable>>,
        binary_layout: SchemaLayout,
    ) -> JsObservableQuery {
        match self {
            Self::Full { schema } => JsObservableQuery::new(inner, schema, binary_layout),
            Self::Projection { schema, columns } => {
                JsObservableQuery::new_with_projection(inner, schema, columns, binary_layout)
            }
        }
    }

    fn into_delta_js(
        self,
        inner: Rc<RefCell<ObservableQuery>>,
        binary_layout: SchemaLayout,
        bridge_profiler: Rc<RefCell<IvmBridgeProfiler>>,
    ) -> JsIvmObservableQuery {
        match self {
            Self::Full { schema } => {
                JsIvmObservableQuery::new(inner, schema, binary_layout, bridge_profiler)
            }
            Self::Projection { schema, columns } => JsIvmObservableQuery::new_with_projection(
                inner,
                schema,
                columns,
                binary_layout,
                bridge_profiler,
            ),
        }
    }
}

pub(crate) struct SnapshotKernelPlan {
    pub compiled_plan: CompiledPhysicalPlan,
    pub initial_rows: Vec<Rc<Row>>,
    pub initial_summary: QueryResultSummary,
}

pub(crate) struct DeltaKernelPlan {
    pub dataflow: DataflowNode,
    pub compiled_ivm_plan: CompiledIvmPlan,
    pub compiled_bootstrap_plan: CompiledBootstrapPlan,
    pub initial_rows: Vec<Rc<Row>>,
    pub source_bindings: Vec<TraceBootstrapSourceBinding>,
    pub trace_init_profile: Option<TraceInitProfile>,
}

pub(crate) enum KernelPlan {
    Snapshot(SnapshotKernelPlan),
    Delta(DeltaKernelPlan),
}

pub(crate) struct RowsSnapshotAdapterPlan {
    pub projection: RowsProjection,
    pub binary_layout: SchemaLayout,
    pub dependency_table_bindings: Vec<(TableId, String)>,
    pub partial_refresh: Option<RowsSnapshotPartialRefreshState>,
    pub root_subset_refresh: Option<RowsSnapshotRootSubsetPlan>,
}

#[derive(Clone, Debug)]
pub(crate) struct RowsSnapshotOrderKey {
    pub output_index: usize,
    pub order: SortOrder,
}

#[derive(Clone, Debug)]
pub(crate) struct RowsSnapshotJoinEdge {
    pub left_table: String,
    pub left_column: String,
    pub right_table: String,
    pub right_column: String,
}

#[derive(Clone, Debug)]
pub(crate) enum RowsSnapshotLookupPrimitive {
    PrimaryKey,
    SingleColumnIndex { index_name: String },
    ScanFallback,
}

#[derive(Clone, Debug)]
pub(crate) struct RowsSnapshotDirectedJoinEdge {
    pub source_column_index: usize,
    pub target_table_id: TableId,
    pub target_table: String,
    pub target_column_index: usize,
    pub lookup: RowsSnapshotLookupPrimitive,
}

#[derive(Clone, Debug)]
pub(crate) struct RowsSnapshotDependencyGraph {
    pub root_table_id: TableId,
    pub table_names: HashMap<TableId, String>,
    pub edges_by_source: HashMap<TableId, Vec<RowsSnapshotDirectedJoinEdge>>,
}

impl RowsSnapshotDependencyGraph {
    pub fn table_name(&self, table_id: TableId) -> Option<&str> {
        self.table_names.get(&table_id).map(String::as_str)
    }

    pub fn edges_from(&self, table_id: TableId) -> &[RowsSnapshotDirectedJoinEdge] {
        self.edges_by_source
            .get(&table_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RowsSnapshotPartialRefreshMetadata {
    pub root_table: String,
    pub root_pk_output_indices: Vec<usize>,
    pub order_keys: Vec<RowsSnapshotOrderKey>,
    pub visible_offset: usize,
    pub visible_limit: usize,
    pub overscan: usize,
    pub candidate_limit: usize,
    pub dependency_graph: RowsSnapshotDependencyGraph,
}

#[derive(Clone, Debug)]
pub(crate) struct RowsSnapshotRootSubsetMetadata {
    pub root_table: String,
    pub root_pk_store_indices: Vec<usize>,
    pub root_pk_output_indices: Vec<usize>,
    pub dependency_graph: RowsSnapshotDependencyGraph,
}

pub(crate) struct RowsSnapshotRootSubsetPlan {
    pub metadata: RowsSnapshotRootSubsetMetadata,
    pub compiled_plans: RowsSnapshotRootSubsetVariants,
}

pub(crate) struct RowsSnapshotRootSubsetVariants {
    pub small: CompiledPhysicalPlan,
    pub large: CompiledPhysicalPlan,
}

impl RowsSnapshotRootSubsetVariants {
    pub fn select(&self, variant: RootSubsetPlanVariant) -> &CompiledPhysicalPlan {
        match variant {
            RootSubsetPlanVariant::Small => &self.small,
            RootSubsetPlanVariant::Large => &self.large,
        }
    }
}

pub(crate) struct RowsSnapshotPartialRefreshState {
    pub metadata: RowsSnapshotPartialRefreshMetadata,
    pub initial_candidate_rows: Vec<Rc<Row>>,
}

pub(crate) struct RowsDeltaAdapterPlan {
    pub projection: RowsProjection,
    pub binary_layout: SchemaLayout,
}

pub(crate) struct GraphqlSnapshotAdapterPlan {
    pub catalog: GraphqlCatalog,
    pub field: BoundRootField,
    pub dependency_table_bindings: Vec<(TableId, String)>,
    pub root_subset_refresh: Option<RowsSnapshotRootSubsetPlan>,
}

pub(crate) struct GraphqlDeltaAdapterPlan {
    pub catalog: GraphqlCatalog,
    pub field: BoundRootField,
    pub dependency_table_bindings: Vec<(TableId, String)>,
}

pub(crate) enum AdapterPlan {
    RowsSnapshot(RowsSnapshotAdapterPlan),
    RowsDelta(RowsDeltaAdapterPlan),
    GraphqlSnapshot(GraphqlSnapshotAdapterPlan),
    GraphqlDelta(GraphqlDeltaAdapterPlan),
}

fn trace_bootstrap_sql_point_lookup_keys(key: &Value) -> Vec<Value> {
    match key {
        Value::Int32(value) => alloc::vec![Value::Int32(*value), Value::Int64(*value as i64)],
        Value::Int64(value) => {
            let mut keys = alloc::vec![Value::Int64(*value)];
            if i32::try_from(*value).is_ok() {
                keys.push(Value::Int32(*value as i32));
            }
            keys
        }
        _ => alloc::vec![key.clone()],
    }
}

fn trace_index_column_arity(schema: &Table, index_name: &str) -> Option<usize> {
    schema
        .get_index(index_name)
        .or_else(|| {
            schema
                .indices()
                .iter()
                .find(|candidate| candidate.normalized_name() == index_name)
        })
        .or_else(|| {
            schema.primary_key().filter(|candidate| {
                candidate.name() == index_name || candidate.normalized_name() == index_name
            })
        })
        .map(|index| index.columns().len())
}

fn trace_index_scan_access_path(
    schema: Option<&Table>,
    index: &str,
    bounds: &IndexBounds,
    limit: Option<usize>,
    offset: Option<usize>,
    reverse: bool,
) -> TraceBootstrapAccessPath {
    let offset = offset.unwrap_or(0);
    match bounds {
        IndexBounds::Scalar(range) => TraceBootstrapAccessPath::IndexScanScalar {
            index: index.into(),
            range: Some(range.clone()),
            limit,
            offset,
            reverse,
        },
        IndexBounds::Composite(range) => TraceBootstrapAccessPath::IndexScanComposite {
            index: index.into(),
            range: Some(range.clone()),
            limit,
            offset,
            reverse,
        },
        IndexBounds::Unbounded => {
            let arity = schema
                .and_then(|schema| trace_index_column_arity(schema, index))
                .unwrap_or(1);
            if arity > 1 {
                TraceBootstrapAccessPath::IndexScanComposite {
                    index: index.into(),
                    range: None,
                    limit,
                    offset,
                    reverse,
                }
            } else {
                TraceBootstrapAccessPath::IndexScanScalar {
                    index: index.into(),
                    range: None,
                    limit,
                    offset,
                    reverse,
                }
            }
        }
    }
}

fn push_trace_bootstrap_source_binding(
    bindings: &mut Vec<TraceBootstrapSourceBinding>,
    table_ids: &HashMap<String, TableId>,
    table_name: &str,
    access_path: TraceBootstrapAccessPath,
    covers_source_filter: bool,
) {
    let Some(table_id) = table_ids.get(table_name).copied() else {
        return;
    };

    bindings.push(TraceBootstrapSourceBinding {
        source_index: bindings.len(),
        table_id,
        table_name: table_name.into(),
        access_path,
        covers_source_filter,
    });
}

fn collect_trace_bootstrap_source_bindings_into(
    plan: &PhysicalPlan,
    table_ids: &HashMap<String, TableId>,
    table_schemas: &HashMap<String, Table>,
    bindings: &mut Vec<TraceBootstrapSourceBinding>,
) {
    match plan {
        PhysicalPlan::TableScan { table } => push_trace_bootstrap_source_binding(
            bindings,
            table_ids,
            table,
            TraceBootstrapAccessPath::FullScan,
            false,
        ),
        PhysicalPlan::IndexScan {
            table,
            index,
            bounds,
            limit,
            offset,
            reverse,
        } => push_trace_bootstrap_source_binding(
            bindings,
            table_ids,
            table,
            trace_index_scan_access_path(
                table_schemas.get(table),
                index,
                bounds,
                *limit,
                *offset,
                *reverse,
            ),
            true,
        ),
        PhysicalPlan::IndexGet {
            table,
            index,
            key,
            limit,
        } => push_trace_bootstrap_source_binding(
            bindings,
            table_ids,
            table,
            TraceBootstrapAccessPath::IndexGet {
                index: index.clone(),
                key: key.clone(),
                limit: *limit,
            },
            true,
        ),
        PhysicalPlan::IndexInGet { table, index, keys } => push_trace_bootstrap_source_binding(
            bindings,
            table_ids,
            table,
            TraceBootstrapAccessPath::IndexInGet {
                index: index.clone(),
                keys: keys.clone(),
            },
            true,
        ),
        PhysicalPlan::GinIndexScan {
            table,
            index,
            key,
            value,
            query_type,
            ..
        } => {
            let access_path = match (query_type.as_str(), value.as_ref()) {
                ("eq", Some(value)) => TraceBootstrapAccessPath::GinKeyValue {
                    index: index.clone(),
                    key: key.clone(),
                    value: value.clone(),
                },
                ("contains", _) | ("exists", _) => TraceBootstrapAccessPath::GinKey {
                    index: index.clone(),
                    key: key.clone(),
                },
                _ => TraceBootstrapAccessPath::FullScan,
            };
            push_trace_bootstrap_source_binding(bindings, table_ids, table, access_path, true);
        }
        PhysicalPlan::GinIndexScanMulti {
            table,
            index,
            pairs,
            match_all,
            ..
        } => push_trace_bootstrap_source_binding(
            bindings,
            table_ids,
            table,
            TraceBootstrapAccessPath::GinMulti {
                index: index.clone(),
                pairs: pairs.clone(),
                match_all: *match_all,
            },
            true,
        ),
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::HashAggregate { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::TopN { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::NoOp { input } => {
            collect_trace_bootstrap_source_bindings_into(input, table_ids, table_schemas, bindings);
        }
        PhysicalPlan::HashJoin { left, right, .. }
        | PhysicalPlan::SortMergeJoin { left, right, .. }
        | PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::CrossProduct { left, right }
        | PhysicalPlan::Union { left, right, .. } => {
            collect_trace_bootstrap_source_bindings_into(left, table_ids, table_schemas, bindings);
            collect_trace_bootstrap_source_bindings_into(right, table_ids, table_schemas, bindings);
        }
        PhysicalPlan::IndexNestedLoopJoin {
            outer,
            inner_table,
            outer_is_left,
            ..
        } => {
            if *outer_is_left {
                collect_trace_bootstrap_source_bindings_into(
                    outer,
                    table_ids,
                    table_schemas,
                    bindings,
                );
                push_trace_bootstrap_source_binding(
                    bindings,
                    table_ids,
                    inner_table,
                    TraceBootstrapAccessPath::FullScan,
                    false,
                );
            } else {
                push_trace_bootstrap_source_binding(
                    bindings,
                    table_ids,
                    inner_table,
                    TraceBootstrapAccessPath::FullScan,
                    false,
                );
                collect_trace_bootstrap_source_bindings_into(
                    outer,
                    table_ids,
                    table_schemas,
                    bindings,
                );
            }
        }
        PhysicalPlan::Empty => {}
    }
}

pub(crate) fn collect_trace_bootstrap_source_bindings(
    plan: &PhysicalPlan,
    table_ids: &HashMap<String, TableId>,
    table_schemas: &HashMap<String, Table>,
) -> Vec<TraceBootstrapSourceBinding> {
    let mut bindings = Vec::new();
    collect_trace_bootstrap_source_bindings_into(plan, table_ids, table_schemas, &mut bindings);
    bindings
}

fn trace_bootstrap_slot_capacity_hint(
    store: &cynos_storage::RowStore,
    binding: &TraceBootstrapSourceBinding,
) -> Option<usize> {
    match &binding.access_path {
        TraceBootstrapAccessPath::FullScan => Some(store.len()),
        TraceBootstrapAccessPath::IndexScanScalar { limit, .. }
        | TraceBootstrapAccessPath::IndexScanComposite { limit, .. } => *limit,
        TraceBootstrapAccessPath::IndexGet { limit, .. } => Some(limit.unwrap_or(1)),
        TraceBootstrapAccessPath::IndexInGet { keys, .. } => Some(keys.len()),
        TraceBootstrapAccessPath::GinKeyValue { index, key, value } => {
            Some(store.gin_index_cost_key_value(index, key, value))
        }
        TraceBootstrapAccessPath::GinKey { index, key } => {
            Some(store.gin_index_cost_key(index, key))
        }
        TraceBootstrapAccessPath::GinMulti {
            index,
            pairs,
            match_all,
        } => {
            let costs = pairs
                .iter()
                .map(|(key, value)| store.gin_index_cost_key_value(index, key, value));
            if *match_all {
                costs.min()
            } else {
                Some(costs.fold(0usize, usize::saturating_add).min(store.len()))
            }
        }
    }
}

fn push_trace_bootstrap_slot_row(
    arena: &TraceTupleArena,
    slot: &mut Vec<TraceTupleHandle>,
    row: &Rc<Row>,
    now_fn: Option<fn() -> f64>,
    handle_wrap_ms: &mut f64,
) {
    if let Some(now_fn) = now_fn {
        let started_at = now_fn();
        slot.push(arena.base_rc(Rc::clone(row)));
        *handle_wrap_ms += now_fn() - started_at;
    } else {
        slot.push(arena.base_rc(Rc::clone(row)));
    }
}

fn visit_trace_bootstrap_binding_rows_into_slot(
    cache: &TableCache,
    binding: &TraceBootstrapSourceBinding,
    arena: &TraceTupleArena,
    slot: &mut Vec<TraceTupleHandle>,
    now_fn: Option<fn() -> f64>,
) -> f64 {
    let Some(store) = cache.get_table(&binding.table_name) else {
        return 0.0;
    };

    if let Some(capacity_hint) = trace_bootstrap_slot_capacity_hint(store, binding) {
        slot.reserve(capacity_hint);
    }

    let mut handle_wrap_ms = 0.0;
    match &binding.access_path {
        TraceBootstrapAccessPath::FullScan => {
            store.visit_rows(|row| {
                push_trace_bootstrap_slot_row(arena, slot, row, now_fn, &mut handle_wrap_ms);
                true
            });
        }
        TraceBootstrapAccessPath::IndexScanScalar {
            index,
            range,
            limit,
            offset,
            reverse,
        } => {
            store.visit_index_scan_with_options(
                index,
                range.as_ref(),
                *limit,
                *offset,
                *reverse,
                |row| {
                    push_trace_bootstrap_slot_row(arena, slot, row, now_fn, &mut handle_wrap_ms);
                    true
                },
            );
        }
        TraceBootstrapAccessPath::IndexScanComposite {
            index,
            range,
            limit,
            offset,
            reverse,
        } => {
            store.visit_index_scan_composite_with_options(
                index,
                range.as_ref(),
                *limit,
                *offset,
                *reverse,
                |row| {
                    push_trace_bootstrap_slot_row(arena, slot, row, now_fn, &mut handle_wrap_ms);
                    true
                },
            );
        }
        TraceBootstrapAccessPath::IndexGet { index, key, limit } => {
            let mut seen_ids = BTreeSet::new();
            let mut remaining = limit.unwrap_or(usize::MAX);
            let enforce_limit = limit.is_some();

            for probe_key in trace_bootstrap_sql_point_lookup_keys(key) {
                if enforce_limit && remaining == 0 {
                    break;
                }

                let range = KeyRange::only(probe_key);
                let mut keep_scanning = true;
                store.visit_index_scan_with_options(
                    index,
                    Some(&range),
                    if enforce_limit { Some(remaining) } else { None },
                    0,
                    false,
                    |row| {
                        if !seen_ids.insert(row.id()) {
                            return true;
                        }

                        push_trace_bootstrap_slot_row(
                            arena,
                            slot,
                            row,
                            now_fn,
                            &mut handle_wrap_ms,
                        );
                        if enforce_limit {
                            remaining = remaining.saturating_sub(1);
                        }
                        keep_scanning = !enforce_limit || remaining > 0;
                        keep_scanning
                    },
                );
                if !keep_scanning {
                    break;
                }
            }
        }
        TraceBootstrapAccessPath::IndexInGet { index, keys } => {
            let mut seen_ids = BTreeSet::new();
            for key in keys {
                for probe_key in trace_bootstrap_sql_point_lookup_keys(key) {
                    let range = KeyRange::only(probe_key);
                    store.visit_index_scan_with_options(
                        index,
                        Some(&range),
                        None,
                        0,
                        false,
                        |row| {
                            if seen_ids.insert(row.id()) {
                                push_trace_bootstrap_slot_row(
                                    arena,
                                    slot,
                                    row,
                                    now_fn,
                                    &mut handle_wrap_ms,
                                );
                            }
                            true
                        },
                    );
                }
            }
        }
        TraceBootstrapAccessPath::GinKeyValue { index, key, value } => {
            store.visit_gin_index_by_key_value(index, key, value, |row| {
                push_trace_bootstrap_slot_row(arena, slot, row, now_fn, &mut handle_wrap_ms);
                true
            });
        }
        TraceBootstrapAccessPath::GinKey { index, key } => {
            store.visit_gin_index_by_key(index, key, |row| {
                push_trace_bootstrap_slot_row(arena, slot, row, now_fn, &mut handle_wrap_ms);
                true
            });
        }
        TraceBootstrapAccessPath::GinMulti {
            index,
            pairs,
            match_all,
        } => {
            let pair_refs: Vec<(&str, &str)> = pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect();
            if *match_all {
                store.visit_gin_index_by_key_values_all(index, &pair_refs, |row| {
                    push_trace_bootstrap_slot_row(arena, slot, row, now_fn, &mut handle_wrap_ms);
                    true
                });
            } else {
                store.visit_gin_index_by_key_values_any(index, &pair_refs, |row| {
                    push_trace_bootstrap_slot_row(arena, slot, row, now_fn, &mut handle_wrap_ms);
                    true
                });
            }
        }
    }

    handle_wrap_ms
}

fn visit_trace_bootstrap_binding_rows(
    cache: &TableCache,
    binding: &TraceBootstrapSourceBinding,
    emit: &mut dyn FnMut(Rc<Row>),
) {
    let Some(store) = cache.get_table(&binding.table_name) else {
        return;
    };

    match &binding.access_path {
        TraceBootstrapAccessPath::FullScan => {
            store.visit_rows(|row| {
                emit(Rc::clone(row));
                true
            });
        }
        TraceBootstrapAccessPath::IndexScanScalar {
            index,
            range,
            limit,
            offset,
            reverse,
        } => {
            store.visit_index_scan_with_options(
                index,
                range.as_ref(),
                *limit,
                *offset,
                *reverse,
                |row| {
                    emit(Rc::clone(row));
                    true
                },
            );
        }
        TraceBootstrapAccessPath::IndexScanComposite {
            index,
            range,
            limit,
            offset,
            reverse,
        } => {
            store.visit_index_scan_composite_with_options(
                index,
                range.as_ref(),
                *limit,
                *offset,
                *reverse,
                |row| {
                    emit(Rc::clone(row));
                    true
                },
            );
        }
        TraceBootstrapAccessPath::IndexGet { index, key, limit } => {
            let mut seen_ids = BTreeSet::new();
            let mut remaining = limit.unwrap_or(usize::MAX);
            let enforce_limit = limit.is_some();

            for probe_key in trace_bootstrap_sql_point_lookup_keys(key) {
                if enforce_limit && remaining == 0 {
                    break;
                }

                let range = KeyRange::only(probe_key);
                let mut keep_scanning = true;
                store.visit_index_scan_with_options(
                    index,
                    Some(&range),
                    if enforce_limit { Some(remaining) } else { None },
                    0,
                    false,
                    |row| {
                        if !seen_ids.insert(row.id()) {
                            return true;
                        }

                        emit(Rc::clone(row));
                        if enforce_limit {
                            remaining = remaining.saturating_sub(1);
                        }
                        keep_scanning = !enforce_limit || remaining > 0;
                        keep_scanning
                    },
                );
                if !keep_scanning {
                    break;
                }
            }
        }
        TraceBootstrapAccessPath::IndexInGet { index, keys } => {
            let mut seen_ids = BTreeSet::new();
            for key in keys {
                for probe_key in trace_bootstrap_sql_point_lookup_keys(key) {
                    let range = KeyRange::only(probe_key);
                    store.visit_index_scan_with_options(
                        index,
                        Some(&range),
                        None,
                        0,
                        false,
                        |row| {
                            if seen_ids.insert(row.id()) {
                                emit(Rc::clone(row));
                            }
                            true
                        },
                    );
                }
            }
        }
        TraceBootstrapAccessPath::GinKeyValue { index, key, value } => {
            store.visit_gin_index_by_key_value(index, key, value, |row| {
                emit(Rc::clone(row));
                true
            });
        }
        TraceBootstrapAccessPath::GinKey { index, key } => {
            store.visit_gin_index_by_key(index, key, |row| {
                emit(Rc::clone(row));
                true
            });
        }
        TraceBootstrapAccessPath::GinMulti {
            index,
            pairs,
            match_all,
        } => {
            let pair_refs: Vec<(&str, &str)> = pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect();
            if *match_all {
                store.visit_gin_index_by_key_values_all(index, &pair_refs, |row| {
                    emit(Rc::clone(row));
                    true
                });
            } else {
                store.visit_gin_index_by_key_values_any(index, &pair_refs, |row| {
                    emit(Rc::clone(row));
                    true
                });
            }
        }
    }
}

pub(crate) struct LivePlanDescriptor {
    pub engine: LiveEngineKind,
    #[allow(dead_code)]
    pub output: LiveOutputKind,
    pub dependencies: LiveDependencySet,
}

pub(crate) struct LivePlan {
    pub descriptor: LivePlanDescriptor,
    pub kernel: KernelPlan,
    pub adapter: AdapterPlan,
}

impl LivePlan {
    pub fn rows_snapshot(
        dependencies: LiveDependencySet,
        compiled_plan: CompiledPhysicalPlan,
        initial_rows: Vec<Rc<Row>>,
        initial_summary: QueryResultSummary,
        projection: RowsProjection,
        binary_layout: SchemaLayout,
        dependency_table_bindings: Vec<(TableId, String)>,
        partial_refresh: Option<RowsSnapshotPartialRefreshState>,
        root_subset_refresh: Option<RowsSnapshotRootSubsetPlan>,
    ) -> Self {
        Self {
            descriptor: LivePlanDescriptor {
                engine: LiveEngineKind::Snapshot,
                output: LiveOutputKind::RowsSnapshot,
                dependencies,
            },
            kernel: KernelPlan::Snapshot(SnapshotKernelPlan {
                compiled_plan,
                initial_rows,
                initial_summary,
            }),
            adapter: AdapterPlan::RowsSnapshot(RowsSnapshotAdapterPlan {
                projection,
                binary_layout,
                dependency_table_bindings,
                partial_refresh,
                root_subset_refresh,
            }),
        }
    }

    pub fn rows_delta(
        dependencies: LiveDependencySet,
        dataflow: DataflowNode,
        compiled_ivm_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial_rows: Vec<Rc<Row>>,
        source_bindings: Vec<TraceBootstrapSourceBinding>,
        projection: RowsProjection,
        binary_layout: SchemaLayout,
        trace_init_profile: TraceInitProfile,
    ) -> Self {
        Self {
            descriptor: LivePlanDescriptor {
                engine: LiveEngineKind::Delta,
                output: LiveOutputKind::RowsDelta,
                dependencies,
            },
            kernel: KernelPlan::Delta(DeltaKernelPlan {
                dataflow,
                compiled_ivm_plan,
                compiled_bootstrap_plan,
                initial_rows,
                source_bindings,
                trace_init_profile: Some(trace_init_profile),
            }),
            adapter: AdapterPlan::RowsDelta(RowsDeltaAdapterPlan {
                projection,
                binary_layout,
            }),
        }
    }

    pub fn graphql_snapshot(
        dependencies: LiveDependencySet,
        compiled_plan: CompiledPhysicalPlan,
        initial_rows: Vec<Rc<Row>>,
        initial_summary: QueryResultSummary,
        catalog: GraphqlCatalog,
        field: BoundRootField,
        dependency_table_bindings: Vec<(TableId, String)>,
        root_subset_refresh: Option<RowsSnapshotRootSubsetPlan>,
    ) -> Self {
        Self {
            descriptor: LivePlanDescriptor {
                engine: LiveEngineKind::Snapshot,
                output: LiveOutputKind::GraphqlSnapshot,
                dependencies,
            },
            kernel: KernelPlan::Snapshot(SnapshotKernelPlan {
                compiled_plan,
                initial_rows,
                initial_summary,
            }),
            adapter: AdapterPlan::GraphqlSnapshot(GraphqlSnapshotAdapterPlan {
                catalog,
                field,
                dependency_table_bindings,
                root_subset_refresh,
            }),
        }
    }

    pub fn graphql_delta(
        dependencies: LiveDependencySet,
        dataflow: DataflowNode,
        compiled_ivm_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial_rows: Vec<Rc<Row>>,
        source_bindings: Vec<TraceBootstrapSourceBinding>,
        catalog: GraphqlCatalog,
        field: BoundRootField,
        dependency_table_bindings: Vec<(TableId, String)>,
    ) -> Self {
        Self {
            descriptor: LivePlanDescriptor {
                engine: LiveEngineKind::Delta,
                output: LiveOutputKind::GraphqlDelta,
                dependencies,
            },
            kernel: KernelPlan::Delta(DeltaKernelPlan {
                dataflow,
                compiled_ivm_plan,
                compiled_bootstrap_plan,
                initial_rows,
                source_bindings,
                trace_init_profile: None,
            }),
            adapter: AdapterPlan::GraphqlDelta(GraphqlDeltaAdapterPlan {
                catalog,
                field,
                dependency_table_bindings,
            }),
        }
    }

    pub fn materialize_rows_snapshot(
        self,
        cache: Rc<RefCell<TableCache>>,
        registry: Rc<RefCell<LiveRegistry>>,
    ) -> JsObservableQuery {
        let dependencies = self.descriptor.dependencies;
        let kernel = match self.kernel {
            KernelPlan::Snapshot(plan) => plan,
            KernelPlan::Delta(_) => {
                unreachable!("rows snapshot live plans must use snapshot kernel")
            }
        };
        let adapter = match self.adapter {
            AdapterPlan::RowsSnapshot(plan) => plan,
            AdapterPlan::RowsDelta(_)
            | AdapterPlan::GraphqlSnapshot(_)
            | AdapterPlan::GraphqlDelta(_) => {
                unreachable!("rows snapshot live plans must use rows snapshot adapters")
            }
        };

        let observable = Rc::new(RefCell::new(ReQueryObservable::new_with_summary(
            kernel.compiled_plan,
            cache,
            kernel.initial_rows,
            kernel.initial_summary,
            adapter.dependency_table_bindings,
            adapter.partial_refresh,
            adapter.root_subset_refresh,
        )));
        registry.borrow_mut().register_snapshot(
            SnapshotSubscription::Rows(observable.clone()),
            &dependencies,
        );
        adapter
            .projection
            .into_snapshot_js(observable, adapter.binary_layout)
    }

    pub fn materialize_rows_delta(
        self,
        cache: Rc<RefCell<TableCache>>,
        registry: Rc<RefCell<LiveRegistry>>,
    ) -> JsIvmObservableQuery {
        let dependencies = self.descriptor.dependencies;
        let kernel = match self.kernel {
            KernelPlan::Delta(plan) => plan,
            KernelPlan::Snapshot(_) => unreachable!("rows delta live plans must use delta kernel"),
        };
        let adapter = match self.adapter {
            AdapterPlan::RowsDelta(plan) => plan,
            AdapterPlan::RowsSnapshot(_)
            | AdapterPlan::GraphqlSnapshot(_)
            | AdapterPlan::GraphqlDelta(_) => {
                unreachable!("rows delta live plans must use rows delta adapters")
            }
        };

        let bridge_profiler = registry.borrow().ivm_bridge_profiler();
        let mut trace_init_profile = kernel.trace_init_profile.unwrap_or_default();
        trace_init_profile.source_table_count = kernel
            .source_bindings
            .iter()
            .map(|binding| binding.table_id)
            .collect::<HashSet<_>>()
            .len();
        trace_init_profile.initial_row_count = kernel.initial_rows.len();
        let init_started_at = now_ms();
        let cache_for_loader = cache.clone();
        let source_bindings = kernel.source_bindings.clone();
        let source_filter_coverage: Vec<bool> = kernel
            .source_bindings
            .iter()
            .map(|binding| binding.covers_source_filter)
            .collect();
        let source_access_ms = Rc::new(RefCell::new(0.0));
        let source_emit_ms = Rc::new(RefCell::new(0.0));
        let source_access_ms_ref = source_access_ms.clone();
        let source_emit_ms_ref = source_emit_ms.clone();
        #[cfg(feature = "benchmark")]
        let bootstrap_now_fn = Some(now_ms as fn() -> f64);
        #[cfg(not(feature = "benchmark"))]
        let bootstrap_now_fn = None;
        let (observable, bootstrap_profile) =
            ObservableQuery::with_compiled_source_slot_visitor_profiled_with_filter_coverage(
                kernel.dataflow,
                kernel.compiled_ivm_plan,
                kernel.compiled_bootstrap_plan,
                kernel.initial_rows,
                move |table_id, source_index, arena, slot| {
                    let load_started_at = bootstrap_now_fn.map(|now_fn| now_fn());
                    let handle_wrap_ms = {
                        let cache = cache_for_loader.borrow();
                        let Some(binding) = source_bindings.get(source_index) else {
                            return;
                        };
                        debug_assert_eq!(binding.source_index, source_index);
                        debug_assert_eq!(binding.table_id, table_id);
                        visit_trace_bootstrap_binding_rows_into_slot(
                            &cache,
                            binding,
                            arena,
                            slot,
                            bootstrap_now_fn,
                        )
                    };
                    if let (Some(now_fn), Some(started_at)) = (bootstrap_now_fn, load_started_at) {
                        let total_ms = now_fn() - started_at;
                        *source_emit_ms_ref.borrow_mut() += handle_wrap_ms;
                        *source_access_ms_ref.borrow_mut() += (total_ms - handle_wrap_ms).max(0.0);
                    }
                },
                Some(source_filter_coverage),
                bootstrap_now_fn,
            );
        let observable = Rc::new(RefCell::new(observable));
        trace_init_profile.source_access_ms = *source_access_ms.borrow();
        trace_init_profile.source_emit_ms = *source_emit_ms.borrow();
        trace_init_profile.source_bootstrap_ms =
            trace_init_profile.source_access_ms + trace_init_profile.source_emit_ms;
        trace_init_profile.bootstrap_scan_ms = trace_init_profile.source_bootstrap_ms;
        trace_init_profile.materialized_view_init_ms = now_ms() - init_started_at;
        trace_init_profile.source_visit_ms = bootstrap_profile.source_visit_ms;
        trace_init_profile.source_handle_wrap_ms =
            trace_init_profile.source_emit_ms + bootstrap_profile.source_handle_wrap_ms;
        trace_init_profile.bootstrap_runtime_node_ms = bootstrap_profile.bootstrap_runtime_node_ms;
        trace_init_profile.bootstrap_slot_ms = bootstrap_profile.bootstrap_slot_ms;
        trace_init_profile.filter_bootstrap_ms = bootstrap_profile.filter_bootstrap_ms;
        trace_init_profile.project_bootstrap_ms = bootstrap_profile.project_bootstrap_ms;
        trace_init_profile.map_bootstrap_ms = bootstrap_profile.map_bootstrap_ms;
        trace_init_profile.join_build_ms = bootstrap_profile.join_build_ms;
        trace_init_profile.join_finalize_ms = bootstrap_profile.join_finalize_ms;
        trace_init_profile.join_emit_ms = bootstrap_profile.join_emit_ms;
        trace_init_profile.join_bootstrap_ms = bootstrap_profile.join_bootstrap_ms;
        trace_init_profile.aggregate_bootstrap_ms = bootstrap_profile.aggregate_bootstrap_ms;
        trace_init_profile.root_sink_ms = bootstrap_profile.root_sink_ms;
        trace_init_profile.visible_store_init_ms = bootstrap_profile.visible_store_init_ms;
        trace_init_profile.bootstrap_execute_ms = (trace_init_profile.materialized_view_init_ms
            - trace_init_profile.source_bootstrap_ms)
            .max(0.0);
        trace_init_profile.total_ms += trace_init_profile.materialized_view_init_ms;
        registry
            .borrow()
            .record_trace_init_profile(trace_init_profile.clone());
        registry
            .borrow_mut()
            .register_delta(DeltaSubscription::Rows(observable.clone()), &dependencies);
        adapter
            .projection
            .into_delta_js(observable, adapter.binary_layout, bridge_profiler)
    }

    pub fn materialize_graphql_snapshot(
        self,
        cache: Rc<RefCell<TableCache>>,
        registry: Rc<RefCell<LiveRegistry>>,
    ) -> JsGraphqlSubscription {
        let dependencies = self.descriptor.dependencies;
        let kernel = match self.kernel {
            KernelPlan::Snapshot(plan) => plan,
            KernelPlan::Delta(_) => {
                unreachable!("GraphQL snapshot live plans must use snapshot kernel")
            }
        };
        let adapter = match self.adapter {
            AdapterPlan::GraphqlSnapshot(plan) => plan,
            AdapterPlan::RowsSnapshot(_)
            | AdapterPlan::RowsDelta(_)
            | AdapterPlan::GraphqlDelta(_) => {
                unreachable!("GraphQL snapshot live plans must use GraphQL snapshot adapters")
            }
        };

        let root_table_ids = dependencies.root_tables.iter().copied().collect();
        let observable = Rc::new(RefCell::new(GraphqlSubscriptionObservable::new(
            kernel.compiled_plan,
            cache,
            adapter.catalog,
            adapter.field,
            adapter.dependency_table_bindings,
            adapter.root_subset_refresh,
            root_table_ids,
            kernel.initial_rows,
            kernel.initial_summary,
        )));
        registry.borrow_mut().register_snapshot(
            SnapshotSubscription::Graphql(observable.clone()),
            &dependencies,
        );
        JsGraphqlSubscription::new_snapshot(observable)
    }

    pub fn materialize_graphql_delta(
        self,
        cache: Rc<RefCell<TableCache>>,
        registry: Rc<RefCell<LiveRegistry>>,
    ) -> JsGraphqlSubscription {
        let dependencies = self.descriptor.dependencies;
        let kernel = match self.kernel {
            KernelPlan::Delta(plan) => plan,
            KernelPlan::Snapshot(_) => {
                unreachable!("GraphQL delta live plans must use delta kernel")
            }
        };
        let adapter = match self.adapter {
            AdapterPlan::GraphqlDelta(plan) => plan,
            AdapterPlan::RowsSnapshot(_)
            | AdapterPlan::RowsDelta(_)
            | AdapterPlan::GraphqlSnapshot(_) => {
                unreachable!("GraphQL delta live plans must use GraphQL delta adapters")
            }
        };

        let cache_for_loader = cache.clone();
        let source_bindings = kernel.source_bindings.clone();
        let observable = Rc::new(RefCell::new(
            GraphqlDeltaObservable::new_with_compiled_source_visitor(
                kernel.dataflow,
                kernel.compiled_ivm_plan,
                kernel.compiled_bootstrap_plan,
                cache,
                adapter.catalog,
                adapter.field,
                adapter.dependency_table_bindings,
                kernel.initial_rows,
                move |table_id, source_index, emit| {
                    let cache_ref = cache_for_loader.borrow();
                    let Some(binding) = source_bindings.get(source_index) else {
                        return;
                    };
                    debug_assert_eq!(binding.source_index, source_index);
                    debug_assert_eq!(binding.table_id, table_id);
                    visit_trace_bootstrap_binding_rows(&cache_ref, binding, emit);
                },
            ),
        ));
        registry.borrow_mut().register_delta(
            DeltaSubscription::Graphql(observable.clone()),
            &dependencies,
        );
        JsGraphqlSubscription::new_delta(observable)
    }
}

#[derive(Clone)]
pub(crate) enum SnapshotSubscription {
    Rows(Rc<RefCell<ReQueryObservable>>),
    Graphql(Rc<RefCell<GraphqlSubscriptionObservable>>),
}

impl SnapshotSubscription {
    fn subscription_count(&self) -> usize {
        match self {
            Self::Rows(query) => query.borrow().subscription_count(),
            Self::Graphql(query) => query.borrow().subscription_count(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum DeltaSubscription {
    Rows(Rc<RefCell<ObservableQuery>>),
    Graphql(Rc<RefCell<GraphqlDeltaObservable>>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeltaSubscriptionKind {
    Rows,
    Graphql,
}

impl DeltaSubscription {
    fn subscription_count(&self) -> usize {
        match self {
            Self::Rows(query) => query.borrow().subscription_count(),
            Self::Graphql(query) => query.borrow().subscription_count(),
        }
    }

    fn kind(&self) -> DeltaSubscriptionKind {
        match self {
            Self::Rows(_) => DeltaSubscriptionKind::Rows,
            Self::Graphql(_) => DeltaSubscriptionKind::Graphql,
        }
    }

    #[allow(dead_code)]
    fn on_table_change(&self, table_id: TableId, deltas: Vec<Delta<Row>>) {
        match self {
            Self::Rows(query) => query.borrow_mut().on_table_change(table_id, deltas),
            Self::Graphql(query) => query.borrow_mut().on_table_change(table_id, deltas),
        }
    }

    #[cfg(feature = "benchmark")]
    fn on_table_change_profiled(
        &self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
    ) -> DeltaQueryUpdateProfile {
        match self {
            Self::Rows(query) => DeltaQueryUpdateProfile::from_trace(
                query
                    .borrow_mut()
                    .on_table_change_profiled(table_id, deltas, Some(now_ms)),
            ),
            Self::Graphql(query) => DeltaQueryUpdateProfile::from_graphql(
                query
                    .borrow_mut()
                    .on_table_change_profiled(table_id, deltas),
            ),
        }
    }
}

fn dispatch_delta_query(
    query: &DeltaSubscription,
    table_id: TableId,
    deltas: Vec<Delta<Row>>,
    profile: &mut DeltaFlushProfile,
) {
    profile.delta_query_count += 1;
    match query.kind() {
        DeltaSubscriptionKind::Rows => profile.rows_query_count += 1,
        DeltaSubscriptionKind::Graphql => profile.graphql_query_count += 1,
    }

    let query_started_at = now_ms();
    #[cfg(feature = "benchmark")]
    let update_profile = query.on_table_change_profiled(table_id, deltas);
    #[cfg(not(feature = "benchmark"))]
    query.on_table_change(table_id, deltas);
    profile.query_on_table_change_ms += now_ms() - query_started_at;
    #[cfg(feature = "benchmark")]
    {
        profile.source_dispatch_ms += update_profile.source_dispatch_ms;
        profile.unary_execute_ms += update_profile.unary_execute_ms;
        profile.join_execute_ms += update_profile.join_execute_ms;
        profile.aggregate_execute_ms += update_profile.aggregate_execute_ms;
        profile.result_apply_ms += update_profile.result_apply_ms;
        profile.graphql_view_update_ms += update_profile.graphql_view_update_ms;
        profile.graphql_invalidation_ms += update_profile.graphql_invalidation_ms;
        profile.graphql_render_ms += update_profile.graphql_render_ms;
        profile.graphql_encode_ms += update_profile.graphql_encode_ms;
        profile.graphql_emit_ms += update_profile.graphql_emit_ms;
    }
}

#[cfg(feature = "benchmark")]
#[derive(Clone, Debug, Default)]
struct DeltaQueryUpdateProfile {
    source_dispatch_ms: f64,
    unary_execute_ms: f64,
    join_execute_ms: f64,
    aggregate_execute_ms: f64,
    result_apply_ms: f64,
    graphql_view_update_ms: f64,
    graphql_invalidation_ms: f64,
    graphql_render_ms: f64,
    graphql_encode_ms: f64,
    graphql_emit_ms: f64,
}

#[cfg(feature = "benchmark")]
impl DeltaQueryUpdateProfile {
    fn from_trace(profile: TraceUpdateProfile) -> Self {
        Self {
            source_dispatch_ms: profile.source_dispatch_ms,
            unary_execute_ms: profile.unary_execute_ms,
            join_execute_ms: profile.join_execute_ms,
            aggregate_execute_ms: profile.aggregate_execute_ms,
            result_apply_ms: profile.result_apply_ms,
            ..Self::default()
        }
    }

    fn from_graphql(profile: GraphqlDeltaProfile) -> Self {
        Self {
            graphql_view_update_ms: profile.view_update_ms,
            graphql_invalidation_ms: profile.invalidation_ms,
            graphql_render_ms: profile.render_ms,
            graphql_encode_ms: profile.encode_ms,
            graphql_emit_ms: profile.emit_ms,
            ..Self::default()
        }
    }
}

pub(crate) struct LiveRegistry {
    snapshot_queries: HashMap<TableId, Vec<SnapshotSubscription>>,
    delta_queries: HashMap<TableId, Vec<DeltaSubscription>>,
    pending_changes: Rc<RefCell<HashMap<TableId, HashSet<u64>>>>,
    pending_deltas: Rc<RefCell<HashMap<TableId, Vec<Delta<Row>>>>>,
    flush_scheduled: Rc<RefCell<bool>>,
    self_ref: Option<Rc<RefCell<LiveRegistry>>>,
    last_trace_init_profile: RefCell<Option<TraceInitProfile>>,
    #[cfg(feature = "benchmark")]
    last_snapshot_init_profile: RefCell<Option<SnapshotInitProfile>>,
    last_delta_flush_profile: RefCell<Option<DeltaFlushProfile>>,
    last_snapshot_flush_profile: RefCell<Option<SnapshotFlushProfile>>,
    ivm_bridge_profiler: Rc<RefCell<IvmBridgeProfiler>>,
    #[cfg(target_arch = "wasm32")]
    flush_closure: Option<Closure<dyn FnMut(JsValue)>>,
}

impl LiveRegistry {
    pub fn new() -> Self {
        Self {
            snapshot_queries: HashMap::new(),
            delta_queries: HashMap::new(),
            pending_changes: Rc::new(RefCell::new(HashMap::new())),
            pending_deltas: Rc::new(RefCell::new(HashMap::new())),
            flush_scheduled: Rc::new(RefCell::new(false)),
            self_ref: None,
            last_trace_init_profile: RefCell::new(None),
            #[cfg(feature = "benchmark")]
            last_snapshot_init_profile: RefCell::new(None),
            last_delta_flush_profile: RefCell::new(None),
            last_snapshot_flush_profile: RefCell::new(None),
            ivm_bridge_profiler: Rc::new(RefCell::new(IvmBridgeProfiler::default())),
            #[cfg(target_arch = "wasm32")]
            flush_closure: None,
        }
    }

    pub fn set_self_ref(&mut self, self_ref: Rc<RefCell<LiveRegistry>>) {
        self.self_ref = Some(self_ref);
    }

    pub fn register_snapshot(
        &mut self,
        query: SnapshotSubscription,
        dependencies: &LiveDependencySet,
    ) {
        for &table_id in &dependencies.tables {
            self.snapshot_queries
                .entry(table_id)
                .or_insert_with(Vec::new)
                .push(query.clone());
        }
    }

    pub fn register_delta(&mut self, query: DeltaSubscription, dependencies: &LiveDependencySet) {
        for &table_id in &dependencies.tables {
            self.delta_queries
                .entry(table_id)
                .or_insert_with(Vec::new)
                .push(query.clone());
        }
    }

    fn has_active_snapshot_queries(&self, table_id: TableId) -> bool {
        self.snapshot_queries
            .get(&table_id)
            .map(|queries| queries.iter().any(|query| query.subscription_count() > 0))
            .unwrap_or(false)
    }

    fn flush_snapshot_lane(
        &self,
        changes: HashMap<TableId, HashSet<u64>>,
        delta_changes: &HashMap<TableId, Vec<Delta<Row>>>,
    ) {
        let started_at = now_ms();
        let mut profile = SnapshotFlushProfile {
            changed_table_count: changes.len(),
            ..SnapshotFlushProfile::default()
        };
        let mut merged_rows: HashMap<
            usize,
            (
                Rc<RefCell<ReQueryObservable>>,
                HashMap<TableId, HashSet<u64>>,
            ),
        > = HashMap::new();
        let mut merged_graphql: HashMap<
            usize,
            (
                Rc<RefCell<GraphqlSubscriptionObservable>>,
                HashMap<TableId, HashSet<u64>>,
            ),
        > = HashMap::new();

        let merge_started_at = now_ms();
        for (table_id, changed_ids) in changes {
            if let Some(queries) = self.snapshot_queries.get(&table_id) {
                for query in queries {
                    match query {
                        SnapshotSubscription::Rows(query) => {
                            let entry = merged_rows
                                .entry(Rc::as_ptr(query) as usize)
                                .or_insert_with(|| (query.clone(), HashMap::new()));
                            entry
                                .1
                                .entry(table_id)
                                .or_insert_with(HashSet::new)
                                .extend(changed_ids.iter().copied());
                        }
                        SnapshotSubscription::Graphql(query) => {
                            let entry = merged_graphql
                                .entry(Rc::as_ptr(query) as usize)
                                .or_insert_with(|| (query.clone(), HashMap::new()));
                            entry.1.insert(table_id, changed_ids.clone());
                        }
                    }
                }
            }
        }
        profile.invalidation_merge_ms = now_ms() - merge_started_at;

        for (_, (query, changes)) in merged_rows {
            let query_started_at = now_ms();
            let sample = query
                .borrow_mut()
                .on_change_with_deltas_profiled(&changes, delta_changes);
            profile.rows_query_on_change_ms += now_ms() - query_started_at;
            profile.record_rows_query(&sample);
        }

        for (_, (query, changes)) in merged_graphql {
            let query_started_at = now_ms();
            #[cfg(feature = "benchmark")]
            let sample = query
                .borrow_mut()
                .on_change_with_deltas_profiled(&changes, delta_changes);
            #[cfg(not(feature = "benchmark"))]
            query
                .borrow_mut()
                .on_change_with_deltas(&changes, delta_changes);
            profile.graphql_query_count += 1;
            profile.graphql_query_on_change_ms += now_ms() - query_started_at;
            #[cfg(feature = "benchmark")]
            {
                profile.graphql_root_refresh_ms += sample.root_refresh_ms;
                profile.graphql_batch_invalidation_ms += sample.batch_invalidation_ms;
                profile.graphql_render_ms += sample.render_ms;
                profile.graphql_encode_ms += sample.encode_ms;
                profile.graphql_emit_ms += sample.emit_ms;
            }
        }

        profile.total_ms = now_ms() - started_at;
        *self.last_snapshot_flush_profile.borrow_mut() = Some(profile);
    }

    pub fn on_table_change(&mut self, table_id: TableId, changed_ids: &HashSet<u64>) {
        {
            let mut pending = self.pending_changes.borrow_mut();
            pending
                .entry(table_id)
                .or_insert_with(HashSet::new)
                .extend(changed_ids.iter().copied());
        }

        let mut scheduled = self.flush_scheduled.borrow_mut();
        if !*scheduled {
            *scheduled = true;
            drop(scheduled);
            self.schedule_flush();
        }
    }

    pub fn on_table_change_delta(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
        changed_ids: &HashSet<u64>,
    ) {
        {
            let mut pending = self.pending_deltas.borrow_mut();
            pending
                .entry(table_id)
                .or_insert_with(Vec::new)
                .extend(deltas);
        }

        {
            let mut pending = self.pending_changes.borrow_mut();
            pending
                .entry(table_id)
                .or_insert_with(HashSet::new)
                .extend(changed_ids.iter().copied());
        }

        let mut scheduled = self.flush_scheduled.borrow_mut();
        if !*scheduled {
            *scheduled = true;
            drop(scheduled);
            self.schedule_flush();
        }
    }

    fn flush_delta_lane(&self, delta_changes: &mut HashMap<TableId, Vec<Delta<Row>>>) {
        let started_at = now_ms();
        let mut profile = DeltaFlushProfile::default();
        self.ivm_bridge_profiler.borrow_mut().begin_flush();

        let table_ids: Vec<_> = delta_changes.keys().copied().collect();
        for table_id in table_ids {
            let Some(deltas) = delta_changes.get(&table_id) else {
                continue;
            };
            profile.delta_table_count += 1;
            profile.delta_row_count += deltas.len();
            let Some(queries) = self.delta_queries.get(&table_id) else {
                continue;
            };
            let active_queries = queries
                .iter()
                .filter(|query| query.subscription_count() > 0)
                .collect::<Vec<_>>();
            if active_queries.is_empty() {
                continue;
            }

            if !self.has_active_snapshot_queries(table_id) {
                let mut owned_deltas = delta_changes.remove(&table_id).unwrap_or_default();
                let last_index = active_queries.len().saturating_sub(1);
                for (index, query) in active_queries.iter().enumerate() {
                    let query_deltas = if index == last_index {
                        core::mem::take(&mut owned_deltas)
                    } else {
                        let clone_started_at = now_ms();
                        let cloned = owned_deltas.clone();
                        profile.clone_ms += now_ms() - clone_started_at;
                        cloned
                    };
                    dispatch_delta_query(query, table_id, query_deltas, &mut profile);
                }
            } else {
                for query in active_queries {
                    let clone_started_at = now_ms();
                    let deltas_clone = deltas.clone();
                    profile.clone_ms += now_ms() - clone_started_at;
                    dispatch_delta_query(query, table_id, deltas_clone, &mut profile);
                }
            }
        }

        self.ivm_bridge_profiler.borrow_mut().end_flush();
        profile.total_ms = now_ms() - started_at;
        *self.last_delta_flush_profile.borrow_mut() = Some(profile);
    }

    fn schedule_flush(&mut self) {
        #[cfg(target_arch = "wasm32")]
        {
            if self.flush_closure.is_none() {
                if let Some(ref self_ref) = self.self_ref {
                    let self_ref_clone = self_ref.clone();
                    let pending_changes = self.pending_changes.clone();
                    let pending_deltas = self.pending_deltas.clone();
                    let flush_scheduled = self.flush_scheduled.clone();

                    self.flush_closure = Some(Closure::new(move |_: JsValue| {
                        *flush_scheduled.borrow_mut() = false;

                        let mut delta_changes: HashMap<TableId, Vec<Delta<Row>>> =
                            pending_deltas.borrow_mut().drain().collect();
                        let changes: HashMap<TableId, HashSet<u64>> =
                            pending_changes.borrow_mut().drain().collect();

                        {
                            let registry = self_ref_clone.borrow();
                            registry.flush_delta_lane(&mut delta_changes);
                            registry.flush_snapshot_lane(changes, &delta_changes);
                        }

                        {
                            let mut registry = self_ref_clone.borrow_mut();
                            registry.gc_dead_queries();
                        }
                    }));
                }
            }

            if let Some(ref closure) = self.flush_closure {
                let promise = js_sys::Promise::resolve(&JsValue::UNDEFINED);
                let _ = promise.then(closure);
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            self.flush_sync();
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn flush_sync(&mut self) {
        *self.flush_scheduled.borrow_mut() = false;

        let mut delta_changes: HashMap<TableId, Vec<Delta<Row>>> =
            self.pending_deltas.borrow_mut().drain().collect();
        self.flush_delta_lane(&mut delta_changes);

        let changes: HashMap<TableId, HashSet<u64>> =
            self.pending_changes.borrow_mut().drain().collect();
        self.flush_snapshot_lane(changes, &delta_changes);

        self.gc_dead_queries();
    }

    #[allow(dead_code)]
    pub fn flush(&mut self) {
        *self.flush_scheduled.borrow_mut() = false;

        let mut delta_changes: HashMap<TableId, Vec<Delta<Row>>> =
            self.pending_deltas.borrow_mut().drain().collect();
        self.flush_delta_lane(&mut delta_changes);

        let changes: HashMap<TableId, HashSet<u64>> =
            self.pending_changes.borrow_mut().drain().collect();
        self.flush_snapshot_lane(changes, &delta_changes);

        self.gc_dead_queries();
    }

    fn gc_dead_queries(&mut self) {
        for queries in self.snapshot_queries.values_mut() {
            queries.retain(|query| query.subscription_count() > 0);
        }
        self.snapshot_queries
            .retain(|_, queries| !queries.is_empty());

        for queries in self.delta_queries.values_mut() {
            queries.retain(|query| query.subscription_count() > 0);
        }
        self.delta_queries.retain(|_, queries| !queries.is_empty());
    }

    #[allow(dead_code)]
    pub fn query_count(&self) -> usize {
        let snapshot_count: usize = self
            .snapshot_queries
            .values()
            .map(|queries| queries.len())
            .sum();
        let delta_count: usize = self
            .delta_queries
            .values()
            .map(|queries| queries.len())
            .sum();
        snapshot_count + delta_count
    }

    pub fn take_last_delta_flush_profile(&self) -> Option<DeltaFlushProfile> {
        self.last_delta_flush_profile.borrow_mut().take()
    }

    pub fn record_trace_init_profile(&self, profile: TraceInitProfile) {
        *self.last_trace_init_profile.borrow_mut() = Some(profile);
    }

    pub fn take_last_trace_init_profile(&self) -> Option<TraceInitProfile> {
        self.last_trace_init_profile.borrow_mut().take()
    }

    #[cfg(feature = "benchmark")]
    pub fn record_snapshot_init_profile(&self, profile: SnapshotInitProfile) {
        *self.last_snapshot_init_profile.borrow_mut() = Some(profile);
    }

    #[cfg(feature = "benchmark")]
    pub fn take_last_snapshot_init_profile(&self) -> Option<SnapshotInitProfile> {
        self.last_snapshot_init_profile.borrow_mut().take()
    }

    pub fn take_last_snapshot_flush_profile(&self) -> Option<SnapshotFlushProfile> {
        self.last_snapshot_flush_profile.borrow_mut().take()
    }

    pub fn take_last_ivm_bridge_profile(&self) -> Option<IvmBridgeProfile> {
        self.ivm_bridge_profiler.borrow_mut().take_last()
    }

    pub fn ivm_bridge_profiler(&self) -> Rc<RefCell<IvmBridgeProfiler>> {
        self.ivm_bridge_profiler.clone()
    }

    #[allow(dead_code)]
    pub fn has_pending_changes(&self) -> bool {
        !self.pending_changes.borrow().is_empty() || !self.pending_deltas.borrow().is_empty()
    }
}

impl Default for LiveRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, JsonbValue, Row, Value};
    use cynos_query::ast::{Expr, JoinType};
    use cynos_storage::TableCache;

    fn jsonb_value(json: &str) -> Value {
        Value::Jsonb(JsonbValue::new(json.as_bytes().to_vec()))
    }

    fn build_trace_source_test_schemas() -> HashMap<String, Table> {
        let users = TableBuilder::new("users")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("team_id", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_users_team_id", &["team_id"], false)
            .unwrap()
            .build()
            .unwrap();

        let issues = TableBuilder::new("issues")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("user_id", DataType::Int64)
            .unwrap()
            .add_column("metadata", DataType::Jsonb)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_jsonb_index("idx_issues_metadata_status", "metadata", &["status"])
            .unwrap()
            .build()
            .unwrap();

        let mut schemas = HashMap::new();
        schemas.insert("users".into(), users);
        schemas.insert("issues".into(), issues);
        schemas
    }

    #[test]
    fn test_delta_flush_moves_single_delta_query_without_clone_when_snapshot_does_not_need_deltas()
    {
        let mut registry = LiveRegistry::new();
        let query = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));
        let observed = Rc::new(RefCell::new(0usize));
        {
            let observed = observed.clone();
            query.borrow_mut().subscribe_raw_deltas(move |deltas| {
                *observed.borrow_mut() += deltas.len();
            });
        }
        registry.register_delta(
            DeltaSubscription::Rows(query),
            &LiveDependencySet::new(vec![1], Vec::new()),
        );

        let mut delta_changes = HashMap::new();
        delta_changes.insert(1, vec![Delta::insert(Row::new(1, vec![Value::Int64(10)]))]);
        registry.flush_delta_lane(&mut delta_changes);

        let profile = registry.take_last_delta_flush_profile().unwrap();
        assert_eq!(profile.delta_table_count, 1);
        assert_eq!(profile.delta_query_count, 1);
        assert_eq!(profile.clone_ms, 0.0);
        assert_eq!(*observed.borrow(), 1);
        assert!(!delta_changes.contains_key(&1));
    }

    #[test]
    fn test_collect_trace_bootstrap_source_bindings_preserves_source_order_and_access_paths() {
        let table_schemas = build_trace_source_test_schemas();
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);
        table_ids.insert("issues".into(), 2u32);

        let users_pk = table_schemas
            .get("users")
            .and_then(Table::primary_key)
            .map(|index| index.name().to_string())
            .unwrap();

        let plan = PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::IndexGet {
                table: "users".into(),
                index: users_pk,
                key: Value::Int64(7),
                limit: None,
            }),
            right: Box::new(PhysicalPlan::GinIndexScan {
                table: "issues".into(),
                index: "idx_issues_metadata_status".into(),
                key: "status".into(),
                value: Some("open".into()),
                query_type: "eq".into(),
                recheck: Some(Expr::eq(
                    Expr::Function {
                        name: "JSONB_EXTRACT".into(),
                        args: alloc::vec![
                            Expr::column("issues", "metadata", 2),
                            Expr::literal("$.status"),
                        ],
                    },
                    Expr::literal("open"),
                )),
            }),
            condition: Expr::eq(
                Expr::column("users", "id", 0),
                Expr::column("issues", "user_id", 1),
            ),
            join_type: JoinType::Inner,
            output_tables: alloc::vec!["users".into(), "issues".into()],
        };

        let bindings = collect_trace_bootstrap_source_bindings(&plan, &table_ids, &table_schemas);
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].source_index, 0);
        assert_eq!(bindings[0].table_id, 1);
        assert_eq!(bindings[0].table_name, "users");
        assert!(matches!(
            bindings[0].access_path,
            TraceBootstrapAccessPath::IndexGet { .. }
        ));
        assert!(bindings[0].covers_source_filter);
        assert_eq!(bindings[1].source_index, 1);
        assert_eq!(bindings[1].table_id, 2);
        assert_eq!(bindings[1].table_name, "issues");
        assert!(matches!(
            bindings[1].access_path,
            TraceBootstrapAccessPath::GinKeyValue { .. }
        ));
        assert!(bindings[1].covers_source_filter);
    }

    #[test]
    fn test_collect_trace_bootstrap_source_bindings_keeps_per_source_bindings_for_same_table() {
        let table_schemas = build_trace_source_test_schemas();
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);

        let users_pk = table_schemas
            .get("users")
            .and_then(Table::primary_key)
            .map(|index| index.name().to_string())
            .unwrap();

        let plan = PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::TableScan {
                table: "users".into(),
            }),
            right: Box::new(PhysicalPlan::IndexGet {
                table: "users".into(),
                index: users_pk,
                key: Value::Int64(42),
                limit: None,
            }),
            condition: Expr::eq(
                Expr::column("users", "id", 0),
                Expr::column("users", "team_id", 1),
            ),
            join_type: JoinType::Inner,
            output_tables: alloc::vec!["users".into(), "users".into()],
        };

        let bindings = collect_trace_bootstrap_source_bindings(&plan, &table_ids, &table_schemas);
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].table_id, 1);
        assert_eq!(bindings[1].table_id, 1);
        assert_eq!(bindings[0].source_index, 0);
        assert_eq!(bindings[1].source_index, 1);
        assert!(matches!(
            bindings[0].access_path,
            TraceBootstrapAccessPath::FullScan
        ));
        assert!(!bindings[0].covers_source_filter);
        assert!(matches!(
            bindings[1].access_path,
            TraceBootstrapAccessPath::IndexGet { .. }
        ));
        assert!(bindings[1].covers_source_filter);
    }

    #[test]
    fn test_collect_trace_bootstrap_source_bindings_does_not_mark_index_join_fallback_scan_as_covered(
    ) {
        let table_schemas = build_trace_source_test_schemas();
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);

        let users_pk = table_schemas
            .get("users")
            .and_then(Table::primary_key)
            .map(|index| index.name().to_string())
            .unwrap();

        let plan = PhysicalPlan::IndexNestedLoopJoin {
            outer: Box::new(PhysicalPlan::IndexGet {
                table: "users".into(),
                index: users_pk,
                key: Value::Int64(42),
                limit: None,
            }),
            inner_table: "users".into(),
            inner_index: "idx_users_team_id".into(),
            condition: Expr::eq(
                Expr::column("users", "id", 0),
                Expr::column("users", "team_id", 1),
            ),
            join_type: JoinType::Inner,
            outer_is_left: true,
            output_tables: alloc::vec!["users".into(), "users".into()],
        };

        let bindings = collect_trace_bootstrap_source_bindings(&plan, &table_ids, &table_schemas);
        assert_eq!(bindings.len(), 2);
        assert!(matches!(
            bindings[0].access_path,
            TraceBootstrapAccessPath::IndexGet { .. }
        ));
        assert!(bindings[0].covers_source_filter);
        assert!(matches!(
            bindings[1].access_path,
            TraceBootstrapAccessPath::FullScan
        ));
        assert!(!bindings[1].covers_source_filter);
    }

    #[test]
    fn test_visit_trace_bootstrap_binding_rows_uses_index_and_gin_access_paths() {
        let mut cache = TableCache::new();
        for schema in build_trace_source_test_schemas().into_values() {
            cache.create_table(schema).unwrap();
        }

        let users_pk = cache
            .get_table("users")
            .and_then(|store| store.schema().primary_key())
            .map(|index| index.name().to_string())
            .unwrap();

        {
            let store = cache.get_table_mut("users").unwrap();
            store
                .insert(Row::new(1, alloc::vec![Value::Int64(1), Value::Int64(10)]))
                .unwrap();
            store
                .insert(Row::new(2, alloc::vec![Value::Int64(2), Value::Int64(20)]))
                .unwrap();
        }

        {
            let store = cache.get_table_mut("issues").unwrap();
            store
                .insert(Row::new(
                    11,
                    alloc::vec![
                        Value::Int64(11),
                        Value::Int64(1),
                        jsonb_value("{\"status\":\"open\"}"),
                    ],
                ))
                .unwrap();
            store
                .insert(Row::new(
                    12,
                    alloc::vec![
                        Value::Int64(12),
                        Value::Int64(2),
                        jsonb_value("{\"status\":\"closed\"}"),
                    ],
                ))
                .unwrap();
        }

        let index_binding = TraceBootstrapSourceBinding {
            source_index: 0,
            table_id: 1,
            table_name: "users".into(),
            access_path: TraceBootstrapAccessPath::IndexGet {
                index: users_pk,
                key: Value::Int64(2),
                limit: None,
            },
            covers_source_filter: true,
        };

        let mut index_rows = Vec::new();
        visit_trace_bootstrap_binding_rows(&cache, &index_binding, &mut |row| {
            index_rows.push(row.id());
        });
        assert_eq!(index_rows, alloc::vec![2]);

        let gin_binding = TraceBootstrapSourceBinding {
            source_index: 1,
            table_id: 2,
            table_name: "issues".into(),
            access_path: TraceBootstrapAccessPath::GinKeyValue {
                index: "idx_issues_metadata_status".into(),
                key: "status".into(),
                value: "open".into(),
            },
            covers_source_filter: true,
        };

        let mut gin_rows = Vec::new();
        visit_trace_bootstrap_binding_rows(&cache, &gin_binding, &mut |row| {
            gin_rows.push(row.id());
        });
        assert_eq!(gin_rows, alloc::vec![11]);
    }
}
