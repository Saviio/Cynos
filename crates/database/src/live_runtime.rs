use crate::binary_protocol::SchemaLayout;
#[cfg(feature = "benchmark")]
use crate::profiling::SnapshotInitProfile;
use crate::profiling::{
    now_ms, DeltaFlushProfile, IvmBridgeProfile, IvmBridgeProfiler, SnapshotFlushProfile,
    TraceInitProfile,
};
use crate::query_engine::{CompiledPhysicalPlan, QueryResultSummary, RootSubsetPlanVariant};
use crate::reactive_bridge::{
    GraphqlDeltaObservable, GraphqlSubscriptionObservable, JsGraphqlSubscription,
    JsIvmObservableQuery, JsObservableQuery, ReQueryObservable,
};
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::schema::Table;
use cynos_core::Row;
use cynos_gql::{bind::BoundRootField, GraphqlCatalog};
use cynos_incremental::{CompiledBootstrapPlan, CompiledIvmPlan, DataflowNode, Delta, TableId};
use cynos_query::ast::SortOrder;
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
    pub source_table_bindings: Vec<(TableId, String)>,
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
        source_table_bindings: Vec<(TableId, String)>,
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
                source_table_bindings,
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
            }),
        }
    }

    pub fn graphql_delta(
        dependencies: LiveDependencySet,
        dataflow: DataflowNode,
        compiled_ivm_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial_rows: Vec<Rc<Row>>,
        source_table_bindings: Vec<(TableId, String)>,
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
                source_table_bindings,
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
        trace_init_profile.source_table_count = kernel.source_table_bindings.len();
        trace_init_profile.initial_row_count = kernel.initial_rows.len();
        let init_started_at = now_ms();
        let cache_for_loader = cache.clone();
        let bindings_by_id: HashMap<TableId, String> =
            kernel.source_table_bindings.iter().cloned().collect();
        let source_bootstrap_ms = Rc::new(RefCell::new(0.0));
        let source_bootstrap_ms_ref = source_bootstrap_ms.clone();
        let observable = Rc::new(RefCell::new(ObservableQuery::with_compiled_source_visitor(
            kernel.dataflow,
            kernel.compiled_ivm_plan,
            kernel.compiled_bootstrap_plan,
            kernel.initial_rows,
            move |table_id, emit| {
                let load_started_at = now_ms();
                {
                    let cache = cache_for_loader.borrow();
                    let Some(table_name) = bindings_by_id.get(&table_id) else {
                        return;
                    };
                    let Some(store) = cache.get_table(table_name) else {
                        return;
                    };
                    for row in store.scan() {
                        emit(row);
                    }
                };
                *source_bootstrap_ms_ref.borrow_mut() += now_ms() - load_started_at;
            },
        )));
        trace_init_profile.source_bootstrap_ms = *source_bootstrap_ms.borrow();
        trace_init_profile.bootstrap_scan_ms = trace_init_profile.source_bootstrap_ms;
        trace_init_profile.materialized_view_init_ms = now_ms() - init_started_at;
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
        let bindings_by_id: HashMap<TableId, String> =
            kernel.source_table_bindings.iter().cloned().collect();
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
                move |table_id, emit| {
                    let cache_ref = cache_for_loader.borrow();
                    let Some(table_name) = bindings_by_id.get(&table_id) else {
                        return;
                    };
                    let Some(store) = cache_ref.get_table(table_name) else {
                        return;
                    };
                    for row in store.scan() {
                        emit(row);
                    }
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

    fn on_table_change(&self, table_id: TableId, deltas: Vec<Delta<Row>>) {
        match self {
            Self::Rows(query) => query.borrow_mut().on_table_change(table_id, deltas),
            Self::Graphql(query) => query.borrow_mut().on_table_change(table_id, deltas),
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

    fn flush_snapshot_lane(&self, changes: HashMap<TableId, HashSet<u64>>) {
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
            let sample = query.borrow_mut().on_change_profiled(&changes);
            profile.rows_query_on_change_ms += now_ms() - query_started_at;
            profile.record_rows_query(&sample);
        }

        for (_, (query, changes)) in merged_graphql {
            let query_started_at = now_ms();
            query.borrow_mut().on_change(&changes);
            profile.graphql_query_count += 1;
            profile.graphql_query_on_change_ms += now_ms() - query_started_at;
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

    fn flush_delta_lane(&self, delta_changes: &HashMap<TableId, Vec<Delta<Row>>>) {
        let started_at = now_ms();
        let mut profile = DeltaFlushProfile::default();
        self.ivm_bridge_profiler.borrow_mut().begin_flush();

        for (table_id, deltas) in delta_changes {
            profile.delta_table_count += 1;
            profile.delta_row_count += deltas.len();
            if let Some(queries) = self.delta_queries.get(table_id) {
                for query in queries {
                    profile.delta_query_count += 1;
                    match query.kind() {
                        DeltaSubscriptionKind::Rows => profile.rows_query_count += 1,
                        DeltaSubscriptionKind::Graphql => profile.graphql_query_count += 1,
                    }

                    let clone_started_at = now_ms();
                    let deltas_clone = deltas.clone();
                    profile.clone_ms += now_ms() - clone_started_at;

                    let query_started_at = now_ms();
                    query.on_table_change(*table_id, deltas_clone);
                    profile.query_on_table_change_ms += now_ms() - query_started_at;
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

                        let delta_changes: HashMap<TableId, Vec<Delta<Row>>> =
                            pending_deltas.borrow_mut().drain().collect();
                        let changes: HashMap<TableId, HashSet<u64>> =
                            pending_changes.borrow_mut().drain().collect();

                        {
                            let registry = self_ref_clone.borrow();
                            registry.flush_delta_lane(&delta_changes);
                            registry.flush_snapshot_lane(changes);
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

        let delta_changes: HashMap<TableId, Vec<Delta<Row>>> =
            self.pending_deltas.borrow_mut().drain().collect();
        self.flush_delta_lane(&delta_changes);

        let changes: HashMap<TableId, HashSet<u64>> =
            self.pending_changes.borrow_mut().drain().collect();
        self.flush_snapshot_lane(changes);

        self.gc_dead_queries();
    }

    #[allow(dead_code)]
    pub fn flush(&mut self) {
        *self.flush_scheduled.borrow_mut() = false;

        let delta_changes: HashMap<TableId, Vec<Delta<Row>>> =
            self.pending_deltas.borrow_mut().drain().collect();
        self.flush_delta_lane(&delta_changes);

        let changes: HashMap<TableId, HashSet<u64>> =
            self.pending_changes.borrow_mut().drain().collect();
        self.flush_snapshot_lane(changes);

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
