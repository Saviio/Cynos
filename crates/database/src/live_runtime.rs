use crate::binary_protocol::SchemaLayout;
use crate::query_engine::{CompiledPhysicalPlan, QueryResultSummary};
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
use cynos_incremental::{DataflowNode, Delta, TableId};
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
    ) -> JsIvmObservableQuery {
        match self {
            Self::Full { schema } => JsIvmObservableQuery::new(inner, schema, binary_layout),
            Self::Projection { schema, columns } => {
                JsIvmObservableQuery::new_with_projection(inner, schema, columns, binary_layout)
            }
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
    pub initial_rows: Vec<Row>,
}

pub(crate) enum KernelPlan {
    Snapshot(SnapshotKernelPlan),
    Delta(DeltaKernelPlan),
}

pub(crate) struct RowsSnapshotAdapterPlan {
    pub projection: RowsProjection,
    pub binary_layout: SchemaLayout,
}

pub(crate) struct RowsDeltaAdapterPlan {
    pub projection: RowsProjection,
    pub binary_layout: SchemaLayout,
}

pub(crate) struct GraphqlSnapshotAdapterPlan {
    pub catalog: GraphqlCatalog,
    pub field: BoundRootField,
    pub batch_plan: cynos_gql::GraphqlBatchPlan,
    pub dependency_table_bindings: Vec<(TableId, String)>,
}

pub(crate) struct GraphqlDeltaAdapterPlan {
    pub catalog: GraphqlCatalog,
    pub field: BoundRootField,
    pub batch_plan: cynos_gql::GraphqlBatchPlan,
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
            }),
        }
    }

    pub fn rows_delta(
        dependencies: LiveDependencySet,
        dataflow: DataflowNode,
        initial_rows: Vec<Row>,
        projection: RowsProjection,
        binary_layout: SchemaLayout,
    ) -> Self {
        Self {
            descriptor: LivePlanDescriptor {
                engine: LiveEngineKind::Delta,
                output: LiveOutputKind::RowsDelta,
                dependencies,
            },
            kernel: KernelPlan::Delta(DeltaKernelPlan {
                dataflow,
                initial_rows,
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
        batch_plan: cynos_gql::GraphqlBatchPlan,
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
                batch_plan,
                dependency_table_bindings,
            }),
        }
    }

    pub fn graphql_delta(
        dependencies: LiveDependencySet,
        dataflow: DataflowNode,
        initial_rows: Vec<Row>,
        catalog: GraphqlCatalog,
        field: BoundRootField,
        batch_plan: cynos_gql::GraphqlBatchPlan,
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
                initial_rows,
            }),
            adapter: AdapterPlan::GraphqlDelta(GraphqlDeltaAdapterPlan {
                catalog,
                field,
                batch_plan,
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

        let observable = Rc::new(RefCell::new(ObservableQuery::with_initial(
            kernel.dataflow,
            kernel.initial_rows,
        )));
        registry
            .borrow_mut()
            .register_delta(DeltaSubscription::Rows(observable.clone()), &dependencies);
        adapter
            .projection
            .into_delta_js(observable, adapter.binary_layout)
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
            adapter.batch_plan,
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

        let observable = Rc::new(RefCell::new(GraphqlDeltaObservable::new(
            kernel.dataflow,
            cache,
            adapter.catalog,
            adapter.field,
            adapter.batch_plan,
            adapter.dependency_table_bindings,
            kernel.initial_rows,
        )));
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

impl DeltaSubscription {
    fn subscription_count(&self) -> usize {
        match self {
            Self::Rows(query) => query.borrow().subscription_count(),
            Self::Graphql(query) => query.borrow().subscription_count(),
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
        let mut merged_rows: HashMap<usize, (Rc<RefCell<ReQueryObservable>>, HashSet<u64>)> =
            HashMap::new();
        let mut merged_graphql: HashMap<
            usize,
            (
                Rc<RefCell<GraphqlSubscriptionObservable>>,
                HashMap<TableId, HashSet<u64>>,
            ),
        > = HashMap::new();

        for (table_id, changed_ids) in changes {
            if let Some(queries) = self.snapshot_queries.get(&table_id) {
                for query in queries {
                    match query {
                        SnapshotSubscription::Rows(query) => {
                            let entry = merged_rows
                                .entry(Rc::as_ptr(query) as usize)
                                .or_insert_with(|| (query.clone(), HashSet::new()));
                            entry.1.extend(changed_ids.iter().copied());
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

        for (_, (query, changed_ids)) in merged_rows {
            query.borrow_mut().on_change(&changed_ids);
        }

        for (_, (query, changes)) in merged_graphql {
            query.borrow_mut().on_change(&changes);
        }
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
        for (table_id, deltas) in delta_changes {
            if let Some(queries) = self.delta_queries.get(table_id) {
                for query in queries {
                    query.on_table_change(*table_id, deltas.clone());
                }
            }
        }
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
