//! Observable query implementation.
//!
//! This module provides `ObservableQuery` which wraps a dataflow and maintains
//! the current result set, notifying subscribers when changes occur.
//!
//! The key feature is the `changes()` method which returns an iterator/stream
//! that yields the initial result followed by incremental changes.

use crate::change_set::ChangeSet;
use crate::subscription::{
    RawDeltaSubscriptionManager, SubscriptionId, SubscriptionManager, TraceBatchSubscriptionManager,
};
use alloc::vec::Vec;
use cynos_core::{Row, Value};
use cynos_incremental::{
    BootstrapExecutionProfile, CompiledBootstrapPlan, CompiledIvmPlan, DataflowNode, Delta,
    MaterializedView, TableId, TraceDeltaBatch, TraceTupleArena, TraceTupleHandle,
    TraceUpdateProfile,
};
use hashbrown::HashMap;

/// An observable query that tracks changes and notifies subscribers.
///
/// This struct wraps a `MaterializedView` and adds subscription management.
/// Subscribers receive `ChangeSet` notifications when the underlying data changes.
///
/// # Example
///
/// ```ignore
/// use cynos_reactive::ObservableQuery;
/// use cynos_incremental::DataflowNode;
///
/// let dataflow = DataflowNode::filter(
///     DataflowNode::source(1),
///     |row| row.get(1).and_then(|v| v.as_i64()).map(|age| age > 18).unwrap_or(false)
/// );
///
/// let mut query = ObservableQuery::new(dataflow);
///
/// // Subscribe to changes
/// let sub_id = query.subscribe(|changes| {
///     println!("Added: {}, Removed: {}", changes.added.len(), changes.removed.len());
/// });
///
/// // When table changes occur, subscribers are notified
/// query.on_table_change(1, deltas);
/// ```
pub struct ObservableQuery {
    /// The underlying materialized view
    view: MaterializedView,
    /// Subscription manager for change notifications
    subscriptions: SubscriptionManager,
    /// Raw delta subscribers used by low-level bridge paths.
    raw_delta_subscriptions: RawDeltaSubscriptionManager,
    /// Trace batch subscribers used by JS delta bridge paths.
    trace_batch_subscriptions: TraceBatchSubscriptionManager,
    /// Reused scratch buffer for plain `ChangeSet` subscribers.
    change_set_scratch: ChangeSet,
    /// Whether initial value has been emitted
    initialized: bool,
}

impl ObservableQuery {
    /// Creates a new observable query from a dataflow node.
    pub fn new(dataflow: DataflowNode) -> Self {
        Self {
            view: MaterializedView::new(dataflow),
            subscriptions: SubscriptionManager::new(),
            raw_delta_subscriptions: RawDeltaSubscriptionManager::new(),
            trace_batch_subscriptions: TraceBatchSubscriptionManager::new(),
            change_set_scratch: ChangeSet::new(),
            initialized: false,
        }
    }

    /// Creates an observable query with an initial result set.
    pub fn with_initial(dataflow: DataflowNode, initial: Vec<Row>) -> Self {
        Self {
            view: MaterializedView::with_initial(dataflow, initial),
            subscriptions: SubscriptionManager::new(),
            raw_delta_subscriptions: RawDeltaSubscriptionManager::new(),
            trace_batch_subscriptions: TraceBatchSubscriptionManager::new(),
            change_set_scratch: ChangeSet::new(),
            initialized: true,
        }
    }

    /// Creates an observable query with an initial result set and bootstrapped source state.
    pub fn with_sources(
        dataflow: DataflowNode,
        initial: Vec<Row>,
        source_rows: &HashMap<TableId, Vec<Row>>,
    ) -> Self {
        Self {
            view: MaterializedView::with_sources(dataflow, initial, source_rows),
            subscriptions: SubscriptionManager::new(),
            raw_delta_subscriptions: RawDeltaSubscriptionManager::new(),
            trace_batch_subscriptions: TraceBatchSubscriptionManager::new(),
            change_set_scratch: ChangeSet::new(),
            initialized: true,
        }
    }

    /// Creates an observable query with precompiled IVM metadata and lazy source loading.
    pub fn with_compiled_loader<F>(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial: Vec<alloc::rc::Rc<Row>>,
        load_source_rows: F,
    ) -> Self
    where
        F: FnMut(TableId) -> Vec<alloc::rc::Rc<Row>>,
    {
        Self {
            view: MaterializedView::with_compiled_loader_and_bootstrap(
                dataflow,
                compiled_plan,
                compiled_bootstrap_plan,
                initial,
                load_source_rows,
            ),
            subscriptions: SubscriptionManager::new(),
            raw_delta_subscriptions: RawDeltaSubscriptionManager::new(),
            trace_batch_subscriptions: TraceBatchSubscriptionManager::new(),
            change_set_scratch: ChangeSet::new(),
            initialized: true,
        }
    }

    /// Creates an observable query with store-backed bootstrap streaming.
    pub fn with_compiled_source_visitor<F>(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial: Vec<alloc::rc::Rc<Row>>,
        visit_source_rows: F,
    ) -> Self
    where
        F: FnMut(TableId, usize, &mut dyn FnMut(alloc::rc::Rc<Row>)),
    {
        Self {
            view: MaterializedView::with_compiled_source_visitor_and_bootstrap(
                dataflow,
                compiled_plan,
                compiled_bootstrap_plan,
                initial,
                visit_source_rows,
            ),
            subscriptions: SubscriptionManager::new(),
            raw_delta_subscriptions: RawDeltaSubscriptionManager::new(),
            trace_batch_subscriptions: TraceBatchSubscriptionManager::new(),
            change_set_scratch: ChangeSet::new(),
            initialized: true,
        }
    }

    #[doc(hidden)]
    pub fn with_compiled_source_visitor_profiled<F>(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial: Vec<alloc::rc::Rc<Row>>,
        visit_source_rows: F,
        now_fn: Option<fn() -> f64>,
    ) -> (Self, BootstrapExecutionProfile)
    where
        F: FnMut(TableId, usize, &mut dyn FnMut(alloc::rc::Rc<Row>)),
    {
        Self::with_compiled_source_visitor_profiled_with_filter_coverage(
            dataflow,
            compiled_plan,
            compiled_bootstrap_plan,
            initial,
            visit_source_rows,
            None,
            now_fn,
        )
    }

    #[doc(hidden)]
    pub fn with_compiled_source_visitor_profiled_with_filter_coverage<F>(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial: Vec<alloc::rc::Rc<Row>>,
        visit_source_rows: F,
        source_filter_coverage: Option<Vec<bool>>,
        now_fn: Option<fn() -> f64>,
    ) -> (Self, BootstrapExecutionProfile)
    where
        F: FnMut(TableId, usize, &mut dyn FnMut(alloc::rc::Rc<Row>)),
    {
        let (view, bootstrap_profile) =
            MaterializedView::with_compiled_source_visitor_and_bootstrap_profiled_with_filter_coverage(
                dataflow,
                compiled_plan,
                compiled_bootstrap_plan,
                initial,
                visit_source_rows,
                source_filter_coverage,
                now_fn,
            );
        (
            Self {
                view,
                subscriptions: SubscriptionManager::new(),
                raw_delta_subscriptions: RawDeltaSubscriptionManager::new(),
                trace_batch_subscriptions: TraceBatchSubscriptionManager::new(),
                change_set_scratch: ChangeSet::new(),
                initialized: true,
            },
            bootstrap_profile,
        )
    }

    #[doc(hidden)]
    pub fn with_compiled_source_slot_visitor_profiled_with_filter_coverage<F>(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial: Vec<alloc::rc::Rc<Row>>,
        visit_source_slot: F,
        source_filter_coverage: Option<Vec<bool>>,
        now_fn: Option<fn() -> f64>,
    ) -> (Self, BootstrapExecutionProfile)
    where
        F: FnMut(TableId, usize, &TraceTupleArena, &mut Vec<TraceTupleHandle>),
    {
        let (view, bootstrap_profile) =
            MaterializedView::with_compiled_source_slot_visitor_and_bootstrap_profiled_with_filter_coverage(
                dataflow,
                compiled_plan,
                compiled_bootstrap_plan,
                initial,
                visit_source_slot,
                source_filter_coverage,
                now_fn,
            );
        (
            Self {
                view,
                subscriptions: SubscriptionManager::new(),
                raw_delta_subscriptions: RawDeltaSubscriptionManager::new(),
                trace_batch_subscriptions: TraceBatchSubscriptionManager::new(),
                change_set_scratch: ChangeSet::new(),
                initialized: true,
            },
            bootstrap_profile,
        )
    }

    /// Initializes join state from source data.
    /// This must be called for join queries to properly track incremental changes.
    pub fn initialize_join_state(
        &mut self,
        left_rows: &[Row],
        right_rows: &[Row],
        left_key_fn: impl Fn(&Row) -> Vec<Value>,
        right_key_fn: impl Fn(&Row) -> Vec<Value>,
    ) {
        self.view
            .initialize_join_state(left_rows, right_rows, left_key_fn, right_key_fn);
    }

    /// Returns the current result as a Vec.
    #[inline]
    pub fn result(&self) -> Vec<Row> {
        self.view.result()
    }

    /// Returns the current result as shared rows for bridge/binary paths.
    #[inline]
    pub fn result_rc(&self) -> Vec<alloc::rc::Rc<Row>> {
        self.view.result_rc()
    }

    /// Returns shared row references without cloning the current result vector.
    #[inline]
    pub fn result_row_refs(&self) -> impl Iterator<Item = &alloc::rc::Rc<Row>> + '_ {
        self.view.result_row_refs()
    }

    /// Returns the number of rows in the result.
    #[inline]
    pub fn len(&self) -> usize {
        self.view.len()
    }

    /// Returns true if the result is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.view.is_empty()
    }

    /// Returns the tables this query depends on.
    #[inline]
    pub fn dependencies(&self) -> &[TableId] {
        self.view.dependencies()
    }

    /// Checks if this query depends on the given table.
    #[inline]
    pub fn depends_on(&self, table_id: TableId) -> bool {
        self.view.depends_on(table_id)
    }

    /// Subscribes to changes with the given callback.
    ///
    /// The callback will be invoked whenever the query result changes.
    /// Returns a subscription ID that can be used to unsubscribe.
    pub fn subscribe<F>(&mut self, callback: F) -> SubscriptionId
    where
        F: Fn(&ChangeSet) + 'static,
    {
        self.subscriptions.subscribe(callback)
    }

    /// Subscribes to raw deltas without constructing a `ChangeSet`.
    pub fn subscribe_raw_deltas<F>(&mut self, callback: F) -> SubscriptionId
    where
        F: Fn(&[Delta<Row>]) + 'static,
    {
        self.raw_delta_subscriptions.subscribe(callback)
    }

    /// Subscribes to internal trace batches without materializing full rows.
    pub fn subscribe_trace_batches<F>(&mut self, callback: F) -> SubscriptionId
    where
        F: Fn(&TraceDeltaBatch) + 'static,
    {
        self.trace_batch_subscriptions.subscribe(callback)
    }

    /// Unsubscribes by ID.
    ///
    /// Returns true if the subscription was found and removed.
    pub fn unsubscribe(&mut self, id: SubscriptionId) -> bool {
        self.subscriptions.unsubscribe(id)
            || self.raw_delta_subscriptions.unsubscribe(id)
            || self.trace_batch_subscriptions.unsubscribe(id)
    }

    /// Returns the number of active subscriptions.
    #[inline]
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
            + self.raw_delta_subscriptions.len()
            + self.trace_batch_subscriptions.len()
    }

    /// Handles changes to a source table.
    ///
    /// Propagates the deltas through the dataflow, updates the result,
    /// and notifies all subscribers of the changes.
    ///
    /// Uses `from_deltas_only` to avoid the O(result_set) clone of
    /// `self.view.result()`. Subscribers receive only added/removed rows.
    /// If a subscriber needs the full result, it can call `result()` explicitly.
    pub fn on_table_change(&mut self, table_id: TableId, deltas: Vec<Delta<Row>>) {
        let _ = self.on_table_change_profiled(table_id, deltas, None);
    }

    #[doc(hidden)]
    pub fn on_table_change_profiled(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
        now_fn: Option<fn() -> f64>,
    ) -> TraceUpdateProfile {
        // Skip dataflow propagation entirely when no one is listening.
        // The result_map will be stale, but getResult() is only called
        // right after subscribe, at which point we re-initialize anyway.
        if self.subscriptions.is_empty()
            && self.raw_delta_subscriptions.is_empty()
            && self.trace_batch_subscriptions.is_empty()
        {
            return TraceUpdateProfile::default();
        }

        if self.trace_batch_subscriptions.is_empty() {
            let (output_deltas, profile) =
                self.view.on_table_change_profiled(table_id, deltas, now_fn);

            if !output_deltas.is_empty() {
                if !self.raw_delta_subscriptions.is_empty() {
                    self.raw_delta_subscriptions.notify_all(&output_deltas);
                }
                if !self.subscriptions.is_empty() {
                    self.change_set_scratch
                        .replace_from_deltas_only(&output_deltas);
                    self.subscriptions.notify_all(&self.change_set_scratch);
                }
            }

            return profile;
        }

        let (output_batch, profile) = self
            .view
            .on_table_change_batch_profiled(table_id, deltas, now_fn);

        if !output_batch.is_empty() {
            if !self.trace_batch_subscriptions.is_empty() {
                self.trace_batch_subscriptions.notify_all(&output_batch);
            }

            let materialized =
                if self.raw_delta_subscriptions.is_empty() && self.subscriptions.is_empty() {
                    None
                } else {
                    Some(output_batch.materialize_rows())
                };

            if !self.raw_delta_subscriptions.is_empty() {
                if let Some(ref output_deltas) = materialized {
                    self.raw_delta_subscriptions.notify_all(output_deltas);
                }
            }
            if !self.subscriptions.is_empty() {
                if let Some(ref output_deltas) = materialized {
                    self.change_set_scratch
                        .replace_from_deltas_only(output_deltas);
                    self.subscriptions.notify_all(&self.change_set_scratch);
                }
            }
        }

        profile
    }

    /// Initializes the query with the given rows and notifies subscribers.
    ///
    /// This should be called once after creating the query to set the initial
    /// result and notify subscribers of the initial state.
    pub fn initialize(&mut self, rows: Vec<Row>) {
        if !self.initialized {
            self.view.set_result(rows.clone());
            self.initialized = true;

            if !rows.is_empty() {
                let changes = ChangeSet::initial(rows);
                self.subscriptions.notify_all(&changes);
            }
        }
    }

    /// Returns whether the query has been initialized.
    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Creates a `Changes` iterator that yields the initial result followed by changes.
    ///
    /// This is the key API for reactive queries. The first value pushed is the
    /// initial result set (as additions), and subsequent values are incremental
    /// changes after applying diffs.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let query = ObservableQuery::new(dataflow);
    /// let changes = query.changes();
    ///
    /// // First call returns initial result
    /// // Subsequent calls return incremental changes
    /// ```
    pub fn changes(&mut self) -> Changes<'_> {
        Changes {
            query: self,
            emitted_initial: false,
        }
    }

    /// Clears the result and resets the query.
    pub fn clear(&mut self) {
        self.view.clear();
        self.initialized = false;
    }
}

/// An iterator-like struct for observing query changes.
///
/// The first call to `next()` returns the initial result set (as a ChangeSet
/// with all rows as additions). Subsequent changes are returned as they occur.
pub struct Changes<'a> {
    query: &'a mut ObservableQuery,
    emitted_initial: bool,
}

impl<'a> Changes<'a> {
    /// Gets the initial change set (all current rows as additions).
    ///
    /// This should be called first to get the initial state.
    pub fn initial(&mut self) -> ChangeSet {
        if !self.emitted_initial {
            self.emitted_initial = true;
            ChangeSet::initial(self.query.result())
        } else {
            ChangeSet::new()
        }
    }

    /// Processes table changes and returns the resulting change set.
    ///
    /// This method propagates the deltas through the dataflow and returns
    /// the changes to the query result.
    pub fn process(&mut self, table_id: TableId, deltas: Vec<Delta<Row>>) -> ChangeSet {
        let output_deltas = self.query.view.on_table_change(table_id, deltas);
        let current_result = self.query.view.result();
        ChangeSet::from_deltas(&output_deltas, current_result)
    }

    /// Returns the current result as a Vec.
    #[inline]
    pub fn result(&self) -> Vec<Row> {
        self.query.result()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::rc::Rc;
    use alloc::vec;
    use core::cell::RefCell;
    use cynos_core::Value;

    fn make_row(id: u64, age: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(age)])
    }

    #[test]
    fn test_observable_query_new() {
        let dataflow = DataflowNode::source(1);
        let query = ObservableQuery::new(dataflow);

        assert!(query.is_empty());
        assert!(!query.is_initialized());
        assert_eq!(query.dependencies(), &[1]);
    }

    #[test]
    fn test_observable_query_with_initial() {
        let dataflow = DataflowNode::source(1);
        let initial = vec![make_row(1, 25), make_row(2, 30)];
        let query = ObservableQuery::with_initial(dataflow, initial);

        assert_eq!(query.len(), 2);
        assert!(query.is_initialized());
    }

    #[test]
    fn test_observable_query_subscribe() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let id1 = query.subscribe(|_| {});
        let id2 = query.subscribe(|_| {});

        assert_eq!(query.subscription_count(), 2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_observable_query_unsubscribe() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let id = query.subscribe(|_| {});
        assert_eq!(query.subscription_count(), 1);

        assert!(query.unsubscribe(id));
        assert_eq!(query.subscription_count(), 0);
    }

    #[test]
    fn test_observable_query_on_table_change() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let changes_received = Rc::new(RefCell::new(Vec::new()));
        let changes_clone = changes_received.clone();

        query.subscribe(move |changes| {
            changes_clone.borrow_mut().push(changes.clone());
        });

        let deltas = vec![
            Delta::insert(make_row(1, 25)),
            Delta::insert(make_row(2, 30)),
        ];

        query.on_table_change(1, deltas);

        assert_eq!(query.len(), 2);
        assert_eq!(changes_received.borrow().len(), 1);
        assert_eq!(changes_received.borrow()[0].added.len(), 2);
    }

    #[test]
    fn test_observable_query_filter() {
        let dataflow = DataflowNode::filter(DataflowNode::source(1), |row| {
            row.get(1)
                .and_then(|v| v.as_i64())
                .map(|age| age > 18)
                .unwrap_or(false)
        });
        let mut query = ObservableQuery::new(dataflow);

        let changes_received = Rc::new(RefCell::new(Vec::new()));
        let changes_clone = changes_received.clone();

        query.subscribe(move |changes| {
            changes_clone.borrow_mut().push(changes.clone());
        });

        let deltas = vec![
            Delta::insert(make_row(1, 25)), // passes filter
            Delta::insert(make_row(2, 15)), // filtered out
            Delta::insert(make_row(3, 30)), // passes filter
        ];

        query.on_table_change(1, deltas);

        assert_eq!(query.len(), 2);
        assert_eq!(changes_received.borrow().len(), 1);
        assert_eq!(changes_received.borrow()[0].added.len(), 2);
    }

    #[test]
    fn test_observable_query_initialize() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let changes_received = Rc::new(RefCell::new(Vec::new()));
        let changes_clone = changes_received.clone();

        query.subscribe(move |changes| {
            changes_clone.borrow_mut().push(changes.clone());
        });

        let initial = vec![make_row(1, 25), make_row(2, 30)];
        query.initialize(initial);

        assert!(query.is_initialized());
        assert_eq!(query.len(), 2);
        assert_eq!(changes_received.borrow().len(), 1);
        assert_eq!(changes_received.borrow()[0].added.len(), 2);
    }

    #[test]
    fn test_observable_query_initialize_once() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let call_count = Rc::new(RefCell::new(0));
        let call_count_clone = call_count.clone();

        query.subscribe(move |_| {
            *call_count_clone.borrow_mut() += 1;
        });

        query.initialize(vec![make_row(1, 25)]);
        query.initialize(vec![make_row(2, 30)]); // Should be ignored

        assert_eq!(*call_count.borrow(), 1);
        assert_eq!(query.len(), 1); // Still has first initialization
    }

    #[test]
    fn test_observable_query_changes_initial() {
        let dataflow = DataflowNode::source(1);
        let initial = vec![make_row(1, 25), make_row(2, 30)];
        let mut query = ObservableQuery::with_initial(dataflow, initial);

        let mut changes = query.changes();
        let initial_changes = changes.initial();

        assert_eq!(initial_changes.added.len(), 2);
        assert!(initial_changes.removed.is_empty());

        // Second call should return empty
        let second = changes.initial();
        assert!(second.is_empty());
    }

    #[test]
    fn test_observable_query_changes_process() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let mut changes = query.changes();

        // Get initial (empty)
        let initial = changes.initial();
        assert!(initial.is_empty());

        // Process some changes
        let deltas = vec![Delta::insert(make_row(1, 25))];
        let change_set = changes.process(1, deltas);

        assert_eq!(change_set.added.len(), 1);
        assert_eq!(changes.result().len(), 1);
    }

    #[test]
    fn test_observable_query_delete() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let changes_received = Rc::new(RefCell::new(Vec::new()));
        let changes_clone = changes_received.clone();

        query.subscribe(move |changes| {
            changes_clone.borrow_mut().push(changes.clone());
        });

        // Insert
        query.on_table_change(1, vec![Delta::insert(make_row(1, 25))]);
        assert_eq!(query.len(), 1);

        // Delete
        query.on_table_change(1, vec![Delta::delete(make_row(1, 25))]);
        assert_eq!(query.len(), 0);

        assert_eq!(changes_received.borrow().len(), 2);
        assert_eq!(changes_received.borrow()[1].removed.len(), 1);
    }

    #[test]
    fn test_observable_query_wrong_table() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let call_count = Rc::new(RefCell::new(0));
        let call_count_clone = call_count.clone();

        query.subscribe(move |_| {
            *call_count_clone.borrow_mut() += 1;
        });

        // Changes to table 2 should not affect query depending on table 1
        query.on_table_change(2, vec![Delta::insert(make_row(1, 25))]);

        assert_eq!(*call_count.borrow(), 0);
        assert!(query.is_empty());
    }

    #[test]
    fn test_observable_query_clear() {
        let dataflow = DataflowNode::source(1);
        let initial = vec![make_row(1, 25)];
        let mut query = ObservableQuery::with_initial(dataflow, initial);

        assert!(query.is_initialized());
        assert_eq!(query.len(), 1);

        query.clear();

        assert!(!query.is_initialized());
        assert!(query.is_empty());
    }

    // ==================== 补充测试 ====================

    #[test]
    fn test_empty_deltas_no_notification() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let call_count = Rc::new(RefCell::new(0));
        let call_count_clone = call_count.clone();

        query.subscribe(move |_| {
            *call_count_clone.borrow_mut() += 1;
        });

        // Empty deltas should not trigger notification
        query.on_table_change(1, vec![]);

        assert_eq!(*call_count.borrow(), 0);
    }

    #[test]
    fn test_filter_all_filtered_no_notification() {
        let dataflow = DataflowNode::filter(DataflowNode::source(1), |row| {
            row.get(1)
                .and_then(|v| v.as_i64())
                .map(|age| age > 100) // Very high threshold
                .unwrap_or(false)
        });
        let mut query = ObservableQuery::new(dataflow);

        let call_count = Rc::new(RefCell::new(0));
        let call_count_clone = call_count.clone();

        query.subscribe(move |_| {
            *call_count_clone.borrow_mut() += 1;
        });

        // All rows filtered out - should not trigger notification
        let deltas = vec![
            Delta::insert(make_row(1, 25)),
            Delta::insert(make_row(2, 30)),
        ];
        query.on_table_change(1, deltas);

        assert_eq!(*call_count.borrow(), 0);
        assert!(query.is_empty());
    }

    #[test]
    fn test_multiple_subscribers() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let count1 = Rc::new(RefCell::new(0));
        let count2 = Rc::new(RefCell::new(0));
        let count3 = Rc::new(RefCell::new(0));

        let c1 = count1.clone();
        let c2 = count2.clone();
        let c3 = count3.clone();

        query.subscribe(move |_| *c1.borrow_mut() += 1);
        query.subscribe(move |_| *c2.borrow_mut() += 10);
        query.subscribe(move |_| *c3.borrow_mut() += 100);

        query.on_table_change(1, vec![Delta::insert(make_row(1, 25))]);

        assert_eq!(*count1.borrow(), 1);
        assert_eq!(*count2.borrow(), 10);
        assert_eq!(*count3.borrow(), 100);
    }

    #[test]
    fn test_unsubscribe_middle_subscriber() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let count1 = Rc::new(RefCell::new(0));
        let count2 = Rc::new(RefCell::new(0));
        let count3 = Rc::new(RefCell::new(0));

        let c1 = count1.clone();
        let c2 = count2.clone();
        let c3 = count3.clone();

        let _id1 = query.subscribe(move |_| *c1.borrow_mut() += 1);
        let id2 = query.subscribe(move |_| *c2.borrow_mut() += 1);
        let _id3 = query.subscribe(move |_| *c3.borrow_mut() += 1);

        // Unsubscribe middle one
        query.unsubscribe(id2);

        query.on_table_change(1, vec![Delta::insert(make_row(1, 25))]);

        assert_eq!(*count1.borrow(), 1);
        assert_eq!(*count2.borrow(), 0); // Unsubscribed
        assert_eq!(*count3.borrow(), 1);
    }

    #[test]
    fn test_changes_multiple_incremental_updates() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let mut changes = query.changes();

        // Initial (empty)
        let initial = changes.initial();
        assert!(initial.is_empty());

        // First insert
        let cs1 = changes.process(1, vec![Delta::insert(make_row(1, 25))]);
        assert_eq!(cs1.added.len(), 1);
        assert_eq!(changes.result().len(), 1);

        // Second insert
        let cs2 = changes.process(1, vec![Delta::insert(make_row(2, 30))]);
        assert_eq!(cs2.added.len(), 1);
        assert_eq!(changes.result().len(), 2);

        // Delete first row
        let cs3 = changes.process(1, vec![Delta::delete(make_row(1, 25))]);
        assert_eq!(cs3.removed.len(), 1);
        assert_eq!(changes.result().len(), 1);
    }

    #[test]
    fn test_changes_with_filter() {
        let dataflow = DataflowNode::filter(DataflowNode::source(1), |row| {
            row.get(1)
                .and_then(|v| v.as_i64())
                .map(|age| age > 18)
                .unwrap_or(false)
        });
        let mut query = ObservableQuery::new(dataflow);

        let mut changes = query.changes();
        let _ = changes.initial();

        // Insert one that passes, one that doesn't
        let cs = changes.process(
            1,
            vec![
                Delta::insert(make_row(1, 15)), // filtered
                Delta::insert(make_row(2, 25)), // passes
            ],
        );

        assert_eq!(cs.added.len(), 1);
        assert_eq!(changes.result().len(), 1);
    }

    #[test]
    fn test_initialize_empty_no_notification() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let call_count = Rc::new(RefCell::new(0));
        let call_count_clone = call_count.clone();

        query.subscribe(move |_| {
            *call_count_clone.borrow_mut() += 1;
        });

        // Initialize with empty - should not notify
        query.initialize(vec![]);

        assert!(query.is_initialized());
        assert_eq!(*call_count.borrow(), 0);
    }

    #[test]
    fn test_project_columns() {
        // Project only column 0 (id)
        let dataflow = DataflowNode::project(DataflowNode::source(1), vec![0]);
        let mut query = ObservableQuery::new(dataflow);

        let changes_received = Rc::new(RefCell::new(Vec::new()));
        let changes_clone = changes_received.clone();

        query.subscribe(move |changes| {
            changes_clone.borrow_mut().push(changes.clone());
        });

        query.on_table_change(1, vec![Delta::insert(make_row(1, 25))]);

        assert_eq!(changes_received.borrow().len(), 1);
        // Projected row should only have 1 column
        let added = &changes_received.borrow()[0].added;
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].len(), 1);
    }

    #[test]
    fn test_batch_insert_delete() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        let changes_received = Rc::new(RefCell::new(Vec::new()));
        let changes_clone = changes_received.clone();

        query.subscribe(move |changes| {
            changes_clone.borrow_mut().push(changes.clone());
        });

        // Batch: insert 3, delete 1
        let deltas = vec![
            Delta::insert(make_row(1, 25)),
            Delta::insert(make_row(2, 30)),
            Delta::insert(make_row(3, 35)),
            Delta::delete(make_row(2, 30)),
        ];

        query.on_table_change(1, deltas);

        // Should have net 2 rows
        assert_eq!(query.len(), 2);

        // Single notification with all changes
        assert_eq!(changes_received.borrow().len(), 1);
        let cs = &changes_received.borrow()[0];
        assert_eq!(cs.added.len(), 3);
        assert_eq!(cs.removed.len(), 1);
    }

    #[test]
    fn test_unsubscribe_nonexistent() {
        let dataflow = DataflowNode::source(1);
        let mut query = ObservableQuery::new(dataflow);

        // Unsubscribe non-existent ID should return false
        assert!(!query.unsubscribe(999));
    }

    #[test]
    fn test_subscribe_after_data() {
        let dataflow = DataflowNode::source(1);
        let initial = vec![make_row(1, 25), make_row(2, 30)];
        let mut query = ObservableQuery::with_initial(dataflow, initial);

        // Subscribe after data exists
        let call_count = Rc::new(RefCell::new(0));
        let call_count_clone = call_count.clone();

        query.subscribe(move |_| {
            *call_count_clone.borrow_mut() += 1;
        });

        // Subscribing doesn't trigger callback (only changes do)
        assert_eq!(*call_count.borrow(), 0);

        // But new changes do
        query.on_table_change(1, vec![Delta::insert(make_row(3, 35))]);
        assert_eq!(*call_count.borrow(), 1);
    }
}
