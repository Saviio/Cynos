//! Query registry and notification system.
//!
//! This module provides `QueryRegistry` which tracks observable queries
//! and routes table changes to the appropriate queries.

use crate::observable::ObservableQuery;
use alloc::rc::{Rc, Weak};
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::Row;
use cynos_incremental::{Delta, TableId};
use hashbrown::HashMap;

/// Unique identifier for a registered query.
pub type QueryId = u64;

/// A registry that tracks observable queries and routes changes to them.
///
/// The registry maintains a mapping from table IDs to the queries that
/// depend on them. When a table changes, the registry notifies all
/// relevant queries.
///
/// # Example
///
/// ```ignore
/// use cynos_reactive::{QueryRegistry, ObservableQuery};
/// use cynos_incremental::DataflowNode;
/// use std::rc::Rc;
/// use std::cell::RefCell;
///
/// let mut registry = QueryRegistry::new();
///
/// let query = Rc::new(RefCell::new(ObservableQuery::new(
///     DataflowNode::source(1)
/// )));
///
/// let query_id = registry.register(query.clone());
///
/// // When table 1 changes, the query will be notified
/// registry.on_table_change(1, deltas);
/// ```
pub struct QueryRegistry {
    /// Table ID -> queries that depend on it
    table_queries: HashMap<TableId, Vec<Weak<RefCell<ObservableQuery>>>>,
    /// Query ID -> query reference (for unregistration)
    queries: HashMap<QueryId, Weak<RefCell<ObservableQuery>>>,
    /// Next query ID to assign
    next_id: QueryId,
}

impl Default for QueryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryRegistry {
    /// Creates a new query registry.
    pub fn new() -> Self {
        Self {
            table_queries: HashMap::new(),
            queries: HashMap::new(),
            next_id: 1,
        }
    }

    /// Registers a query and returns its ID.
    ///
    /// The query will be notified when any of its dependent tables change.
    pub fn register(&mut self, query: Rc<RefCell<ObservableQuery>>) -> QueryId {
        let id = self.next_id;
        self.next_id += 1;

        // Get the tables this query depends on
        let dependencies = query.borrow().dependencies().to_vec();

        // Store weak reference for each dependency
        let weak = Rc::downgrade(&query);
        for table_id in dependencies {
            self.table_queries
                .entry(table_id)
                .or_default()
                .push(weak.clone());
        }

        // Store in queries map
        self.queries.insert(id, weak);

        id
    }

    /// Unregisters a query by ID.
    ///
    /// Returns true if the query was found and removed.
    pub fn unregister(&mut self, query_id: QueryId) -> bool {
        if self.queries.remove(&query_id).is_some() {
            // Clean up stale weak references
            self.cleanup();
            true
        } else {
            false
        }
    }

    /// Notifies all queries that depend on the given table of changes.
    pub fn on_table_change(&self, table_id: TableId, deltas: Vec<Delta<Row>>) {
        if let Some(queries) = self.table_queries.get(&table_id) {
            for query_ref in queries {
                if let Some(query) = query_ref.upgrade() {
                    query.borrow_mut().on_table_change(table_id, deltas.clone());
                }
            }
        }
    }

    /// Returns the number of registered queries.
    pub fn query_count(&self) -> usize {
        self.queries.len()
    }

    /// Returns true if there are no registered queries.
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }

    /// Returns the number of queries depending on a specific table.
    pub fn queries_for_table(&self, table_id: TableId) -> usize {
        self.table_queries
            .get(&table_id)
            .map(|v| v.iter().filter(|w| w.strong_count() > 0).count())
            .unwrap_or(0)
    }

    /// Cleans up stale weak references.
    pub fn cleanup(&mut self) {
        // Remove stale entries from table_queries
        for queries in self.table_queries.values_mut() {
            queries.retain(|w| w.strong_count() > 0);
        }

        // Remove empty table entries
        self.table_queries.retain(|_, v| !v.is_empty());

        // Remove stale entries from queries map
        self.queries.retain(|_, w| w.strong_count() > 0);
    }

    /// Clears all registered queries.
    pub fn clear(&mut self) {
        self.table_queries.clear();
        self.queries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::boxed::Box;
    use alloc::vec;
    use cynos_core::Value;
    use cynos_incremental::DataflowNode;

    fn make_row(id: u64, value: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(value)])
    }

    #[test]
    fn test_query_registry_new() {
        let registry = QueryRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.query_count(), 0);
    }

    #[test]
    fn test_query_registry_register() {
        let mut registry = QueryRegistry::new();

        let query = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));

        let id = registry.register(query.clone());

        assert_eq!(id, 1);
        assert_eq!(registry.query_count(), 1);
        assert_eq!(registry.queries_for_table(1), 1);

        // Keep query alive for the assertions
        drop(query);
    }

    #[test]
    fn test_query_registry_register_multiple() {
        let mut registry = QueryRegistry::new();

        let query1 = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));
        let query2 = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));
        let query3 = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(2))));

        registry.register(query1.clone());
        registry.register(query2.clone());
        registry.register(query3.clone());

        assert_eq!(registry.query_count(), 3);
        assert_eq!(registry.queries_for_table(1), 2);
        assert_eq!(registry.queries_for_table(2), 1);

        // Keep queries alive for the assertions
        drop((query1, query2, query3));
    }

    #[test]
    fn test_query_registry_unregister() {
        let mut registry = QueryRegistry::new();

        let query = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));

        let id = registry.register(query);
        assert_eq!(registry.query_count(), 1);

        assert!(registry.unregister(id));
        // Note: The weak reference is still there until cleanup or the Rc is dropped
    }

    #[test]
    fn test_query_registry_on_table_change() {
        let mut registry = QueryRegistry::new();

        let query = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));

        let changes_received = Rc::new(RefCell::new(0));
        let changes_clone = changes_received.clone();

        query.borrow_mut().subscribe(move |_| {
            *changes_clone.borrow_mut() += 1;
        });

        registry.register(query.clone());

        let deltas = vec![Delta::insert(make_row(1, 25))];
        registry.on_table_change(1, deltas);

        assert_eq!(*changes_received.borrow(), 1);
        assert_eq!(query.borrow().len(), 1);
    }

    #[test]
    fn test_query_registry_on_table_change_multiple_queries() {
        let mut registry = QueryRegistry::new();

        let query1 = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));
        let query2 = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));

        let count = Rc::new(RefCell::new(0));
        let count1 = count.clone();
        let count2 = count.clone();

        query1.borrow_mut().subscribe(move |_| {
            *count1.borrow_mut() += 1;
        });
        query2.borrow_mut().subscribe(move |_| {
            *count2.borrow_mut() += 1;
        });

        registry.register(query1.clone());
        registry.register(query2.clone());

        let deltas = vec![Delta::insert(make_row(1, 25))];
        registry.on_table_change(1, deltas);

        assert_eq!(*count.borrow(), 2);

        // Keep queries alive
        drop((query1, query2));
    }

    #[test]
    fn test_query_registry_on_table_change_wrong_table() {
        let mut registry = QueryRegistry::new();

        let query = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));

        let call_count = Rc::new(RefCell::new(0));
        let call_count_clone = call_count.clone();

        query.borrow_mut().subscribe(move |_| {
            *call_count_clone.borrow_mut() += 1;
        });

        registry.register(query);

        // Changes to table 2 should not affect query depending on table 1
        let deltas = vec![Delta::insert(make_row(1, 25))];
        registry.on_table_change(2, deltas);

        assert_eq!(*call_count.borrow(), 0);
    }

    #[test]
    fn test_query_registry_cleanup() {
        let mut registry = QueryRegistry::new();

        {
            let query = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));
            registry.register(query);
            // query is dropped here
        }

        assert_eq!(registry.query_count(), 1); // Still has entry

        registry.cleanup();

        assert_eq!(registry.query_count(), 0); // Cleaned up
        assert_eq!(registry.queries_for_table(1), 0);
    }

    #[test]
    fn test_query_registry_clear() {
        let mut registry = QueryRegistry::new();

        let query1 = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));
        let query2 = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(2))));

        registry.register(query1);
        registry.register(query2);

        assert_eq!(registry.query_count(), 2);

        registry.clear();

        assert!(registry.is_empty());
    }

    // ==================== 补充测试 ====================

    #[test]
    fn test_query_depends_on_multiple_tables() {
        let mut registry = QueryRegistry::new();

        // Query depends on both table 1 and table 2 (via join)
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: cynos_incremental::JoinKeySpec::Constant(vec![]),
            right_key: cynos_incremental::JoinKeySpec::Constant(vec![]),
            join_type: cynos_incremental::JoinType::Inner,
            left_width: 0,
            right_width: 0,
        };

        let query = Rc::new(RefCell::new(ObservableQuery::new(dataflow)));

        let count = Rc::new(RefCell::new(0));
        let count_clone = count.clone();

        query.borrow_mut().subscribe(move |_| {
            *count_clone.borrow_mut() += 1;
        });

        registry.register(query.clone());

        // Query should be registered for both tables
        assert_eq!(registry.queries_for_table(1), 1);
        assert_eq!(registry.queries_for_table(2), 1);

        drop(query);
    }

    #[test]
    fn test_partial_cleanup() {
        let mut registry = QueryRegistry::new();

        let query1 = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));

        // Create query2 in a block so it gets dropped
        {
            let query2 = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));
            registry.register(query1.clone());
            registry.register(query2.clone());

            // Both queries alive
            assert_eq!(registry.queries_for_table(1), 2);
            // query2 dropped here
        }

        // query2 is dropped, but query1 is still alive
        registry.cleanup();

        assert_eq!(registry.queries_for_table(1), 1);
        assert_eq!(registry.query_count(), 1);

        drop(query1);
    }

    #[test]
    fn test_on_table_change_with_dropped_query() {
        let mut registry = QueryRegistry::new();

        {
            let query = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));
            registry.register(query);
            // query dropped here
        }

        // Should not panic when notifying dropped queries
        let deltas = vec![Delta::insert(make_row(1, 25))];
        registry.on_table_change(1, deltas);

        // After notification, stale refs should still be there (until cleanup)
        assert_eq!(registry.query_count(), 1);
    }

    #[test]
    fn test_empty_table_change() {
        let mut registry = QueryRegistry::new();

        let query = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));

        let count = Rc::new(RefCell::new(0));
        let count_clone = count.clone();

        query.borrow_mut().subscribe(move |_| {
            *count_clone.borrow_mut() += 1;
        });

        registry.register(query.clone());

        // Empty deltas
        registry.on_table_change(1, vec![]);

        assert_eq!(*count.borrow(), 0);

        drop(query);
    }

    #[test]
    fn test_unregister_cleans_table_queries() {
        let mut registry = QueryRegistry::new();

        let query = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));
        let id = registry.register(query.clone());

        assert_eq!(registry.queries_for_table(1), 1);

        registry.unregister(id);
        registry.cleanup();

        // After unregister + cleanup, table should have no queries
        // (Note: unregister removes from queries map, cleanup removes stale weak refs)
        drop(query);
        registry.cleanup();
        assert_eq!(registry.queries_for_table(1), 0);
    }

    #[test]
    fn test_multiple_tables_independent() {
        let mut registry = QueryRegistry::new();

        let query1 = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));
        let query2 = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(2))));

        let count1 = Rc::new(RefCell::new(0));
        let count2 = Rc::new(RefCell::new(0));

        let c1 = count1.clone();
        let c2 = count2.clone();

        query1
            .borrow_mut()
            .subscribe(move |_| *c1.borrow_mut() += 1);
        query2
            .borrow_mut()
            .subscribe(move |_| *c2.borrow_mut() += 1);

        registry.register(query1.clone());
        registry.register(query2.clone());

        // Change table 1 - only query1 notified
        registry.on_table_change(1, vec![Delta::insert(make_row(1, 25))]);
        assert_eq!(*count1.borrow(), 1);
        assert_eq!(*count2.borrow(), 0);

        // Change table 2 - only query2 notified
        registry.on_table_change(2, vec![Delta::insert(make_row(2, 30))]);
        assert_eq!(*count1.borrow(), 1);
        assert_eq!(*count2.borrow(), 1);

        drop((query1, query2));
    }

    #[test]
    fn test_nonexistent_table_change() {
        let mut registry = QueryRegistry::new();

        let query = Rc::new(RefCell::new(ObservableQuery::new(DataflowNode::source(1))));

        let count = Rc::new(RefCell::new(0));
        let count_clone = count.clone();

        query.borrow_mut().subscribe(move |_| {
            *count_clone.borrow_mut() += 1;
        });

        registry.register(query.clone());

        // Change to table 999 (doesn't exist)
        registry.on_table_change(999, vec![Delta::insert(make_row(1, 25))]);

        assert_eq!(*count.borrow(), 0);

        drop(query);
    }
}
