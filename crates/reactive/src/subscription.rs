//! Subscription management for reactive queries.
//!
//! This module provides subscription IDs and a manager for tracking
//! active subscriptions to observable queries.

use crate::change_set::ChangeSet;
use alloc::boxed::Box;
use alloc::vec::Vec;
use cynos_core::Row;
use cynos_incremental::{Delta, TraceDeltaBatch};
use hashbrown::HashMap;

/// Unique identifier for a subscription.
pub type SubscriptionId = u64;

/// Callback type for change notifications.
pub type ChangeCallback = Box<dyn Fn(&ChangeSet)>;

/// Callback type for raw delta notifications.
pub type RawDeltaCallback = Box<dyn Fn(&[Delta<Row>])>;

/// Callback type for late-materialized trace batches.
pub type TraceBatchCallback = Box<dyn Fn(&TraceDeltaBatch)>;

/// A subscription to query changes.
pub struct Subscription {
    /// Unique identifier
    id: SubscriptionId,
    /// Callback to invoke on changes
    callback: ChangeCallback,
    /// Whether this subscription is active
    active: bool,
}

impl Subscription {
    /// Creates a new subscription.
    pub fn new<F>(id: SubscriptionId, callback: F) -> Self
    where
        F: Fn(&ChangeSet) + 'static,
    {
        Self {
            id,
            callback: Box::new(callback),
            active: true,
        }
    }

    /// Returns the subscription ID.
    #[inline]
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Returns whether this subscription is active.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Deactivates this subscription.
    #[inline]
    pub fn deactivate(&mut self) {
        self.active = false;
    }

    /// Notifies this subscription of changes.
    pub fn notify(&self, changes: &ChangeSet) {
        if self.active {
            (self.callback)(changes);
        }
    }
}

/// Manages subscriptions for an observable query.
pub struct SubscriptionManager {
    /// Active subscriptions
    subscriptions: HashMap<SubscriptionId, Subscription>,
    /// Next subscription ID to assign
    next_id: SubscriptionId,
}

/// A raw-delta subscription that receives `Delta<Row>` slices directly.
pub struct RawDeltaSubscription {
    id: SubscriptionId,
    callback: RawDeltaCallback,
    active: bool,
}

impl RawDeltaSubscription {
    pub fn new<F>(id: SubscriptionId, callback: F) -> Self
    where
        F: Fn(&[Delta<Row>]) + 'static,
    {
        Self {
            id,
            callback: Box::new(callback),
            active: true,
        }
    }

    #[inline]
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        self.active
    }

    #[inline]
    pub fn deactivate(&mut self) {
        self.active = false;
    }

    pub fn notify(&self, deltas: &[Delta<Row>]) {
        if self.active {
            (self.callback)(deltas);
        }
    }
}

/// Manages raw-delta subscriptions for an observable query.
pub struct RawDeltaSubscriptionManager {
    subscriptions: HashMap<SubscriptionId, RawDeltaSubscription>,
    next_id: SubscriptionId,
}

/// A trace-batch subscription that receives `TraceDeltaBatch` values directly.
pub struct TraceBatchSubscription {
    _id: SubscriptionId,
    callback: TraceBatchCallback,
    active: bool,
}

impl TraceBatchSubscription {
    pub fn new<F>(id: SubscriptionId, callback: F) -> Self
    where
        F: Fn(&TraceDeltaBatch) + 'static,
    {
        Self {
            _id: id,
            callback: Box::new(callback),
            active: true,
        }
    }

    pub fn notify(&self, batch: &TraceDeltaBatch) {
        if self.active {
            (self.callback)(batch);
        }
    }
}

/// Manages trace-batch subscriptions for an observable query.
pub struct TraceBatchSubscriptionManager {
    subscriptions: HashMap<SubscriptionId, TraceBatchSubscription>,
    next_id: SubscriptionId,
}

impl Default for RawDeltaSubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for TraceBatchSubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RawDeltaSubscriptionManager {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            next_id: 1,
        }
    }

    pub fn subscribe<F>(&mut self, callback: F) -> SubscriptionId
    where
        F: Fn(&[Delta<Row>]) + 'static,
    {
        let id = self.next_id;
        self.next_id += 1;

        let subscription = RawDeltaSubscription::new(id, callback);
        self.subscriptions.insert(id, subscription);
        id
    }

    pub fn unsubscribe(&mut self, id: SubscriptionId) -> bool {
        self.subscriptions.remove(&id).is_some()
    }

    pub fn notify_all(&self, deltas: &[Delta<Row>]) {
        for subscription in self.subscriptions.values() {
            subscription.notify(deltas);
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

impl TraceBatchSubscriptionManager {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            next_id: 1,
        }
    }

    pub fn subscribe<F>(&mut self, callback: F) -> SubscriptionId
    where
        F: Fn(&TraceDeltaBatch) + 'static,
    {
        let id = self.next_id;
        self.next_id += 1;

        let subscription = TraceBatchSubscription::new(id, callback);
        self.subscriptions.insert(id, subscription);
        id
    }

    pub fn unsubscribe(&mut self, id: SubscriptionId) -> bool {
        self.subscriptions.remove(&id).is_some()
    }

    pub fn notify_all(&self, batch: &TraceDeltaBatch) {
        for subscription in self.subscriptions.values() {
            subscription.notify(batch);
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionManager {
    /// Creates a new subscription manager.
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            next_id: 1,
        }
    }

    /// Subscribes to changes with the given callback.
    ///
    /// Returns the subscription ID that can be used to unsubscribe.
    pub fn subscribe<F>(&mut self, callback: F) -> SubscriptionId
    where
        F: Fn(&ChangeSet) + 'static,
    {
        let id = self.next_id;
        self.next_id += 1;

        let subscription = Subscription::new(id, callback);
        self.subscriptions.insert(id, subscription);

        id
    }

    /// Unsubscribes by ID.
    ///
    /// Returns true if the subscription was found and removed.
    pub fn unsubscribe(&mut self, id: SubscriptionId) -> bool {
        self.subscriptions.remove(&id).is_some()
    }

    /// Notifies a specific subscription of changes.
    pub fn notify(&self, id: SubscriptionId, changes: &ChangeSet) {
        if let Some(sub) = self.subscriptions.get(&id) {
            sub.notify(changes);
        }
    }

    /// Notifies all active subscriptions of changes.
    pub fn notify_all(&self, changes: &ChangeSet) {
        for sub in self.subscriptions.values() {
            sub.notify(changes);
        }
    }

    /// Returns the number of active subscriptions.
    #[inline]
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    /// Returns true if there are no subscriptions.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    /// Returns all subscription IDs.
    pub fn subscription_ids(&self) -> Vec<SubscriptionId> {
        self.subscriptions.keys().copied().collect()
    }

    /// Clears all subscriptions.
    pub fn clear(&mut self) {
        self.subscriptions.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::rc::Rc;
    use alloc::vec;
    use core::cell::RefCell;
    use cynos_core::{Row, Value};

    fn make_row(id: u64, value: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(value)])
    }

    #[test]
    fn test_subscription_new() {
        let sub = Subscription::new(1, |_| {});
        assert_eq!(sub.id(), 1);
        assert!(sub.is_active());
    }

    #[test]
    fn test_subscription_deactivate() {
        let mut sub = Subscription::new(1, |_| {});
        sub.deactivate();
        assert!(!sub.is_active());
    }

    #[test]
    fn test_subscription_notify() {
        let called = Rc::new(RefCell::new(false));
        let called_clone = called.clone();

        let sub = Subscription::new(1, move |_| {
            *called_clone.borrow_mut() = true;
        });

        let changes = ChangeSet::initial(vec![make_row(1, 10)]);
        sub.notify(&changes);

        assert!(*called.borrow());
    }

    #[test]
    fn test_subscription_notify_inactive() {
        let called = Rc::new(RefCell::new(false));
        let called_clone = called.clone();

        let mut sub = Subscription::new(1, move |_| {
            *called_clone.borrow_mut() = true;
        });
        sub.deactivate();

        let changes = ChangeSet::initial(vec![make_row(1, 10)]);
        sub.notify(&changes);

        assert!(!*called.borrow());
    }

    #[test]
    fn test_subscription_manager_subscribe() {
        let mut manager = SubscriptionManager::new();

        let id1 = manager.subscribe(|_| {});
        let id2 = manager.subscribe(|_| {});

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(manager.len(), 2);
    }

    #[test]
    fn test_subscription_manager_unsubscribe() {
        let mut manager = SubscriptionManager::new();

        let id = manager.subscribe(|_| {});
        assert_eq!(manager.len(), 1);

        assert!(manager.unsubscribe(id));
        assert_eq!(manager.len(), 0);

        assert!(!manager.unsubscribe(id)); // Already removed
    }

    #[test]
    fn test_subscription_manager_notify_all() {
        let mut manager = SubscriptionManager::new();

        let count = Rc::new(RefCell::new(0));
        let count1 = count.clone();
        let count2 = count.clone();

        manager.subscribe(move |_| {
            *count1.borrow_mut() += 1;
        });
        manager.subscribe(move |_| {
            *count2.borrow_mut() += 1;
        });

        let changes = ChangeSet::initial(vec![make_row(1, 10)]);
        manager.notify_all(&changes);

        assert_eq!(*count.borrow(), 2);
    }

    #[test]
    fn test_subscription_manager_notify_specific() {
        let mut manager = SubscriptionManager::new();

        let count = Rc::new(RefCell::new(0));
        let count1 = count.clone();
        let count2 = count.clone();

        let id1 = manager.subscribe(move |_| {
            *count1.borrow_mut() += 1;
        });
        let _id2 = manager.subscribe(move |_| {
            *count2.borrow_mut() += 10;
        });

        let changes = ChangeSet::initial(vec![make_row(1, 10)]);
        manager.notify(id1, &changes);

        assert_eq!(*count.borrow(), 1);
    }

    #[test]
    fn test_subscription_manager_clear() {
        let mut manager = SubscriptionManager::new();

        manager.subscribe(|_| {});
        manager.subscribe(|_| {});

        assert_eq!(manager.len(), 2);
        manager.clear();
        assert!(manager.is_empty());
    }
}
