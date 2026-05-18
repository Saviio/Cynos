//! Change set for tracking query result changes.
//!
//! A ChangeSet represents the difference between two query result states,
//! containing added, removed, and modified rows.

use alloc::vec::Vec;
use cynos_core::Row;
use cynos_incremental::Delta;

/// A set of changes to query results.
///
/// This struct tracks:
/// - `added`: Rows that were inserted
/// - `removed`: Rows that were deleted
/// - `modified`: Rows that were updated (old value, new value)
/// - `current_result`: The complete current result set after applying changes
#[derive(Clone, Debug, Default)]
pub struct ChangeSet {
    /// Rows that were added to the result
    pub added: Vec<Row>,
    /// Rows that were removed from the result
    pub removed: Vec<Row>,
    /// Rows that were modified (old, new)
    pub modified: Vec<(Row, Row)>,
    /// The complete current result set after applying changes
    pub current_result: Vec<Row>,
}

impl ChangeSet {
    /// Creates a new empty change set.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a change set from a slice of deltas.
    ///
    /// Deltas with diff > 0 become additions, diff < 0 become removals.
    pub fn from_deltas(deltas: &[Delta<Row>], current_result: Vec<Row>) -> Self {
        let mut changes = Self::new();
        changes.current_result = current_result;
        for delta in deltas {
            if delta.is_insert() {
                changes.added.push(delta.data.clone());
            } else if delta.is_delete() {
                changes.removed.push(delta.data.clone());
            }
        }
        changes
    }

    /// Creates a change set from deltas without computing current_result.
    ///
    /// This is the O(delta) path — avoids the expensive result() clone.
    /// Consumers that only need added/removed should use this.
    pub fn from_deltas_only(deltas: &[Delta<Row>]) -> Self {
        let mut changes = Self::new();
        changes.replace_from_deltas_only(deltas);
        changes
    }

    /// Replaces this change set with additions/removals derived from deltas.
    ///
    /// This preserves vector capacity across notifications so callers can reuse
    /// a single `ChangeSet` scratch buffer on hot subscription paths.
    pub fn replace_from_deltas_only(&mut self, deltas: &[Delta<Row>]) {
        self.added.clear();
        self.removed.clear();
        self.modified.clear();
        self.current_result.clear();

        for delta in deltas {
            if delta.is_insert() {
                self.added.push(delta.data.clone());
            } else if delta.is_delete() {
                self.removed.push(delta.data.clone());
            }
        }
    }

    /// Creates a change set representing an initial result set.
    ///
    /// All rows are treated as additions.
    /// Note: This clones the rows for the added list. For large datasets,
    /// consider using `initial_no_added` if you don't need the added list.
    pub fn initial(rows: Vec<Row>) -> Self {
        Self {
            added: rows.clone(),
            removed: Vec::new(),
            modified: Vec::new(),
            current_result: rows,
        }
    }

    /// Creates a change set representing an initial result set without populating added.
    ///
    /// This is more efficient for large datasets when you only need current_result.
    pub fn initial_result_only(rows: Vec<Row>) -> Self {
        Self {
            added: Vec::new(),
            removed: Vec::new(),
            modified: Vec::new(),
            current_result: rows,
        }
    }

    /// Returns true if there are no changes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.modified.is_empty()
    }

    /// Returns the total number of changes.
    #[inline]
    pub fn len(&self) -> usize {
        self.added.len() + self.removed.len() + self.modified.len()
    }

    /// Merges another change set into this one.
    pub fn merge(&mut self, other: ChangeSet) {
        self.added.extend(other.added);
        self.removed.extend(other.removed);
        self.modified.extend(other.modified);
    }

    /// Clears all changes.
    pub fn clear(&mut self) {
        self.added.clear();
        self.removed.clear();
        self.modified.clear();
    }

    /// Adds an inserted row.
    #[inline]
    pub fn add(&mut self, row: Row) {
        self.added.push(row);
    }

    /// Adds a removed row.
    #[inline]
    pub fn remove(&mut self, row: Row) {
        self.removed.push(row);
    }

    /// Adds a modified row pair.
    #[inline]
    pub fn modify(&mut self, old: Row, new: Row) {
        self.modified.push((old, new));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use cynos_core::Value;

    fn make_row(id: u64, value: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(value)])
    }

    #[test]
    fn test_change_set_new() {
        let cs = ChangeSet::new();
        assert!(cs.is_empty());
        assert_eq!(cs.len(), 0);
    }

    #[test]
    fn test_change_set_from_deltas() {
        let deltas = vec![
            Delta::insert(make_row(1, 10)),
            Delta::insert(make_row(2, 20)),
            Delta::delete(make_row(3, 30)),
        ];

        let current = vec![make_row(1, 10), make_row(2, 20)];
        let cs = ChangeSet::from_deltas(&deltas, current);
        assert_eq!(cs.added.len(), 2);
        assert_eq!(cs.removed.len(), 1);
        assert!(cs.modified.is_empty());
        assert_eq!(cs.current_result.len(), 2);
    }

    #[test]
    fn test_change_set_initial() {
        let rows = vec![make_row(1, 10), make_row(2, 20)];
        let cs = ChangeSet::initial(rows);

        assert_eq!(cs.added.len(), 2);
        assert!(cs.removed.is_empty());
        assert!(cs.modified.is_empty());
        assert_eq!(cs.current_result.len(), 2);
    }

    #[test]
    fn test_change_set_merge() {
        let mut cs1 = ChangeSet::new();
        cs1.add(make_row(1, 10));

        let mut cs2 = ChangeSet::new();
        cs2.add(make_row(2, 20));
        cs2.remove(make_row(3, 30));

        cs1.merge(cs2);

        assert_eq!(cs1.added.len(), 2);
        assert_eq!(cs1.removed.len(), 1);
    }

    #[test]
    fn test_change_set_modify() {
        let mut cs = ChangeSet::new();
        cs.modify(make_row(1, 10), make_row(1, 20));

        assert!(cs.added.is_empty());
        assert!(cs.removed.is_empty());
        assert_eq!(cs.modified.len(), 1);
        assert_eq!(cs.len(), 1);
    }

    #[test]
    fn test_change_set_clear() {
        let mut cs = ChangeSet::new();
        cs.add(make_row(1, 10));
        cs.remove(make_row(2, 20));

        assert!(!cs.is_empty());
        cs.clear();
        assert!(cs.is_empty());
    }

    #[test]
    fn test_change_set_replace_from_deltas_only_resets_previous_contents() {
        let mut cs = ChangeSet::initial(vec![make_row(1, 10), make_row(2, 20)]);
        cs.modify(make_row(3, 30), make_row(3, 31));

        cs.replace_from_deltas_only(&[
            Delta::insert(make_row(4, 40)),
            Delta::delete(make_row(5, 50)),
        ]);

        assert_eq!(cs.added.len(), 1);
        assert_eq!(cs.removed.len(), 1);
        assert!(cs.modified.is_empty());
        assert!(cs.current_result.is_empty());
    }
}
