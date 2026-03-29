//! Row structure for Cynos database.
//!
//! This module defines the `Row` struct which represents a single row in a table.

use crate::value::Value;
use alloc::vec::Vec;
use core::hash::{Hash, Hasher};
use core::sync::atomic::{AtomicU64, Ordering};

/// Unique identifier for a row.
pub type RowId = u64;

/// A dummy row ID used for rows that don't correspond to a DB entry
/// (e.g., the result of joining two rows).
pub const DUMMY_ROW_ID: RowId = u64::MAX;

const ROW_ID_HASH_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
const ROW_ID_HASH_PRIME: u64 = 0x0000_0001_0000_01B3;
const JOIN_ROW_ID_DOMAIN: u64 = 0x4A4F_494E_5F49_4E4E;
const LEFT_JOIN_NULL_ROW_ID_DOMAIN: u64 = 0x4A4F_494E_5F4C_4E55;
const RIGHT_JOIN_NULL_ROW_ID_DOMAIN: u64 = 0x4A4F_494E_5F52_4E55;
const AGGREGATE_GROUP_ROW_ID_DOMAIN: u64 = 0x4147_475F_4752_4F55;

/// Global row ID counter for generating unique row IDs.
static NEXT_ROW_ID: AtomicU64 = AtomicU64::new(0);

struct RowIdHasher {
    state: u64,
}

impl RowIdHasher {
    #[inline]
    fn new(domain: u64) -> Self {
        Self {
            state: ROW_ID_HASH_OFFSET ^ domain.rotate_left(7),
        }
    }
}

impl Hasher for RowIdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.state
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state ^= u64::from(byte);
            self.state = self.state.wrapping_mul(ROW_ID_HASH_PRIME);
        }
    }
}

#[inline]
fn finalize_derived_row_id(id: u64) -> RowId {
    if id == DUMMY_ROW_ID {
        DUMMY_ROW_ID.wrapping_sub(1)
    } else {
        id
    }
}

#[inline]
fn hash_row_id<F>(domain: u64, feed: F) -> RowId
where
    F: FnOnce(&mut RowIdHasher),
{
    let mut hasher = RowIdHasher::new(domain);
    feed(&mut hasher);
    finalize_derived_row_id(hasher.finish())
}

/// Gets the next unique row ID.
pub fn next_row_id() -> RowId {
    NEXT_ROW_ID.fetch_add(1, Ordering::SeqCst)
}

/// Reserves a range of row IDs and returns the starting ID.
/// This is useful for bulk inserts where we need to allocate multiple IDs at once.
pub fn reserve_row_ids(count: u64) -> RowId {
    NEXT_ROW_ID.fetch_add(count, Ordering::SeqCst)
}

/// Sets the next row ID. Used by storage backends during initialization.
pub fn set_next_row_id(id: RowId) {
    NEXT_ROW_ID.store(id, Ordering::SeqCst);
}

/// Sets the next row ID only if it's greater than the current value.
pub fn set_next_row_id_if_greater(id: RowId) {
    NEXT_ROW_ID.fetch_max(id, Ordering::SeqCst);
}

/// Derives a deterministic row ID for an inner join row.
#[inline]
pub fn join_row_id(left_id: RowId, right_id: RowId) -> RowId {
    hash_row_id(JOIN_ROW_ID_DOMAIN, |hasher| {
        left_id.hash(hasher);
        right_id.hash(hasher);
    })
}

/// Derives a deterministic row ID for a left/full outer join row with a NULL right side.
#[inline]
pub fn left_join_null_row_id(left_id: RowId) -> RowId {
    hash_row_id(LEFT_JOIN_NULL_ROW_ID_DOMAIN, |hasher| {
        left_id.hash(hasher);
    })
}

/// Derives a deterministic row ID for a right/full outer join row with a NULL left side.
#[inline]
pub fn right_join_null_row_id(right_id: RowId) -> RowId {
    hash_row_id(RIGHT_JOIN_NULL_ROW_ID_DOMAIN, |hasher| {
        right_id.hash(hasher);
    })
}

/// Derives a deterministic row ID for an aggregate group row from its group key.
#[inline]
pub fn aggregate_group_row_id(group_key: &[Value]) -> RowId {
    hash_row_id(AGGREGATE_GROUP_ROW_ID_DOMAIN, |hasher| {
        group_key.len().hash(hasher);
        for value in group_key {
            value.hash(hasher);
        }
    })
}

/// A row in a database table.
#[derive(Clone, Debug)]
pub struct Row {
    /// Unique identifier for this row.
    id: RowId,
    /// Version number for change detection. Incremented on each update.
    /// For JOIN results, this is the sum of source row versions.
    version: u64,
    /// Values stored in this row, indexed by column position.
    values: Vec<Value>,
}

impl Row {
    /// Creates a new row with the given ID and values.
    /// Version defaults to 1 for new rows.
    pub fn new(id: RowId, values: Vec<Value>) -> Self {
        Self {
            id,
            version: 1,
            values,
        }
    }

    /// Creates a new row with the given ID, version, and values.
    pub fn new_with_version(id: RowId, version: u64, values: Vec<Value>) -> Self {
        Self {
            id,
            version,
            values,
        }
    }

    /// Creates a new row with an automatically assigned ID.
    pub fn create(values: Vec<Value>) -> Self {
        Self::new(next_row_id(), values)
    }

    /// Creates a dummy row (for join results, etc.).
    pub fn dummy(values: Vec<Value>) -> Self {
        Self::new(DUMMY_ROW_ID, values)
    }

    /// Creates a dummy row with a specific version (for join results).
    pub fn dummy_with_version(version: u64, values: Vec<Value>) -> Self {
        Self::new_with_version(DUMMY_ROW_ID, version, values)
    }

    /// Returns the row ID.
    #[inline]
    pub fn id(&self) -> RowId {
        self.id
    }

    /// Sets the row ID.
    pub fn set_id(&mut self, id: RowId) {
        self.id = id;
    }

    /// Returns the version number.
    #[inline]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Sets the version number.
    #[inline]
    pub fn set_version(&mut self, version: u64) {
        self.version = version;
    }

    /// Increments the version number and returns the new value.
    #[inline]
    pub fn increment_version(&mut self) -> u64 {
        self.version = self.version.wrapping_add(1);
        self.version
    }

    /// Returns a reference to the values.
    #[inline]
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Returns a mutable reference to the values.
    #[inline]
    pub fn values_mut(&mut self) -> &mut Vec<Value> {
        &mut self.values
    }

    /// Gets a value at the given column index.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Gets a mutable reference to a value at the given column index.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }

    /// Sets a value at the given column index.
    pub fn set(&mut self, index: usize, value: Value) -> bool {
        if index < self.values.len() {
            self.values[index] = value;
            true
        } else {
            false
        }
    }

    /// Returns the number of values in this row.
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if this row has no values.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns true if this is a dummy row.
    #[inline]
    pub fn is_dummy(&self) -> bool {
        self.id == DUMMY_ROW_ID
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.values == other.values
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_row_new() {
        let row = Row::new(1, vec![Value::Int64(42), Value::String("Alice".into())]);
        assert_eq!(row.id(), 1);
        assert_eq!(row.version(), 1);
        assert_eq!(row.len(), 2);
    }

    #[test]
    fn test_row_get_value() {
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        assert_eq!(row.get(0), Some(&Value::Int64(1)));
        assert_eq!(row.get(1), Some(&Value::String("Alice".into())));
        assert_eq!(row.get(2), None);
    }

    #[test]
    fn test_row_set_value() {
        let mut row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        assert!(row.set(0, Value::Int64(100)));
        assert_eq!(row.get(0), Some(&Value::Int64(100)));
        assert!(!row.set(10, Value::Int64(999)));
    }

    #[test]
    fn test_row_create() {
        set_next_row_id(100);
        let row1 = Row::create(vec![Value::Int32(1)]);
        let row2 = Row::create(vec![Value::Int32(2)]);
        assert_eq!(row1.id(), 100);
        assert_eq!(row2.id(), 101);
    }

    #[test]
    fn test_row_dummy() {
        let row = Row::dummy(vec![Value::Int32(1)]);
        assert!(row.is_dummy());
        assert_eq!(row.id(), DUMMY_ROW_ID);
    }

    #[test]
    fn test_row_equality() {
        let row1 = Row::new(1, vec![Value::Int32(42)]);
        let row2 = Row::new(1, vec![Value::Int32(42)]);
        let row3 = Row::new(2, vec![Value::Int32(42)]);
        assert_eq!(row1, row2);
        assert_ne!(row1, row3);
    }

    #[test]
    fn test_row_version() {
        let mut row = Row::new(1, vec![Value::Int32(42)]);
        assert_eq!(row.version(), 1);

        row.increment_version();
        assert_eq!(row.version(), 2);

        row.set_version(10);
        assert_eq!(row.version(), 10);
    }

    #[test]
    fn test_row_dummy_with_version() {
        let row = Row::dummy_with_version(5, vec![Value::Int32(1)]);
        assert!(row.is_dummy());
        assert_eq!(row.version(), 5);
    }

    #[test]
    fn test_join_row_id_is_deterministic() {
        assert_eq!(join_row_id(1, 2), join_row_id(1, 2));
        assert_ne!(join_row_id(1, 2), join_row_id(2, 1));
    }

    #[test]
    fn test_derived_row_id_domains_do_not_collide_for_same_input() {
        let base = 42;
        assert_ne!(join_row_id(base, 7), left_join_null_row_id(base));
        assert_ne!(left_join_null_row_id(base), right_join_null_row_id(base));
    }

    #[test]
    fn test_aggregate_group_row_id_depends_on_group_key() {
        assert_eq!(
            aggregate_group_row_id(&[Value::Int64(1), Value::String("A".into())]),
            aggregate_group_row_id(&[Value::Int64(1), Value::String("A".into())]),
        );
        assert_ne!(
            aggregate_group_row_id(&[Value::Int64(1)]),
            aggregate_group_row_id(&[Value::Int64(2)]),
        );
    }
}
