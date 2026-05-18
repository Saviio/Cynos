//! Index trait definitions for Cynos database.
//!
//! This module defines the core `Index` trait that all index implementations must satisfy.

use alloc::vec::Vec;
use cynos_core::RowId;

/// A key range for index queries.
#[derive(Clone, Debug, PartialEq)]
pub enum KeyRange<K> {
    /// All keys
    All,
    /// A single key (equality)
    Only(K),
    /// Keys >= lower bound
    LowerBound { value: K, exclusive: bool },
    /// Keys <= upper bound
    UpperBound { value: K, exclusive: bool },
    /// Keys between lower and upper bounds
    Bound {
        lower: K,
        upper: K,
        lower_exclusive: bool,
        upper_exclusive: bool,
    },
}

impl<K: Clone + Ord> KeyRange<K> {
    /// Creates a range for all keys.
    pub fn all() -> Self {
        KeyRange::All
    }

    /// Creates a range for a single key.
    pub fn only(key: K) -> Self {
        KeyRange::Only(key)
    }

    /// Creates a range with a lower bound.
    pub fn lower_bound(value: K, exclusive: bool) -> Self {
        KeyRange::LowerBound { value, exclusive }
    }

    /// Creates a range with an upper bound.
    pub fn upper_bound(value: K, exclusive: bool) -> Self {
        KeyRange::UpperBound { value, exclusive }
    }

    /// Creates a range with both bounds.
    pub fn bound(lower: K, upper: K, lower_exclusive: bool, upper_exclusive: bool) -> Self {
        KeyRange::Bound {
            lower,
            upper,
            lower_exclusive,
            upper_exclusive,
        }
    }

    /// Returns true if this range represents a single value (equality).
    pub fn is_only(&self) -> bool {
        matches!(self, KeyRange::Only(_))
    }

    /// Returns true if this range represents all values (unbounded).
    pub fn is_all(&self) -> bool {
        matches!(self, KeyRange::All)
    }

    /// Checks if two ranges overlap.
    pub fn overlaps(&self, other: &KeyRange<K>) -> bool {
        // All overlaps with everything
        if self.is_all() || other.is_all() {
            return true;
        }

        match (self, other) {
            (KeyRange::Only(k1), KeyRange::Only(k2)) => k1 == k2,
            (KeyRange::Only(k), range) | (range, KeyRange::Only(k)) => range.contains(k),
            (KeyRange::LowerBound { .. }, KeyRange::LowerBound { .. }) => {
                // Two lower bounds always overlap (they extend to infinity)
                true
            }
            (KeyRange::UpperBound { .. }, KeyRange::UpperBound { .. }) => {
                // Two upper bounds always overlap (they extend to negative infinity)
                true
            }
            (
                KeyRange::LowerBound {
                    value: lower,
                    exclusive: lower_ex,
                },
                KeyRange::UpperBound {
                    value: upper,
                    exclusive: upper_ex,
                },
            )
            | (
                KeyRange::UpperBound {
                    value: upper,
                    exclusive: upper_ex,
                },
                KeyRange::LowerBound {
                    value: lower,
                    exclusive: lower_ex,
                },
            ) => {
                // Check if lower <= upper (with exclusivity)
                if *lower_ex || *upper_ex {
                    lower < upper
                } else {
                    lower <= upper
                }
            }
            (
                KeyRange::Bound {
                    lower: l1,
                    upper: u1,
                    lower_exclusive: le1,
                    upper_exclusive: ue1,
                },
                KeyRange::Bound {
                    lower: l2,
                    upper: u2,
                    lower_exclusive: le2,
                    upper_exclusive: ue2,
                },
            ) => {
                // Two bounded ranges overlap if neither is completely before the other
                // Range 1 is before Range 2 if u1 < l2 (or u1 <= l2 if either bound is exclusive)
                let first_before_second = if *ue1 || *le2 { u1 <= l2 } else { u1 < l2 };
                let second_before_first = if *ue2 || *le1 { u2 <= l1 } else { u2 < l1 };
                !first_before_second && !second_before_first
            }
            (
                KeyRange::Bound {
                    upper,
                    upper_exclusive,
                    ..
                },
                KeyRange::LowerBound { value, exclusive },
            )
            | (
                KeyRange::LowerBound { value, exclusive },
                KeyRange::Bound {
                    upper,
                    upper_exclusive,
                    ..
                },
            ) => {
                // Bound overlaps with lower bound if upper >= value
                if *upper_exclusive || *exclusive {
                    upper > value
                } else {
                    upper >= value
                }
            }
            (
                KeyRange::Bound {
                    lower,
                    lower_exclusive,
                    ..
                },
                KeyRange::UpperBound { value, exclusive },
            )
            | (
                KeyRange::UpperBound { value, exclusive },
                KeyRange::Bound {
                    lower,
                    lower_exclusive,
                    ..
                },
            ) => {
                // Bound overlaps with upper bound if lower <= value
                if *lower_exclusive || *exclusive {
                    lower < value
                } else {
                    lower <= value
                }
            }
            _ => true, // Default to true for All cases (already handled above)
        }
    }

    /// Checks if a key is within this range.
    pub fn contains(&self, key: &K) -> bool {
        match self {
            KeyRange::All => true,
            KeyRange::Only(k) => key == k,
            KeyRange::LowerBound { value, exclusive } => {
                if *exclusive {
                    key > value
                } else {
                    key >= value
                }
            }
            KeyRange::UpperBound { value, exclusive } => {
                if *exclusive {
                    key < value
                } else {
                    key <= value
                }
            }
            KeyRange::Bound {
                lower,
                upper,
                lower_exclusive,
                upper_exclusive,
            } => {
                let lower_ok = if *lower_exclusive {
                    key > lower
                } else {
                    key >= lower
                };
                let upper_ok = if *upper_exclusive {
                    key < upper
                } else {
                    key <= upper
                };
                lower_ok && upper_ok
            }
        }
    }
}

/// Core trait for all index implementations.
pub trait Index<K> {
    /// Adds a key-value pair to the index.
    /// For unique indexes, this will fail if the key already exists.
    fn add(&mut self, key: K, value: RowId) -> Result<(), IndexError>;

    /// Adds multiple key-value pairs to the index in batch.
    ///
    /// Implementations may override this to provide a more efficient merge path.
    fn add_batch(&mut self, entries: &[(K, RowId)]) -> Result<(), IndexError>
    where
        K: Clone,
    {
        for (key, row_id) in entries {
            self.add(key.clone(), *row_id)?;
        }
        Ok(())
    }

    /// Sets a key-value pair, replacing any existing values for the key.
    fn set(&mut self, key: K, value: RowId);

    /// Gets all row IDs associated with a key.
    fn get(&self, key: &K) -> Vec<RowId>;

    /// Removes a key (and optionally a specific value) from the index.
    /// If value is None, removes all values for the key.
    fn remove(&mut self, key: &K, value: Option<RowId>);

    /// Removes multiple key-value pairs from the index in batch.
    /// This can be more efficient than calling remove() multiple times.
    /// Default implementation calls remove() for each entry.
    fn remove_batch(&mut self, entries: &[(K, RowId)])
    where
        K: Clone,
    {
        for (key, row_id) in entries {
            self.remove(key, Some(*row_id));
        }
    }

    /// Checks if the index contains the given key.
    fn contains_key(&self, key: &K) -> bool;

    /// Returns the number of entries in the index.
    fn len(&self) -> usize;

    /// Returns true if the index is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears all entries from the index.
    fn clear(&mut self);

    /// Returns the minimum key and its values.
    fn min(&self) -> Option<(&K, Vec<RowId>)>;

    /// Returns the maximum key and its values.
    fn max(&self) -> Option<(&K, Vec<RowId>)>;

    /// Returns the estimated cost (number of rows) for a key range query.
    fn cost(&self, range: &KeyRange<K>) -> usize;
}

/// Trait for indexes that support range queries.
pub trait RangeIndex<K>: Index<K> {
    /// Gets all row IDs within the given key range.
    fn get_range(
        &self,
        range: Option<&KeyRange<K>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
    ) -> Vec<RowId>;
}

/// Error type for index operations.
#[derive(Clone, Debug, PartialEq)]
pub enum IndexError {
    /// Attempted to insert a duplicate key in a unique index.
    DuplicateKey,
    /// Key not found.
    KeyNotFound,
}

impl core::fmt::Display for IndexError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IndexError::DuplicateKey => write!(f, "Duplicate key in unique index"),
            IndexError::KeyNotFound => write!(f, "Key not found"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_range_all() {
        let range: KeyRange<i32> = KeyRange::all();
        assert!(range.contains(&-100));
        assert!(range.contains(&0));
        assert!(range.contains(&100));
    }

    #[test]
    fn test_key_range_only() {
        let range = KeyRange::only(5);
        assert!(!range.contains(&4));
        assert!(range.contains(&5));
        assert!(!range.contains(&6));
    }

    #[test]
    fn test_key_range_lower_bound() {
        let range = KeyRange::lower_bound(5, false);
        assert!(!range.contains(&4));
        assert!(range.contains(&5));
        assert!(range.contains(&6));

        let range_ex = KeyRange::lower_bound(5, true);
        assert!(!range_ex.contains(&4));
        assert!(!range_ex.contains(&5));
        assert!(range_ex.contains(&6));
    }

    #[test]
    fn test_key_range_upper_bound() {
        let range = KeyRange::upper_bound(5, false);
        assert!(range.contains(&4));
        assert!(range.contains(&5));
        assert!(!range.contains(&6));

        let range_ex = KeyRange::upper_bound(5, true);
        assert!(range_ex.contains(&4));
        assert!(!range_ex.contains(&5));
        assert!(!range_ex.contains(&6));
    }

    #[test]
    fn test_key_range_bound() {
        let range = KeyRange::bound(3, 7, false, false);
        assert!(!range.contains(&2));
        assert!(range.contains(&3));
        assert!(range.contains(&5));
        assert!(range.contains(&7));
        assert!(!range.contains(&8));

        let range_ex = KeyRange::bound(3, 7, true, true);
        assert!(!range_ex.contains(&3));
        assert!(range_ex.contains(&5));
        assert!(!range_ex.contains(&7));
    }

    // ==================== Additional KeyRange Tests ====================

    /// Test KeyRange::contains with string keys
    #[test]
    fn test_key_range_contains_string() {
        let range = KeyRange::bound("B", "D", false, false);
        assert!(!range.contains(&"A"));
        assert!(range.contains(&"B"));
        assert!(range.contains(&"C"));
        assert!(range.contains(&"D"));
        assert!(!range.contains(&"E"));
    }

    /// Test KeyRange equality
    #[test]
    fn test_key_range_equality() {
        assert_eq!(KeyRange::<i32>::all(), KeyRange::all());
        assert_eq!(KeyRange::only(1), KeyRange::only(1));
        assert_ne!(KeyRange::only(1), KeyRange::only(2));
        assert_eq!(
            KeyRange::bound(1, 2, true, false),
            KeyRange::bound(1, 2, true, false)
        );
        assert_ne!(
            KeyRange::bound(1, 2, false, false),
            KeyRange::bound(1, 2, true, false)
        );
    }

    /// Test KeyRange with negative numbers
    #[test]
    fn test_key_range_negative() {
        let range = KeyRange::bound(-10, 10, false, false);
        assert!(!range.contains(&-11));
        assert!(range.contains(&-10));
        assert!(range.contains(&0));
        assert!(range.contains(&10));
        assert!(!range.contains(&11));
    }

    /// Test KeyRange::only is equivalent to bound with same lower and upper
    #[test]
    fn test_key_range_only_vs_bound() {
        let only = KeyRange::only(5);
        let bound = KeyRange::bound(5, 5, false, false);

        // Both should contain only 5
        assert!(only.contains(&5));
        assert!(bound.contains(&5));
        assert!(!only.contains(&4));
        assert!(!bound.contains(&4));
        assert!(!only.contains(&6));
        assert!(!bound.contains(&6));
    }

    /// Test KeyRange with mixed exclusive bounds
    #[test]
    fn test_key_range_mixed_exclusive() {
        // Lower inclusive, upper exclusive: [5, 10)
        let range1 = KeyRange::bound(5, 10, false, true);
        assert!(range1.contains(&5));
        assert!(range1.contains(&9));
        assert!(!range1.contains(&10));

        // Lower exclusive, upper inclusive: (5, 10]
        let range2 = KeyRange::bound(5, 10, true, false);
        assert!(!range2.contains(&5));
        assert!(range2.contains(&6));
        assert!(range2.contains(&10));
    }

    /// Test KeyRange with floating point-like behcynos (using i32 scaled)
    #[test]
    fn test_key_range_boundary_precision() {
        // Test exact boundary conditions
        let range = KeyRange::bound(0, 10, true, true);
        assert!(!range.contains(&0));
        assert!(range.contains(&1));
        assert!(range.contains(&9));
        assert!(!range.contains(&10));
    }

    /// Test empty range (lower > upper should contain nothing)
    #[test]
    fn test_key_range_empty() {
        // This is an invalid range but we should handle it gracefully
        let range = KeyRange::bound(10, 5, false, false);
        // No values should be in this range
        assert!(!range.contains(&5));
        assert!(!range.contains(&7));
        assert!(!range.contains(&10));
    }

    /// Test KeyRange clone
    #[test]
    fn test_key_range_clone() {
        let range = KeyRange::bound(1, 10, false, true);
        let cloned = range.clone();
        assert_eq!(range, cloned);
        assert!(cloned.contains(&5));
    }

    // ==================== is_only and is_all Tests ====================

    /// Test is_only method
    #[test]
    fn test_key_range_is_only() {
        assert!(!KeyRange::<i32>::upper_bound(20, false).is_only());
        assert!(!KeyRange::<i32>::all().is_only());
        assert!(KeyRange::only(20).is_only());
        assert!(!KeyRange::lower_bound(20, false).is_only());
        assert!(!KeyRange::bound(10, 20, false, false).is_only());
    }

    /// Test is_all method
    #[test]
    fn test_key_range_is_all() {
        assert!(!KeyRange::only(20).is_all());
        assert!(!KeyRange::<i32>::upper_bound(20, false).is_all());
        assert!(KeyRange::<i32>::all().is_all());
        assert!(!KeyRange::<i32>::lower_bound(20, false).is_all());
    }

    // ==================== Overlaps Tests ====================

    /// Test overlaps - self overlap
    #[test]
    fn test_key_range_overlaps_self() {
        let ranges = [
            KeyRange::<i32>::all(),
            KeyRange::upper_bound(1, false),
            KeyRange::upper_bound(1, true),
            KeyRange::lower_bound(1, false),
            KeyRange::lower_bound(1, true),
            KeyRange::only(1),
            KeyRange::bound(5, 10, false, false),
            KeyRange::bound(5, 10, true, false),
            KeyRange::bound(5, 10, false, true),
            KeyRange::bound(5, 10, true, true),
        ];

        for range in &ranges {
            assert!(range.overlaps(range), "Range should overlap with itself");
            assert!(
                range.overlaps(&KeyRange::all()),
                "Range should overlap with all"
            );
            assert!(
                KeyRange::all().overlaps(range),
                "All should overlap with range"
            );
        }
    }

    /// Test overlaps - overlapping ranges
    #[test]
    fn test_key_range_overlaps_true() {
        let up_to_1 = KeyRange::upper_bound(1, false);
        let up_to_1_ex = KeyRange::upper_bound(1, true);
        let up_to_2 = KeyRange::upper_bound(2, false);
        let at_least_1 = KeyRange::lower_bound(1, false);
        let only_1 = KeyRange::only(1);
        let only_2 = KeyRange::only(2);
        let r1 = KeyRange::bound(5, 10, false, false);
        let r2 = KeyRange::bound(5, 10, true, false);
        let r3 = KeyRange::bound(5, 10, false, true);
        let r4 = KeyRange::bound(5, 10, true, true);
        let r5 = KeyRange::bound(10, 11, false, false);
        let r6 = KeyRange::bound(1, 5, false, false);

        let overlapping_pairs = [
            (&up_to_1, &up_to_1_ex),
            (&up_to_1, &up_to_2),
            (&up_to_1, &only_1),
            (&up_to_1, &at_least_1),
            (&up_to_1, &r6),
            (&up_to_1_ex, &up_to_2),
            (&at_least_1, &only_1),
            (&at_least_1, &only_2),
            (&at_least_1, &r1),
            (&at_least_1, &r6),
            (&r1, &r2),
            (&r1, &r3),
            (&r1, &r4),
            (&r1, &r5),
            (&r1, &r6),
            (&r2, &r3),
            (&r2, &r4),
        ];

        for (a, b) in &overlapping_pairs {
            assert!(a.overlaps(b), "Expected {:?} to overlap with {:?}", a, b);
            assert!(b.overlaps(a), "Expected {:?} to overlap with {:?}", b, a);
        }
    }

    /// Test overlaps - non-overlapping ranges
    #[test]
    fn test_key_range_overlaps_false() {
        let up_to_1 = KeyRange::upper_bound(1, false);
        let up_to_1_ex = KeyRange::upper_bound(1, true);
        let at_least_1_ex = KeyRange::lower_bound(1, true);
        let at_least_2 = KeyRange::lower_bound(2, false);
        let only_1 = KeyRange::only(1);
        let only_2 = KeyRange::only(2);
        let r3 = KeyRange::bound(5, 10, false, true);
        let r4 = KeyRange::bound(5, 10, true, true);
        let r5 = KeyRange::bound(10, 11, false, false);
        let r6 = KeyRange::bound(1, 5, false, false);
        let r2 = KeyRange::bound(5, 10, true, false);

        let excluding_pairs = [
            (&up_to_1, &only_2),
            (&up_to_1_ex, &r6),
            (&up_to_1, &at_least_1_ex),
            (&up_to_1, &at_least_2),
            (&up_to_1_ex, &at_least_1_ex),
            (&up_to_1_ex, &only_1),
            (&up_to_1_ex, &only_2),
            (&only_1, &at_least_1_ex),
            (&only_1, &at_least_2),
            (&r3, &r5),
            (&r4, &r5),
            (&r2, &r6),
            (&r4, &r6),
        ];

        for (a, b) in &excluding_pairs {
            assert!(
                !a.overlaps(b),
                "Expected {:?} NOT to overlap with {:?}",
                a,
                b
            );
            assert!(
                !b.overlaps(a),
                "Expected {:?} NOT to overlap with {:?}",
                b,
                a
            );
        }
    }
}
