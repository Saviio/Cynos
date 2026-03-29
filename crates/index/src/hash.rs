//! Hash index implementation for Cynos database.
//!
//! This module provides a hash-based index for O(1) point queries.

use crate::stats::IndexStats;
use crate::traits::{Index, IndexError, KeyRange, RangeIndex};
use alloc::vec::Vec;
use cynos_core::RowId;
use hashbrown::HashMap;

/// A hash-based index for O(1) point queries.
///
/// This index uses a HashMap internally and supports both unique and non-unique indexes.
/// It does not support range queries efficiently.
#[derive(Debug)]
pub struct HashIndex<K> {
    /// The underlying map from keys to row IDs.
    map: HashMap<K, Vec<RowId>>,
    /// Whether this is a unique index.
    unique: bool,
    /// Statistics for this index.
    stats: IndexStats,
}

impl<K: Eq + core::hash::Hash + Clone> HashIndex<K> {
    /// Creates a new hash index.
    pub fn new(unique: bool) -> Self {
        Self {
            map: HashMap::new(),
            unique,
            stats: IndexStats::new(),
        }
    }

    /// Returns whether this is a unique index.
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// Returns the statistics for this index.
    pub fn stats(&self) -> &IndexStats {
        &self.stats
    }

    /// Returns the number of distinct keys currently stored in the index.
    pub fn distinct_key_count(&self) -> usize {
        self.map.len()
    }

    /// Returns all row IDs in the index.
    pub fn get_all_row_ids(&self) -> Vec<RowId> {
        self.map.values().flatten().copied().collect()
    }
}

impl<K: Eq + core::hash::Hash + Clone + Ord> Index<K> for HashIndex<K> {
    fn add(&mut self, key: K, value: RowId) -> Result<(), IndexError> {
        if self.unique && self.map.contains_key(&key) {
            return Err(IndexError::DuplicateKey);
        }

        self.map.entry(key).or_insert_with(Vec::new).push(value);
        self.stats.add_rows(1);
        Ok(())
    }

    fn set(&mut self, key: K, value: RowId) {
        let old_count = self.map.get(&key).map(|v| v.len()).unwrap_or(0);
        self.map.insert(key, alloc::vec![value]);

        if old_count > 0 {
            self.stats.remove_rows(old_count);
        }
        self.stats.add_rows(1);
    }

    fn get(&self, key: &K) -> Vec<RowId> {
        self.map.get(key).cloned().unwrap_or_default()
    }

    fn remove(&mut self, key: &K, value: Option<RowId>) {
        match value {
            Some(v) => {
                if let Some(values) = self.map.get_mut(key) {
                    let original_len = values.len();
                    values.retain(|&x| x != v);
                    let removed = original_len - values.len();
                    if removed > 0 {
                        self.stats.remove_rows(removed);
                    }
                    if values.is_empty() {
                        self.map.remove(key);
                    }
                }
            }
            None => {
                if let Some(values) = self.map.remove(key) {
                    self.stats.remove_rows(values.len());
                }
            }
        }
    }

    fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    fn len(&self) -> usize {
        self.stats.total_rows()
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn clear(&mut self) {
        self.map.clear();
        self.stats.clear();
    }

    fn min(&self) -> Option<(&K, Vec<RowId>)> {
        // HashMap is unordered, so we need to iterate to find the minimum
        self.map
            .keys()
            .min()
            .map(|k| (k, self.map.get(k).unwrap().clone()))
    }

    fn max(&self) -> Option<(&K, Vec<RowId>)> {
        // HashMap is unordered, so we need to iterate to find the maximum
        self.map
            .keys()
            .max()
            .map(|k| (k, self.map.get(k).unwrap().clone()))
    }

    fn cost(&self, range: &KeyRange<K>) -> usize {
        match range {
            KeyRange::All => self.stats.total_rows(),
            KeyRange::Only(key) => self.map.get(key).map(|v| v.len()).unwrap_or(0),
            _ => self.stats.total_rows(), // Hash index doesn't efficiently support range queries
        }
    }
}

impl<K: Eq + core::hash::Hash + Clone + Ord> RangeIndex<K> for HashIndex<K> {
    fn get_range(
        &self,
        range: Option<&KeyRange<K>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
    ) -> Vec<RowId> {
        let mut entries: Vec<(&K, &Vec<RowId>)> = self
            .map
            .iter()
            .filter(|(key, _)| match range {
                Some(range) => range.contains(key),
                None => true,
            })
            .collect();

        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        if reverse {
            entries.reverse();
        }

        let iter = entries
            .into_iter()
            .flat_map(|(_, row_ids)| row_ids.iter().copied());
        match limit {
            Some(limit) => iter.skip(skip).take(limit).collect(),
            None => iter.skip(skip).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_hash_index_unique() {
        let mut index: HashIndex<i32> = HashIndex::new(true);

        assert!(index.add(1, 100).is_ok());
        assert!(index.add(2, 200).is_ok());
        assert!(index.add(1, 101).is_err()); // Duplicate key

        assert_eq!(index.get(&1), vec![100]);
        assert_eq!(index.get(&2), vec![200]);
        assert_eq!(index.get(&3), Vec::<RowId>::new());
    }

    #[test]
    fn test_hash_index_non_unique() {
        let mut index: HashIndex<i32> = HashIndex::new(false);

        assert!(index.add(1, 100).is_ok());
        assert!(index.add(1, 101).is_ok()); // Duplicate key allowed
        assert!(index.add(2, 200).is_ok());

        assert_eq!(index.get(&1), vec![100, 101]);
        assert_eq!(index.get(&2), vec![200]);
    }

    #[test]
    fn test_hash_index_set() {
        let mut index: HashIndex<i32> = HashIndex::new(false);

        index.set(1, 100);
        index.set(1, 101); // Replaces previous value

        assert_eq!(index.get(&1), vec![101]);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_hash_index_remove() {
        let mut index: HashIndex<i32> = HashIndex::new(false);

        index.add(1, 100).unwrap();
        index.add(1, 101).unwrap();
        index.add(2, 200).unwrap();

        // Remove specific value
        index.remove(&1, Some(100));
        assert_eq!(index.get(&1), vec![101]);

        // Remove all values for key
        index.remove(&1, None);
        assert_eq!(index.get(&1), Vec::<RowId>::new());
        assert!(!index.contains_key(&1));
    }

    #[test]
    fn test_hash_index_contains_key() {
        let mut index: HashIndex<i32> = HashIndex::new(true);

        index.add(1, 100).unwrap();

        assert!(index.contains_key(&1));
        assert!(!index.contains_key(&2));
    }

    #[test]
    fn test_hash_index_clear() {
        let mut index: HashIndex<i32> = HashIndex::new(true);

        index.add(1, 100).unwrap();
        index.add(2, 200).unwrap();

        index.clear();

        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_hash_index_min_max() {
        let mut index: HashIndex<i32> = HashIndex::new(true);

        index.add(5, 500).unwrap();
        index.add(1, 100).unwrap();
        index.add(10, 1000).unwrap();

        let (min_key, min_vals) = index.min().unwrap();
        assert_eq!(*min_key, 1);
        assert_eq!(min_vals, vec![100]);

        let (max_key, max_vals) = index.max().unwrap();
        assert_eq!(*max_key, 10);
        assert_eq!(max_vals, vec![1000]);
    }

    #[test]
    fn test_hash_index_stats() {
        let mut index: HashIndex<i32> = HashIndex::new(false);

        index.add(1, 100).unwrap();
        index.add(1, 101).unwrap();
        index.add(2, 200).unwrap();

        assert_eq!(index.stats().total_rows(), 3);

        index.remove(&1, Some(100));
        assert_eq!(index.stats().total_rows(), 2);

        index.clear();
        assert_eq!(index.stats().total_rows(), 0);
    }

    #[test]
    fn test_hash_index_cost() {
        let mut index: HashIndex<i32> = HashIndex::new(false);

        index.add(1, 100).unwrap();
        index.add(1, 101).unwrap();
        index.add(2, 200).unwrap();

        assert_eq!(index.cost(&KeyRange::All), 3);
        assert_eq!(index.cost(&KeyRange::only(1)), 2);
        assert_eq!(index.cost(&KeyRange::only(2)), 1);
        assert_eq!(index.cost(&KeyRange::only(3)), 0);
    }

    // ==================== Additional Hash Index Tests ====================

    /// Test empty hash index
    #[test]
    fn test_hash_index_empty() {
        let index: HashIndex<i32> = HashIndex::new(true);
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert!(index.min().is_none());
        assert!(index.max().is_none());
        assert!(!index.contains_key(&1));
    }

    /// Test hash index with string keys
    #[test]
    fn test_hash_index_string_keys() {
        let mut index: HashIndex<&str> = HashIndex::new(true);

        index.add("apple", 1).unwrap();
        index.add("banana", 2).unwrap();
        index.add("cherry", 3).unwrap();

        assert_eq!(index.get(&"apple"), vec![1]);
        assert_eq!(index.get(&"banana"), vec![2]);
        assert!(index.contains_key(&"cherry"));
        assert!(!index.contains_key(&"date"));
    }

    /// Test hash index remove non-existent key
    #[test]
    fn test_hash_index_remove_nonexistent() {
        let mut index: HashIndex<i32> = HashIndex::new(true);

        index.add(1, 100).unwrap();
        let len_before = index.len();

        index.remove(&999, None);
        assert_eq!(index.len(), len_before);

        index.remove(&1, Some(999)); // Wrong value
        assert_eq!(index.len(), len_before);
    }

    /// Test hash index remove specific value from non-unique
    #[test]
    fn test_hash_index_remove_specific_value() {
        let mut index: HashIndex<i32> = HashIndex::new(false);

        index.add(1, 100).unwrap();
        index.add(1, 200).unwrap();
        index.add(1, 300).unwrap();

        index.remove(&1, Some(200));
        assert_eq!(index.get(&1), vec![100, 300]);
        assert_eq!(index.len(), 2);
    }

    /// Test hash index stats with set operation
    #[test]
    fn test_hash_index_stats_with_set() {
        let mut index: HashIndex<i32> = HashIndex::new(false);

        index.add(1, 100).unwrap();
        index.add(1, 200).unwrap();
        assert_eq!(index.stats().total_rows(), 2);

        // Set replaces all values for the key
        index.set(1, 999);
        assert_eq!(index.stats().total_rows(), 1);
        assert_eq!(index.get(&1), vec![999]);
    }

    /// Test hash index large scale
    #[test]
    fn test_hash_index_large_scale() {
        let mut index: HashIndex<i32> = HashIndex::new(true);

        for i in 0..1000 {
            index.add(i, i as u64).unwrap();
        }

        assert_eq!(index.len(), 1000);

        for i in 0..1000 {
            assert!(index.contains_key(&i));
            assert_eq!(index.get(&i), vec![i as u64]);
        }

        // Delete half
        for i in (0..1000).step_by(2) {
            index.remove(&i, None);
        }

        assert_eq!(index.len(), 500);
    }

    /// Test hash index min/max with single element
    #[test]
    fn test_hash_index_min_max_single() {
        let mut index: HashIndex<i32> = HashIndex::new(true);

        index.add(42, 420).unwrap();

        let (min_key, min_vals) = index.min().unwrap();
        let (max_key, max_vals) = index.max().unwrap();

        assert_eq!(*min_key, 42);
        assert_eq!(*max_key, 42);
        assert_eq!(min_vals, vec![420]);
        assert_eq!(max_vals, vec![420]);
    }

    /// Test hash index is_unique
    #[test]
    fn test_hash_index_is_unique() {
        let unique_index: HashIndex<i32> = HashIndex::new(true);
        let non_unique_index: HashIndex<i32> = HashIndex::new(false);

        assert!(unique_index.is_unique());
        assert!(!non_unique_index.is_unique());
    }

    /// Test hash index cost with range queries
    #[test]
    fn test_hash_index_cost_range() {
        let mut index: HashIndex<i32> = HashIndex::new(false);

        for i in 0..10 {
            index.add(i, i as u64).unwrap();
        }

        // Hash index returns total_rows for range queries (not efficient)
        let range = KeyRange::lower_bound(5, false);
        assert_eq!(index.cost(&range), 10);

        let range = KeyRange::upper_bound(5, false);
        assert_eq!(index.cost(&range), 10);
    }

    /// Test hash index duplicate key error
    #[test]
    fn test_hash_index_duplicate_error() {
        let mut index: HashIndex<i32> = HashIndex::new(true);

        assert!(index.add(1, 100).is_ok());
        let result = index.add(1, 200);
        assert!(result.is_err());

        match result {
            Err(IndexError::DuplicateKey) => {}
            _ => panic!("Expected DuplicateKey error"),
        }
    }

    /// Test hash index clear and reuse
    #[test]
    fn test_hash_index_clear_reuse() {
        let mut index: HashIndex<i32> = HashIndex::new(true);

        index.add(1, 100).unwrap();
        index.add(2, 200).unwrap();
        index.clear();

        assert!(index.is_empty());

        // Should be able to add same keys again
        index.add(1, 1000).unwrap();
        index.add(2, 2000).unwrap();

        assert_eq!(index.get(&1), vec![1000]);
        assert_eq!(index.get(&2), vec![2000]);
    }
}
