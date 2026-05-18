//! Posting list implementation for GIN index.
//!
//! A posting list is a sorted list of row IDs that contain a particular key.

use alloc::vec::Vec;
use cynos_core::RowId;

/// A posting list storing row IDs in sorted order.
///
/// The packed representation keeps append-heavy insert workloads and batch
/// intersections cache-friendly while preserving deterministic sorted scans.
#[derive(Debug, Clone, Default)]
pub struct PostingList {
    rows: Vec<RowId>,
}

impl PostingList {
    /// Creates a new empty posting list.
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// Creates a posting list from a row-id vector that is expected to be pre-sorted and unique.
    ///
    /// Public callers still get release-safe invariant preservation: invalid input is normalized
    /// before storage. Internal GIN builders use `from_sorted_unique_unchecked` only after they
    /// have already maintained the invariant while constructing the vector.
    pub fn from_sorted_unique(mut rows: Vec<RowId>) -> Self {
        if !is_strictly_sorted_unique(rows.as_slice()) {
            rows.sort_unstable();
            rows.dedup();
        }
        Self { rows }
    }

    pub(crate) fn from_sorted_unique_unchecked(rows: Vec<RowId>) -> Self {
        debug_assert!(rows.windows(2).all(|window| window[0] < window[1]));
        Self { rows }
    }

    /// Adds a row ID to the posting list.
    pub fn add(&mut self, row_id: RowId) {
        match self.rows.last().copied() {
            None => self.rows.push(row_id),
            Some(last) if row_id > last => self.rows.push(row_id),
            Some(last) if row_id == last => {}
            Some(_) => match self.rows.binary_search(&row_id) {
                Ok(_) => {}
                Err(index) => self.rows.insert(index, row_id),
            },
        }
    }

    /// Merges another sorted, unique posting list into this posting list.
    pub fn merge(&mut self, other: &PostingList) {
        self.merge_sorted_unique(other.rows.as_slice());
    }

    /// Merges a sorted, unique slice of row IDs into this posting list.
    pub fn merge_sorted_unique(&mut self, other: &[RowId]) {
        if other.is_empty() {
            return;
        }
        if self.rows.is_empty() {
            self.rows.extend_from_slice(other);
            return;
        }
        if self.rows.last().copied().unwrap_or(0) < other[0] {
            self.rows.extend_from_slice(other);
            return;
        }

        let mut merged = Vec::with_capacity(self.rows.len() + other.len());
        let mut left = 0usize;
        let mut right = 0usize;

        while left < self.rows.len() && right < other.len() {
            let lhs = self.rows[left];
            let rhs = other[right];
            if lhs < rhs {
                merged.push(lhs);
                left += 1;
            } else if lhs > rhs {
                merged.push(rhs);
                right += 1;
            } else {
                merged.push(lhs);
                left += 1;
                right += 1;
            }
        }

        if left < self.rows.len() {
            merged.extend_from_slice(&self.rows[left..]);
        }
        if right < other.len() {
            merged.extend_from_slice(&other[right..]);
        }

        self.rows = merged;
    }

    /// Removes a row ID from the posting list.
    /// Returns true if the row was present.
    pub fn remove(&mut self, row_id: RowId) -> bool {
        match self.rows.binary_search(&row_id) {
            Ok(index) => {
                self.rows.remove(index);
                true
            }
            Err(_) => false,
        }
    }

    /// Checks if the posting list contains a row ID.
    pub fn contains(&self, row_id: RowId) -> bool {
        self.rows.binary_search(&row_id).is_ok()
    }

    /// Returns the number of row IDs in the posting list.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns true if the posting list is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Converts the posting list to a vector.
    pub fn to_vec(&self) -> Vec<RowId> {
        self.rows.clone()
    }

    /// Returns an iterator over the row IDs.
    pub fn iter(&self) -> impl Iterator<Item = RowId> + '_ {
        self.rows.iter().copied()
    }

    /// Computes the intersection of two posting lists.
    pub fn intersect(&self, other: &PostingList) -> PostingList {
        let mut result = Vec::with_capacity(core::cmp::min(self.len(), other.len()));
        let mut left = 0usize;
        let mut right = 0usize;

        while left < self.rows.len() && right < other.rows.len() {
            let lhs = self.rows[left];
            let rhs = other.rows[right];
            if lhs < rhs {
                left += 1;
            } else if lhs > rhs {
                right += 1;
            } else {
                result.push(lhs);
                left += 1;
                right += 1;
            }
        }

        PostingList::from_sorted_unique_unchecked(result)
    }

    /// Intersects this posting list with a sorted list of candidate row IDs.
    ///
    /// This avoids repeated membership lookups when the caller already has an
    /// ordered intermediate result, which is common in multi-term GIN AND scans.
    pub fn intersect_sorted_candidates(&self, candidates: &[RowId]) -> Vec<RowId> {
        if candidates.is_empty() || self.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(core::cmp::min(candidates.len(), self.len()));
        let mut candidate_idx = 0usize;
        let mut posting_idx = 0usize;

        while candidate_idx < candidates.len() && posting_idx < self.rows.len() {
            let candidate = candidates[candidate_idx];
            let posting_row = self.rows[posting_idx];

            if posting_row < candidate {
                posting_idx += 1;
            } else if posting_row > candidate {
                candidate_idx += 1;
            } else {
                result.push(candidate);
                candidate_idx += 1;
                posting_idx += 1;
            }
        }

        result
    }

    /// Computes the union of two posting lists.
    pub fn union(&self, other: &PostingList) -> PostingList {
        let mut result = Vec::with_capacity(self.len() + other.len());
        let mut left = 0usize;
        let mut right = 0usize;

        while left < self.rows.len() && right < other.rows.len() {
            let lhs = self.rows[left];
            let rhs = other.rows[right];
            if lhs < rhs {
                result.push(lhs);
                left += 1;
            } else if lhs > rhs {
                result.push(rhs);
                right += 1;
            } else {
                result.push(lhs);
                left += 1;
                right += 1;
            }
        }

        if left < self.rows.len() {
            result.extend_from_slice(&self.rows[left..]);
        }
        if right < other.rows.len() {
            result.extend_from_slice(&other.rows[right..]);
        }

        PostingList::from_sorted_unique_unchecked(result)
    }

    /// Computes the difference of two posting lists (self - other).
    pub fn difference(&self, other: &PostingList) -> PostingList {
        let mut result = Vec::with_capacity(self.len());
        let mut left = 0usize;
        let mut right = 0usize;

        while left < self.rows.len() {
            if right >= other.rows.len() {
                result.extend_from_slice(&self.rows[left..]);
                break;
            }

            let lhs = self.rows[left];
            let rhs = other.rows[right];
            if lhs < rhs {
                result.push(lhs);
                left += 1;
            } else if lhs > rhs {
                right += 1;
            } else {
                left += 1;
                right += 1;
            }
        }

        PostingList::from_sorted_unique_unchecked(result)
    }
}

fn is_strictly_sorted_unique(rows: &[RowId]) -> bool {
    rows.windows(2).all(|window| window[0] < window[1])
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_posting_list_new() {
        let pl = PostingList::new();
        assert!(pl.is_empty());
        assert_eq!(pl.len(), 0);
    }

    #[test]
    fn test_posting_list_add() {
        let mut pl = PostingList::new();
        pl.add(1);
        pl.add(3);
        pl.add(2);

        assert_eq!(pl.len(), 3);
        assert!(pl.contains(1));
        assert!(pl.contains(2));
        assert!(pl.contains(3));
        assert!(!pl.contains(4));
    }

    #[test]
    fn test_posting_list_public_sorted_constructor_normalizes_input() {
        let pl = PostingList::from_sorted_unique(vec![3, 1, 2, 2, 1]);

        assert_eq!(pl.to_vec(), vec![1, 2, 3]);
    }

    #[test]
    fn test_posting_list_add_duplicate() {
        let mut pl = PostingList::new();
        pl.add(1);
        pl.add(1);
        pl.add(1);

        assert_eq!(pl.len(), 1);
    }

    #[test]
    fn test_posting_list_remove() {
        let mut pl = PostingList::new();
        pl.add(1);
        pl.add(2);
        pl.add(3);

        assert!(pl.remove(2));
        assert!(!pl.remove(2)); // Already removed
        assert_eq!(pl.len(), 2);
        assert!(!pl.contains(2));
    }

    #[test]
    fn test_posting_list_to_vec() {
        let mut pl = PostingList::new();
        pl.add(3);
        pl.add(1);
        pl.add(2);

        let vec = pl.to_vec();
        assert_eq!(vec, vec![1, 2, 3]); // Sorted
    }

    #[test]
    fn test_posting_list_intersect() {
        let mut pl1 = PostingList::new();
        pl1.add(1);
        pl1.add(2);
        pl1.add(3);

        let mut pl2 = PostingList::new();
        pl2.add(2);
        pl2.add(3);
        pl2.add(4);

        let result = pl1.intersect(&pl2);
        assert_eq!(result.to_vec(), vec![2, 3]);
    }

    #[test]
    fn test_posting_list_union() {
        let mut pl1 = PostingList::new();
        pl1.add(1);
        pl1.add(2);

        let mut pl2 = PostingList::new();
        pl2.add(2);
        pl2.add(3);

        let result = pl1.union(&pl2);
        assert_eq!(result.to_vec(), vec![1, 2, 3]);
    }

    #[test]
    fn test_posting_list_difference() {
        let mut pl1 = PostingList::new();
        pl1.add(1);
        pl1.add(2);
        pl1.add(3);

        let mut pl2 = PostingList::new();
        pl2.add(2);

        let result = pl1.difference(&pl2);
        assert_eq!(result.to_vec(), vec![1, 3]);
    }

    #[test]
    fn test_posting_list_iter() {
        let mut pl = PostingList::new();
        pl.add(3);
        pl.add(1);
        pl.add(2);

        let collected: Vec<_> = pl.iter().collect();
        assert_eq!(collected, vec![1, 2, 3]);
    }

    #[test]
    fn test_intersect_sorted_candidates() {
        let mut pl = PostingList::new();
        pl.add(2);
        pl.add(4);
        pl.add(6);
        pl.add(8);

        let result = pl.intersect_sorted_candidates(&[1, 2, 3, 4, 8, 9]);
        assert_eq!(result, vec![2, 4, 8]);
    }

    #[test]
    fn test_posting_list_merge_sorted_unique() {
        let mut pl = PostingList::from_sorted_unique(vec![1, 3, 5]);
        pl.merge_sorted_unique(&[2, 3, 4, 6]);
        assert_eq!(pl.to_vec(), vec![1, 2, 3, 4, 5, 6]);
    }
}
