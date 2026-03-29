//! B+Tree implementation.

use super::iter::BTreeIterator;
use super::node::{Node, NodeId};
use crate::comparator::{Comparator, SimpleComparator};
use crate::stats::IndexStats;
use crate::traits::{Index, IndexError, KeyRange, RangeIndex};
use alloc::vec::Vec;
use cynos_core::RowId;

/// Default order (branching factor) for the B+Tree.
/// Optimized for L1 cache (64 keys * 8 bytes = 512 bytes per node).
#[allow(dead_code)]
pub const DEFAULT_ORDER: usize = 64;

/// A B+Tree index for efficient range queries.
#[derive(Debug)]
pub struct BTreeIndex<K> {
    /// Arena of all nodes.
    arena: Vec<Node<K>>,
    /// Root node ID.
    root: NodeId,
    /// Maximum number of keys per node.
    order: usize,
    /// Whether this is a unique index.
    unique: bool,
    /// Comparator for key ordering.
    comparator: SimpleComparator,
    /// Statistics for this index.
    stats: IndexStats,
}

impl<K: Clone + Ord> BTreeIndex<K> {
    /// Creates a new B+Tree index with the given order.
    pub fn new(order: usize, unique: bool) -> Self {
        Self::with_comparator(order, unique, SimpleComparator::asc())
    }

    /// Creates a new B+Tree index with a custom comparator.
    pub fn with_comparator(order: usize, unique: bool, comparator: SimpleComparator) -> Self {
        let mut arena = Vec::new();
        let root = Self::alloc_node(&mut arena, Node::new_leaf());

        Self {
            arena,
            root,
            order,
            unique,
            comparator,
            stats: IndexStats::new(),
        }
    }

    /// Returns the statistics for this index.
    pub fn stats(&self) -> &IndexStats {
        &self.stats
    }

    /// Returns the number of distinct keys currently stored in the tree.
    pub fn distinct_key_count(&self) -> usize {
        self.arena
            .iter()
            .filter(|node| node.is_leaf)
            .map(Node::key_count)
            .sum()
    }

    /// Returns whether this is a unique index.
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// Allocates a new node in the arena and returns its ID.
    fn alloc_node(arena: &mut Vec<Node<K>>, node: Node<K>) -> NodeId {
        let id = arena.len();
        arena.push(node);
        id
    }

    /// Finds the leaf node that should contain the given key.
    fn find_leaf(&self, key: &K) -> NodeId {
        let mut current = self.root;

        loop {
            let node = &self.arena[current];
            if node.is_leaf {
                return current;
            }

            // Find the child to descend into
            let pos = self.find_child_position(node, key);
            current = node.children[pos];
        }
    }

    /// Finds the position of the child to descend into for an internal node.
    /// Uses binary search for O(log n) performance instead of linear scan.
    #[inline]
    fn find_child_position(&self, node: &Node<K>, key: &K) -> usize {
        // Binary search: find the first key that is greater than the search key
        // The child at that position is where we should descend
        let pos = node
            .keys
            .partition_point(|k| !self.comparator.is_less(key, k));
        // partition_point returns the index where the predicate becomes false
        // i.e., the first position where key < k
        pos.min(node.children.len().saturating_sub(1))
    }

    /// Inserts a key-value pair into the tree.
    fn insert(&mut self, key: K, value: RowId) -> Result<(), IndexError> {
        let leaf_id = self.find_leaf(&key);

        // Check for duplicate key in unique index
        if self.unique {
            let leaf = &self.arena[leaf_id];
            if let Some(_) = leaf.find_key(&key) {
                return Err(IndexError::DuplicateKey);
            }
        }

        // Insert into leaf
        let pos = self.arena[leaf_id].find_key_position(&key);
        self.arena[leaf_id].insert_at(pos, key.clone(), value);
        self.stats.add_rows(1);

        // Check if we need to split
        if self.arena[leaf_id].key_count() >= self.order {
            self.split_leaf(leaf_id);
        }

        Ok(())
    }

    /// Splits a leaf node.
    fn split_leaf(&mut self, leaf_id: NodeId) {
        let mid = self.arena[leaf_id].key_count() / 2;

        // Create new leaf with right half
        let mut new_leaf = Node::new_leaf();
        new_leaf.keys = self.arena[leaf_id].keys.split_off(mid);
        new_leaf.values = self.arena[leaf_id].values.split_off(mid);
        new_leaf.next = self.arena[leaf_id].next;
        new_leaf.prev = Some(leaf_id);
        new_leaf.parent = self.arena[leaf_id].parent;

        let new_leaf_id = Self::alloc_node(&mut self.arena, new_leaf);

        // Update next pointer of old leaf
        if let Some(next_id) = self.arena[leaf_id].next {
            self.arena[next_id].prev = Some(new_leaf_id);
        }
        self.arena[leaf_id].next = Some(new_leaf_id);

        // Get the key to promote
        let promote_key = self.arena[new_leaf_id].keys[0].clone();

        // Insert into parent
        self.insert_into_parent(leaf_id, promote_key, new_leaf_id);
    }

    /// Inserts a key and child into the parent node.
    fn insert_into_parent(&mut self, left_id: NodeId, key: K, right_id: NodeId) {
        let parent_id = self.arena[left_id].parent;

        match parent_id {
            None => {
                // Create new root
                let mut new_root = Node::new_internal();
                new_root.children.push(left_id);
                new_root.children.push(right_id);
                new_root.keys.push(key);

                let new_root_id = Self::alloc_node(&mut self.arena, new_root);
                self.arena[left_id].parent = Some(new_root_id);
                self.arena[right_id].parent = Some(new_root_id);
                self.root = new_root_id;
            }
            Some(parent_id) => {
                // Find position to insert
                let pos = self.find_child_position(&self.arena[parent_id], &key);
                self.arena[parent_id].keys.insert(pos, key);
                self.arena[parent_id].children.insert(pos + 1, right_id);
                self.arena[right_id].parent = Some(parent_id);

                // Check if parent needs to split
                if self.arena[parent_id].key_count() >= self.order {
                    self.split_internal(parent_id);
                }
            }
        }
    }

    /// Splits an internal node.
    fn split_internal(&mut self, node_id: NodeId) {
        let mid = self.arena[node_id].key_count() / 2;

        // Get the key to promote (middle key)
        let promote_key = self.arena[node_id].keys[mid].clone();

        // Create new internal node with right half
        let mut new_node = Node::new_internal();
        new_node.keys = self.arena[node_id].keys.split_off(mid + 1);
        new_node.children = self.arena[node_id].children.split_off(mid + 1);
        new_node.parent = self.arena[node_id].parent;

        // Remove the promoted key from the left node
        self.arena[node_id].keys.pop();

        let new_node_id = Self::alloc_node(&mut self.arena, new_node);

        // Update parent pointers of moved children
        let children_to_update: Vec<NodeId> = self.arena[new_node_id].children.clone();
        for child_id in children_to_update {
            self.arena[child_id].parent = Some(new_node_id);
        }

        // Insert into parent
        self.insert_into_parent(node_id, promote_key, new_node_id);
    }

    /// Removes a key (and optionally a specific value) from the tree.
    fn delete(&mut self, key: &K, value: Option<RowId>) {
        let leaf_id = self.find_leaf(key);
        let leaf = &self.arena[leaf_id];

        if let Some(pos) = leaf.find_key(key) {
            let removed = self.arena[leaf_id].remove_at(pos, value);
            self.stats.remove_rows(removed);

            // Handle underflow if needed (simplified - just check if empty)
            if self.arena[leaf_id].is_empty() && leaf_id != self.root {
                self.handle_underflow(leaf_id);
            }
        }
    }

    /// Handles underflow after deletion (simplified version).
    fn handle_underflow(&mut self, node_id: NodeId) {
        let parent_id = match self.arena[node_id].parent {
            Some(p) => p,
            None => return, // Root node, nothing to do
        };

        let node = &self.arena[node_id];
        let min_keys = (self.order - 1) / 2;

        if node.key_count() >= min_keys {
            return; // No underflow
        }

        // Find position in parent
        let pos = self.arena[parent_id]
            .children
            .iter()
            .position(|&c| c == node_id)
            .unwrap();

        // Try to borrow from left sibling
        if pos > 0 {
            let left_sibling_id = self.arena[parent_id].children[pos - 1];
            if self.arena[left_sibling_id].key_count() > min_keys {
                self.borrow_from_left(node_id, left_sibling_id, parent_id, pos);
                return;
            }
        }

        // Try to borrow from right sibling
        if pos < self.arena[parent_id].children.len() - 1 {
            let right_sibling_id = self.arena[parent_id].children[pos + 1];
            if self.arena[right_sibling_id].key_count() > min_keys {
                self.borrow_from_right(node_id, right_sibling_id, parent_id, pos);
                return;
            }
        }

        // Merge with a sibling
        if pos > 0 {
            let left_sibling_id = self.arena[parent_id].children[pos - 1];
            self.merge_nodes(left_sibling_id, node_id, parent_id, pos - 1);
        } else if pos < self.arena[parent_id].children.len() - 1 {
            let right_sibling_id = self.arena[parent_id].children[pos + 1];
            self.merge_nodes(node_id, right_sibling_id, parent_id, pos);
        }
    }

    /// Borrows a key from the left sibling.
    fn borrow_from_left(
        &mut self,
        node_id: NodeId,
        left_id: NodeId,
        parent_id: NodeId,
        pos: usize,
    ) {
        let is_leaf = self.arena[node_id].is_leaf;

        if is_leaf {
            // Move last key-value from left to front of node
            let key = self.arena[left_id].keys.pop().unwrap();
            let values = self.arena[left_id].values.pop().unwrap();
            self.arena[node_id].keys.insert(0, key.clone());
            self.arena[node_id].values.insert(0, values);

            // Update parent key
            self.arena[parent_id].keys[pos - 1] = key;
        } else {
            // Move parent key down and last key from left up
            let parent_key = self.arena[parent_id].keys[pos - 1].clone();
            let left_key = self.arena[left_id].keys.pop().unwrap();
            let left_child = self.arena[left_id].children.pop().unwrap();

            self.arena[node_id].keys.insert(0, parent_key);
            self.arena[node_id].children.insert(0, left_child);
            self.arena[parent_id].keys[pos - 1] = left_key;

            // Update parent pointer of moved child
            self.arena[left_child].parent = Some(node_id);
        }
    }

    /// Borrows a key from the right sibling.
    fn borrow_from_right(
        &mut self,
        node_id: NodeId,
        right_id: NodeId,
        parent_id: NodeId,
        pos: usize,
    ) {
        let is_leaf = self.arena[node_id].is_leaf;

        if is_leaf {
            // Move first key-value from right to end of node
            let key = self.arena[right_id].keys.remove(0);
            let values = self.arena[right_id].values.remove(0);
            self.arena[node_id].keys.push(key);
            self.arena[node_id].values.push(values);

            // Update parent key
            let new_separator = self.arena[right_id].keys[0].clone();
            self.arena[parent_id].keys[pos] = new_separator;
        } else {
            // Move parent key down and first key from right up
            let parent_key = self.arena[parent_id].keys[pos].clone();
            let right_key = self.arena[right_id].keys.remove(0);
            let right_child = self.arena[right_id].children.remove(0);

            self.arena[node_id].keys.push(parent_key);
            self.arena[node_id].children.push(right_child);
            self.arena[parent_id].keys[pos] = right_key;

            // Update parent pointer of moved child
            self.arena[right_child].parent = Some(node_id);
        }
    }

    /// Merges two sibling nodes.
    fn merge_nodes(&mut self, left_id: NodeId, right_id: NodeId, parent_id: NodeId, pos: usize) {
        let is_leaf = self.arena[left_id].is_leaf;

        if is_leaf {
            // Move all keys and values from right to left
            let right_keys: Vec<K> = self.arena[right_id].keys.drain(..).collect();
            let right_values: Vec<Vec<RowId>> = self.arena[right_id].values.drain(..).collect();

            self.arena[left_id].keys.extend(right_keys);
            self.arena[left_id].values.extend(right_values);

            // Update leaf chain
            self.arena[left_id].next = self.arena[right_id].next;
            if let Some(next_id) = self.arena[right_id].next {
                self.arena[next_id].prev = Some(left_id);
            }
        } else {
            // Move separator key from parent
            let separator = self.arena[parent_id].keys[pos].clone();
            self.arena[left_id].keys.push(separator);

            // Move all keys and children from right to left
            let right_keys: Vec<K> = self.arena[right_id].keys.drain(..).collect();
            let right_children: Vec<NodeId> = self.arena[right_id].children.drain(..).collect();

            self.arena[left_id].keys.extend(right_keys);

            // Update parent pointers before extending
            for &child_id in &right_children {
                self.arena[child_id].parent = Some(left_id);
            }
            self.arena[left_id].children.extend(right_children);
        }

        // Remove separator and right child from parent
        self.arena[parent_id].keys.remove(pos);
        self.arena[parent_id].children.remove(pos + 1);

        // Check if parent is now empty (and is root)
        if parent_id == self.root && self.arena[parent_id].keys.is_empty() {
            self.root = left_id;
            self.arena[left_id].parent = None;
        } else if parent_id != self.root {
            self.handle_underflow(parent_id);
        }
    }

    /// Returns the leftmost leaf node.
    fn leftmost_leaf(&self) -> NodeId {
        let mut current = self.root;
        while !self.arena[current].is_leaf {
            current = self.arena[current].children[0];
        }
        current
    }

    /// Returns the rightmost leaf node.
    fn rightmost_leaf(&self) -> NodeId {
        let mut current = self.root;
        while !self.arena[current].is_leaf {
            let children = &self.arena[current].children;
            current = children[children.len() - 1];
        }
        current
    }

    /// Finds the leaf node and position for a key range start.
    fn find_range_start(&self, range: &KeyRange<K>) -> Option<(NodeId, usize)> {
        match range {
            KeyRange::All => {
                let leaf = self.leftmost_leaf();
                if self.arena[leaf].is_empty() {
                    None
                } else {
                    Some((leaf, 0))
                }
            }
            KeyRange::Only(key) | KeyRange::LowerBound { value: key, .. } => {
                let leaf = self.find_leaf(key);
                let node = &self.arena[leaf];
                let pos = node.find_key_position(key);
                if pos < node.key_count() {
                    Some((leaf, pos))
                } else if let Some(next) = node.next {
                    Some((next, 0))
                } else {
                    None
                }
            }
            KeyRange::UpperBound { .. } => {
                let leaf = self.leftmost_leaf();
                if self.arena[leaf].is_empty() {
                    None
                } else {
                    Some((leaf, 0))
                }
            }
            KeyRange::Bound { lower, .. } => {
                let leaf = self.find_leaf(lower);
                let node = &self.arena[leaf];
                let pos = node.find_key_position(lower);
                if pos < node.key_count() {
                    Some((leaf, pos))
                } else if let Some(next) = node.next {
                    Some((next, 0))
                } else {
                    None
                }
            }
        }
    }

    #[inline]
    fn key_past_forward_range(range: &KeyRange<K>, key: &K) -> bool {
        match range {
            KeyRange::All | KeyRange::LowerBound { .. } => false,
            KeyRange::Only(value) => key > value,
            KeyRange::UpperBound { value, exclusive } => {
                if *exclusive {
                    key >= value
                } else {
                    key > value
                }
            }
            KeyRange::Bound {
                upper,
                upper_exclusive,
                ..
            } => {
                if *upper_exclusive {
                    key >= upper
                } else {
                    key > upper
                }
            }
        }
    }

    #[inline]
    fn key_past_reverse_range(range: &KeyRange<K>, key: &K) -> bool {
        match range {
            KeyRange::All | KeyRange::UpperBound { .. } => false,
            KeyRange::Only(value) => key < value,
            KeyRange::LowerBound { value, exclusive } => {
                if *exclusive {
                    key <= value
                } else {
                    key < value
                }
            }
            KeyRange::Bound {
                lower,
                lower_exclusive,
                ..
            } => {
                if *lower_exclusive {
                    key <= lower
                } else {
                    key < lower
                }
            }
        }
    }

    /// Visits row IDs within the given key range without first materializing a `Vec<RowId>`.
    /// Return `false` from the visitor to stop early.
    pub fn visit_range<F>(
        &self,
        range: Option<&KeyRange<K>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
        mut visitor: F,
    ) where
        F: FnMut(RowId) -> bool,
    {
        let range = range.cloned().unwrap_or(KeyRange::All);

        let start = if reverse {
            match &range {
                KeyRange::All | KeyRange::LowerBound { .. } => {
                    let leaf = self.rightmost_leaf();
                    if self.arena[leaf].is_empty() {
                        None
                    } else {
                        Some((leaf, self.arena[leaf].key_count() - 1))
                    }
                }
                KeyRange::Only(key)
                | KeyRange::UpperBound { value: key, .. }
                | KeyRange::Bound { upper: key, .. } => {
                    let leaf = self.find_leaf(key);
                    let node = &self.arena[leaf];
                    let pos = node.find_key_position(key);
                    if pos > 0 {
                        Some((leaf, pos - 1))
                    } else if let Some(prev) = node.prev {
                        let prev_node = &self.arena[prev];
                        if prev_node.is_empty() {
                            None
                        } else {
                            Some((prev, prev_node.key_count() - 1))
                        }
                    } else {
                        None
                    }
                }
            }
        } else {
            self.find_range_start(&range)
        };

        let Some((start_node, start_pos)) = start else {
            return;
        };

        let mut skipped = 0usize;
        let mut emitted = 0usize;

        for (key, row_id) in BTreeIterator::new_at(&self.arena, start_node, start_pos, reverse) {
            if range.contains(key) {
                if skipped < skip {
                    skipped += 1;
                    continue;
                }
                if let Some(limit) = limit {
                    if emitted >= limit {
                        break;
                    }
                }
                if !visitor(row_id) {
                    break;
                }
                emitted += 1;
                continue;
            }

            if (!reverse && Self::key_past_forward_range(&range, key))
                || (reverse && Self::key_past_reverse_range(&range, key))
            {
                break;
            }
        }
    }
}

impl<K: Clone + Ord> Index<K> for BTreeIndex<K> {
    fn add(&mut self, key: K, value: RowId) -> Result<(), IndexError> {
        self.insert(key, value)
    }

    fn set(&mut self, key: K, value: RowId) {
        // Remove existing values for the key first
        self.delete(&key, None);
        // Then insert the new value
        let _ = self.insert(key, value);
    }

    fn get(&self, key: &K) -> Vec<RowId> {
        let leaf_id = self.find_leaf(key);
        let leaf = &self.arena[leaf_id];

        if let Some(pos) = leaf.find_key(key) {
            leaf.values[pos].clone()
        } else {
            Vec::new()
        }
    }

    fn remove(&mut self, key: &K, value: Option<RowId>) {
        self.delete(key, value);
    }

    fn remove_batch(&mut self, entries: &[(K, RowId)]) {
        if entries.is_empty() {
            return;
        }

        // Sort entries by key for better cache locality and to process same-key entries together
        let mut sorted_entries: Vec<_> = entries.iter().collect();
        sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Track nodes that need underflow handling (defer to end for efficiency)
        let mut underflow_nodes: Vec<NodeId> = Vec::new();

        // Process entries in sorted order
        let mut i = 0;
        while i < sorted_entries.len() {
            let key = &sorted_entries[i].0;
            let leaf_id = self.find_leaf(key);

            // Collect all row_ids to remove for this key
            let mut row_ids_to_remove: Vec<RowId> = Vec::new();
            while i < sorted_entries.len() && &sorted_entries[i].0 == key {
                row_ids_to_remove.push(sorted_entries[i].1);
                i += 1;
            }

            // Remove all row_ids for this key at once
            let leaf = &self.arena[leaf_id];
            if let Some(pos) = leaf.find_key(key) {
                let values = &mut self.arena[leaf_id].values[pos];
                let original_len = values.len();

                // Use HashSet for O(1) lookup when removing multiple values
                if row_ids_to_remove.len() > 4 {
                    let to_remove: hashbrown::HashSet<RowId> =
                        row_ids_to_remove.iter().copied().collect();
                    values.retain(|v| !to_remove.contains(v));
                } else {
                    // For small number of removals, linear scan is faster
                    for row_id in &row_ids_to_remove {
                        if let Some(idx) = values.iter().position(|&v| v == *row_id) {
                            values.swap_remove(idx);
                        }
                    }
                }

                let removed = original_len - values.len();
                self.stats.remove_rows(removed);

                // If all values removed, remove the key
                if values.is_empty() {
                    self.arena[leaf_id].keys.remove(pos);
                    self.arena[leaf_id].values.remove(pos);

                    // Track for deferred underflow handling
                    if self.arena[leaf_id].is_empty() && leaf_id != self.root {
                        if !underflow_nodes.contains(&leaf_id) {
                            underflow_nodes.push(leaf_id);
                        }
                    }
                }
            }
        }

        // Handle underflows at the end (deferred for efficiency)
        // Process in reverse order to handle children before parents
        for node_id in underflow_nodes.into_iter().rev() {
            if !self.arena[node_id].is_empty() {
                continue; // Node was filled by subsequent operations
            }
            if node_id == self.root {
                continue; // Root doesn't need underflow handling
            }
            self.handle_underflow(node_id);
        }
    }

    fn contains_key(&self, key: &K) -> bool {
        let leaf_id = self.find_leaf(key);
        self.arena[leaf_id].find_key(key).is_some()
    }

    fn len(&self) -> usize {
        self.stats.total_rows()
    }

    fn clear(&mut self) {
        self.arena.clear();
        self.root = Self::alloc_node(&mut self.arena, Node::new_leaf());
        self.stats.clear();
    }

    fn min(&self) -> Option<(&K, Vec<RowId>)> {
        let leaf_id = self.leftmost_leaf();
        let leaf = &self.arena[leaf_id];
        if leaf.is_empty() {
            None
        } else {
            Some((&leaf.keys[0], leaf.values[0].clone()))
        }
    }

    fn max(&self) -> Option<(&K, Vec<RowId>)> {
        let leaf_id = self.rightmost_leaf();
        let leaf = &self.arena[leaf_id];
        if leaf.is_empty() {
            None
        } else {
            let last = leaf.key_count() - 1;
            Some((&leaf.keys[last], leaf.values[last].clone()))
        }
    }

    fn cost(&self, range: &KeyRange<K>) -> usize {
        match range {
            KeyRange::All => self.stats.total_rows(),
            KeyRange::Only(key) => self.get(key).len(),
            _ => self.stats.total_rows(), // Simplified estimation
        }
    }
}

impl<K: Clone + Ord> RangeIndex<K> for BTreeIndex<K> {
    fn get_range(
        &self,
        range: Option<&KeyRange<K>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
    ) -> Vec<RowId> {
        let mut result = Vec::new();
        self.visit_range(range, reverse, limit, skip, |row_id| {
            result.push(row_id);
            true
        });
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_btree_new() {
        let tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_btree_insert_get() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        tree.add(10, 100).unwrap();
        tree.add(20, 200).unwrap();
        tree.add(5, 50).unwrap();

        assert_eq!(tree.get(&10), vec![100]);
        assert_eq!(tree.get(&20), vec![200]);
        assert_eq!(tree.get(&5), vec![50]);
        assert_eq!(tree.get(&15), Vec::<RowId>::new());
    }

    #[test]
    fn test_btree_unique_constraint() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        tree.add(10, 100).unwrap();
        assert!(tree.add(10, 101).is_err());
    }

    #[test]
    fn test_btree_non_unique() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);

        tree.add(10, 100).unwrap();
        tree.add(10, 101).unwrap();

        assert_eq!(tree.get(&10), vec![100, 101]);
    }

    #[test]
    fn test_btree_remove() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);

        tree.add(10, 100).unwrap();
        tree.add(10, 101).unwrap();
        tree.add(20, 200).unwrap();

        // Remove specific value
        tree.remove(&10, Some(100));
        assert_eq!(tree.get(&10), vec![101]);

        // Remove all values for key
        tree.remove(&10, None);
        assert_eq!(tree.get(&10), Vec::<RowId>::new());
        assert!(!tree.contains_key(&10));
    }

    #[test]
    fn test_btree_contains_key() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        tree.add(10, 100).unwrap();

        assert!(tree.contains_key(&10));
        assert!(!tree.contains_key(&20));
    }

    #[test]
    fn test_btree_min_max() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        tree.add(10, 100).unwrap();
        tree.add(5, 50).unwrap();
        tree.add(20, 200).unwrap();

        let (min_key, min_vals) = tree.min().unwrap();
        assert_eq!(*min_key, 5);
        assert_eq!(min_vals, vec![50]);

        let (max_key, max_vals) = tree.max().unwrap();
        assert_eq!(*max_key, 20);
        assert_eq!(max_vals, vec![200]);
    }

    #[test]
    fn test_btree_clear() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        tree.add(10, 100).unwrap();
        tree.add(20, 200).unwrap();

        tree.clear();

        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_btree_visit_range_matches_get_range_forward() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);
        for (key, row_id) in [(1, 10), (2, 20), (2, 21), (3, 30), (4, 40)] {
            tree.add(key, row_id).unwrap();
        }

        let range = KeyRange::bound(2, 3, false, false);
        let expected = tree.get_range(Some(&range), false, Some(2), 1);
        let mut actual = Vec::new();
        tree.visit_range(Some(&range), false, Some(2), 1, |row_id| {
            actual.push(row_id);
            true
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_btree_visit_range_matches_get_range_reverse() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);
        for (key, row_id) in [(1, 10), (2, 20), (2, 21), (3, 30), (4, 40)] {
            tree.add(key, row_id).unwrap();
        }

        let range = KeyRange::bound(2, 4, false, false);
        let expected = tree.get_range(Some(&range), true, Some(3), 1);
        let mut actual = Vec::new();
        tree.visit_range(Some(&range), true, Some(3), 1, |row_id| {
            actual.push(row_id);
            true
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_btree_set() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        tree.set(10, 100);
        assert_eq!(tree.get(&10), vec![100]);

        tree.set(10, 101);
        assert_eq!(tree.get(&10), vec![101]);
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn test_btree_split() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        // Insert enough keys to trigger splits
        for i in 0..20 {
            tree.add(i, i as u64).unwrap();
        }

        // Verify all keys are present
        for i in 0..20 {
            assert!(tree.contains_key(&i));
            assert_eq!(tree.get(&i), vec![i as u64]);
        }
    }

    #[test]
    fn test_btree_range_all() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..10 {
            tree.add(i, i as u64).unwrap();
        }

        let result = tree.get_range(None, false, None, 0);
        assert_eq!(result, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_btree_range_with_limit_skip() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..10 {
            tree.add(i, i as u64).unwrap();
        }

        let result = tree.get_range(None, false, Some(3), 2);
        assert_eq!(result, vec![2, 3, 4]);
    }

    #[test]
    fn test_btree_range_reverse() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..10 {
            tree.add(i, i as u64).unwrap();
        }

        let result = tree.get_range(None, true, Some(3), 0);
        assert_eq!(result, vec![9, 8, 7]);
    }

    #[test]
    fn test_btree_range_lower_bound() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..10 {
            tree.add(i, i as u64).unwrap();
        }

        let range = KeyRange::lower_bound(5, false);
        let result = tree.get_range(Some(&range), false, None, 0);
        assert_eq!(result, vec![5, 6, 7, 8, 9]);

        let range_ex = KeyRange::lower_bound(5, true);
        let result = tree.get_range(Some(&range_ex), false, None, 0);
        assert_eq!(result, vec![6, 7, 8, 9]);
    }

    #[test]
    fn test_btree_range_upper_bound() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..10 {
            tree.add(i, i as u64).unwrap();
        }

        let range = KeyRange::upper_bound(5, false);
        let result = tree.get_range(Some(&range), false, None, 0);
        assert_eq!(result, vec![0, 1, 2, 3, 4, 5]);

        let range_ex = KeyRange::upper_bound(5, true);
        let result = tree.get_range(Some(&range_ex), false, None, 0);
        assert_eq!(result, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_btree_range_only() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..10 {
            tree.add(i, i as u64).unwrap();
        }

        let range = KeyRange::only(5);
        let result = tree.get_range(Some(&range), false, None, 0);
        assert_eq!(result, vec![5]);
    }

    #[test]
    fn test_btree_stats() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);

        tree.add(1, 100).unwrap();
        tree.add(1, 101).unwrap();
        tree.add(2, 200).unwrap();

        assert_eq!(tree.stats().total_rows(), 3);

        tree.remove(&1, Some(100));
        assert_eq!(tree.stats().total_rows(), 2);

        tree.clear();
        assert_eq!(tree.stats().total_rows(), 0);
    }

    #[test]
    fn test_btree_sequence() {
        // Test from original JS test
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [13, 9, 21, 17, 5, 11, 3, 25, 27];

        for (i, &num) in sequence.iter().enumerate() {
            tree.add(num, i as u64).unwrap();
        }

        assert_eq!(tree.get(&13), vec![0]);
        assert_eq!(tree.get(&9), vec![1]);
        assert_eq!(tree.get(&21), vec![2]);
    }

    #[test]
    fn test_btree_delete_all() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [13, 9, 21, 17, 5, 11, 3, 25, 27];

        for (i, &num) in sequence.iter().enumerate() {
            tree.add(num, i as u64).unwrap();
        }

        for &num in &sequence {
            tree.remove(&num, None);
        }

        assert!(tree.is_empty());
    }

    // ==================== Split Edge Cases ====================

    /// Test empty tree behcynos
    #[test]
    fn test_empty_tree() {
        let tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
        assert!(tree.min().is_none());
        assert!(tree.max().is_none());
        assert_eq!(tree.get(&1), Vec::<RowId>::new());
        assert!(!tree.contains_key(&1));
    }

    /// Test leaf node as root (no splits yet)
    #[test]
    fn test_leaf_node_as_root() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        // Insert 4 keys - should stay as single leaf root
        tree.add(9, 9).unwrap();
        tree.add(13, 13).unwrap();
        tree.add(17, 17).unwrap();
        tree.add(21, 21).unwrap();

        assert_eq!(tree.len(), 4);
        for &k in &[9, 13, 17, 21] {
            assert!(tree.contains_key(&k));
        }
    }

    /// Test first internal node creation (first split)
    #[test]
    fn test_first_internal_node() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        // Insert 5 keys to trigger first split
        for &k in &[9, 13, 17, 21, 5] {
            tree.add(k, k as u64).unwrap();
        }

        assert_eq!(tree.len(), 5);
        for &k in &[5, 9, 13, 17, 21] {
            assert!(tree.contains_key(&k));
            assert_eq!(tree.get(&k), vec![k as u64]);
        }
    }

    /// Test split case 1: Split of leaf node
    #[test]
    fn test_split_case1() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [13, 9, 21, 17, 5, 11, 3, 25, 27];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        assert_eq!(tree.len(), 9);
        // Verify all keys present and in order
        let result = tree.get_range(None, false, None, 0);
        assert_eq!(result, vec![3, 5, 9, 11, 13, 17, 21, 25, 27]);
    }

    /// Test split case 2: Split inducing new level
    #[test]
    fn test_split_case2() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [
            13, 9, 21, 17, 5, 11, 3, 25, 27, 14, 15, 31, 29, 22, 23, 38, 45, 47, 49,
        ];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        assert_eq!(tree.len(), 19);
        // Verify all keys present
        for &k in &sequence {
            assert!(tree.contains_key(&k));
        }
    }

    /// Test split case 3: Split promoting new key in internal node
    #[test]
    fn test_split_case3() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [
            13, 9, 21, 17, 5, 11, 3, 25, 27, 14, 15, 31, 29, 22, 23, 38, 45, 47, 49, 1,
        ];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        assert_eq!(tree.len(), 20);
        // Verify min and max
        assert_eq!(*tree.min().unwrap().0, 1);
        assert_eq!(*tree.max().unwrap().0, 49);
    }

    /// Test split case 4: Double promotion
    #[test]
    fn test_split_case4() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [
            13, 9, 21, 17, 5, 11, 3, 25, 27, 14, 15, 31, 29, 22, 23, 38, 45, 47, 49, 1, 10, 12, 16,
        ];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        assert_eq!(tree.len(), 23);
        // Verify range query returns sorted order
        let result = tree.get_range(None, false, None, 0);
        let mut expected: Vec<u64> = sequence.iter().map(|&k| k as u64).collect();
        expected.sort();
        assert_eq!(result, expected);
    }

    /// Test split with right links
    #[test]
    fn test_split_case5() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let keys = [
            -995, 371, 370, 369, 368, 367, 366, 365, 364, 363, 97, 98, 99, 100, 101, 102, 103, 104,
            105, 106, 486, 107, 108,
        ];

        for (i, &k) in keys.iter().enumerate() {
            tree.add(k, i as u64).unwrap();
        }

        assert_eq!(tree.len(), 23);
        for &k in &keys {
            assert!(tree.contains_key(&k));
        }
    }

    // ==================== Delete Edge Cases ====================

    /// Test delete from root (simple case)
    #[test]
    fn test_delete_root_simple() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        for &k in &[9, 13, 17, 21] {
            tree.add(k, k as u64).unwrap();
        }

        tree.remove(&9, None);
        tree.remove(&17, None);
        tree.remove(&21, None);
        assert_eq!(tree.get(&13), vec![13]);
        assert_eq!(tree.len(), 1);

        // Remove non-existent key should be no-op
        tree.remove(&22, None);
        assert_eq!(tree.len(), 1);

        tree.remove(&13, None);
        assert!(tree.is_empty());
    }

    /// Test simple delete from leaf
    #[test]
    fn test_delete_simple() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [13, 9, 21, 17, 5, 11, 3, 25, 27];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        tree.remove(&3, None);
        assert!(!tree.contains_key(&3));
        assert_eq!(tree.len(), 8);
    }

    /// Test delete triggering steal from right sibling
    #[test]
    fn test_delete_leaf_steal_from_right() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [13, 9, 21, 17, 5, 11, 3, 25, 27];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        tree.remove(&17, None);
        assert!(!tree.contains_key(&17));
        // Verify tree integrity
        for &k in &[3, 5, 9, 11, 13, 21, 25, 27] {
            assert!(tree.contains_key(&k));
        }
    }

    /// Test delete triggering steal from left sibling
    #[test]
    fn test_delete_leaf_steal_from_left() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [13, 9, 21, 17, 5, 11, 3, 25, 27];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        tree.remove(&17, None);
        tree.remove(&21, None);
        assert!(!tree.contains_key(&17));
        assert!(!tree.contains_key(&21));
        assert_eq!(tree.len(), 7);
    }

    /// Test delete triggering merge with right sibling
    #[test]
    fn test_delete_leaf_merge_right() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [13, 9, 21, 17, 5, 11, 3, 25, 27];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        tree.remove(&17, None);
        tree.remove(&21, None);
        tree.remove(&9, None);
        tree.remove(&13, None);

        // Verify remaining keys
        for &k in &[3, 5, 11, 25, 27] {
            assert!(tree.contains_key(&k));
        }
        assert_eq!(tree.len(), 5);
    }

    /// Test delete triggering merge with left sibling
    #[test]
    fn test_delete_leaf_merge_left() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [13, 9, 21, 17, 5, 11, 3, 25, 27];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        tree.remove(&27, None);
        tree.remove(&25, None);

        for &k in &[3, 5, 9, 11, 13, 17, 21] {
            assert!(tree.contains_key(&k));
        }
        assert_eq!(tree.len(), 7);
    }

    /// Test delete all keys in forward order
    #[test]
    fn test_delete_all_forward() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [
            13, 9, 21, 17, 5, 11, 3, 25, 27, 14, 15, 31, 29, 22, 23, 38, 45, 47, 49, 1, 10, 12, 16,
        ];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        for &k in &sequence {
            tree.remove(&k, None);
        }

        assert!(tree.is_empty());
    }

    /// Test delete all keys in reverse order
    #[test]
    fn test_delete_all_reverse() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [
            13, 9, 21, 17, 5, 11, 3, 25, 27, 14, 15, 31, 29, 22, 23, 38, 45, 47, 49, 1, 10, 12, 16,
        ];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        for &k in sequence.iter().rev() {
            tree.remove(&k, None);
        }

        assert!(tree.is_empty());
    }

    /// Test delete non-existent key
    #[test]
    fn test_delete_none() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [
            13, 9, 21, 17, 5, 11, 3, 25, 27, 14, 15, 31, 29, 22, 23, 38, 45, 47, 49, 1, 10, 12, 16,
        ];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        let len_before = tree.len();
        tree.remove(&18, None); // Non-existent key
        assert_eq!(tree.len(), len_before);
    }

    // ==================== Duplicate Keys Tests ====================

    /// Test duplicate keys in non-unique index
    #[test]
    fn test_duplicate_keys_basic() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);

        tree.add(10, 100).unwrap();
        tree.add(10, 1000).unwrap();
        tree.add(20, 200).unwrap();
        tree.add(20, 2000).unwrap();

        assert_eq!(tree.get(&10), vec![100, 1000]);
        assert_eq!(tree.get(&20), vec![200, 2000]);
        assert_eq!(tree.len(), 4);
    }

    /// Test duplicate keys with contains_key
    #[test]
    fn test_duplicate_keys_contains() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);

        tree.add(10, 100).unwrap();
        tree.add(10, 1000).unwrap();

        assert!(tree.contains_key(&10));
        assert!(!tree.contains_key(&20));
    }

    /// Test duplicate keys delete specific value
    #[test]
    fn test_duplicate_keys_delete_specific() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);

        tree.add(10, 100).unwrap();
        tree.add(10, 1000).unwrap();

        tree.remove(&10, Some(100));
        assert_eq!(tree.get(&10), vec![1000]);
        assert_eq!(tree.len(), 1);
    }

    /// Test duplicate keys delete all
    #[test]
    fn test_duplicate_keys_delete_all() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);
        let sequence = [13, 9, 21, 17, 5];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
            tree.add(k, (k * 1000) as u64).unwrap();
        }

        assert_eq!(tree.len(), 10);

        for &k in &sequence {
            tree.remove(&k, Some(k as u64));
            tree.remove(&k, Some((k * 1000) as u64));
        }

        assert!(tree.is_empty());
    }

    /// Test duplicate keys with range query
    #[test]
    fn test_duplicate_keys_range() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);

        for i in 1..=5 {
            tree.add(i, (i * 10) as u64).unwrap();
            tree.add(i, (i * 100) as u64).unwrap();
        }

        let result = tree.get_range(None, false, None, 0);
        assert_eq!(result, vec![10, 100, 20, 200, 30, 300, 40, 400, 50, 500]);
    }

    // ==================== String Key Tests ====================

    /// Test string keys ascending
    #[test]
    fn test_string_keys_asc() {
        let mut tree: BTreeIndex<&str> = BTreeIndex::new(5, true);

        tree.add("apple", 1).unwrap();
        tree.add("banana", 2).unwrap();
        tree.add("cherry", 3).unwrap();
        tree.add("date", 4).unwrap();

        assert_eq!(tree.get(&"apple"), vec![1]);
        assert_eq!(tree.get(&"banana"), vec![2]);

        let result = tree.get_range(None, false, None, 0);
        assert_eq!(result, vec![1, 2, 3, 4]); // Sorted alphabetically
    }

    /// Test string keys with range
    #[test]
    fn test_string_keys_range() {
        let mut tree: BTreeIndex<&str> = BTreeIndex::new(5, true);

        tree.add("apple", 1).unwrap();
        tree.add("banana", 2).unwrap();
        tree.add("cherry", 3).unwrap();
        tree.add("date", 4).unwrap();
        tree.add("elderberry", 5).unwrap();

        let range = KeyRange::bound("banana", "date", false, false);
        let result = tree.get_range(Some(&range), false, None, 0);
        assert_eq!(result, vec![2, 3, 4]);
    }

    // ==================== Large Scale Tests ====================

    /// Test with many sequential inserts
    #[test]
    fn test_large_sequential() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(64, true);
        let count = 1000;

        for i in 0..count {
            tree.add(i, i as u64).unwrap();
        }

        assert_eq!(tree.len(), count as usize);

        // Verify all keys
        for i in 0..count {
            assert!(tree.contains_key(&i));
            assert_eq!(tree.get(&i), vec![i as u64]);
        }

        // Verify range
        let result = tree.get_range(None, false, Some(10), 0);
        assert_eq!(result, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    /// Test with many reverse inserts
    #[test]
    fn test_large_reverse() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(64, true);
        let count = 1000;

        for i in (0..count).rev() {
            tree.add(i, i as u64).unwrap();
        }

        assert_eq!(tree.len(), count as usize);

        // Verify min/max
        assert_eq!(*tree.min().unwrap().0, 0);
        assert_eq!(*tree.max().unwrap().0, count - 1);
    }

    /// Test insert and delete interleaved
    #[test]
    fn test_insert_delete_interleaved() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        // Insert 100 keys
        for i in 0..100 {
            tree.add(i, i as u64).unwrap();
        }

        // Delete even keys
        for i in (0..100).step_by(2) {
            tree.remove(&i, None);
        }

        assert_eq!(tree.len(), 50);

        // Verify only odd keys remain
        for i in 0..100 {
            if i % 2 == 0 {
                assert!(!tree.contains_key(&i));
            } else {
                assert!(tree.contains_key(&i));
            }
        }
    }

    // ==================== Range Query Edge Cases ====================

    /// Test range query on empty tree
    #[test]
    fn test_range_empty_tree() {
        let tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let result = tree.get_range(None, false, None, 0);
        assert!(result.is_empty());
    }

    /// Test range with skip exceeding count
    #[test]
    fn test_range_skip_exceeds() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..10 {
            tree.add(i, i as u64).unwrap();
        }

        let result = tree.get_range(None, false, None, 100);
        assert!(result.is_empty());
    }

    /// Test bound range query
    #[test]
    fn test_range_bound() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..20 {
            tree.add(i, i as u64).unwrap();
        }

        let range = KeyRange::bound(5, 15, false, false);
        let result = tree.get_range(Some(&range), false, None, 0);
        assert_eq!(result, vec![5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);

        let range_ex = KeyRange::bound(5, 15, true, true);
        let result = tree.get_range(Some(&range_ex), false, None, 0);
        assert_eq!(result, vec![6, 7, 8, 9, 10, 11, 12, 13, 14]);
    }

    /// Test reverse range with limit and skip
    #[test]
    fn test_range_reverse_limit_skip() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..21 {
            tree.add(i, i as u64).unwrap();
        }

        // Skip 5, take 3 in reverse
        let result = tree.get_range(None, true, Some(3), 5);
        assert_eq!(result, vec![15, 14, 13]);
    }

    // ==================== Stats Tests ====================

    /// Test stats with unique tree
    #[test]
    fn test_stats_unique_tree() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);
        let sequence = [13, 9, 21, 17, 5];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
        }

        assert_eq!(tree.stats().total_rows(), 5);

        tree.remove(&9, None);
        tree.remove(&17, None);
        tree.remove(&21, None);
        assert_eq!(tree.stats().total_rows(), 2);

        tree.add(9, 9).unwrap();
        tree.add(21, 21).unwrap();
        assert_eq!(tree.stats().total_rows(), 4);

        tree.set(9, 8);
        assert_eq!(tree.stats().total_rows(), 4);

        tree.clear();
        assert_eq!(tree.stats().total_rows(), 0);
    }

    /// Test stats with non-unique tree
    #[test]
    fn test_stats_non_unique_tree() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);
        let sequence = [13, 9, 21, 17, 5];

        for &k in &sequence {
            tree.add(k, k as u64).unwrap();
            tree.add(k, (k * 1000) as u64).unwrap();
        }

        assert_eq!(tree.stats().total_rows(), 10);

        tree.remove(&21, None); // Remove all values for key
        assert_eq!(tree.stats().total_rows(), 8);

        tree.remove(&17, Some(17)); // Remove specific value
        assert_eq!(tree.stats().total_rows(), 7);
    }

    // ==================== Batch Remove Tests ====================

    #[test]
    fn test_remove_batch_basic() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..10 {
            tree.add(i, i as u64).unwrap();
        }
        assert_eq!(tree.len(), 10);

        // Remove entries 2, 4, 6, 8
        let entries: Vec<(i32, RowId)> = vec![(2, 2), (4, 4), (6, 6), (8, 8)];
        tree.remove_batch(&entries);

        assert_eq!(tree.len(), 6);
        assert!(tree.contains_key(&0));
        assert!(tree.contains_key(&1));
        assert!(!tree.contains_key(&2));
        assert!(tree.contains_key(&3));
        assert!(!tree.contains_key(&4));
        assert!(tree.contains_key(&5));
        assert!(!tree.contains_key(&6));
        assert!(tree.contains_key(&7));
        assert!(!tree.contains_key(&8));
        assert!(tree.contains_key(&9));
    }

    #[test]
    fn test_remove_batch_same_key_multiple_values() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, false);

        // Add multiple values for same key
        tree.add(10, 100).unwrap();
        tree.add(10, 101).unwrap();
        tree.add(10, 102).unwrap();
        tree.add(20, 200).unwrap();

        assert_eq!(tree.len(), 4);

        // Remove two values from key 10
        let entries: Vec<(i32, RowId)> = vec![(10, 100), (10, 102)];
        tree.remove_batch(&entries);

        assert_eq!(tree.len(), 2);
        assert_eq!(tree.get(&10), vec![101]);
        assert_eq!(tree.get(&20), vec![200]);
    }

    #[test]
    fn test_remove_batch_all_entries() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..20 {
            tree.add(i, i as u64).unwrap();
        }

        // Remove all entries
        let entries: Vec<(i32, RowId)> = (0..20).map(|i| (i, i as u64)).collect();
        tree.remove_batch(&entries);

        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_remove_batch_empty() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..5 {
            tree.add(i, i as u64).unwrap();
        }

        // Remove empty batch should be no-op
        let entries: Vec<(i32, RowId)> = vec![];
        tree.remove_batch(&entries);

        assert_eq!(tree.len(), 5);
    }

    #[test]
    fn test_remove_batch_nonexistent_keys() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..5 {
            tree.add(i, i as u64).unwrap();
        }

        // Try to remove keys that don't exist
        let entries: Vec<(i32, RowId)> = vec![(100, 100), (200, 200)];
        tree.remove_batch(&entries);

        // Should not affect existing entries
        assert_eq!(tree.len(), 5);
    }

    #[test]
    fn test_remove_batch_large_scale() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(64, true);

        // Insert 10000 entries
        for i in 0..10000 {
            tree.add(i, i as u64).unwrap();
        }
        assert_eq!(tree.len(), 10000);

        // Remove every other entry (5000 entries)
        let entries: Vec<(i32, RowId)> = (0..10000)
            .filter(|i| i % 2 == 0)
            .map(|i| (i, i as u64))
            .collect();
        tree.remove_batch(&entries);

        assert_eq!(tree.len(), 5000);

        // Verify remaining entries
        for i in 0..10000 {
            if i % 2 == 0 {
                assert!(!tree.contains_key(&i));
            } else {
                assert!(tree.contains_key(&i));
                assert_eq!(tree.get(&i), vec![i as u64]);
            }
        }
    }

    #[test]
    fn test_remove_batch_preserves_order() {
        let mut tree: BTreeIndex<i32> = BTreeIndex::new(5, true);

        for i in 0..10 {
            tree.add(i, i as u64).unwrap();
        }

        // Remove some entries
        let entries: Vec<(i32, RowId)> = vec![(3, 3), (7, 7)];
        tree.remove_batch(&entries);

        // Verify range query still works correctly
        let result = tree.get_range(None, false, None, 0);
        assert_eq!(result, vec![0, 1, 2, 4, 5, 6, 8, 9]);
    }
}
