//! GIN (Generalized Inverted Index) implementation.
//!
//! GIN indexes are used for indexing composite values like JSONB,
//! arrays, and full-text search documents.

mod posting;

pub use posting::PostingList;

use crate::stats::IndexStats;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use cynos_core::RowId;
use hashbrown::{HashMap, HashSet};

/// Synthetic key namespace used for JSONB_CONTAINS trigram prefilters.
pub const CONTAINS_TRIGRAM_KEY_PREFIX: &str = "__cynos_contains3__:";

/// Builds the synthetic key used for path-scoped JSONB_CONTAINS trigrams.
pub fn contains_trigram_key(path: &str) -> String {
    let mut key = String::from(CONTAINS_TRIGRAM_KEY_PREFIX);
    key.push_str(path);
    key
}

fn trigram_code(first: char, second: char, third: char) -> u128 {
    ((first as u128) << 42) | ((second as u128) << 21) | (third as u128)
}

fn push_trigram_string(out: &mut String, first: char, second: char, third: char) {
    out.clear();
    out.push(first);
    out.push(second);
    out.push(third);
}

fn for_each_unique_trigram<F>(value: &str, mut visitor: F) -> usize
where
    F: FnMut(&str),
{
    let mut chars = value.chars();
    let Some(mut first) = chars.next() else {
        return 0;
    };
    let Some(mut second) = chars.next() else {
        return 0;
    };
    let Some(mut third) = chars.next() else {
        return 0;
    };

    let mut seen = HashSet::with_capacity(value.len().saturating_sub(2));
    let mut gram = String::with_capacity(first.len_utf8() + second.len_utf8() + third.len_utf8());
    let mut count = 0usize;

    loop {
        if seen.insert(trigram_code(first, second, third)) {
            push_trigram_string(&mut gram, first, second, third);
            visitor(gram.as_str());
            count += 1;
        }

        let Some(next) = chars.next() else {
            break;
        };
        first = second;
        second = third;
        third = next;
    }

    count
}

/// Extracts unique trigrams from a string for substring prefiltering.
pub fn contains_trigrams(value: &str) -> Vec<String> {
    let mut grams = Vec::new();
    for_each_unique_trigram(value, |gram| grams.push(gram.into()));
    grams.sort_unstable();
    grams
}

/// Builds the synthetic (key, value) pairs used to prefilter JSONB_CONTAINS.
pub fn contains_trigram_pairs(path: &str, needle: &str) -> Vec<(String, String)> {
    let key = contains_trigram_key(path);
    contains_trigrams(needle)
        .into_iter()
        .map(|gram| (key.clone(), gram))
        .collect()
}

fn push_sorted_unique_row(rows: &mut Vec<RowId>, row_id: RowId) {
    match rows.last().copied() {
        None => rows.push(row_id),
        Some(last) if row_id > last => rows.push(row_id),
        Some(last) if row_id == last => {}
        Some(_) => match rows.binary_search(&row_id) {
            Ok(_) => {}
            Err(index) => rows.insert(index, row_id),
        },
    }
}

#[derive(Debug, Clone, Default)]
pub struct GinBulkBuilder {
    key_index: HashMap<String, Vec<RowId>>,
    key_value_index: HashMap<String, HashMap<String, Vec<RowId>>>,
    key_add_count: usize,
}

impl GinBulkBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_key_ref(&mut self, key: &str, row_id: RowId) {
        self.key_add_count += 1;
        if let Some(rows) = self.key_index.get_mut(key) {
            push_sorted_unique_row(rows, row_id);
            return;
        }

        self.key_index.insert(key.into(), vec![row_id]);
    }

    pub fn add_key(&mut self, key: String, row_id: RowId) {
        self.add_key_ref(&key, row_id);
    }

    pub fn add_key_value_ref(&mut self, key: &str, value: &str, row_id: RowId) {
        if let Some(values) = self.key_value_index.get_mut(key) {
            if let Some(rows) = values.get_mut(value) {
                push_sorted_unique_row(rows, row_id);
                return;
            }

            values.insert(value.into(), vec![row_id]);
            return;
        }

        let mut values = HashMap::new();
        values.insert(value.into(), vec![row_id]);
        self.key_value_index.insert(key.into(), values);
    }

    pub fn add_key_value(&mut self, key: String, value: String, row_id: RowId) {
        self.add_key_value_ref(&key, &value, row_id);
    }

    pub fn add_key_values(
        &mut self,
        pairs: impl IntoIterator<Item = (String, String)>,
        row_id: RowId,
    ) {
        for (key, value) in pairs {
            self.add_key_value(key, value, row_id);
        }
    }

    pub fn add_contains_trigrams(&mut self, path: &str, needle: &str, row_id: RowId) -> usize {
        let key = contains_trigram_key(path);
        self.add_contains_trigrams_for_key(&key, needle, row_id)
    }

    pub fn add_contains_trigrams_for_key(
        &mut self,
        contains_key: &str,
        needle: &str,
        row_id: RowId,
    ) -> usize {
        for_each_unique_trigram(needle, |gram| {
            self.add_key_value_ref(contains_key, gram, row_id)
        })
    }

    fn finish(self) -> GinBulkDelta {
        GinBulkDelta {
            key_index: self
                .key_index
                .into_iter()
                .map(|(key, rows)| (key, PostingList::from_sorted_unique(rows)))
                .collect(),
            key_value_index: self
                .key_value_index
                .into_iter()
                .flat_map(|(key, values)| {
                    values.into_iter().map(move |(value, rows)| {
                        ((key.clone(), value), PostingList::from_sorted_unique(rows))
                    })
                })
                .collect(),
            key_add_count: self.key_add_count,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct GinBulkDelta {
    key_index: HashMap<String, PostingList>,
    key_value_index: HashMap<(String, String), PostingList>,
    key_add_count: usize,
}

/// A GIN index for JSONB and other composite types.
///
/// This index maintains two inverted indexes:
/// - `key_index`: Maps keys to row IDs (for `?` key existence queries)
/// - `key_value_index`: Maps (key, value) pairs to row IDs (for `@>` containment queries)
#[derive(Debug, Clone)]
pub struct GinIndex {
    /// Key → Row IDs (for key existence queries)
    key_index: HashMap<String, PostingList>,
    /// (Key, Value) → Row IDs (for containment queries)
    key_value_index: HashMap<(String, String), PostingList>,
    /// Statistics
    stats: IndexStats,
}

impl GinIndex {
    /// Creates a new empty GIN index.
    pub fn new() -> Self {
        Self {
            key_index: HashMap::new(),
            key_value_index: HashMap::new(),
            stats: IndexStats::new(),
        }
    }

    /// Returns the statistics for this index.
    pub fn stats(&self) -> &IndexStats {
        &self.stats
    }

    /// Indexes a key for a given row.
    pub fn add_key(&mut self, key: String, row_id: RowId) {
        self.key_index
            .entry(key)
            .or_insert_with(PostingList::new)
            .add(row_id);
        self.stats.add_rows(1);
    }

    /// Indexes a key-value pair for a given row.
    pub fn add_key_value(&mut self, key: String, value: String, row_id: RowId) {
        self.key_value_index
            .entry((key, value))
            .or_insert_with(PostingList::new)
            .add(row_id);
    }

    /// Indexes multiple keys for a given row.
    pub fn add_keys(&mut self, keys: impl IntoIterator<Item = String>, row_id: RowId) {
        for key in keys {
            self.add_key(key, row_id);
        }
    }

    /// Indexes multiple key-value pairs for a given row.
    pub fn add_key_values(
        &mut self,
        pairs: impl IntoIterator<Item = (String, String)>,
        row_id: RowId,
    ) {
        for (key, value) in pairs {
            self.add_key_value(key, value, row_id);
        }
    }

    pub fn add_contains_trigrams(&mut self, path: &str, needle: &str, row_id: RowId) -> usize {
        let key = contains_trigram_key(path);
        for_each_unique_trigram(needle, |gram| {
            self.add_key_value(key.clone(), gram.into(), row_id);
        })
    }

    pub fn apply_bulk_builder(&mut self, builder: GinBulkBuilder) {
        self.apply_bulk_delta(builder.finish());
    }

    fn apply_bulk_delta(&mut self, delta: GinBulkDelta) {
        self.stats.add_rows(delta.key_add_count);

        for (key, posting) in delta.key_index {
            self.key_index
                .entry(key)
                .and_modify(|existing| existing.merge(&posting))
                .or_insert(posting);
        }

        for (pair, posting) in delta.key_value_index {
            self.key_value_index
                .entry(pair)
                .and_modify(|existing| existing.merge(&posting))
                .or_insert(posting);
        }
    }

    /// Removes a key entry for a given row.
    pub fn remove_key(&mut self, key: &str, row_id: RowId) {
        if let Some(posting) = self.key_index.get_mut(key) {
            if posting.remove(row_id) {
                self.stats.remove_rows(1);
            }
            if posting.is_empty() {
                self.key_index.remove(key);
            }
        }
    }

    /// Removes a key-value entry for a given row.
    pub fn remove_key_value(&mut self, key: &str, value: &str, row_id: RowId) {
        let pair = (key.into(), value.into());
        if let Some(posting) = self.key_value_index.get_mut(&pair) {
            posting.remove(row_id);
            if posting.is_empty() {
                self.key_value_index.remove(&pair);
            }
        }
    }

    /// Checks if a key exists in any row (for `?` operator).
    pub fn contains_key(&self, key: &str) -> bool {
        self.key_index.contains_key(key)
    }

    /// Gets all row IDs that contain the given key (for `?` operator).
    pub fn get_by_key(&self, key: &str) -> Vec<RowId> {
        self.key_index
            .get(key)
            .map(|p| p.to_vec())
            .unwrap_or_default()
    }

    /// Visits row IDs that contain the given key (for `?` operator).
    /// Return `false` from the visitor to stop early.
    pub fn visit_by_key<F>(&self, key: &str, mut visitor: F)
    where
        F: FnMut(RowId) -> bool,
    {
        let Some(posting) = self.key_index.get(key) else {
            return;
        };

        for row_id in posting.iter() {
            if !visitor(row_id) {
                break;
            }
        }
    }

    /// Gets all row IDs that contain the given key-value pair (for `@>` operator).
    pub fn get_by_key_value(&self, key: &str, value: &str) -> Vec<RowId> {
        let pair = (key.into(), value.into());
        self.key_value_index
            .get(&pair)
            .map(|p| p.to_vec())
            .unwrap_or_default()
    }

    /// Visits row IDs that contain the given key-value pair (for `@>` operator).
    /// Return `false` from the visitor to stop early.
    pub fn visit_by_key_value<F>(&self, key: &str, value: &str, mut visitor: F)
    where
        F: FnMut(RowId) -> bool,
    {
        let pair = (key.into(), value.into());
        let Some(posting) = self.key_value_index.get(&pair) else {
            return;
        };

        for row_id in posting.iter() {
            if !visitor(row_id) {
                break;
            }
        }
    }

    /// Gets all row IDs that contain ALL of the given keys (AND query).
    pub fn get_by_keys_all(&self, keys: &[&str]) -> Vec<RowId> {
        if keys.is_empty() {
            return Vec::new();
        }

        let mut postings = Vec::with_capacity(keys.len());
        for key in keys {
            match self.key_index.get(*key) {
                Some(posting) => postings.push(posting),
                None => return Vec::new(),
            }
        }

        postings.sort_unstable_by_key(|posting| posting.len());

        let mut result = postings[0].to_vec();
        for posting in &postings[1..] {
            result = posting.intersect_sorted_candidates(&result);
            if result.is_empty() {
                break;
            }
        }

        result
    }

    /// Gets all row IDs that contain ANY of the given keys (OR query).
    pub fn get_by_keys_any(&self, keys: &[&str]) -> Vec<RowId> {
        let mut result = PostingList::new();

        for key in keys {
            if let Some(posting) = self.key_index.get(*key) {
                result = result.union(posting);
            }
        }

        result.to_vec()
    }

    /// Gets all row IDs that contain ALL of the given key-value pairs (AND query).
    pub fn get_by_key_values_all(&self, pairs: &[(&str, &str)]) -> Vec<RowId> {
        if pairs.is_empty() {
            return Vec::new();
        }

        let mut postings = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            let pair = ((*key).into(), (*value).into());
            match self.key_value_index.get(&pair) {
                Some(posting) => postings.push(posting),
                None => return Vec::new(),
            }
        }

        postings.sort_unstable_by_key(|posting| posting.len());

        let mut result = postings[0].to_vec();
        for posting in &postings[1..] {
            result = posting.intersect_sorted_candidates(&result);
            if result.is_empty() {
                break;
            }
        }

        result
    }

    /// Gets all row IDs that contain ANY of the given key-value pairs (OR query).
    pub fn get_by_key_values_any(&self, pairs: &[(&str, &str)]) -> Vec<RowId> {
        let mut result = PostingList::new();

        for (key, value) in pairs {
            let pair = ((*key).into(), (*value).into());
            if let Some(posting) = self.key_value_index.get(&pair) {
                result = result.union(posting);
            }
        }

        result.to_vec()
    }

    /// Visits row IDs that contain all of the given key-value pairs.
    /// Return `false` from the visitor to stop early.
    pub fn visit_by_key_values_all<F>(&self, pairs: &[(&str, &str)], mut visitor: F)
    where
        F: FnMut(RowId) -> bool,
    {
        for row_id in self.get_by_key_values_all(pairs) {
            if !visitor(row_id) {
                break;
            }
        }
    }

    /// Visits row IDs that contain any of the given key-value pairs.
    /// Return `false` from the visitor to stop early.
    pub fn visit_by_key_values_any<F>(&self, pairs: &[(&str, &str)], mut visitor: F)
    where
        F: FnMut(RowId) -> bool,
    {
        for row_id in self.get_by_key_values_any(pairs) {
            if !visitor(row_id) {
                break;
            }
        }
    }

    /// Returns the number of unique keys in the index.
    pub fn key_count(&self) -> usize {
        self.key_index.len()
    }

    /// Returns the number of unique key-value pairs in the index.
    pub fn key_value_count(&self) -> usize {
        self.key_value_index.len()
    }

    /// Clears the index.
    pub fn clear(&mut self) {
        self.key_index.clear();
        self.key_value_index.clear();
        self.stats.clear();
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.key_index.is_empty() && self.key_value_index.is_empty()
    }

    /// Estimates the cost of a key lookup.
    pub fn cost_key(&self, key: &str) -> usize {
        self.key_index.get(key).map(|p| p.len()).unwrap_or(0)
    }

    /// Estimates the cost of a key-value lookup.
    pub fn cost_key_value(&self, key: &str, value: &str) -> usize {
        let pair = (key.into(), value.into());
        self.key_value_index
            .get(&pair)
            .map(|p| p.len())
            .unwrap_or(0)
    }
}

impl Default for GinIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_gin_new() {
        let gin = GinIndex::new();
        assert!(gin.is_empty());
        assert_eq!(gin.key_count(), 0);
        assert_eq!(gin.key_value_count(), 0);
    }

    #[test]
    fn test_gin_add_key() {
        let mut gin = GinIndex::new();

        gin.add_key("name".into(), 1);
        gin.add_key("name".into(), 2);
        gin.add_key("age".into(), 1);

        assert!(gin.contains_key("name"));
        assert!(gin.contains_key("age"));
        assert!(!gin.contains_key("email"));

        assert_eq!(gin.get_by_key("name"), vec![1, 2]);
        assert_eq!(gin.get_by_key("age"), vec![1]);
    }

    #[test]
    fn test_gin_add_key_value() {
        let mut gin = GinIndex::new();

        gin.add_key_value("status".into(), "active".into(), 1);
        gin.add_key_value("status".into(), "active".into(), 2);
        gin.add_key_value("status".into(), "inactive".into(), 3);

        assert_eq!(gin.get_by_key_value("status", "active"), vec![1, 2]);
        assert_eq!(gin.get_by_key_value("status", "inactive"), vec![3]);
        assert_eq!(
            gin.get_by_key_value("status", "pending"),
            Vec::<RowId>::new()
        );
    }

    #[test]
    fn test_gin_remove_key() {
        let mut gin = GinIndex::new();

        gin.add_key("name".into(), 1);
        gin.add_key("name".into(), 2);

        gin.remove_key("name", 1);
        assert_eq!(gin.get_by_key("name"), vec![2]);

        gin.remove_key("name", 2);
        assert!(!gin.contains_key("name"));
    }

    #[test]
    fn test_gin_get_by_keys_all() {
        let mut gin = GinIndex::new();

        // Row 1 has keys: name, age
        gin.add_key("name".into(), 1);
        gin.add_key("age".into(), 1);

        // Row 2 has keys: name, email
        gin.add_key("name".into(), 2);
        gin.add_key("email".into(), 2);

        // Row 3 has keys: name, age, email
        gin.add_key("name".into(), 3);
        gin.add_key("age".into(), 3);
        gin.add_key("email".into(), 3);

        // Query: rows with both "name" AND "age"
        let result = gin.get_by_keys_all(&["name", "age"]);
        assert_eq!(result, vec![1, 3]);

        // Query: rows with "name", "age", AND "email"
        let result = gin.get_by_keys_all(&["name", "age", "email"]);
        assert_eq!(result, vec![3]);

        // Query: rows with non-existent key
        let result = gin.get_by_keys_all(&["name", "nonexistent"]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_gin_get_by_keys_any() {
        let mut gin = GinIndex::new();

        gin.add_key("name".into(), 1);
        gin.add_key("age".into(), 2);
        gin.add_key("email".into(), 3);

        // Query: rows with "name" OR "age"
        let result = gin.get_by_keys_any(&["name", "age"]);
        assert_eq!(result, vec![1, 2]);

        // Query: rows with "email" OR "nonexistent"
        let result = gin.get_by_keys_any(&["email", "nonexistent"]);
        assert_eq!(result, vec![3]);
    }

    #[test]
    fn test_gin_visit_by_key_value_stops_early() {
        let mut gin = GinIndex::new();
        for row_id in 1..=5 {
            gin.add_key_value("status".into(), "active".into(), row_id);
        }

        let mut visited = Vec::new();
        gin.visit_by_key_value("status", "active", |row_id| {
            visited.push(row_id);
            visited.len() < 2
        });

        assert_eq!(visited, vec![1, 2]);
    }

    #[test]
    fn test_gin_get_by_key_values_all() {
        let mut gin = GinIndex::new();

        // Row 1: status=active, type=user
        gin.add_key_value("status".into(), "active".into(), 1);
        gin.add_key_value("type".into(), "user".into(), 1);

        // Row 2: status=active, type=admin
        gin.add_key_value("status".into(), "active".into(), 2);
        gin.add_key_value("type".into(), "admin".into(), 2);

        // Row 3: status=inactive, type=user
        gin.add_key_value("status".into(), "inactive".into(), 3);
        gin.add_key_value("type".into(), "user".into(), 3);

        // Query: status=active AND type=user
        let result = gin.get_by_key_values_all(&[("status", "active"), ("type", "user")]);
        assert_eq!(result, vec![1]);

        // Query: status=active (any type)
        let result = gin.get_by_key_values_all(&[("status", "active")]);
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn test_gin_get_by_key_values_any() {
        let mut gin = GinIndex::new();

        gin.add_key_value("status".into(), "active".into(), 1);
        gin.add_key_value("type".into(), "user".into(), 1);

        gin.add_key_value("status".into(), "active".into(), 2);
        gin.add_key_value("type".into(), "admin".into(), 2);

        gin.add_key_value("status".into(), "inactive".into(), 3);
        gin.add_key_value("type".into(), "user".into(), 3);

        let result = gin.get_by_key_values_any(&[("status", "active"), ("type", "user")]);
        assert_eq!(result, vec![1, 2, 3]);

        let result = gin.get_by_key_values_any(&[("status", "active")]);
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn test_gin_get_by_key_values_all_order_independent() {
        let mut gin = GinIndex::new();

        for row_id in 1..=100 {
            gin.add_key_value("scope".into(), "all".into(), row_id);
        }
        for row_id in 1..=50 {
            gin.add_key_value("status".into(), "active".into(), row_id);
        }
        for row_id in 11..=20 {
            gin.add_key_value("tenant".into(), "small".into(), row_id);
        }

        let worst_case_order = gin.get_by_key_values_all(&[
            ("scope", "all"),
            ("status", "active"),
            ("tenant", "small"),
        ]);
        let best_case_order = gin.get_by_key_values_all(&[
            ("tenant", "small"),
            ("status", "active"),
            ("scope", "all"),
        ]);

        assert_eq!(worst_case_order, (11..=20).collect::<Vec<_>>());
        assert_eq!(best_case_order, worst_case_order);
    }

    #[test]
    fn test_gin_get_by_keys_all_order_independent() {
        let mut gin = GinIndex::new();

        for row_id in 1..=100 {
            gin.add_key("scope".into(), row_id);
        }
        for row_id in 1..=50 {
            gin.add_key("status".into(), row_id);
        }
        for row_id in 11..=20 {
            gin.add_key("tenant".into(), row_id);
        }

        let worst_case_order = gin.get_by_keys_all(&["scope", "status", "tenant"]);
        let best_case_order = gin.get_by_keys_all(&["tenant", "status", "scope"]);

        assert_eq!(worst_case_order, (11..=20).collect::<Vec<_>>());
        assert_eq!(best_case_order, worst_case_order);
    }

    #[test]
    fn test_gin_clear() {
        let mut gin = GinIndex::new();

        gin.add_key("name".into(), 1);
        gin.add_key_value("status".into(), "active".into(), 1);

        gin.clear();

        assert!(gin.is_empty());
        assert_eq!(gin.key_count(), 0);
        assert_eq!(gin.key_value_count(), 0);
    }

    #[test]
    fn test_gin_cost() {
        let mut gin = GinIndex::new();

        gin.add_key("name".into(), 1);
        gin.add_key("name".into(), 2);
        gin.add_key("name".into(), 3);
        gin.add_key("age".into(), 1);

        assert_eq!(gin.cost_key("name"), 3);
        assert_eq!(gin.cost_key("age"), 1);
        assert_eq!(gin.cost_key("nonexistent"), 0);
    }

    #[test]
    fn test_contains_trigrams_deduplicate_repeated_windows() {
        assert_eq!(contains_trigrams("aaaa"), vec![String::from("aaa")]);
    }

    #[test]
    fn test_contains_trigram_pairs_scope_key_by_path() {
        let pairs = contains_trigram_pairs("tags", "portable");
        assert!(pairs
            .iter()
            .all(|(key, _)| key == "__cynos_contains3__:tags"));
        assert_eq!(
            pairs,
            vec![
                (
                    String::from("__cynos_contains3__:tags"),
                    String::from("abl")
                ),
                (
                    String::from("__cynos_contains3__:tags"),
                    String::from("ble")
                ),
                (
                    String::from("__cynos_contains3__:tags"),
                    String::from("ort")
                ),
                (
                    String::from("__cynos_contains3__:tags"),
                    String::from("por")
                ),
                (
                    String::from("__cynos_contains3__:tags"),
                    String::from("rta")
                ),
                (
                    String::from("__cynos_contains3__:tags"),
                    String::from("tab")
                ),
            ]
        );
    }

    #[test]
    fn test_gin_bulk_builder_merges_into_existing_index() {
        let mut gin = GinIndex::new();
        gin.add_key("status".into(), 1);
        gin.add_key_value("status".into(), "active".into(), 1);

        let mut builder = GinBulkBuilder::new();
        builder.add_key("status".into(), 2);
        builder.add_key("status".into(), 3);
        builder.add_key_value("status".into(), "active".into(), 2);
        builder.add_key_value("status".into(), "active".into(), 3);

        gin.apply_bulk_builder(builder);

        assert_eq!(gin.get_by_key("status"), vec![1, 2, 3]);
        assert_eq!(gin.get_by_key_value("status", "active"), vec![1, 2, 3]);
    }
}
