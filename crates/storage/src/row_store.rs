//! Row storage for Cynos database.
//!
//! This module provides the `RowStore` struct which manages rows for a single table,
//! including primary key and secondary index maintenance.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use cynos_core::schema::{IndexType, Table};
use cynos_core::{Error, Result, Row, RowId, Value};
use cynos_incremental::Delta;
use cynos_index::{
    contains_trigram_pairs, BTreeIndex, GinBulkBuilder, GinIndex, HashIndex, Index, KeyRange,
    RangeIndex,
};
use cynos_jsonb::{JsonbObject, JsonbValue as ParsedJsonbValue};

/// Row ID lookup backend: HashMap (O(1) lookup) or BTreeMap (O(log n) lookup).
#[cfg(feature = "hash-store")]
type RowMap = hashbrown::HashMap<RowId, usize>;
#[cfg(not(feature = "hash-store"))]
type RowMap = BTreeMap<RowId, usize>;

#[derive(Clone)]
struct RowSlot {
    row_id: RowId,
    row: Rc<Row>,
}

#[derive(Clone, Debug)]
struct GinIndexConfig {
    column_idx: usize,
    indexed_paths: Option<Vec<String>>,
}

impl GinIndexConfig {
    fn new(column_idx: usize, indexed_paths: Option<Vec<String>>) -> Self {
        Self {
            column_idx,
            indexed_paths: indexed_paths.filter(|paths| !paths.is_empty()),
        }
    }
}

#[derive(Clone, Debug)]
struct BatchSecondaryDef {
    name: String,
    cols: Vec<usize>,
    unique: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum IndexKey {
    Scalar(Value),
    Composite(Vec<Value>),
}

impl IndexKey {
    #[inline]
    fn scalar(value: Value) -> Self {
        Self::Scalar(value)
    }

    #[inline]
    fn from_values(mut values: Vec<Value>) -> Self {
        if values.len() == 1 {
            Self::Scalar(values.pop().unwrap_or(Value::Null))
        } else {
            Self::Composite(values)
        }
    }

    #[inline]
    fn from_row(row: &Row, col_indices: &[usize]) -> Self {
        let values = col_indices
            .iter()
            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
            .collect();
        Self::from_values(values)
    }

    fn from_scalar_range(range: Option<&KeyRange<Value>>) -> Option<KeyRange<IndexKey>> {
        range.cloned().map(|range| match range {
            KeyRange::All => KeyRange::All,
            KeyRange::Only(value) => KeyRange::Only(IndexKey::scalar(value)),
            KeyRange::LowerBound { value, exclusive } => KeyRange::LowerBound {
                value: IndexKey::scalar(value),
                exclusive,
            },
            KeyRange::UpperBound { value, exclusive } => KeyRange::UpperBound {
                value: IndexKey::scalar(value),
                exclusive,
            },
            KeyRange::Bound {
                lower,
                upper,
                lower_exclusive,
                upper_exclusive,
            } => KeyRange::Bound {
                lower: IndexKey::scalar(lower),
                upper: IndexKey::scalar(upper),
                lower_exclusive,
                upper_exclusive,
            },
        })
    }

    fn from_composite_range(range: Option<&KeyRange<Vec<Value>>>) -> Option<KeyRange<IndexKey>> {
        range.cloned().map(|range| match range {
            KeyRange::All => KeyRange::All,
            KeyRange::Only(values) => KeyRange::Only(IndexKey::from_values(values)),
            KeyRange::LowerBound { value, exclusive } => KeyRange::LowerBound {
                value: IndexKey::from_values(value),
                exclusive,
            },
            KeyRange::UpperBound { value, exclusive } => KeyRange::UpperBound {
                value: IndexKey::from_values(value),
                exclusive,
            },
            KeyRange::Bound {
                lower,
                upper,
                lower_exclusive,
                upper_exclusive,
            } => KeyRange::Bound {
                lower: IndexKey::from_values(lower),
                upper: IndexKey::from_values(upper),
                lower_exclusive,
                upper_exclusive,
            },
        })
    }

    fn to_error_value(&self) -> Value {
        match self {
            Self::Scalar(value) => value.clone(),
            Self::Composite(values) => Value::String(format!("{:?}", values)),
        }
    }
}

/// Trait for index storage that supports both point and range queries.
pub trait IndexStore {
    /// Adds a key-value pair to the index.
    fn add(
        &mut self,
        key: Value,
        row_id: RowId,
    ) -> core::result::Result<(), cynos_index::IndexError>;
    /// Sets a key-value pair, replacing any existing values.
    fn set(&mut self, key: Value, row_id: RowId);
    /// Gets all row IDs for a key.
    fn get(&self, key: &Value) -> Vec<RowId>;
    /// Removes a key-value pair.
    fn remove(&mut self, key: &Value, row_id: Option<RowId>);
    /// Removes multiple key-value pairs in batch (more efficient than multiple remove calls).
    fn remove_batch(&mut self, entries: &[(Value, RowId)]);
    /// Checks if the index contains a key.
    fn contains_key(&self, key: &Value) -> bool;
    /// Returns the number of entries.
    fn len(&self) -> usize;
    /// Returns true if empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Returns whether this is a unique index.
    fn is_unique(&self) -> bool;
    /// Clears all entries.
    fn clear(&mut self);
    /// Gets range of row IDs.
    fn get_range(
        &self,
        range: Option<&KeyRange<Value>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
    ) -> Vec<RowId>;
    /// Visits row IDs in a range without requiring a full intermediate `Vec<RowId>`.
    /// Return `false` from the visitor to stop early.
    fn visit_range<F>(
        &self,
        range: Option<&KeyRange<Value>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
        mut visitor: F,
    ) where
        F: FnMut(RowId) -> bool,
    {
        for row_id in self.get_range(range, reverse, limit, skip) {
            if !visitor(row_id) {
                break;
            }
        }
    }
    /// Returns all row IDs in the index.
    fn get_all(&self) -> Vec<RowId>;
    /// Returns the number of distinct keys currently stored in the index.
    fn distinct_key_count(&self) -> usize;
}

/// Wrapper for BTreeIndex that implements IndexStore.
pub struct BTreeIndexStore {
    inner: BTreeIndex<IndexKey>,
}

impl BTreeIndexStore {
    /// Creates a new BTree index store.
    pub fn new(unique: bool) -> Self {
        Self {
            inner: BTreeIndex::new(64, unique),
        }
    }

    fn add_index_key(
        &mut self,
        key: IndexKey,
        row_id: RowId,
    ) -> core::result::Result<(), cynos_index::IndexError> {
        self.inner.add(key, row_id)
    }

    fn set_index_key(&mut self, key: IndexKey, row_id: RowId) {
        self.inner.set(key, row_id);
    }

    fn get_index_key(&self, key: &IndexKey) -> Vec<RowId> {
        self.inner.get(key)
    }

    fn remove_index_key(&mut self, key: &IndexKey, row_id: Option<RowId>) {
        self.inner.remove(key, row_id);
    }

    fn remove_batch_index_keys(&mut self, entries: &[(IndexKey, RowId)]) {
        self.inner.remove_batch(entries);
    }

    fn contains_index_key(&self, key: &IndexKey) -> bool {
        self.inner.contains_key(key)
    }

    fn get_range_index_keys(
        &self,
        range: Option<&KeyRange<IndexKey>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
    ) -> Vec<RowId> {
        self.inner.get_range(range, reverse, limit, skip)
    }

    fn visit_range_index_keys<F>(
        &self,
        range: Option<&KeyRange<IndexKey>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
        visitor: F,
    ) where
        F: FnMut(RowId) -> bool,
    {
        self.inner.visit_range(range, reverse, limit, skip, visitor);
    }

    fn distinct_key_count(&self) -> usize {
        self.inner.distinct_key_count()
    }
}

impl IndexStore for BTreeIndexStore {
    fn add(
        &mut self,
        key: Value,
        row_id: RowId,
    ) -> core::result::Result<(), cynos_index::IndexError> {
        self.add_index_key(IndexKey::scalar(key), row_id)
    }

    fn set(&mut self, key: Value, row_id: RowId) {
        self.set_index_key(IndexKey::scalar(key), row_id);
    }

    fn get(&self, key: &Value) -> Vec<RowId> {
        self.get_index_key(&IndexKey::scalar(key.clone()))
    }

    fn remove(&mut self, key: &Value, row_id: Option<RowId>) {
        self.remove_index_key(&IndexKey::scalar(key.clone()), row_id);
    }

    fn remove_batch(&mut self, entries: &[(Value, RowId)]) {
        let entries: Vec<(IndexKey, RowId)> = entries
            .iter()
            .map(|(key, row_id)| (IndexKey::scalar(key.clone()), *row_id))
            .collect();
        self.remove_batch_index_keys(&entries);
    }

    fn contains_key(&self, key: &Value) -> bool {
        self.contains_index_key(&IndexKey::scalar(key.clone()))
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_unique(&self) -> bool {
        self.inner.is_unique()
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn get_range(
        &self,
        range: Option<&KeyRange<Value>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
    ) -> Vec<RowId> {
        let range = IndexKey::from_scalar_range(range);
        self.get_range_index_keys(range.as_ref(), reverse, limit, skip)
    }

    fn get_all(&self) -> Vec<RowId> {
        self.get_range_index_keys(None, false, None, 0)
    }

    fn distinct_key_count(&self) -> usize {
        self.inner.distinct_key_count()
    }

    fn visit_range<F>(
        &self,
        range: Option<&KeyRange<Value>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
        visitor: F,
    ) where
        F: FnMut(RowId) -> bool,
    {
        let range = IndexKey::from_scalar_range(range);
        self.visit_range_index_keys(range.as_ref(), reverse, limit, skip, visitor);
    }
}

/// Wrapper for HashIndex that implements IndexStore.
pub struct HashIndexStore {
    inner: HashIndex<IndexKey>,
}

impl HashIndexStore {
    /// Creates a new Hash index store.
    pub fn new(unique: bool) -> Self {
        Self {
            inner: HashIndex::new(unique),
        }
    }

    fn add_index_key(
        &mut self,
        key: IndexKey,
        row_id: RowId,
    ) -> core::result::Result<(), cynos_index::IndexError> {
        self.inner.add(key, row_id)
    }

    fn set_index_key(&mut self, key: IndexKey, row_id: RowId) {
        self.inner.set(key, row_id);
    }

    fn get_index_key(&self, key: &IndexKey) -> Vec<RowId> {
        self.inner.get(key)
    }

    fn remove_index_key(&mut self, key: &IndexKey, row_id: Option<RowId>) {
        self.inner.remove(key, row_id);
    }

    fn remove_batch_index_keys(&mut self, entries: &[(IndexKey, RowId)]) {
        self.inner.remove_batch(entries);
    }

    fn contains_index_key(&self, key: &IndexKey) -> bool {
        self.inner.contains_key(key)
    }

    fn get_range_index_keys(
        &self,
        range: Option<&KeyRange<IndexKey>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
    ) -> Vec<RowId> {
        self.inner.get_range(range, reverse, limit, skip)
    }

    fn visit_range_index_keys<F>(
        &self,
        range: Option<&KeyRange<IndexKey>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
        mut visitor: F,
    ) where
        F: FnMut(RowId) -> bool,
    {
        for row_id in self.get_range_index_keys(range, reverse, limit, skip) {
            if !visitor(row_id) {
                break;
            }
        }
    }

    fn distinct_key_count(&self) -> usize {
        self.inner.distinct_key_count()
    }
}

impl IndexStore for HashIndexStore {
    fn add(
        &mut self,
        key: Value,
        row_id: RowId,
    ) -> core::result::Result<(), cynos_index::IndexError> {
        self.add_index_key(IndexKey::scalar(key), row_id)
    }

    fn set(&mut self, key: Value, row_id: RowId) {
        self.set_index_key(IndexKey::scalar(key), row_id);
    }

    fn get(&self, key: &Value) -> Vec<RowId> {
        self.get_index_key(&IndexKey::scalar(key.clone()))
    }

    fn remove(&mut self, key: &Value, row_id: Option<RowId>) {
        self.remove_index_key(&IndexKey::scalar(key.clone()), row_id);
    }

    fn remove_batch(&mut self, entries: &[(Value, RowId)]) {
        // HashIndex doesn't have optimized batch remove, fall back to individual removes
        for (key, row_id) in entries {
            self.remove_index_key(&IndexKey::scalar(key.clone()), Some(*row_id));
        }
    }

    fn contains_key(&self, key: &Value) -> bool {
        self.contains_index_key(&IndexKey::scalar(key.clone()))
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_unique(&self) -> bool {
        self.inner.is_unique()
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn get_range(
        &self,
        _range: Option<&KeyRange<Value>>,
        _reverse: bool,
        _limit: Option<usize>,
        _skip: usize,
    ) -> Vec<RowId> {
        self.get_all()
    }

    fn get_all(&self) -> Vec<RowId> {
        self.inner.get_all_row_ids()
    }

    fn distinct_key_count(&self) -> usize {
        self.inner.distinct_key_count()
    }

    fn visit_range<F>(
        &self,
        range: Option<&KeyRange<Value>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
        mut visitor: F,
    ) where
        F: FnMut(RowId) -> bool,
    {
        for row_id in self.get_range(range, reverse, limit, skip) {
            if !visitor(row_id) {
                break;
            }
        }
    }
}

enum SecondaryIndexStore {
    BTree(BTreeIndexStore),
    Hash(HashIndexStore),
}

impl SecondaryIndexStore {
    fn new(index_type: IndexType, unique: bool) -> Self {
        match index_type {
            IndexType::Hash => Self::Hash(HashIndexStore::new(unique)),
            IndexType::BTree | IndexType::Gin => Self::BTree(BTreeIndexStore::new(unique)),
        }
    }

    fn add_index_key(
        &mut self,
        key: IndexKey,
        row_id: RowId,
    ) -> core::result::Result<(), cynos_index::IndexError> {
        match self {
            Self::BTree(index) => index.add_index_key(key, row_id),
            Self::Hash(index) => index.add_index_key(key, row_id),
        }
    }

    fn remove_index_key(&mut self, key: &IndexKey, row_id: Option<RowId>) {
        match self {
            Self::BTree(index) => index.remove_index_key(key, row_id),
            Self::Hash(index) => index.remove_index_key(key, row_id),
        }
    }

    fn remove_batch_index_keys(&mut self, entries: &[(IndexKey, RowId)]) {
        match self {
            Self::BTree(index) => index.remove_batch_index_keys(entries),
            Self::Hash(index) => index.remove_batch_index_keys(entries),
        }
    }

    fn contains_index_key(&self, key: &IndexKey) -> bool {
        match self {
            Self::BTree(index) => index.contains_index_key(key),
            Self::Hash(index) => index.contains_index_key(key),
        }
    }

    fn is_unique(&self) -> bool {
        match self {
            Self::BTree(index) => index.is_unique(),
            Self::Hash(index) => index.is_unique(),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::BTree(index) => index.clear(),
            Self::Hash(index) => index.clear(),
        }
    }

    fn distinct_key_count(&self) -> usize {
        match self {
            Self::BTree(index) => index.distinct_key_count(),
            Self::Hash(index) => index.distinct_key_count(),
        }
    }

    fn visit_range_index_keys<F>(
        &self,
        range: Option<&KeyRange<IndexKey>>,
        reverse: bool,
        limit: Option<usize>,
        skip: usize,
        visitor: F,
    ) where
        F: FnMut(RowId) -> bool,
    {
        match self {
            Self::BTree(index) => {
                index.visit_range_index_keys(range, reverse, limit, skip, visitor)
            }
            Self::Hash(index) => index.visit_range_index_keys(range, reverse, limit, skip, visitor),
        }
    }
}

/// Extracts the key value from a row for the given column indices.
fn extract_key(row: &Row, col_indices: &[usize]) -> IndexKey {
    IndexKey::from_row(row, col_indices)
}

fn extract_key_from_values(values: &[Value]) -> IndexKey {
    IndexKey::from_values(values.to_vec())
}

fn composite_range_has_expected_arity(range: &KeyRange<Vec<Value>>, expected: usize) -> bool {
    match range {
        KeyRange::All => true,
        KeyRange::Only(values)
        | KeyRange::LowerBound { value: values, .. }
        | KeyRange::UpperBound { value: values, .. } => values.len() == expected,
        KeyRange::Bound { lower, upper, .. } => lower.len() == expected && upper.len() == expected,
    }
}

/// Row storage for a single table.
pub struct RowStore {
    schema: Table,
    /// Row ID -> slot index lookup for point access.
    rows: RowMap,
    /// Dense row storage used by scans and row materialization.
    row_slots: Vec<RowSlot>,
    /// Slot indices maintained in row_id order for deterministic scans.
    scan_order: Vec<usize>,
    row_id_index: BTreeIndexStore,
    primary_index: Option<BTreeIndexStore>,
    pk_columns: Vec<usize>,
    secondary_indices: BTreeMap<String, SecondaryIndexStore>,
    index_columns: BTreeMap<String, Vec<usize>>,
    /// GIN indexes for JSONB columns
    gin_indices: BTreeMap<String, GinIndex>,
    /// Column index and path configuration for GIN indexes
    gin_index_configs: BTreeMap<String, GinIndexConfig>,
}

impl RowStore {
    /// Creates a new row store for the given table schema.
    pub fn new(schema: Table) -> Self {
        let mut store = Self {
            schema: schema.clone(),
            rows: RowMap::default(),
            row_slots: Vec::new(),
            scan_order: Vec::new(),
            row_id_index: BTreeIndexStore::new(true),
            primary_index: None,
            pk_columns: Vec::new(),
            secondary_indices: BTreeMap::new(),
            index_columns: BTreeMap::new(),
            gin_indices: BTreeMap::new(),
            gin_index_configs: BTreeMap::new(),
        };

        if let Some(pk) = schema.primary_key() {
            store.primary_index = Some(BTreeIndexStore::new(true));
            store.pk_columns = pk
                .columns()
                .iter()
                .filter_map(|c| schema.get_column_index(&c.name))
                .collect();
        }

        for idx in schema.indices() {
            let cols: Vec<usize> = idx
                .columns()
                .iter()
                .filter_map(|c| schema.get_column_index(&c.name))
                .collect();

            // Check if this is a GIN index (for JSONB columns)
            if idx.get_index_type() == IndexType::Gin {
                if let Some(&col_idx) = cols.first() {
                    store
                        .gin_indices
                        .insert(idx.name().to_string(), GinIndex::new());
                    store.gin_index_configs.insert(
                        idx.name().to_string(),
                        GinIndexConfig::new(col_idx, idx.gin_paths().map(|paths| paths.to_vec())),
                    );
                }
            } else {
                store.secondary_indices.insert(
                    idx.name().to_string(),
                    SecondaryIndexStore::new(idx.get_index_type(), idx.is_unique()),
                );
                store.index_columns.insert(idx.name().to_string(), cols);
            }
        }

        store
    }

    /// Returns the table schema.
    pub fn schema(&self) -> &Table {
        &self.schema
    }

    /// Returns the number of distinct primary-key values, if this table has a primary key.
    pub fn primary_index_distinct_key_count(&self) -> Option<usize> {
        self.primary_index
            .as_ref()
            .map(BTreeIndexStore::distinct_key_count)
    }

    /// Returns the number of distinct keys tracked by the named secondary index.
    pub fn secondary_index_distinct_key_count(&self, index_name: &str) -> Option<usize> {
        self.secondary_indices
            .get(index_name)
            .map(SecondaryIndexStore::distinct_key_count)
    }

    /// Returns the posting-list size for a GIN key lookup.
    pub fn gin_index_cost_key(&self, index_name: &str, key: &str) -> usize {
        self.gin_indices
            .get(index_name)
            .map(|gin| gin.cost_key(key))
            .unwrap_or(0)
    }

    /// Returns the posting-list size for a GIN key/value lookup.
    pub fn gin_index_cost_key_value(&self, index_name: &str, key: &str, value: &str) -> usize {
        self.gin_indices
            .get(index_name)
            .map(|gin| gin.cost_key_value(key, value))
            .unwrap_or(0)
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[inline]
    fn scan_position(&self, row_id: RowId) -> core::result::Result<usize, usize> {
        self.scan_order
            .binary_search_by_key(&row_id, |&slot_idx| self.row_slots[slot_idx].row_id)
    }

    #[inline]
    fn row_ref_by_id(&self, row_id: RowId) -> Option<&Rc<Row>> {
        self.rows
            .get(&row_id)
            .and_then(|&slot_idx| self.row_slots.get(slot_idx))
            .map(|slot| &slot.row)
    }

    #[inline]
    fn row_mut_by_id(&mut self, row_id: RowId) -> Option<&mut Rc<Row>> {
        let slot_idx = *self.rows.get(&row_id)?;
        self.row_slots.get_mut(slot_idx).map(|slot| &mut slot.row)
    }

    fn insert_row_slot(&mut self, row_id: RowId, row: Rc<Row>) {
        let slot_idx = self.row_slots.len();
        self.row_slots.push(RowSlot { row_id, row });
        self.rows.insert(row_id, slot_idx);

        let append_only = self
            .scan_order
            .last()
            .map(|&last_slot_idx| self.row_slots[last_slot_idx].row_id < row_id)
            .unwrap_or(true);
        if append_only {
            self.scan_order.push(slot_idx);
            return;
        }

        let scan_pos = self.scan_position(row_id).unwrap_or_else(|pos| pos);
        self.scan_order.insert(scan_pos, slot_idx);
    }

    fn replace_row_slot(&mut self, row_id: RowId, row: Rc<Row>) {
        if let Some(slot) = self.row_mut_by_id(row_id) {
            *slot = row;
        }
    }

    fn remove_row_slot(&mut self, row_id: RowId) -> Option<Rc<Row>> {
        let slot_idx = self.rows.remove(&row_id)?;
        let removed_scan_pos = self.scan_position(row_id).ok();
        let moved_slot_meta = if slot_idx + 1 < self.row_slots.len() {
            let moved_row_id = self.row_slots.last()?.row_id;
            let moved_scan_pos = self.scan_position(moved_row_id).ok();
            Some((moved_row_id, moved_scan_pos))
        } else {
            None
        };
        if let Some(scan_pos) = removed_scan_pos {
            self.scan_order.remove(scan_pos);
        }

        let removed_slot = self.row_slots.swap_remove(slot_idx);
        if slot_idx < self.row_slots.len() {
            let moved_row_id = self.row_slots[slot_idx].row_id;
            self.rows.insert(moved_row_id, slot_idx);
            if let Some((expected_row_id, Some(moved_pos))) = moved_slot_meta {
                debug_assert_eq!(expected_row_id, moved_row_id);
                let adjusted_pos = match removed_scan_pos {
                    Some(removed_pos) if moved_pos > removed_pos => moved_pos - 1,
                    _ => moved_pos,
                };
                self.scan_order[adjusted_pos] = slot_idx;
            }
        }

        Some(removed_slot.row)
    }

    /// Bulk loads rows into an empty store without per-row slot/index interleaving.
    ///
    /// This is intended for initial hydration paths where the table is known to be
    /// empty and no live-query notifications should be emitted between rows.
    pub fn bulk_load(&mut self, rows: Vec<Row>) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        if !self.is_empty() {
            return Err(Error::invalid_operation(
                "Bulk load requires an empty table store",
            ));
        }

        debug_assert!(rows
            .windows(2)
            .all(|window| window[0].id() <= window[1].id()));

        let secondary_defs: Vec<(String, Vec<usize>)> = self
            .index_columns
            .iter()
            .map(|(name, cols)| (name.clone(), cols.clone()))
            .collect();
        let gin_defs: Vec<(String, GinIndexConfig)> = self
            .gin_index_configs
            .iter()
            .map(|(name, config)| (name.clone(), config.clone()))
            .collect();

        for row in &rows {
            let row_id = row.id();
            if self
                .row_id_index
                .add(Value::Int64(row_id as i64), row_id)
                .is_err()
            {
                self.clear();
                return Err(Error::invalid_operation(
                    "Failed to add to row ID index during bulk load",
                ));
            }
        }

        if let Some(ref mut pk_index) = self.primary_index {
            let mut violation = None;
            for row in &rows {
                let pk = extract_key(row, &self.pk_columns);
                if pk_index.add_index_key(pk.clone(), row.id()).is_err() {
                    violation = Some(Error::UniqueConstraint {
                        column: "primary_key".into(),
                        value: pk.to_error_value(),
                    });
                    break;
                }
            }
            if let Some(error) = violation {
                self.clear();
                return Err(error);
            }
        }

        for (idx_name, cols) in &secondary_defs {
            let mut violation = None;
            {
                let Some(idx) = self.secondary_indices.get_mut(idx_name) else {
                    continue;
                };
                for row in &rows {
                    let key = extract_key(row, cols);
                    if idx.add_index_key(key.clone(), row.id()).is_err() {
                        violation = Some(Error::UniqueConstraint {
                            column: idx_name.clone(),
                            value: key.to_error_value(),
                        });
                        break;
                    }
                }
            }
            if let Some(error) = violation {
                self.clear();
                return Err(error);
            }
        }

        for (idx_name, config) in &gin_defs {
            let Some(gin_idx) = self.gin_indices.get_mut(idx_name) else {
                continue;
            };
            let mut builder = GinBulkBuilder::new();
            for row in &rows {
                if let Some(value) = row.get(config.column_idx) {
                    Self::collect_jsonb_value_into_builder(
                        &mut builder,
                        value,
                        row.id(),
                        config.indexed_paths.as_deref(),
                    );
                }
            }
            gin_idx.apply_bulk_builder(builder);
        }

        self.row_slots.reserve(rows.len());
        self.scan_order.reserve(rows.len());

        for (slot_idx, row) in rows.into_iter().enumerate() {
            let row_id = row.id();
            self.rows.insert(row_id, slot_idx);
            self.row_slots.push(RowSlot {
                row_id,
                row: Rc::new(row),
            });
            self.scan_order.push(slot_idx);
        }

        Ok(self.row_slots.len())
    }

    /// Inserts multiple rows while batching GIN index maintenance per insert call.
    ///
    /// The visible semantics remain aligned with sequential inserts: rows accepted
    /// before a later validation failure remain committed.
    pub fn insert_batch(&mut self, rows: Vec<Row>) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let secondary_defs: Vec<BatchSecondaryDef> = self
            .index_columns
            .iter()
            .map(|(name, cols)| BatchSecondaryDef {
                name: name.clone(),
                cols: cols.clone(),
                unique: self
                    .secondary_indices
                    .get(name)
                    .map(|idx| idx.is_unique())
                    .unwrap_or(false),
            })
            .collect();
        if self.is_empty() && self.can_use_bulk_insert_fast_path(&rows, &secondary_defs) {
            return self.bulk_load(rows);
        }
        let gin_defs: Vec<(String, GinIndexConfig)> = self
            .gin_index_configs
            .iter()
            .map(|(name, config)| (name.clone(), config.clone()))
            .collect();

        self.row_slots.reserve(rows.len());
        self.scan_order.reserve(rows.len());

        let mut inserted = 0usize;

        for row in rows {
            let row_id = row.id();

            if self.rows.contains_key(&row_id) {
                return Err(Error::invalid_operation("Row ID already exists"));
            }

            let pk_value = if !self.pk_columns.is_empty() {
                let pk = extract_key(&row, &self.pk_columns);
                if let Some(ref pk_index) = self.primary_index {
                    if pk_index.contains_index_key(&pk) {
                        return Err(Error::UniqueConstraint {
                            column: "primary_key".into(),
                            value: pk.to_error_value(),
                        });
                    }
                }
                Some(pk)
            } else {
                None
            };

            let mut secondary_keys = Vec::with_capacity(secondary_defs.len());
            for def in &secondary_defs {
                let key = extract_key(&row, &def.cols);
                if def.unique {
                    if let Some(idx) = self.secondary_indices.get(&def.name) {
                        if idx.contains_index_key(&key) {
                            return Err(Error::UniqueConstraint {
                                column: def.name.clone(),
                                value: key.to_error_value(),
                            });
                        }
                    }
                }
                secondary_keys.push(key);
            }

            self.row_id_index
                .add(Value::Int64(row_id as i64), row_id)
                .map_err(|_| Error::invalid_operation("Failed to add to row ID index"))?;

            if let (Some(ref mut pk_index), Some(pk)) = (&mut self.primary_index, pk_value.clone())
            {
                if pk_index.add_index_key(pk.clone(), row_id).is_err() {
                    self.row_id_index
                        .remove(&Value::Int64(row_id as i64), Some(row_id));
                    return Err(Error::UniqueConstraint {
                        column: "primary_key".into(),
                        value: pk.to_error_value(),
                    });
                }
            }

            for (secondary_offset, key) in secondary_keys.iter().enumerate() {
                let def = &secondary_defs[secondary_offset];
                if let Some(idx) = self.secondary_indices.get_mut(&def.name) {
                    if idx.add_index_key(key.clone(), row_id).is_err() {
                        for (rollback_offset, rollback_key) in
                            secondary_keys.iter().take(secondary_offset).enumerate()
                        {
                            let rollback_def = &secondary_defs[rollback_offset];
                            if let Some(rollback_idx) =
                                self.secondary_indices.get_mut(&rollback_def.name)
                            {
                                rollback_idx.remove_index_key(rollback_key, Some(row_id));
                            }
                        }
                        if let (Some(ref mut pk_index), Some(pk)) =
                            (&mut self.primary_index, pk_value.as_ref())
                        {
                            pk_index.remove_index_key(pk, Some(row_id));
                        }
                        self.row_id_index
                            .remove(&Value::Int64(row_id as i64), Some(row_id));
                        return Err(Error::UniqueConstraint {
                            column: def.name.clone(),
                            value: key.to_error_value(),
                        });
                    }
                }
            }

            for (idx_name, config) in &gin_defs {
                let Some(gin_idx) = self.gin_indices.get_mut(idx_name) else {
                    continue;
                };
                if let Some(value) = row.get(config.column_idx) {
                    Self::index_jsonb_value(
                        gin_idx,
                        value,
                        row_id,
                        config.indexed_paths.as_deref(),
                    );
                }
            }

            self.insert_row_slot(row_id, Rc::new(row));
            inserted += 1;
        }

        Ok(inserted)
    }

    fn can_use_bulk_insert_fast_path(
        &self,
        rows: &[Row],
        secondary_defs: &[BatchSecondaryDef],
    ) -> bool {
        if !rows
            .windows(2)
            .all(|window| window[0].id() <= window[1].id())
        {
            return false;
        }

        let mut seen_row_ids = BTreeSet::new();
        let mut seen_primary_keys = BTreeSet::new();
        let mut seen_secondary_keys: Vec<Option<BTreeSet<IndexKey>>> = secondary_defs
            .iter()
            .map(|def| def.unique.then(BTreeSet::new))
            .collect();

        for row in rows {
            if !seen_row_ids.insert(row.id()) {
                return false;
            }

            if !self.pk_columns.is_empty() {
                let pk = extract_key(row, &self.pk_columns);
                if !seen_primary_keys.insert(pk) {
                    return false;
                }
            }

            for (def, maybe_seen) in secondary_defs.iter().zip(seen_secondary_keys.iter_mut()) {
                let Some(seen) = maybe_seen.as_mut() else {
                    continue;
                };
                let key = extract_key(row, &def.cols);
                if !seen.insert(key) {
                    return false;
                }
            }
        }

        true
    }

    /// Inserts a row into the store.
    pub fn insert(&mut self, row: Row) -> Result<RowId> {
        let row_id = row.id();

        if self.rows.contains_key(&row_id) {
            return Err(Error::invalid_operation("Row ID already exists"));
        }

        // Check primary key uniqueness
        let pk_value = if !self.pk_columns.is_empty() {
            let pk = extract_key(&row, &self.pk_columns);
            if let Some(ref pk_index) = self.primary_index {
                if pk_index.contains_index_key(&pk) {
                    return Err(Error::UniqueConstraint {
                        column: "primary_key".into(),
                        value: pk.to_error_value(),
                    });
                }
            }
            Some(pk)
        } else {
            None
        };

        // Add to row ID index
        self.row_id_index
            .add(Value::Int64(row_id as i64), row_id)
            .map_err(|_| Error::invalid_operation("Failed to add to row ID index"))?;

        // Add to primary key index
        if let (Some(ref mut pk_index), Some(pk)) = (&mut self.primary_index, pk_value.clone()) {
            if pk_index.add_index_key(pk.clone(), row_id).is_err() {
                self.row_id_index
                    .remove(&Value::Int64(row_id as i64), Some(row_id));
                return Err(Error::UniqueConstraint {
                    column: "primary_key".into(),
                    value: pk.to_error_value(),
                });
            }
        }

        // Add to secondary indices
        // Collect index names first to avoid borrow conflict
        let index_names: Vec<String> = self.index_columns.keys().cloned().collect();
        for idx_name in &index_names {
            let cols = &self.index_columns[idx_name];
            let key = extract_key(&row, cols);
            if let Some(idx) = self.secondary_indices.get_mut(idx_name) {
                if idx.add_index_key(key.clone(), row_id).is_err() {
                    self.rollback_insert(row_id, &row);
                    return Err(Error::UniqueConstraint {
                        column: idx_name.clone(),
                        value: key.to_error_value(),
                    });
                }
            }
        }

        // Add to GIN indices
        let gin_index_names: Vec<String> = self.gin_index_configs.keys().cloned().collect();
        for idx_name in &gin_index_names {
            let Some(config) = self.gin_index_configs.get(idx_name).cloned() else {
                continue;
            };
            if let Some(gin_idx) = self.gin_indices.get_mut(idx_name) {
                if let Some(value) = row.get(config.column_idx) {
                    Self::index_jsonb_value(
                        gin_idx,
                        value,
                        row_id,
                        config.indexed_paths.as_deref(),
                    );
                }
            }
        }

        self.insert_row_slot(row_id, Rc::new(row));
        Ok(row_id)
    }

    fn rollback_insert(&mut self, row_id: RowId, row: &Row) {
        self.row_id_index
            .remove(&Value::Int64(row_id as i64), Some(row_id));

        if let Some(ref mut pk_index) = self.primary_index {
            let pk_value = extract_key(row, &self.pk_columns);
            pk_index.remove_index_key(&pk_value, Some(row_id));
        }

        let index_names: Vec<String> = self.index_columns.keys().cloned().collect();
        for idx_name in &index_names {
            let cols = &self.index_columns[idx_name];
            let key = extract_key(row, cols);
            if let Some(idx) = self.secondary_indices.get_mut(idx_name) {
                idx.remove_index_key(&key, Some(row_id));
            }
        }

        // Remove from GIN indices
        let gin_index_names: Vec<String> = self.gin_index_configs.keys().cloned().collect();
        for idx_name in &gin_index_names {
            let Some(config) = self.gin_index_configs.get(idx_name).cloned() else {
                continue;
            };
            if let Some(gin_idx) = self.gin_indices.get_mut(idx_name) {
                if let Some(value) = row.get(config.column_idx) {
                    Self::remove_jsonb_from_gin(
                        gin_idx,
                        value,
                        row_id,
                        config.indexed_paths.as_deref(),
                    );
                }
            }
        }
    }

    /// Updates a row in the store.
    pub fn update(&mut self, row_id: RowId, new_row: Row) -> Result<()> {
        let old_row = self
            .row_ref_by_id(row_id)
            .cloned()
            .ok_or_else(|| Error::not_found(self.schema.name(), Value::Int64(row_id as i64)))?;

        // Check primary key uniqueness if PK changed
        if !self.pk_columns.is_empty() {
            let old_pk = extract_key(&old_row, &self.pk_columns);
            let new_pk = extract_key(&new_row, &self.pk_columns);
            if let Some(ref pk_index) = self.primary_index {
                if old_pk != new_pk && pk_index.contains_index_key(&new_pk) {
                    return Err(Error::UniqueConstraint {
                        column: "primary_key".into(),
                        value: new_pk.to_error_value(),
                    });
                }
            }
        }

        // Check secondary index uniqueness (only for unique indexes)
        for (idx_name, cols) in &self.index_columns {
            let old_key = extract_key(&old_row, cols);
            let new_key = extract_key(&new_row, cols);
            if let Some(idx) = self.secondary_indices.get(idx_name) {
                if idx.is_unique() && old_key != new_key && idx.contains_index_key(&new_key) {
                    return Err(Error::UniqueConstraint {
                        column: idx_name.clone(),
                        value: new_key.to_error_value(),
                    });
                }
            }
        }

        // Update primary key index
        if !self.pk_columns.is_empty() {
            let old_pk = extract_key(&old_row, &self.pk_columns);
            let new_pk = extract_key(&new_row, &self.pk_columns);
            if let Some(ref mut pk_index) = self.primary_index {
                if old_pk != new_pk {
                    pk_index.remove_index_key(&old_pk, Some(row_id));
                    let _ = pk_index.add_index_key(new_pk, row_id);
                }
            }
        }

        // Update secondary indices
        let index_names: Vec<String> = self.index_columns.keys().cloned().collect();
        for idx_name in &index_names {
            let cols = &self.index_columns[idx_name];
            let old_key = extract_key(&old_row, cols);
            let new_key = extract_key(&new_row, cols);
            if let Some(idx) = self.secondary_indices.get_mut(idx_name) {
                if old_key != new_key {
                    idx.remove_index_key(&old_key, Some(row_id));
                    let _ = idx.add_index_key(new_key, row_id);
                }
            }
        }

        // Update GIN indices
        let gin_index_names: Vec<String> = self.gin_index_configs.keys().cloned().collect();
        for idx_name in &gin_index_names {
            let Some(config) = self.gin_index_configs.get(idx_name).cloned() else {
                continue;
            };
            if let Some(gin_idx) = self.gin_indices.get_mut(idx_name) {
                let old_value = old_row.get(config.column_idx);
                let new_value = new_row.get(config.column_idx);
                // Only update if the JSONB value changed
                if old_value != new_value {
                    if let Some(old_val) = old_value {
                        Self::remove_jsonb_from_gin(
                            gin_idx,
                            old_val,
                            row_id,
                            config.indexed_paths.as_deref(),
                        );
                    }
                    if let Some(new_val) = new_value {
                        Self::index_jsonb_value(
                            gin_idx,
                            new_val,
                            row_id,
                            config.indexed_paths.as_deref(),
                        );
                    }
                }
            }
        }

        self.replace_row_slot(row_id, Rc::new(new_row));
        Ok(())
    }

    /// Deletes a row from the store.
    pub fn delete(&mut self, row_id: RowId) -> Result<Rc<Row>> {
        let row = self
            .remove_row_slot(row_id)
            .ok_or_else(|| Error::not_found(self.schema.name(), Value::Int64(row_id as i64)))?;

        self.row_id_index
            .remove(&Value::Int64(row_id as i64), Some(row_id));

        if !self.pk_columns.is_empty() {
            let pk_value = extract_key(&row, &self.pk_columns);
            if let Some(ref mut pk_index) = self.primary_index {
                pk_index.remove_index_key(&pk_value, Some(row_id));
            }
        }

        let index_names: Vec<String> = self.index_columns.keys().cloned().collect();
        for idx_name in &index_names {
            let cols = &self.index_columns[idx_name];
            let key = extract_key(&row, cols);
            if let Some(idx) = self.secondary_indices.get_mut(idx_name) {
                idx.remove_index_key(&key, Some(row_id));
            }
        }

        // Remove from GIN indices
        let gin_index_names: Vec<String> = self.gin_index_configs.keys().cloned().collect();
        for idx_name in &gin_index_names {
            let Some(config) = self.gin_index_configs.get(idx_name).cloned() else {
                continue;
            };
            if let Some(gin_idx) = self.gin_indices.get_mut(idx_name) {
                if let Some(value) = row.get(config.column_idx) {
                    Self::remove_jsonb_from_gin(
                        gin_idx,
                        value,
                        row_id,
                        config.indexed_paths.as_deref(),
                    );
                }
            }
        }

        Ok(row)
    }

    /// Deletes multiple rows from the store in batch.
    /// This is more efficient than calling delete() multiple times because it:
    /// 1. Batches index removals for better cache locality
    /// 2. Reduces repeated HashMap lookups for index names
    /// Returns the deleted rows.
    pub fn delete_batch(&mut self, row_ids: &[RowId]) -> Vec<Rc<Row>> {
        if row_ids.is_empty() {
            return Vec::new();
        }

        // First pass: remove rows from the main storage and collect them
        let mut deleted_rows: Vec<Rc<Row>> = Vec::with_capacity(row_ids.len());
        for &row_id in row_ids {
            if let Some(row) = self.remove_row_slot(row_id) {
                deleted_rows.push(row);
            }
        }

        if deleted_rows.is_empty() {
            return Vec::new();
        }

        // Prepare batch entries for row_id_index
        let row_id_entries: Vec<(Value, RowId)> = deleted_rows
            .iter()
            .map(|row| (Value::Int64(row.id() as i64), row.id()))
            .collect();
        self.row_id_index.remove_batch(&row_id_entries);

        // Prepare batch entries for primary key index
        if !self.pk_columns.is_empty() {
            if let Some(ref mut pk_index) = self.primary_index {
                let pk_entries: Vec<(IndexKey, RowId)> = deleted_rows
                    .iter()
                    .map(|row| (extract_key(row, &self.pk_columns), row.id()))
                    .collect();
                pk_index.remove_batch_index_keys(&pk_entries);
            }
        }

        // Prepare batch entries for each secondary index
        for (idx_name, cols) in &self.index_columns {
            if let Some(idx) = self.secondary_indices.get_mut(idx_name) {
                let entries: Vec<(IndexKey, RowId)> = deleted_rows
                    .iter()
                    .map(|row| (extract_key(row, cols), row.id()))
                    .collect();
                idx.remove_batch_index_keys(&entries);
            }
        }

        // Remove from GIN indices
        let gin_index_names: Vec<String> = self.gin_index_configs.keys().cloned().collect();
        for idx_name in &gin_index_names {
            let Some(config) = self.gin_index_configs.get(idx_name).cloned() else {
                continue;
            };
            if let Some(gin_idx) = self.gin_indices.get_mut(idx_name) {
                for row in &deleted_rows {
                    if let Some(value) = row.get(config.column_idx) {
                        Self::remove_jsonb_from_gin(
                            gin_idx,
                            value,
                            row.id(),
                            config.indexed_paths.as_deref(),
                        );
                    }
                }
            }
        }

        deleted_rows
    }

    /// Gets a row by ID.
    pub fn get(&self, row_id: RowId) -> Option<Rc<Row>> {
        self.row_ref_by_id(row_id).cloned()
    }

    /// Gets a mutable reference to a row by ID (requires exclusive access).
    /// Note: This clones the Rc and returns a new Row if mutation is needed.
    pub fn get_mut(&mut self, row_id: RowId) -> Option<&mut Row> {
        self.row_mut_by_id(row_id).map(Rc::make_mut)
    }

    /// Returns an iterator over all rows.
    pub fn scan(&self) -> impl Iterator<Item = Rc<Row>> + '_ {
        self.scan_order
            .iter()
            .map(|&slot_idx| self.row_slots[slot_idx].row.clone())
    }

    /// Returns an iterator over row references without cloning the underlying `Rc`.
    pub fn row_refs(&self) -> impl Iterator<Item = &Rc<Row>> + '_ {
        self.scan_order
            .iter()
            .map(|&slot_idx| &self.row_slots[slot_idx].row)
    }

    /// Visits rows in storage order without cloning the underlying `Rc`.
    /// Return `false` from the visitor to stop early.
    pub fn visit_rows<F>(&self, mut visitor: F)
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row in self.row_refs() {
            if !visitor(row) {
                break;
            }
        }
    }

    /// Visits rows identified by a row-id subset in the same deterministic order as table scans.
    /// Missing row ids are skipped.
    pub fn visit_rows_by_ids<F>(&self, row_ids: &[RowId], mut visitor: F)
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for &row_id in row_ids {
            let Some(row) = self.row_ref_by_id(row_id) else {
                continue;
            };
            if !visitor(row) {
                break;
            }
        }
    }

    /// Visits rows identified by an arbitrary row-id iterator.
    /// Missing row ids are skipped and iteration order follows the provided iterator.
    pub fn visit_rows_by_iter<I, F>(&self, row_ids: I, mut visitor: F)
    where
        I: IntoIterator<Item = RowId>,
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row_id in row_ids {
            let Some(row) = self.row_ref_by_id(row_id) else {
                continue;
            };
            if !visitor(row) {
                break;
            }
        }
    }

    /// Returns rows identified by a row-id subset in scan order.
    pub fn get_rows_by_ids(&self, row_ids: &[RowId]) -> Vec<Rc<Row>> {
        let mut rows = Vec::with_capacity(row_ids.len().min(self.len()));
        self.visit_rows_by_ids(row_ids, |row| {
            rows.push(row.clone());
            true
        });
        rows
    }

    /// Counts how many row ids in a subset still exist in the store.
    pub fn count_existing_rows_by_ids(&self, row_ids: &[RowId]) -> usize {
        row_ids
            .iter()
            .filter(|row_id| self.rows.contains_key(row_id))
            .count()
    }

    /// Returns all row IDs.
    pub fn row_ids(&self) -> Vec<RowId> {
        self.scan_order
            .iter()
            .map(|&slot_idx| self.row_slots[slot_idx].row_id)
            .collect()
    }

    /// Gets rows by primary key value.
    pub fn get_by_pk(&self, pk_value: &Value) -> Vec<Rc<Row>> {
        self.get_by_pk_values(core::slice::from_ref(pk_value))
    }

    /// Gets rows by primary key components.
    pub fn get_by_pk_values(&self, pk_values: &[Value]) -> Vec<Rc<Row>> {
        if pk_values.len() != self.pk_columns.len() {
            return Vec::new();
        }

        if let Some(ref pk_index) = self.primary_index {
            let pk_key = extract_key_from_values(pk_values);
            pk_index
                .get_index_key(&pk_key)
                .iter()
                .filter_map(|&id| self.row_ref_by_id(id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Visits row ids matching the provided primary-key components.
    pub fn visit_row_ids_by_pk_values<F>(&self, pk_values: &[Value], mut visitor: F)
    where
        F: FnMut(RowId) -> bool,
    {
        if pk_values.len() != self.pk_columns.len() {
            return;
        }

        let Some(ref pk_index) = self.primary_index else {
            return;
        };
        let pk_key = extract_key_from_values(pk_values);
        for row_id in pk_index.get_index_key(&pk_key) {
            if !visitor(row_id) {
                break;
            }
        }
    }

    /// Finds the first row id matching the provided primary-key components.
    pub fn find_row_id_by_pk_values(&self, pk_values: &[Value]) -> Option<RowId> {
        let mut found = None;
        self.visit_row_ids_by_pk_values(pk_values, |row_id| {
            found = Some(row_id);
            false
        });
        found
    }

    /// Finds existing row ID by primary key.
    pub fn find_row_id_by_pk(&self, row: &Row) -> Option<RowId> {
        if let Some(ref pk_index) = self.primary_index {
            let pk_value = extract_key(row, &self.pk_columns);
            pk_index.get_index_key(&pk_value).first().copied()
        } else {
            None
        }
    }

    /// Checks if a primary key value exists.
    pub fn pk_exists(&self, pk_value: &Value) -> bool {
        self.pk_exists_values(core::slice::from_ref(pk_value))
    }

    /// Checks if primary key components exist.
    pub fn pk_exists_values(&self, pk_values: &[Value]) -> bool {
        if pk_values.len() != self.pk_columns.len() {
            return false;
        }

        if let Some(ref pk_index) = self.primary_index {
            let pk_key = extract_key_from_values(pk_values);
            pk_index.contains_index_key(&pk_key)
        } else {
            false
        }
    }

    /// Gets rows by index scan.
    pub fn index_scan(&self, index_name: &str, range: Option<&KeyRange<Value>>) -> Vec<Rc<Row>> {
        self.index_scan_with_options(index_name, range, None, 0, false)
    }

    /// Gets rows by index scan with limit.
    pub fn index_scan_with_limit(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Value>>,
        limit: Option<usize>,
    ) -> Vec<Rc<Row>> {
        self.index_scan_with_limit_offset(index_name, range, limit, 0)
    }

    /// Gets rows by index scan with limit and offset.
    /// This enables true pushdown of LIMIT/OFFSET to the storage layer.
    pub fn index_scan_with_limit_offset(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Value>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Vec<Rc<Row>> {
        self.index_scan_with_options(index_name, range, limit, offset, false)
    }

    /// Gets rows by index scan with limit, offset, and reverse option.
    /// This enables true pushdown of LIMIT/OFFSET/ORDER to the storage layer.
    pub fn index_scan_with_options(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Value>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    ) -> Vec<Rc<Row>> {
        let mut rows = Vec::new();
        self.visit_index_scan_with_options(index_name, range, limit, offset, reverse, |row| {
            rows.push(row.clone());
            true
        });
        rows
    }

    /// Visits rows by index scan with limit, offset, and reverse option.
    /// Return `false` from the visitor to stop early.
    pub fn visit_index_scan_with_options<F>(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Value>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
        mut visitor: F,
    ) where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let Some(idx) = self.secondary_indices.get(index_name) else {
            return;
        };
        let Some(columns) = self.index_columns.get(index_name) else {
            return;
        };

        if columns.len() == 1 {
            let normalized_range = IndexKey::from_scalar_range(range);
            idx.visit_range_index_keys(
                normalized_range.as_ref(),
                reverse,
                limit,
                offset,
                |row_id| {
                    let Some(row) = self.row_ref_by_id(row_id) else {
                        return true;
                    };
                    visitor(row)
                },
            );
        } else if range.is_none() {
            idx.visit_range_index_keys(None, reverse, limit, offset, |row_id| {
                let Some(row) = self.row_ref_by_id(row_id) else {
                    return true;
                };
                visitor(row)
            });
        }
    }

    /// Visits row ids from a single-column index equality scan without materializing rows.
    pub fn visit_index_row_ids_by_value<F>(&self, index_name: &str, value: &Value, visitor: F)
    where
        F: FnMut(RowId) -> bool,
    {
        let Some(idx) = self.secondary_indices.get(index_name) else {
            return;
        };
        let Some(columns) = self.index_columns.get(index_name) else {
            return;
        };
        if columns.len() != 1 {
            return;
        }

        let range = IndexKey::from_scalar_range(Some(&KeyRange::only(value.clone())));
        idx.visit_range_index_keys(range.as_ref(), false, None, 0, visitor);
    }

    /// Returns whether a row matches the scalar key range for a single-column index.
    pub fn row_matches_index_range(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Value>>,
        row: &Row,
    ) -> bool {
        let Some(columns) = self.index_columns.get(index_name) else {
            return false;
        };
        if columns.len() != 1 {
            return false;
        }

        let key = row.get(columns[0]).cloned().unwrap_or(Value::Null);
        range.map_or(true, |range| range.contains(&key))
    }

    /// Visits rows from an index scan restricted to a row-id subset.
    /// `subset_driven=true` iterates the subset directly; otherwise it intersects an index scan.
    pub fn visit_index_scan_with_options_restricted<F>(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Value>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
        allowed_row_ids: &[RowId],
        subset_driven: bool,
        mut visitor: F,
    ) where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let mut skipped = 0usize;
        let mut emitted = 0usize;
        let mut visit_matched = |row: &Rc<Row>| {
            if skipped < offset {
                skipped += 1;
                return true;
            }
            if let Some(limit) = limit {
                if emitted >= limit {
                    return false;
                }
            }
            emitted += 1;
            visitor(row)
        };

        if subset_driven {
            let mut visit_subset_row_id = |row_id: RowId| {
                let Some(row) = self.row_ref_by_id(row_id) else {
                    return true;
                };
                if !self.row_matches_index_range(index_name, range, row) {
                    return true;
                }
                visit_matched(row)
            };

            if reverse {
                for &row_id in allowed_row_ids.iter().rev() {
                    if !visit_subset_row_id(row_id) {
                        break;
                    }
                }
            } else {
                for &row_id in allowed_row_ids {
                    if !visit_subset_row_id(row_id) {
                        break;
                    }
                }
            }
            return;
        }

        self.visit_index_scan_with_options(index_name, range, None, 0, reverse, |row| {
            if allowed_row_ids.binary_search(&row.id()).is_err() {
                return true;
            }
            visit_matched(row)
        });
    }

    /// Gets rows by composite index scan.
    /// Use this for multi-column indexes where the bounds are real tuple keys.
    pub fn index_scan_composite(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Vec<Value>>>,
    ) -> Vec<Rc<Row>> {
        self.index_scan_composite_with_options(index_name, range, None, 0, false)
    }

    /// Gets rows by composite index scan with limit.
    pub fn index_scan_composite_with_limit(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        limit: Option<usize>,
    ) -> Vec<Rc<Row>> {
        self.index_scan_composite_with_limit_offset(index_name, range, limit, 0)
    }

    /// Gets rows by composite index scan with limit and offset.
    pub fn index_scan_composite_with_limit_offset(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        limit: Option<usize>,
        offset: usize,
    ) -> Vec<Rc<Row>> {
        self.index_scan_composite_with_options(index_name, range, limit, offset, false)
    }

    /// Gets rows by composite index scan with tuple bounds, limit, offset, and reverse option.
    pub fn index_scan_composite_with_options(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    ) -> Vec<Rc<Row>> {
        let mut rows = Vec::new();
        self.visit_index_scan_composite_with_options(
            index_name,
            range,
            limit,
            offset,
            reverse,
            |row| {
                rows.push(row.clone());
                true
            },
        );
        rows
    }

    /// Visits rows by composite index scan with tuple bounds, limit, offset, and reverse option.
    /// Return `false` from the visitor to stop early.
    pub fn visit_index_scan_composite_with_options<F>(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
        mut visitor: F,
    ) where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let Some(idx) = self.secondary_indices.get(index_name) else {
            return;
        };
        let Some(columns) = self.index_columns.get(index_name) else {
            return;
        };
        if columns.len() <= 1 {
            return;
        }

        let normalized_range = match range {
            Some(range) if !composite_range_has_expected_arity(range, columns.len()) => return,
            Some(range) => IndexKey::from_composite_range(Some(range)),
            None => None,
        };

        idx.visit_range_index_keys(
            normalized_range.as_ref(),
            reverse,
            limit,
            offset,
            |row_id| {
                let Some(row) = self.row_ref_by_id(row_id) else {
                    return true;
                };
                visitor(row)
            },
        );
    }

    /// Returns whether a row matches the tuple key range for a composite index.
    pub fn row_matches_index_composite_range(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        row: &Row,
    ) -> bool {
        let Some(columns) = self.index_columns.get(index_name) else {
            return false;
        };
        if columns.len() <= 1 {
            return false;
        }

        let key: Vec<Value> = columns
            .iter()
            .map(|&column_index| row.get(column_index).cloned().unwrap_or(Value::Null))
            .collect();
        range.map_or(true, |range| range.contains(&key))
    }

    /// Visits rows from a composite index scan restricted to a row-id subset.
    /// `subset_driven=true` iterates the subset directly; otherwise it intersects an index scan.
    pub fn visit_index_scan_composite_with_options_restricted<F>(
        &self,
        index_name: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
        allowed_row_ids: &[RowId],
        subset_driven: bool,
        mut visitor: F,
    ) where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let mut skipped = 0usize;
        let mut emitted = 0usize;
        let mut visit_matched = |row: &Rc<Row>| {
            if skipped < offset {
                skipped += 1;
                return true;
            }
            if let Some(limit) = limit {
                if emitted >= limit {
                    return false;
                }
            }
            emitted += 1;
            visitor(row)
        };

        if subset_driven {
            let mut visit_subset_row_id = |row_id: RowId| {
                let Some(row) = self.row_ref_by_id(row_id) else {
                    return true;
                };
                if !self.row_matches_index_composite_range(index_name, range, row) {
                    return true;
                }
                visit_matched(row)
            };

            if reverse {
                for &row_id in allowed_row_ids.iter().rev() {
                    if !visit_subset_row_id(row_id) {
                        break;
                    }
                }
            } else {
                for &row_id in allowed_row_ids {
                    if !visit_subset_row_id(row_id) {
                        break;
                    }
                }
            }
            return;
        }

        self.visit_index_scan_composite_with_options(index_name, range, None, 0, reverse, |row| {
            if allowed_row_ids.binary_search(&row.id()).is_err() {
                return true;
            }
            visit_matched(row)
        });
    }

    /// Clears all rows and indices.
    pub fn clear(&mut self) {
        self.rows.clear();
        self.row_slots.clear();
        self.scan_order.clear();
        self.row_id_index.clear();
        if let Some(ref mut pk_index) = self.primary_index {
            pk_index.clear();
        }
        for idx in self.secondary_indices.values_mut() {
            idx.clear();
        }
        for gin_idx in self.gin_indices.values_mut() {
            gin_idx.clear();
        }
    }

    /// Gets multiple rows by IDs.
    pub fn get_many(&self, row_ids: &[RowId]) -> Vec<Option<Rc<Row>>> {
        row_ids
            .iter()
            .map(|&id| self.row_ref_by_id(id).cloned())
            .collect()
    }

    /// Inserts a row or replaces an existing row with the same primary key.
    /// Returns the row ID and whether it was a replacement.
    pub fn insert_or_replace(&mut self, row: Row) -> Result<(RowId, bool)> {
        // Check if a row with the same PK already exists
        if let Some(existing_row_id) = self.find_row_id_by_pk(&row) {
            // Replace: update the existing row, preserving the original row ID
            let updated_row = Row::new(existing_row_id, row.values().to_vec());
            self.update(existing_row_id, updated_row)?;
            Ok((existing_row_id, true))
        } else {
            // Insert: add as new row
            let row_id = self.insert(row)?;
            Ok((row_id, false))
        }
    }

    /// Checks if a secondary index contains a key (for unique constraint checking).
    pub fn secondary_index_contains(&self, index_name: &str, key: &Value) -> bool {
        self.secondary_index_contains_values(index_name, core::slice::from_ref(key))
    }

    /// Checks if a secondary index contains key components.
    pub fn secondary_index_contains_values(&self, index_name: &str, key_values: &[Value]) -> bool {
        let Some(columns) = self.index_columns.get(index_name) else {
            return false;
        };
        if key_values.len() != columns.len() {
            return false;
        }

        if let Some(idx) = self.secondary_indices.get(index_name) {
            let key = extract_key_from_values(key_values);
            idx.contains_index_key(&key)
        } else {
            false
        }
    }

    /// Gets the primary key columns indices.
    pub fn pk_columns(&self) -> &[usize] {
        &self.pk_columns
    }

    /// Extracts the primary key value from a row.
    pub fn extract_pk(&self, row: &Row) -> Option<Value> {
        self.extract_pk_values(row).map(|pk_values| {
            if pk_values.len() == 1 {
                pk_values.into_iter().next().unwrap_or(Value::Null)
            } else {
                Value::String(format!("{:?}", pk_values))
            }
        })
    }

    /// Extracts the primary key components from a row.
    pub fn extract_pk_values(&self, row: &Row) -> Option<Vec<Value>> {
        if self.pk_columns.is_empty() {
            None
        } else {
            Some(
                self.pk_columns
                    .iter()
                    .map(|&idx| row.get(idx).cloned().unwrap_or(Value::Null))
                    .collect(),
            )
        }
    }

    /// Inserts a row and returns a Delta for IVM propagation.
    pub fn insert_with_delta(&mut self, row: Row) -> Result<Delta<Row>> {
        let row_clone = row.clone();
        self.insert(row)?;
        Ok(Delta::insert(row_clone))
    }

    /// Deletes a row and returns a Delta for IVM propagation.
    pub fn delete_with_delta(&mut self, row_id: RowId) -> Result<Delta<Row>> {
        let row = self.delete(row_id)?;
        Ok(Delta::delete((*row).clone()))
    }

    /// Updates a row and returns Deltas for IVM propagation (delete old + insert new).
    pub fn update_with_delta(
        &mut self,
        row_id: RowId,
        new_row: Row,
    ) -> Result<(Delta<Row>, Delta<Row>)> {
        let old_row = self
            .row_ref_by_id(row_id)
            .ok_or_else(|| Error::not_found(self.schema.name(), Value::Int64(row_id as i64)))?
            .clone();
        let new_row_clone = new_row.clone();
        self.update(row_id, new_row)?;
        Ok((
            Delta::delete((*old_row).clone()),
            Delta::insert(new_row_clone),
        ))
    }

    // ========== GIN Index Methods ==========

    /// Indexes a JSONB value into the GIN index.
    fn index_jsonb_value(
        gin_idx: &mut GinIndex,
        value: &Value,
        row_id: RowId,
        indexed_paths: Option<&[String]>,
    ) {
        let Some(parsed) = Self::parse_jsonb_value(value) else {
            return;
        };

        if let Some(paths) = indexed_paths {
            Self::index_jsonb_selected_paths(gin_idx, &parsed, row_id, paths);
            return;
        }

        let mut current_path = String::new();
        Self::index_jsonb_node(gin_idx, &parsed, row_id, &mut current_path, None);
    }

    fn collect_jsonb_value_into_builder(
        builder: &mut GinBulkBuilder,
        value: &Value,
        row_id: RowId,
        indexed_paths: Option<&[String]>,
    ) {
        let Some(parsed) = Self::parse_jsonb_value(value) else {
            return;
        };

        if let Some(paths) = indexed_paths {
            Self::collect_jsonb_selected_paths_into_builder(builder, &parsed, row_id, paths);
            return;
        }

        let mut current_path = String::new();
        Self::collect_jsonb_node_into_builder(builder, &parsed, row_id, &mut current_path, None);
    }

    fn index_jsonb_node(
        gin_idx: &mut GinIndex,
        value: &ParsedJsonbValue,
        row_id: RowId,
        current_path: &mut String,
        indexed_paths: Option<&[String]>,
    ) {
        match value {
            ParsedJsonbValue::Object(obj) => {
                for (key, child) in obj.iter() {
                    let saved_len = current_path.len();
                    Self::append_gin_path_segment(current_path, key);
                    if Self::should_index_gin_path(indexed_paths, current_path) {
                        gin_idx.add_key(current_path.clone(), row_id);
                        Self::index_jsonb_scalar(gin_idx, current_path, child, row_id);
                        Self::index_jsonb_contains_prefilter(gin_idx, current_path, child, row_id);
                    }
                    if Self::should_descend_gin_path(indexed_paths, current_path) {
                        Self::index_jsonb_node(gin_idx, child, row_id, current_path, indexed_paths);
                    }
                    current_path.truncate(saved_len);
                }
            }
            ParsedJsonbValue::Array(items) => {
                for (idx, child) in items.iter().enumerate() {
                    let saved_len = current_path.len();
                    let segment = idx.to_string();
                    Self::append_gin_path_segment(current_path, &segment);
                    if Self::should_index_gin_path(indexed_paths, current_path) {
                        gin_idx.add_key(current_path.clone(), row_id);
                        Self::index_jsonb_scalar(gin_idx, current_path, child, row_id);
                        Self::index_jsonb_contains_prefilter(gin_idx, current_path, child, row_id);
                    }
                    if Self::should_descend_gin_path(indexed_paths, current_path) {
                        Self::index_jsonb_node(gin_idx, child, row_id, current_path, indexed_paths);
                    }
                    current_path.truncate(saved_len);
                }
            }
            _ => {}
        }
    }

    fn index_jsonb_scalar(
        gin_idx: &mut GinIndex,
        current_path: &str,
        value: &ParsedJsonbValue,
        row_id: RowId,
    ) {
        if let Some(value_str) = Self::jsonb_scalar_to_index_value(value) {
            gin_idx.add_key_value(current_path.into(), value_str, row_id);
        }
    }

    fn index_jsonb_contains_prefilter(
        gin_idx: &mut GinIndex,
        current_path: &str,
        value: &ParsedJsonbValue,
        row_id: RowId,
    ) {
        let value_str = value.stringify_for_contains();
        gin_idx.add_contains_trigrams(current_path, &value_str, row_id);
    }

    fn collect_jsonb_node_into_builder(
        builder: &mut GinBulkBuilder,
        value: &ParsedJsonbValue,
        row_id: RowId,
        current_path: &mut String,
        indexed_paths: Option<&[String]>,
    ) {
        match value {
            ParsedJsonbValue::Object(obj) => {
                for (key, child) in obj.iter() {
                    let saved_len = current_path.len();
                    Self::append_gin_path_segment(current_path, key);
                    if Self::should_index_gin_path(indexed_paths, current_path) {
                        builder.add_key(current_path.clone(), row_id);
                        Self::collect_jsonb_scalar_into_builder(
                            builder,
                            current_path,
                            child,
                            row_id,
                        );
                        Self::collect_jsonb_contains_prefilter_into_builder(
                            builder,
                            current_path,
                            child,
                            row_id,
                        );
                    }
                    if Self::should_descend_gin_path(indexed_paths, current_path) {
                        Self::collect_jsonb_node_into_builder(
                            builder,
                            child,
                            row_id,
                            current_path,
                            indexed_paths,
                        );
                    }
                    current_path.truncate(saved_len);
                }
            }
            ParsedJsonbValue::Array(items) => {
                for (idx, child) in items.iter().enumerate() {
                    let saved_len = current_path.len();
                    let segment = idx.to_string();
                    Self::append_gin_path_segment(current_path, &segment);
                    if Self::should_index_gin_path(indexed_paths, current_path) {
                        builder.add_key(current_path.clone(), row_id);
                        Self::collect_jsonb_scalar_into_builder(
                            builder,
                            current_path,
                            child,
                            row_id,
                        );
                        Self::collect_jsonb_contains_prefilter_into_builder(
                            builder,
                            current_path,
                            child,
                            row_id,
                        );
                    }
                    if Self::should_descend_gin_path(indexed_paths, current_path) {
                        Self::collect_jsonb_node_into_builder(
                            builder,
                            child,
                            row_id,
                            current_path,
                            indexed_paths,
                        );
                    }
                    current_path.truncate(saved_len);
                }
            }
            _ => {}
        }
    }

    fn collect_jsonb_scalar_into_builder(
        builder: &mut GinBulkBuilder,
        current_path: &str,
        value: &ParsedJsonbValue,
        row_id: RowId,
    ) {
        if let Some(value_str) = Self::jsonb_scalar_to_index_value(value) {
            builder.add_key_value(current_path.into(), value_str, row_id);
        }
    }

    fn collect_jsonb_contains_prefilter_into_builder(
        builder: &mut GinBulkBuilder,
        current_path: &str,
        value: &ParsedJsonbValue,
        row_id: RowId,
    ) {
        let value_str = value.stringify_for_contains();
        builder.add_contains_trigrams(current_path, &value_str, row_id);
    }

    fn index_jsonb_selected_paths(
        gin_idx: &mut GinIndex,
        value: &ParsedJsonbValue,
        row_id: RowId,
        indexed_paths: &[String],
    ) {
        for path in indexed_paths {
            let Some(selected) = Self::jsonb_value_at_encoded_gin_path(value, path) else {
                continue;
            };
            gin_idx.add_key(path.clone(), row_id);
            Self::index_jsonb_scalar(gin_idx, path, selected, row_id);
            Self::index_jsonb_contains_prefilter(gin_idx, path, selected, row_id);
        }
    }

    fn collect_jsonb_selected_paths_into_builder(
        builder: &mut GinBulkBuilder,
        value: &ParsedJsonbValue,
        row_id: RowId,
        indexed_paths: &[String],
    ) {
        for path in indexed_paths {
            let Some(selected) = Self::jsonb_value_at_encoded_gin_path(value, path) else {
                continue;
            };
            builder.add_key(path.clone(), row_id);
            Self::collect_jsonb_scalar_into_builder(builder, path, selected, row_id);
            Self::collect_jsonb_contains_prefilter_into_builder(builder, path, selected, row_id);
        }
    }

    /// Removes JSONB value from the GIN index.
    fn remove_jsonb_from_gin(
        gin_idx: &mut GinIndex,
        value: &Value,
        row_id: RowId,
        indexed_paths: Option<&[String]>,
    ) {
        let Some(parsed) = Self::parse_jsonb_value(value) else {
            return;
        };

        if let Some(paths) = indexed_paths {
            Self::remove_jsonb_selected_paths(gin_idx, &parsed, row_id, paths);
            return;
        }

        let mut current_path = String::new();
        Self::remove_jsonb_node(gin_idx, &parsed, row_id, &mut current_path, None);
    }

    fn remove_jsonb_node(
        gin_idx: &mut GinIndex,
        value: &ParsedJsonbValue,
        row_id: RowId,
        current_path: &mut String,
        indexed_paths: Option<&[String]>,
    ) {
        match value {
            ParsedJsonbValue::Object(obj) => {
                for (key, child) in obj.iter() {
                    let saved_len = current_path.len();
                    Self::append_gin_path_segment(current_path, key);
                    if Self::should_index_gin_path(indexed_paths, current_path) {
                        gin_idx.remove_key(current_path, row_id);
                        Self::remove_jsonb_scalar(gin_idx, current_path, child, row_id);
                        Self::remove_jsonb_contains_prefilter(gin_idx, current_path, child, row_id);
                    }
                    if Self::should_descend_gin_path(indexed_paths, current_path) {
                        Self::remove_jsonb_node(
                            gin_idx,
                            child,
                            row_id,
                            current_path,
                            indexed_paths,
                        );
                    }
                    current_path.truncate(saved_len);
                }
            }
            ParsedJsonbValue::Array(items) => {
                for (idx, child) in items.iter().enumerate() {
                    let saved_len = current_path.len();
                    let segment = idx.to_string();
                    Self::append_gin_path_segment(current_path, &segment);
                    if Self::should_index_gin_path(indexed_paths, current_path) {
                        gin_idx.remove_key(current_path, row_id);
                        Self::remove_jsonb_scalar(gin_idx, current_path, child, row_id);
                        Self::remove_jsonb_contains_prefilter(gin_idx, current_path, child, row_id);
                    }
                    if Self::should_descend_gin_path(indexed_paths, current_path) {
                        Self::remove_jsonb_node(
                            gin_idx,
                            child,
                            row_id,
                            current_path,
                            indexed_paths,
                        );
                    }
                    current_path.truncate(saved_len);
                }
            }
            _ => {}
        }
    }

    fn should_index_gin_path(indexed_paths: Option<&[String]>, current_path: &str) -> bool {
        indexed_paths.map_or(true, |paths| {
            paths
                .iter()
                .any(|indexed_path| indexed_path == current_path)
        })
    }

    fn should_descend_gin_path(indexed_paths: Option<&[String]>, current_path: &str) -> bool {
        indexed_paths.map_or(true, |paths| {
            let mut prefix = String::with_capacity(current_path.len() + 1);
            prefix.push_str(current_path);
            prefix.push('.');
            paths
                .iter()
                .any(|indexed_path| indexed_path.starts_with(&prefix))
        })
    }

    fn remove_jsonb_scalar(
        gin_idx: &mut GinIndex,
        current_path: &str,
        value: &ParsedJsonbValue,
        row_id: RowId,
    ) {
        if let Some(value_str) = Self::jsonb_scalar_to_index_value(value) {
            gin_idx.remove_key_value(current_path, &value_str, row_id);
        }
    }

    fn remove_jsonb_contains_prefilter(
        gin_idx: &mut GinIndex,
        current_path: &str,
        value: &ParsedJsonbValue,
        row_id: RowId,
    ) {
        let value_str = value.stringify_for_contains();
        for (key, gram) in contains_trigram_pairs(current_path, &value_str) {
            gin_idx.remove_key_value(&key, &gram, row_id);
        }
    }

    fn remove_jsonb_selected_paths(
        gin_idx: &mut GinIndex,
        value: &ParsedJsonbValue,
        row_id: RowId,
        indexed_paths: &[String],
    ) {
        for path in indexed_paths {
            let Some(selected) = Self::jsonb_value_at_encoded_gin_path(value, path) else {
                continue;
            };
            gin_idx.remove_key(path, row_id);
            Self::remove_jsonb_scalar(gin_idx, path, selected, row_id);
            Self::remove_jsonb_contains_prefilter(gin_idx, path, selected, row_id);
        }
    }

    fn parse_jsonb_value(value: &Value) -> Option<ParsedJsonbValue> {
        let Value::Jsonb(jsonb) = value else {
            return None;
        };
        let json_str = core::str::from_utf8(&jsonb.0).ok()?;
        parse_json_text(json_str)
    }

    fn jsonb_scalar_to_index_value(value: &ParsedJsonbValue) -> Option<String> {
        match value {
            ParsedJsonbValue::Null => Some("null".into()),
            ParsedJsonbValue::Bool(b) => Some(if *b { "true" } else { "false" }.into()),
            ParsedJsonbValue::Number(n) => Some(format!("{}", n)),
            ParsedJsonbValue::String(s) => Some(s.clone()),
            ParsedJsonbValue::Object(_) | ParsedJsonbValue::Array(_) => None,
        }
    }

    fn append_gin_path_segment(path: &mut String, segment: &str) {
        if !path.is_empty() {
            path.push('.');
        }

        if segment.is_empty() {
            path.push('\\');
            path.push('0');
            return;
        }

        for ch in segment.chars() {
            match ch {
                '\\' => {
                    path.push('\\');
                    path.push('\\');
                }
                '.' => {
                    path.push('\\');
                    path.push('.');
                }
                _ => path.push(ch),
            }
        }
    }

    /// Queries the GIN index by key-value pair.
    pub fn gin_index_get_by_key_value(
        &self,
        index_name: &str,
        key: &str,
        value: &str,
    ) -> Vec<Rc<Row>> {
        if let Some(gin_idx) = self.gin_indices.get(index_name) {
            let row_ids = gin_idx.get_by_key_value(key, value);
            row_ids
                .iter()
                .filter_map(|&id| self.row_ref_by_id(id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Visits rows matching a GIN key-value query without materializing the full result.
    /// Return `false` from the visitor to stop early.
    pub fn visit_gin_index_by_key_value<F>(
        &self,
        index_name: &str,
        key: &str,
        value: &str,
        mut visitor: F,
    ) where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let Some(gin_idx) = self.gin_indices.get(index_name) else {
            return;
        };

        gin_idx.visit_by_key_value(key, value, |row_id| {
            let Some(row) = self.row_ref_by_id(row_id) else {
                return true;
            };
            visitor(row)
        });
    }

    /// Queries the GIN index by key existence.
    pub fn gin_index_get_by_key(&self, index_name: &str, key: &str) -> Vec<Rc<Row>> {
        if let Some(gin_idx) = self.gin_indices.get(index_name) {
            let row_ids = gin_idx.get_by_key(key);
            row_ids
                .iter()
                .filter_map(|&id| self.row_ref_by_id(id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Visits rows matching a GIN key-existence query without materializing the full result.
    /// Return `false` from the visitor to stop early.
    pub fn visit_gin_index_by_key<F>(&self, index_name: &str, key: &str, mut visitor: F)
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let Some(gin_idx) = self.gin_indices.get(index_name) else {
            return;
        };

        gin_idx.visit_by_key(key, |row_id| {
            let Some(row) = self.row_ref_by_id(row_id) else {
                return true;
            };
            visitor(row)
        });
    }

    /// Returns whether a row contains the indexed JSON path.
    pub fn row_matches_gin_key(&self, index_name: &str, row: &Row, key: &str) -> bool {
        self.row_gin_path_value(index_name, row, key).is_some()
    }

    /// Returns whether a row contains the indexed JSON path with the expected scalar value.
    pub fn row_matches_gin_key_value(
        &self,
        index_name: &str,
        row: &Row,
        key: &str,
        value: &str,
    ) -> bool {
        self.row_contains_gin_token_pair(index_name, row, key, value)
    }

    /// Returns whether a row satisfies a multi-key GIN predicate.
    pub fn row_matches_gin_key_values(
        &self,
        index_name: &str,
        row: &Row,
        pairs: &[(&str, &str)],
        match_all: bool,
    ) -> bool {
        if match_all {
            pairs
                .iter()
                .all(|(key, value)| self.row_matches_gin_key_value(index_name, row, key, value))
        } else {
            pairs
                .iter()
                .any(|(key, value)| self.row_matches_gin_key_value(index_name, row, key, value))
        }
    }

    /// Visits rows matching a GIN key-value query restricted to a row-id subset.
    pub fn visit_gin_index_by_key_value_restricted<F>(
        &self,
        index_name: &str,
        key: &str,
        value: &str,
        allowed_row_ids: &[RowId],
        subset_driven: bool,
        mut visitor: F,
    ) where
        F: FnMut(&Rc<Row>) -> bool,
    {
        if subset_driven {
            self.visit_rows_by_ids(allowed_row_ids, |row| {
                if self.row_matches_gin_key_value(index_name, row, key, value) {
                    visitor(row)
                } else {
                    true
                }
            });
            return;
        }

        self.visit_gin_index_by_key_value(index_name, key, value, |row| {
            if allowed_row_ids.binary_search(&row.id()).is_ok() {
                visitor(row)
            } else {
                true
            }
        });
    }

    /// Visits rows matching a GIN key-existence query restricted to a row-id subset.
    pub fn visit_gin_index_by_key_restricted<F>(
        &self,
        index_name: &str,
        key: &str,
        allowed_row_ids: &[RowId],
        subset_driven: bool,
        mut visitor: F,
    ) where
        F: FnMut(&Rc<Row>) -> bool,
    {
        if subset_driven {
            self.visit_rows_by_ids(allowed_row_ids, |row| {
                if self.row_matches_gin_key(index_name, row, key) {
                    visitor(row)
                } else {
                    true
                }
            });
            return;
        }

        self.visit_gin_index_by_key(index_name, key, |row| {
            if allowed_row_ids.binary_search(&row.id()).is_ok() {
                visitor(row)
            } else {
                true
            }
        });
    }

    /// Queries the GIN index by multiple key-value pairs (AND query).
    /// Returns rows that match ALL of the given key-value pairs.
    pub fn gin_index_get_by_key_values_all(
        &self,
        index_name: &str,
        pairs: &[(&str, &str)],
    ) -> Vec<Rc<Row>> {
        if let Some(gin_idx) = self.gin_indices.get(index_name) {
            let row_ids = gin_idx.get_by_key_values_all(pairs);
            row_ids
                .iter()
                .filter_map(|&id| self.row_ref_by_id(id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Queries the GIN index by multiple key-value pairs (OR query).
    /// Returns rows that match ANY of the given key-value pairs.
    pub fn gin_index_get_by_key_values_any(
        &self,
        index_name: &str,
        pairs: &[(&str, &str)],
    ) -> Vec<Rc<Row>> {
        if let Some(gin_idx) = self.gin_indices.get(index_name) {
            let row_ids = gin_idx.get_by_key_values_any(pairs);
            row_ids
                .iter()
                .filter_map(|&id| self.row_ref_by_id(id).cloned())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Visits rows matching all GIN key-value pairs without materializing the full row set.
    /// Return `false` from the visitor to stop early.
    pub fn visit_gin_index_by_key_values_all<F>(
        &self,
        index_name: &str,
        pairs: &[(&str, &str)],
        mut visitor: F,
    ) where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let Some(gin_idx) = self.gin_indices.get(index_name) else {
            return;
        };

        gin_idx.visit_by_key_values_all(pairs, |row_id| {
            let Some(row) = self.row_ref_by_id(row_id) else {
                return true;
            };
            visitor(row)
        });
    }

    /// Visits rows matching any GIN key-value pairs without materializing the full row set.
    /// Return `false` from the visitor to stop early.
    pub fn visit_gin_index_by_key_values_any<F>(
        &self,
        index_name: &str,
        pairs: &[(&str, &str)],
        mut visitor: F,
    ) where
        F: FnMut(&Rc<Row>) -> bool,
    {
        let Some(gin_idx) = self.gin_indices.get(index_name) else {
            return;
        };

        gin_idx.visit_by_key_values_any(pairs, |row_id| {
            let Some(row) = self.row_ref_by_id(row_id) else {
                return true;
            };
            visitor(row)
        });
    }

    /// Visits rows matching a multi-predicate GIN query restricted to a row-id subset.
    pub fn visit_gin_index_by_key_values_restricted<F>(
        &self,
        index_name: &str,
        pairs: &[(&str, &str)],
        match_all: bool,
        allowed_row_ids: &[RowId],
        subset_driven: bool,
        mut visitor: F,
    ) where
        F: FnMut(&Rc<Row>) -> bool,
    {
        if subset_driven {
            self.visit_rows_by_ids(allowed_row_ids, |row| {
                if self.row_matches_gin_key_values(index_name, row, pairs, match_all) {
                    visitor(row)
                } else {
                    true
                }
            });
            return;
        }

        let mut visit_intersected = |row: &Rc<Row>| {
            if allowed_row_ids.binary_search(&row.id()).is_ok() {
                visitor(row)
            } else {
                true
            }
        };

        if match_all {
            self.visit_gin_index_by_key_values_all(index_name, pairs, |row| visit_intersected(row));
        } else {
            self.visit_gin_index_by_key_values_any(index_name, pairs, |row| visit_intersected(row));
        }
    }

    /// Returns the raw row IDs from the GIN index for a given key.
    /// This is useful for testing to detect ghost entries (entries that point to deleted rows).
    #[cfg(test)]
    pub fn gin_index_get_raw_row_ids(&self, index_name: &str, key: &str) -> Vec<RowId> {
        if let Some(gin_idx) = self.gin_indices.get(index_name) {
            gin_idx.get_by_key(key)
        } else {
            Vec::new()
        }
    }

    /// Returns the raw row IDs from the GIN index for a given key-value pair.
    /// This is useful for testing to detect ghost entries.
    #[cfg(test)]
    pub fn gin_index_get_raw_row_ids_by_kv(
        &self,
        index_name: &str,
        key: &str,
        value: &str,
    ) -> Vec<RowId> {
        if let Some(gin_idx) = self.gin_indices.get(index_name) {
            gin_idx.get_by_key_value(key, value)
        } else {
            Vec::new()
        }
    }

    fn row_gin_path_value(
        &self,
        index_name: &str,
        row: &Row,
        key: &str,
    ) -> Option<ParsedJsonbValue> {
        let config = self.gin_index_configs.get(index_name)?;
        let value = row.get(config.column_idx)?;
        let parsed = Self::parse_jsonb_value(value)?;
        Self::jsonb_value_at_encoded_gin_path(&parsed, key).cloned()
    }

    fn row_contains_gin_token_pair(
        &self,
        index_name: &str,
        row: &Row,
        key: &str,
        value: &str,
    ) -> bool {
        let Some(config) = self.gin_index_configs.get(index_name) else {
            return false;
        };
        let Some(row_value) = row.get(config.column_idx) else {
            return false;
        };
        let Some(parsed) = Self::parse_jsonb_value(row_value) else {
            return false;
        };

        let mut current_path = String::new();
        let mut matched = false;
        Self::visit_jsonb_gin_pairs(
            &parsed,
            &mut current_path,
            config.indexed_paths.as_deref(),
            &mut |candidate_key, candidate_value| {
                if candidate_key == key && candidate_value == value {
                    matched = true;
                    false
                } else {
                    true
                }
            },
        );
        matched
    }

    fn jsonb_value_at_encoded_gin_path<'a>(
        value: &'a ParsedJsonbValue,
        path: &str,
    ) -> Option<&'a ParsedJsonbValue> {
        let segments = decode_gin_path_segments(path);
        let mut current = value;
        for segment in segments {
            current = match current {
                ParsedJsonbValue::Object(object) => object.get(&segment)?,
                ParsedJsonbValue::Array(items) => {
                    let index = segment.parse::<usize>().ok()?;
                    items.get(index)?
                }
                ParsedJsonbValue::Null
                | ParsedJsonbValue::Bool(_)
                | ParsedJsonbValue::Number(_)
                | ParsedJsonbValue::String(_) => return None,
            };
        }
        Some(current)
    }

    fn visit_jsonb_gin_pairs<F>(
        value: &ParsedJsonbValue,
        current_path: &mut String,
        indexed_paths: Option<&[String]>,
        visitor: &mut F,
    ) -> bool
    where
        F: FnMut(&str, &str) -> bool,
    {
        match value {
            ParsedJsonbValue::Object(object) => {
                for (key, child) in object.iter() {
                    let saved_len = current_path.len();
                    Self::append_gin_path_segment(current_path, key);
                    if Self::should_index_gin_path(indexed_paths, current_path) {
                        if let Some(value_str) = Self::jsonb_scalar_to_index_value(child) {
                            if !visitor(current_path, value_str.as_str()) {
                                return false;
                            }
                        }
                        for (token_key, gram) in
                            contains_trigram_pairs(current_path, &child.stringify_for_contains())
                        {
                            if !visitor(token_key.as_str(), gram.as_str()) {
                                return false;
                            }
                        }
                    }
                    if Self::should_descend_gin_path(indexed_paths, current_path)
                        && !Self::visit_jsonb_gin_pairs(child, current_path, indexed_paths, visitor)
                    {
                        return false;
                    }
                    current_path.truncate(saved_len);
                }
                true
            }
            ParsedJsonbValue::Array(items) => {
                for (index, child) in items.iter().enumerate() {
                    let saved_len = current_path.len();
                    let segment = index.to_string();
                    Self::append_gin_path_segment(current_path, &segment);
                    if Self::should_index_gin_path(indexed_paths, current_path) {
                        if let Some(value_str) = Self::jsonb_scalar_to_index_value(child) {
                            if !visitor(current_path, value_str.as_str()) {
                                return false;
                            }
                        }
                        for (token_key, gram) in
                            contains_trigram_pairs(current_path, &child.stringify_for_contains())
                        {
                            if !visitor(token_key.as_str(), gram.as_str()) {
                                return false;
                            }
                        }
                    }
                    if Self::should_descend_gin_path(indexed_paths, current_path)
                        && !Self::visit_jsonb_gin_pairs(child, current_path, indexed_paths, visitor)
                    {
                        return false;
                    }
                    current_path.truncate(saved_len);
                }
                true
            }
            ParsedJsonbValue::Null
            | ParsedJsonbValue::Bool(_)
            | ParsedJsonbValue::Number(_)
            | ParsedJsonbValue::String(_) => true,
        }
    }
}

fn decode_gin_path_segments(path: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut chars = path.chars();

    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                let Some(escaped) = chars.next() else {
                    current.push('\\');
                    break;
                };
                match escaped {
                    '0' if current.is_empty() => {}
                    other => current.push(other),
                }
            }
            '.' => {
                segments.push(current);
                current = String::new();
            }
            other => current.push(other),
        }
    }

    segments.push(current);
    segments
}

fn parse_json_text(s: &str) -> Option<ParsedJsonbValue> {
    let s = s.trim();
    if s == "null" {
        return Some(ParsedJsonbValue::Null);
    }
    if s == "true" {
        return Some(ParsedJsonbValue::Bool(true));
    }
    if s == "false" {
        return Some(ParsedJsonbValue::Bool(false));
    }
    if let Ok(n) = s.parse::<f64>() {
        return Some(ParsedJsonbValue::Number(n));
    }
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        return Some(ParsedJsonbValue::String(unescape_json(&s[1..s.len() - 1])));
    }
    if s.starts_with('{') {
        return parse_json_object(s);
    }
    if s.starts_with('[') {
        return parse_json_array(s);
    }
    None
}

fn unescape_json(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
                Some('r') => result.push('\r'),
                Some('"') => result.push('"'),
                Some('\\') => result.push('\\'),
                Some('/') => result.push('/'),
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn parse_json_object(s: &str) -> Option<ParsedJsonbValue> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return None;
    }

    let inner = s[1..s.len() - 1].trim();
    if inner.is_empty() {
        return Some(ParsedJsonbValue::Object(JsonbObject::new()));
    }

    let mut obj = JsonbObject::new();
    for pair in split_json_top_level(inner, ',') {
        let pair = pair.trim();
        let colon_pos = find_json_colon(pair)?;
        let key_str = pair[..colon_pos].trim();
        let value_str = pair[colon_pos + 1..].trim();

        if !(key_str.starts_with('"') && key_str.ends_with('"') && key_str.len() >= 2) {
            return None;
        }

        let key = unescape_json(&key_str[1..key_str.len() - 1]);
        obj.insert(key, parse_json_text(value_str)?);
    }

    Some(ParsedJsonbValue::Object(obj))
}

fn parse_json_array(s: &str) -> Option<ParsedJsonbValue> {
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        return None;
    }

    let inner = s[1..s.len() - 1].trim();
    if inner.is_empty() {
        return Some(ParsedJsonbValue::Array(Vec::new()));
    }

    let mut values = Vec::new();
    for value in split_json_top_level(inner, ',') {
        values.push(parse_json_text(value.trim())?);
    }

    Some(ParsedJsonbValue::Array(values))
}

fn split_json_top_level(s: &str, separator: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    let mut start = 0usize;

    for (i, c) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if c == '\\' && in_string {
            escape = true;
            continue;
        }
        if c == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        if c == '{' || c == '[' {
            depth += 1;
        } else if c == '}' || c == ']' {
            depth -= 1;
        } else if c == separator && depth == 0 {
            parts.push(&s[start..i]);
            start = i + c.len_utf8();
        }
    }

    if start <= s.len() {
        parts.push(&s[start..]);
    }

    parts
}

fn find_json_colon(s: &str) -> Option<usize> {
    let mut in_string = false;
    let mut escape = false;

    for (i, c) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if c == '\\' && in_string {
            escape = true;
            continue;
        }
        if c == '"' {
            in_string = !in_string;
            continue;
        }
        if !in_string && c == ':' {
            return Some(i);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::collections::BTreeSet;
    use alloc::vec;
    use cynos_core::schema::TableBuilder;
    use cynos_core::DataType;

    fn test_schema() -> Table {
        TableBuilder::new("test")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    fn test_schema_with_index() -> Table {
        TableBuilder::new("test")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("value", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_value", &["value"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    fn test_schema_with_hash_index() -> Table {
        TableBuilder::new("test_hash")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("value", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_hash_index("idx_value_hash", &["value"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_row_store_insert() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        assert!(store.insert(row).is_ok());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_row_store_get() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(row).unwrap();
        let retrieved = store.get(1);
        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.unwrap().get(1),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn test_row_store_update() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(row).unwrap();
        let new_row = Row::new(1, vec![Value::Int64(1), Value::String("Bob".into())]);
        assert!(store.update(1, new_row).is_ok());
        let retrieved = store.get(1);
        assert_eq!(
            retrieved.unwrap().get(1),
            Some(&Value::String("Bob".into()))
        );
    }

    #[test]
    fn test_row_store_delete() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(row).unwrap();
        assert!(store.delete(1).is_ok());
        assert_eq!(store.len(), 0);
        assert!(store.get(1).is_none());
    }

    #[test]
    fn test_row_store_pk_uniqueness() {
        let mut store = RowStore::new(test_schema());
        let row1 = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        let row2 = Row::new(2, vec![Value::Int64(1), Value::String("Bob".into())]);
        store.insert(row1).unwrap();
        assert!(store.insert(row2).is_err());
    }

    #[test]
    fn test_row_store_bulk_load_builds_scan_and_secondary_indexes() {
        let mut store = RowStore::new(test_schema_with_index());
        let rows = vec![
            Row::new(10, vec![Value::Int64(1), Value::Int64(100)]),
            Row::new(11, vec![Value::Int64(2), Value::Int64(200)]),
            Row::new(12, vec![Value::Int64(3), Value::Int64(100)]),
        ];

        let loaded = store.bulk_load(rows).unwrap();
        assert_eq!(loaded, 3);
        assert_eq!(store.len(), 3);

        let scan_ids: Vec<_> = store.scan().map(|row| row.id()).collect();
        assert_eq!(scan_ids, vec![10, 11, 12]);

        let pk_rows = store.get_by_pk(&Value::Int64(2));
        assert_eq!(pk_rows.len(), 1);
        assert_eq!(pk_rows[0].id(), 11);

        let indexed = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(100))));
        assert_eq!(
            indexed.iter().map(|row| row.id()).collect::<Vec<_>>(),
            vec![10, 12]
        );
    }

    #[test]
    fn test_row_store_bulk_load_rejects_duplicate_keys_without_leaking_state() {
        let mut store = RowStore::new(test_schema());
        let rows = vec![
            Row::new(10, vec![Value::Int64(1), Value::String("Alice".into())]),
            Row::new(11, vec![Value::Int64(1), Value::String("Bob".into())]),
        ];

        assert!(store.bulk_load(rows).is_err());
        assert!(store.is_empty());
        assert!(store.scan().next().is_none());
    }

    #[test]
    fn test_row_store_scan() {
        let mut store = RowStore::new(test_schema());
        store
            .insert(Row::new(
                1,
                vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();
        store
            .insert(Row::new(
                2,
                vec![Value::Int64(2), Value::String("Bob".into())],
            ))
            .unwrap();
        let rows: Vec<_> = store.scan().collect();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_row_store_scan_preserves_row_id_order_after_delete() {
        let mut store = RowStore::new(test_schema());
        store
            .insert(Row::new(
                2,
                vec![Value::Int64(2), Value::String("Bob".into())],
            ))
            .unwrap();
        store
            .insert(Row::new(
                1,
                vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();
        store
            .insert(Row::new(
                3,
                vec![Value::Int64(3), Value::String("Charlie".into())],
            ))
            .unwrap();

        store.delete(2).unwrap();

        let row_ids: Vec<_> = store.scan().map(|row| row.id()).collect();
        assert_eq!(row_ids, vec![1, 3]);
    }

    #[test]
    fn test_row_store_index_maintenance() {
        let mut store = RowStore::new(test_schema_with_index());
        let row = Row::new(1, vec![Value::Int64(1), Value::Int64(100)]);
        store.insert(row).unwrap();
        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(100))));
        assert_eq!(results.len(), 1);
        store.delete(1).unwrap();
        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(100))));
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_row_store_hash_index_point_lookup() {
        let mut store = RowStore::new(test_schema_with_hash_index());
        store
            .insert(Row::new(1, vec![Value::Int64(1), Value::Int64(100)]))
            .unwrap();
        store
            .insert(Row::new(2, vec![Value::Int64(2), Value::Int64(200)]))
            .unwrap();

        let results = store.index_scan("idx_value_hash", Some(&KeyRange::only(Value::Int64(200))));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id(), 2);
    }

    #[test]
    fn test_row_store_hash_index_range_scan_is_correct() {
        let mut store = RowStore::new(test_schema_with_hash_index());
        store
            .insert(Row::new(1, vec![Value::Int64(1), Value::Int64(100)]))
            .unwrap();
        store
            .insert(Row::new(2, vec![Value::Int64(2), Value::Int64(200)]))
            .unwrap();
        store
            .insert(Row::new(3, vec![Value::Int64(3), Value::Int64(300)]))
            .unwrap();

        let results = store.index_scan(
            "idx_value_hash",
            Some(&KeyRange::bound(
                Value::Int64(150),
                Value::Int64(300),
                false,
                false,
            )),
        );

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id(), 2);
        assert_eq!(results[1].id(), 3);
    }

    #[test]
    fn test_restricted_index_scan_subset_driven_reverse_preserves_subset_order() {
        let mut store = RowStore::new(test_schema_with_index());
        store
            .insert(Row::new(1, vec![Value::Int64(1), Value::Int64(100)]))
            .unwrap();
        store
            .insert(Row::new(2, vec![Value::Int64(2), Value::Int64(200)]))
            .unwrap();
        store
            .insert(Row::new(3, vec![Value::Int64(3), Value::Int64(300)]))
            .unwrap();

        let mut row_ids = Vec::new();
        store.visit_index_scan_with_options_restricted(
            "idx_value",
            None,
            None,
            0,
            true,
            &[1, 3],
            true,
            |row| {
                row_ids.push(row.id());
                true
            },
        );

        assert_eq!(row_ids, vec![3, 1]);
    }

    #[test]
    fn test_row_store_clear() {
        let mut store = RowStore::new(test_schema());
        store
            .insert(Row::new(
                1,
                vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();
        store
            .insert(Row::new(
                2,
                vec![Value::Int64(2), Value::String("Bob".into())],
            ))
            .unwrap();
        store.clear();
        assert!(store.is_empty());
    }

    // === Additional tests for better coverage ===

    fn test_schema_composite_pk() -> Table {
        TableBuilder::new("test")
            .unwrap()
            .add_column("id1", DataType::String)
            .unwrap()
            .add_column("id2", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id1", "id2"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    fn test_schema_with_composite_index() -> Table {
        TableBuilder::new("test_composite_index")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("a", DataType::Int64)
            .unwrap()
            .add_column("b", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_a_b", &["a", "b"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    fn test_schema_with_unique_index() -> Table {
        TableBuilder::new("test")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("email", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_email", &["email"], true) // unique index
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_composite_primary_key() {
        let mut store = RowStore::new(test_schema_composite_pk());

        let row1 = Row::new(
            1,
            vec![
                Value::String("pk1".into()),
                Value::Int64(100),
                Value::String("Name1".into()),
            ],
        );
        assert!(store.insert(row1).is_ok());

        // Same id1, different id2 - should succeed
        let row2 = Row::new(
            2,
            vec![
                Value::String("pk1".into()),
                Value::Int64(200),
                Value::String("Name2".into()),
            ],
        );
        assert!(store.insert(row2).is_ok());

        // Same composite key - should fail
        let row3 = Row::new(
            3,
            vec![
                Value::String("pk1".into()),
                Value::Int64(100),
                Value::String("Name3".into()),
            ],
        );
        assert!(store.insert(row3).is_err());
    }

    #[test]
    fn test_composite_primary_key_lookup_by_values() {
        let mut store = RowStore::new(test_schema_composite_pk());

        store
            .insert(Row::new(
                1,
                vec![
                    Value::String("pk1".into()),
                    Value::Int64(100),
                    Value::String("Name1".into()),
                ],
            ))
            .unwrap();
        store
            .insert(Row::new(
                2,
                vec![
                    Value::String("pk1".into()),
                    Value::Int64(200),
                    Value::String("Name2".into()),
                ],
            ))
            .unwrap();

        let key = alloc::vec![Value::String("pk1".into()), Value::Int64(200)];
        let rows = store.get_by_pk_values(&key);

        assert_eq!(
            rows.len(),
            1,
            "Composite PK lookup should find exactly one row"
        );
        assert_eq!(rows[0].id(), 2);
        assert!(
            store.pk_exists_values(&key),
            "Composite PK existence check should use the same tuple semantics",
        );
    }

    #[test]
    fn test_insert_or_replace_existing_composite_pk() {
        let mut store = RowStore::new(test_schema_composite_pk());

        store
            .insert(Row::new(
                1,
                vec![
                    Value::String("pk1".into()),
                    Value::Int64(100),
                    Value::String("Name1".into()),
                ],
            ))
            .unwrap();

        let replacement = Row::new(
            99,
            vec![
                Value::String("pk1".into()),
                Value::Int64(100),
                Value::String("Updated".into()),
            ],
        );

        let (row_id, replaced) = store.insert_or_replace(replacement).unwrap();
        assert_eq!(
            row_id, 1,
            "Composite PK replace should preserve the existing row id"
        );
        assert!(replaced);
        assert_eq!(store.len(), 1);

        let stored = store.get(1).unwrap();
        assert_eq!(stored.get(2), Some(&Value::String("Updated".into())));
    }

    #[test]
    fn test_composite_secondary_index_scan_preserves_tuple_order_and_pagination() {
        let mut store = RowStore::new(test_schema_with_composite_index());

        for (id, a, b) in [(1_u64, 1_i64, 2_i64), (2, 1, 10), (3, 2, 1)] {
            store
                .insert(Row::new(
                    id,
                    vec![Value::Int64(id as i64), Value::Int64(a), Value::Int64(b)],
                ))
                .unwrap();
        }

        let ordered_ids: Vec<RowId> = store
            .index_scan_with_options("idx_a_b", None, None, 0, false)
            .iter()
            .map(|row| row.id())
            .collect();
        assert_eq!(
            ordered_ids,
            vec![1, 2, 3],
            "Composite secondary index should follow true tuple order `(a, b)`",
        );

        let paged_ids: Vec<RowId> = store
            .index_scan_with_options("idx_a_b", None, Some(1), 1, false)
            .iter()
            .map(|row| row.id())
            .collect();
        assert_eq!(
            paged_ids,
            vec![2],
            "Pagination over a composite index should use tuple order, not string order",
        );
    }

    #[test]
    fn test_composite_secondary_index_range_scan_uses_tuple_bounds() {
        let mut store = RowStore::new(test_schema_with_composite_index());

        for (id, a, b) in [
            (1_u64, 1_i64, 1_i64),
            (2, 1, 2),
            (3, 1, 10),
            (4, 2, 1),
            (5, 2, 5),
        ] {
            store
                .insert(Row::new(
                    id,
                    vec![Value::Int64(id as i64), Value::Int64(a), Value::Int64(b)],
                ))
                .unwrap();
        }

        let range = KeyRange::bound(
            alloc::vec![Value::Int64(1), Value::Int64(2)],
            alloc::vec![Value::Int64(2), Value::Int64(1)],
            false,
            false,
        );

        let ids: Vec<RowId> = store
            .index_scan_composite("idx_a_b", Some(&range))
            .iter()
            .map(|row| row.id())
            .collect();

        assert_eq!(
            ids,
            vec![2, 3, 4],
            "Composite range scan should honor inclusive tuple bounds",
        );
    }

    #[test]
    fn test_composite_secondary_index_range_scan_reverse_limit_offset() {
        let mut store = RowStore::new(test_schema_with_composite_index());

        for (id, a, b) in [
            (1_u64, 1_i64, 1_i64),
            (2, 1, 2),
            (3, 1, 10),
            (4, 2, 1),
            (5, 2, 5),
        ] {
            store
                .insert(Row::new(
                    id,
                    vec![Value::Int64(id as i64), Value::Int64(a), Value::Int64(b)],
                ))
                .unwrap();
        }

        let range = KeyRange::lower_bound(alloc::vec![Value::Int64(1), Value::Int64(2)], false);

        let ids: Vec<RowId> = store
            .index_scan_composite_with_options("idx_a_b", Some(&range), Some(2), 1, true)
            .iter()
            .map(|row| row.id())
            .collect();

        assert_eq!(
            ids,
            vec![4, 3],
            "Reverse composite range scans should still honor LIMIT/OFFSET in tuple order",
        );
    }

    #[test]
    fn test_composite_secondary_index_range_scan_rejects_wrong_arity() {
        let mut store = RowStore::new(test_schema_with_composite_index());

        store
            .insert(Row::new(
                1,
                vec![Value::Int64(1), Value::Int64(1), Value::Int64(1)],
            ))
            .unwrap();

        let wrong_arity = KeyRange::only(alloc::vec![Value::Int64(1)]);
        let rows = store.index_scan_composite("idx_a_b", Some(&wrong_arity));
        assert!(
            rows.is_empty(),
            "Composite range scan API should reject bounds whose arity does not match the index",
        );
    }

    #[test]
    fn test_insert_or_replace_new() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);

        let (row_id, replaced) = store.insert_or_replace(row).unwrap();
        assert_eq!(row_id, 1);
        assert!(!replaced);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_insert_or_replace_existing() {
        let mut store = RowStore::new(test_schema());

        // Insert first row
        let row1 = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(row1).unwrap();

        // Replace with same PK but different row ID
        let row2 = Row::new(2, vec![Value::Int64(1), Value::String("Updated".into())]);
        let (row_id, replaced) = store.insert_or_replace(row2).unwrap();

        assert_eq!(row_id, 1); // Should preserve original row ID
        assert!(replaced);
        assert_eq!(store.len(), 1);

        let stored = store.get(1).unwrap();
        assert_eq!(stored.get(1), Some(&Value::String("Updated".into())));
    }

    #[test]
    fn test_update_pk_violation() {
        let mut store = RowStore::new(test_schema());

        // Insert two rows
        let row1 = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        let row2 = Row::new(2, vec![Value::Int64(2), Value::String("Bob".into())]);
        store.insert(row1).unwrap();
        store.insert(row2).unwrap();

        // Try to update row2 to have the same PK as row1
        let row2_updated = Row::new(
            2,
            vec![Value::Int64(1), Value::String("Bob Updated".into())],
        );
        let result = store.update(2, row2_updated);
        assert!(result.is_err());
    }

    #[test]
    fn test_unique_index_violation() {
        let mut store = RowStore::new(test_schema_with_unique_index());

        let row1 = Row::new(
            1,
            vec![Value::Int64(1), Value::String("alice@test.com".into())],
        );
        store.insert(row1).unwrap();

        // Try to insert with same email (unique index violation)
        let row2 = Row::new(
            2,
            vec![Value::Int64(2), Value::String("alice@test.com".into())],
        );
        let result = store.insert(row2);
        assert!(result.is_err());
    }

    #[test]
    fn test_unique_index_update_violation() {
        let mut store = RowStore::new(test_schema_with_unique_index());

        let row1 = Row::new(
            1,
            vec![Value::Int64(1), Value::String("alice@test.com".into())],
        );
        let row2 = Row::new(
            2,
            vec![Value::Int64(2), Value::String("bob@test.com".into())],
        );
        store.insert(row1).unwrap();
        store.insert(row2).unwrap();

        // Try to update row2 to have the same email as row1
        let row2_updated = Row::new(
            2,
            vec![Value::Int64(2), Value::String("alice@test.com".into())],
        );
        let result = store.update(2, row2_updated);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_then_insert_same_pk() {
        let mut store = RowStore::new(test_schema());

        // Insert and delete
        let row1 = Row::new(1, vec![Value::Int64(100), Value::String("Alice".into())]);
        store.insert(row1).unwrap();
        store.delete(1).unwrap();

        // Insert with same PK should succeed
        let row2 = Row::new(2, vec![Value::Int64(100), Value::String("Bob".into())]);
        assert!(store.insert(row2).is_ok());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_index_update_maintenance() {
        let mut store = RowStore::new(test_schema_with_index());

        // Insert row
        let row = Row::new(1, vec![Value::Int64(1), Value::Int64(100)]);
        store.insert(row).unwrap();

        // Verify index has the value
        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(100))));
        assert_eq!(results.len(), 1);

        // Update the indexed value
        let updated = Row::new(1, vec![Value::Int64(1), Value::Int64(200)]);
        store.update(1, updated).unwrap();

        // Old value should not be in index
        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(100))));
        assert_eq!(results.len(), 0);

        // New value should be in index
        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(200))));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_visit_index_scan_matches_materialized_scan() {
        let mut store = RowStore::new(test_schema_with_index());
        for i in 1..=5 {
            store
                .insert(Row::new(
                    i,
                    vec![Value::Int64(i as i64), Value::Int64((i * 100) as i64)],
                ))
                .unwrap();
        }

        let range = KeyRange::bound(Value::Int64(200), Value::Int64(500), false, false);
        let expected: Vec<_> = store
            .index_scan_with_options("idx_value", Some(&range), Some(2), 1, true)
            .into_iter()
            .map(|row| row.id())
            .collect();

        let mut actual = Vec::new();
        store.visit_index_scan_with_options("idx_value", Some(&range), Some(2), 1, true, |row| {
            actual.push(row.id());
            true
        });

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_visit_composite_index_scan_matches_materialized_scan() {
        let mut store = RowStore::new(test_schema_with_composite_index());
        for (row_id, a, b) in [(1, 1, 10), (2, 1, 20), (3, 2, 10), (4, 2, 20)] {
            store
                .insert(Row::new(
                    row_id,
                    vec![
                        Value::Int64(row_id as i64),
                        Value::Int64(a),
                        Value::Int64(b),
                    ],
                ))
                .unwrap();
        }

        let range = KeyRange::bound(
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(2), Value::Int64(20)],
            false,
            false,
        );
        let expected: Vec<_> = store
            .index_scan_composite_with_options("idx_a_b", Some(&range), Some(3), 1, false)
            .into_iter()
            .map(|row| row.id())
            .collect();

        let mut actual = Vec::new();
        store.visit_index_scan_composite_with_options(
            "idx_a_b",
            Some(&range),
            Some(3),
            1,
            false,
            |row| {
                actual.push(row.id());
                true
            },
        );

        assert_eq!(actual, expected);
    }

    // === Delta integration tests ===

    #[test]
    fn test_insert_with_delta() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        let delta = store.insert_with_delta(row.clone()).unwrap();

        assert_eq!(delta.diff(), 1);
        assert_eq!(delta.data(), &row);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_delete_with_delta() {
        let mut store = RowStore::new(test_schema());
        let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(row.clone()).unwrap();

        let delta = store.delete_with_delta(1).unwrap();
        assert_eq!(delta.diff(), -1);
        assert_eq!(delta.data(), &row);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_update_with_delta() {
        let mut store = RowStore::new(test_schema());
        let old_row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
        store.insert(old_row.clone()).unwrap();

        let new_row = Row::new(1, vec![Value::Int64(1), Value::String("Bob".into())]);
        let (delete_delta, insert_delta) = store.update_with_delta(1, new_row.clone()).unwrap();

        assert_eq!(delete_delta.diff(), -1);
        assert_eq!(delete_delta.data(), &old_row);
        assert_eq!(insert_delta.diff(), 1);
        assert_eq!(insert_delta.data(), &new_row);
        assert_eq!(store.len(), 1);
    }

    // ==================== Batch Delete Tests ====================

    #[test]
    fn test_delete_batch_basic() {
        let mut store = RowStore::new(test_schema());

        // Insert 10 rows
        for i in 1..=10 {
            let row = Row::new(
                i,
                vec![Value::Int64(i as i64), Value::String(format!("Name{}", i))],
            );
            store.insert(row).unwrap();
        }
        assert_eq!(store.len(), 10);

        // Delete rows 2, 4, 6, 8
        let deleted = store.delete_batch(&[2, 4, 6, 8]);
        assert_eq!(deleted.len(), 4);
        assert_eq!(store.len(), 6);

        // Verify remaining rows
        assert!(store.get(1).is_some());
        assert!(store.get(2).is_none());
        assert!(store.get(3).is_some());
        assert!(store.get(4).is_none());
        assert!(store.get(5).is_some());
        assert!(store.get(6).is_none());
        assert!(store.get(7).is_some());
        assert!(store.get(8).is_none());
        assert!(store.get(9).is_some());
        assert!(store.get(10).is_some());
    }

    #[test]
    fn test_delete_batch_with_index() {
        let mut store = RowStore::new(test_schema_with_index());

        // Insert rows with indexed values
        for i in 1..=5 {
            let row = Row::new(
                i,
                vec![Value::Int64(i as i64), Value::Int64((i * 100) as i64)],
            );
            store.insert(row).unwrap();
        }

        // Delete rows 2 and 4
        store.delete_batch(&[2, 4]);

        // Verify index is updated correctly
        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(200))));
        assert_eq!(results.len(), 0); // Row 2 was deleted

        let results = store.index_scan("idx_value", Some(&KeyRange::only(Value::Int64(300))));
        assert_eq!(results.len(), 1); // Row 3 still exists
    }

    #[test]
    fn test_delete_batch_empty() {
        let mut store = RowStore::new(test_schema());

        for i in 1..=5 {
            let row = Row::new(
                i,
                vec![Value::Int64(i as i64), Value::String(format!("Name{}", i))],
            );
            store.insert(row).unwrap();
        }

        // Delete empty batch should be no-op
        let deleted = store.delete_batch(&[]);
        assert_eq!(deleted.len(), 0);
        assert_eq!(store.len(), 5);
    }

    #[test]
    fn test_delete_batch_nonexistent() {
        let mut store = RowStore::new(test_schema());

        for i in 1..=5 {
            let row = Row::new(
                i,
                vec![Value::Int64(i as i64), Value::String(format!("Name{}", i))],
            );
            store.insert(row).unwrap();
        }

        // Try to delete nonexistent rows
        let deleted = store.delete_batch(&[100, 200, 300]);
        assert_eq!(deleted.len(), 0);
        assert_eq!(store.len(), 5);
    }

    #[test]
    fn test_delete_batch_all() {
        let mut store = RowStore::new(test_schema());

        for i in 1..=10 {
            let row = Row::new(
                i,
                vec![Value::Int64(i as i64), Value::String(format!("Name{}", i))],
            );
            store.insert(row).unwrap();
        }

        // Delete all rows
        let row_ids: Vec<_> = (1..=10).collect();
        let deleted = store.delete_batch(&row_ids);
        assert_eq!(deleted.len(), 10);
        assert!(store.is_empty());
    }

    #[test]
    fn test_delete_batch_pk_freed() {
        let mut store = RowStore::new(test_schema());

        // Insert row with PK=100
        let row = Row::new(1, vec![Value::Int64(100), Value::String("Alice".into())]);
        store.insert(row).unwrap();

        // Delete it
        store.delete_batch(&[1]);

        // Should be able to insert another row with same PK
        let row2 = Row::new(2, vec![Value::Int64(100), Value::String("Bob".into())]);
        assert!(store.insert(row2).is_ok());
    }

    // ==================== GIN Index Bug Tests ====================
    // These tests verify Bug 1: GIN index not updated in update/delete operations

    fn test_schema_with_gin_index() -> Table {
        TableBuilder::new("test_jsonb")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("data", DataType::Jsonb)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_data_gin", &["data"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    fn test_schema_with_path_gin_index() -> Table {
        TableBuilder::new("test_jsonb")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("data", DataType::Jsonb)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_jsonb_index("idx_data_gin", "data", &["address.city", "status"])
            .unwrap()
            .build()
            .unwrap()
    }

    fn make_jsonb(json_str: &str) -> Value {
        Value::Jsonb(cynos_core::JsonbValue(json_str.as_bytes().to_vec()))
    }

    fn test_contains_trigram_key(path: &str) -> String {
        alloc::format!("__cynos_contains3__:{path}")
    }

    fn test_contains_trigrams(value: &str) -> Vec<String> {
        let chars: Vec<char> = value.chars().collect();
        if chars.len() < 3 {
            return Vec::new();
        }

        let mut grams = BTreeSet::new();
        for window in chars.windows(3) {
            let gram: String = window.iter().collect();
            grams.insert(gram);
        }

        grams.into_iter().collect()
    }

    #[test]
    fn test_gin_index_insert_and_query() {
        let mut store = RowStore::new(test_schema_with_gin_index());

        // Insert a row with JSONB data
        let row = Row::new(
            1,
            vec![
                Value::Int64(1),
                make_jsonb(r#"{"name": "Alice", "status": "active"}"#),
            ],
        );
        store.insert(row).unwrap();

        // Query by key should find the row
        let results = store.gin_index_get_by_key("idx_data_gin", "name");
        assert_eq!(results.len(), 1, "GIN index should find row by key 'name'");

        // Query by key-value should find the row
        let results = store.gin_index_get_by_key_value("idx_data_gin", "status", "active");
        assert_eq!(
            results.len(),
            1,
            "GIN index should find row by key-value 'status=active'"
        );
    }

    #[test]
    fn test_gin_index_bulk_load_and_query() {
        let mut store = RowStore::new(test_schema_with_gin_index());
        let rows = vec![
            Row::new(
                1,
                vec![
                    Value::Int64(1),
                    make_jsonb(r#"{"name":"Alice","status":"active"}"#),
                ],
            ),
            Row::new(
                2,
                vec![
                    Value::Int64(2),
                    make_jsonb(r#"{"name":"Bob","status":"inactive"}"#),
                ],
            ),
        ];

        store.bulk_load(rows).unwrap();

        assert_eq!(store.gin_index_get_by_key("idx_data_gin", "name").len(), 2);
        assert_eq!(
            store
                .gin_index_get_by_key_value("idx_data_gin", "status", "active")
                .iter()
                .map(|row| row.id())
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn test_gin_index_insert_batch_updates_non_empty_store() {
        let mut store = RowStore::new(test_schema_with_gin_index());
        store
            .insert(Row::new(
                1,
                vec![
                    Value::Int64(1),
                    make_jsonb(r#"{"name":"Alice","status":"active"}"#),
                ],
            ))
            .unwrap();

        store
            .insert_batch(vec![
                Row::new(
                    2,
                    vec![
                        Value::Int64(2),
                        make_jsonb(r#"{"name":"Bob","status":"inactive"}"#),
                    ],
                ),
                Row::new(
                    3,
                    vec![
                        Value::Int64(3),
                        make_jsonb(r#"{"name":"Cara","status":"active"}"#),
                    ],
                ),
            ])
            .unwrap();

        assert_eq!(store.gin_index_get_by_key("idx_data_gin", "name").len(), 3);
        assert_eq!(
            store
                .gin_index_get_by_key_value("idx_data_gin", "status", "active")
                .iter()
                .map(|row| row.id())
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[test]
    fn test_insert_batch_preserves_prefix_rows_and_gin_on_unique_violation() {
        let schema = TableBuilder::new("test_batch_insert_unique_gin")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("email", DataType::String)
            .unwrap()
            .add_column("data", DataType::Jsonb)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_email", &["email"], true)
            .unwrap()
            .add_index("idx_data_gin", &["data"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut store = RowStore::new(schema);
        let result = store.insert_batch(vec![
            Row::new(
                1,
                vec![
                    Value::Int64(1),
                    Value::String("alice@test.com".into()),
                    make_jsonb(r#"{"role":"admin"}"#),
                ],
            ),
            Row::new(
                2,
                vec![
                    Value::Int64(2),
                    Value::String("alice@test.com".into()),
                    make_jsonb(r#"{"role":"user"}"#),
                ],
            ),
        ]);

        assert!(result.is_err());
        assert_eq!(store.len(), 1);
        assert!(store.get(1).is_some());
        assert!(store.get(2).is_none());
        assert_eq!(
            store
                .gin_index_get_by_key_value("idx_data_gin", "role", "admin")
                .iter()
                .map(|row| row.id())
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn test_gin_index_visit_stops_early() {
        let mut store = RowStore::new(test_schema_with_gin_index());
        for row_id in 1..=5 {
            store
                .insert(Row::new(
                    row_id,
                    vec![
                        Value::Int64(row_id as i64),
                        make_jsonb(r#"{"status":"active"}"#),
                    ],
                ))
                .unwrap();
        }

        let mut visited = Vec::new();
        store.visit_gin_index_by_key_value("idx_data_gin", "status", "active", |row| {
            visited.push(row.id());
            visited.len() < 2
        });

        assert_eq!(visited, vec![1, 2]);
    }

    #[test]
    fn test_gin_index_query_any_key_values() {
        let mut store = RowStore::new(test_schema_with_gin_index());
        store
            .insert(Row::new(
                1,
                vec![
                    Value::Int64(1),
                    make_jsonb(r#"{"status":"active","type":"user"}"#),
                ],
            ))
            .unwrap();
        store
            .insert(Row::new(
                2,
                vec![
                    Value::Int64(2),
                    make_jsonb(r#"{"status":"active","type":"admin"}"#),
                ],
            ))
            .unwrap();
        store
            .insert(Row::new(
                3,
                vec![
                    Value::Int64(3),
                    make_jsonb(r#"{"status":"inactive","type":"user"}"#),
                ],
            ))
            .unwrap();

        let any_results =
            store.gin_index_get_by_key_values_any("idx_data_gin", &[("status", "active")]);
        assert_eq!(
            any_results.iter().map(|row| row.id()).collect::<Vec<_>>(),
            vec![1, 2]
        );

        let any_results = store.gin_index_get_by_key_values_any(
            "idx_data_gin",
            &[("status", "active"), ("type", "user")],
        );
        assert_eq!(
            any_results.iter().map(|row| row.id()).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn test_gin_index_nested_paths_and_scalars() {
        let mut store = RowStore::new(test_schema_with_gin_index());

        store
            .insert(Row::new(
                1,
                vec![
                    Value::Int64(1),
                    make_jsonb(
                        r#"{"name":"Alice","address":{"city":"Beijing","zip":"100000"},"tags":["vip","premium"]}"#,
                    ),
                ],
            ))
            .unwrap();

        let nested_key_rows = store.gin_index_get_by_key("idx_data_gin", "address.city");
        assert_eq!(
            nested_key_rows.len(),
            1,
            "GIN index should expose nested key postings",
        );

        let nested_value_rows =
            store.gin_index_get_by_key_value("idx_data_gin", "address.city", "Beijing");
        assert_eq!(
            nested_value_rows.len(),
            1,
            "GIN index should expose nested key/value postings",
        );

        let array_value_rows =
            store.gin_index_get_by_key_value("idx_data_gin", "tags.1", "premium");
        assert_eq!(
            array_value_rows.len(),
            1,
            "GIN index should expose indexed array element postings",
        );
    }

    #[test]
    fn test_gin_index_respects_configured_paths() {
        let mut store = RowStore::new(test_schema_with_path_gin_index());

        store
            .insert(Row::new(
                1,
                vec![
                    Value::Int64(1),
                    make_jsonb(
                        r#"{"name":"Alice","status":"active","address":{"city":"Beijing","zip":"100000"},"tags":["vip","premium"]}"#,
                    ),
                ],
            ))
            .unwrap();

        assert_eq!(
            store
                .gin_index_get_by_key_value("idx_data_gin", "address.city", "Beijing")
                .len(),
            1,
            "configured nested path should be indexed",
        );
        assert_eq!(
            store
                .gin_index_get_by_key_value("idx_data_gin", "status", "active")
                .len(),
            1,
            "configured top-level path should be indexed",
        );
        assert_eq!(
            store.gin_index_get_by_key("idx_data_gin", "name").len(),
            0,
            "unconfigured sibling path should not be indexed",
        );
        assert_eq!(
            store
                .gin_index_get_by_key_value("idx_data_gin", "tags.1", "premium")
                .len(),
            0,
            "unconfigured array path should not be indexed",
        );
    }

    #[test]
    fn test_gin_contains_prefilter_indexes_array_value_trigrams() {
        let mut store = RowStore::new(test_schema_with_gin_index());

        store
            .insert(Row::new(
                1,
                vec![
                    Value::Int64(1),
                    make_jsonb(r#"{"tags":["portable","travel"],"status":"active"}"#),
                ],
            ))
            .unwrap();

        let contains_key = test_contains_trigram_key("tags");

        for gram in test_contains_trigrams("portable") {
            let raw_ids =
                store.gin_index_get_raw_row_ids_by_kv("idx_data_gin", &contains_key, &gram);
            assert_eq!(
                raw_ids,
                vec![1],
                "GIN contains prefilter should index trigram {gram:?} for $.tags",
            );
        }
    }

    #[test]
    fn test_gin_contains_prefilter_updates_trigram_postings() {
        let mut store = RowStore::new(test_schema_with_gin_index());

        store
            .insert(Row::new(
                1,
                vec![
                    Value::Int64(1),
                    make_jsonb(r#"{"tags":["portable","travel"],"status":"active"}"#),
                ],
            ))
            .unwrap();

        let contains_key = test_contains_trigram_key("tags");
        let old_gram = "ort";
        let new_gram = "esk";

        assert_eq!(
            store.gin_index_get_raw_row_ids_by_kv("idx_data_gin", &contains_key, old_gram),
            vec![1],
            "Before update: old trigram posting should exist",
        );

        store
            .update(
                1,
                Row::new(
                    1,
                    vec![
                        Value::Int64(1),
                        make_jsonb(r#"{"tags":["desktop","office"],"status":"active"}"#),
                    ],
                ),
            )
            .unwrap();

        assert!(
            store
                .gin_index_get_raw_row_ids_by_kv("idx_data_gin", &contains_key, old_gram)
                .is_empty(),
            "After update: old trigram posting should be removed",
        );
        assert_eq!(
            store.gin_index_get_raw_row_ids_by_kv("idx_data_gin", &contains_key, new_gram),
            vec![1],
            "After update: new trigram posting should be added",
        );
    }

    #[test]
    fn test_gin_index_delete_bug() {
        // This test demonstrates Bug 1: GIN index not updated on delete
        let mut store = RowStore::new(test_schema_with_gin_index());

        // Insert a row with JSONB data
        let row = Row::new(
            1,
            vec![
                Value::Int64(1),
                make_jsonb(r#"{"name": "Alice", "status": "active"}"#),
            ],
        );
        store.insert(row).unwrap();

        // Verify GIN index has the entry (using raw row IDs to detect ghost entries)
        let raw_ids = store.gin_index_get_raw_row_ids("idx_data_gin", "name");
        assert_eq!(
            raw_ids.len(),
            1,
            "Before delete: GIN index should have entry"
        );

        // Delete the row
        store.delete(1).unwrap();

        // BUG: GIN index still has the ghost entry
        // The public API filters out deleted rows, but the index itself still has the entry
        let raw_ids = store.gin_index_get_raw_row_ids("idx_data_gin", "name");
        // This assertion will FAIL before the fix, demonstrating the bug
        assert_eq!(
            raw_ids.len(),
            0,
            "After delete: GIN index should NOT have ghost entries"
        );
    }

    #[test]
    fn test_gin_index_update_bug() {
        // This test demonstrates Bug 1: GIN index not updated on update
        let mut store = RowStore::new(test_schema_with_gin_index());

        // Insert a row with JSONB data
        let row = Row::new(
            1,
            vec![
                Value::Int64(1),
                make_jsonb(r#"{"name": "Alice", "status": "active"}"#),
            ],
        );
        store.insert(row).unwrap();

        // Verify initial state (using raw row IDs)
        let raw_ids = store.gin_index_get_raw_row_ids_by_kv("idx_data_gin", "status", "active");
        assert_eq!(
            raw_ids.len(),
            1,
            "Before update: should find 'status=active'"
        );

        // Update the row with different JSONB data
        let new_row = Row::new(
            1,
            vec![
                Value::Int64(1),
                make_jsonb(r#"{"name": "Alice", "status": "inactive"}"#),
            ],
        );
        store.update(1, new_row).unwrap();

        // BUG: Old value still in GIN index (ghost entry)
        let raw_ids = store.gin_index_get_raw_row_ids_by_kv("idx_data_gin", "status", "active");
        assert_eq!(
            raw_ids.len(),
            0,
            "After update: old value 'status=active' should NOT be in GIN index"
        );

        // BUG: New value not in GIN index
        let raw_ids = store.gin_index_get_raw_row_ids_by_kv("idx_data_gin", "status", "inactive");
        assert_eq!(
            raw_ids.len(),
            1,
            "After update: new value 'status=inactive' should be in GIN index"
        );
    }

    #[test]
    fn test_gin_index_delete_batch_bug() {
        // This test demonstrates Bug 1: GIN index not updated on delete_batch
        let mut store = RowStore::new(test_schema_with_gin_index());

        // Insert multiple rows
        for i in 1..=3 {
            let row = Row::new(
                i,
                vec![
                    Value::Int64(i as i64),
                    make_jsonb(&format!(r#"{{"user": "user{}"}}"#, i)),
                ],
            );
            store.insert(row).unwrap();
        }

        // Verify all entries exist (using raw row IDs)
        let raw_ids = store.gin_index_get_raw_row_ids("idx_data_gin", "user");
        assert_eq!(
            raw_ids.len(),
            3,
            "Before delete_batch: should have 3 entries"
        );

        // Delete rows 1 and 2
        store.delete_batch(&[1, 2]);

        // BUG: GIN index still has ghost entries
        let raw_ids = store.gin_index_get_raw_row_ids("idx_data_gin", "user");
        assert_eq!(
            raw_ids.len(),
            1,
            "After delete_batch: should only have 1 entry (row 3)"
        );
    }

    #[test]
    fn test_gin_index_rollback_insert_bug() {
        // This test demonstrates Bug 1: GIN index not cleaned up on rollback_insert
        // We need a schema with both a unique secondary index and a GIN index
        let schema = TableBuilder::new("test_rollback")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("email", DataType::String)
            .unwrap()
            .add_column("data", DataType::Jsonb)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_email", &["email"], true) // unique index
            .unwrap()
            .add_index("idx_data_gin", &["data"], false) // GIN index
            .unwrap()
            .build()
            .unwrap();

        let mut store = RowStore::new(schema);

        // Insert first row
        let row1 = Row::new(
            1,
            vec![
                Value::Int64(1),
                Value::String("alice@test.com".into()),
                make_jsonb(r#"{"role": "admin"}"#),
            ],
        );
        store.insert(row1).unwrap();

        // Verify initial state
        let raw_ids = store.gin_index_get_raw_row_ids("idx_data_gin", "role");
        assert_eq!(raw_ids.len(), 1, "After first insert: should have 1 entry");

        // Try to insert second row with same email (will fail due to unique constraint)
        let row2 = Row::new(
            2,
            vec![
                Value::Int64(2),
                Value::String("alice@test.com".into()), // duplicate email
                make_jsonb(r#"{"role": "user"}"#),
            ],
        );
        let result = store.insert(row2);
        assert!(
            result.is_err(),
            "Insert should fail due to unique constraint"
        );

        // BUG: The GIN index may have been partially updated before rollback
        // After rollback, only row 1's data should be in the GIN index
        let raw_ids = store.gin_index_get_raw_row_ids("idx_data_gin", "role");
        assert_eq!(
            raw_ids.len(),
            1,
            "After failed insert: GIN index should only have row 1's entry"
        );
    }

    #[test]
    fn test_clear_clears_gin_index_bug() {
        let mut store = RowStore::new(test_schema_with_gin_index());

        store
            .insert(Row::new(
                1,
                vec![
                    Value::Int64(1),
                    make_jsonb(r#"{"name": "Alice", "status": "active"}"#),
                ],
            ))
            .unwrap();
        store
            .insert(Row::new(
                2,
                vec![
                    Value::Int64(2),
                    make_jsonb(r#"{"name": "Bob", "status": "inactive"}"#),
                ],
            ))
            .unwrap();

        assert_eq!(
            store
                .gin_index_get_raw_row_ids("idx_data_gin", "status")
                .len(),
            2,
            "Before clear: GIN index should contain both rows",
        );

        store.clear();

        assert!(
            store.is_empty(),
            "After clear: row store should not keep any rows",
        );
        assert_eq!(
            store
                .gin_index_get_raw_row_ids("idx_data_gin", "status")
                .len(),
            0,
            "After clear: GIN index should not retain stale postings",
        );
    }

    // ==================== Defect 1 Test: Composite PK serialization collision ====================
    // This test demonstrates Defect 1: Composite PK key collision when values contain separator

    fn test_schema_composite_pk_string_string() -> Table {
        TableBuilder::new("test")
            .unwrap()
            .add_column("id1", DataType::String)
            .unwrap()
            .add_column("id2", DataType::String)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id1", "id2"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    fn test_schema_composite_pk_int_int() -> Table {
        TableBuilder::new("test")
            .unwrap()
            .add_column("id1", DataType::Int32)
            .unwrap()
            .add_column("id2", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id1", "id2"], false)
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_composite_pk_separator_collision_defect() {
        // Test that different composite keys don't collide
        let mut store = RowStore::new(test_schema_composite_pk_string_string());

        let row1 = Row::new(
            1,
            vec![
                Value::String("a\")|String(\"b".into()),
                Value::String("c".into()),
                Value::String("Name1".into()),
            ],
        );
        assert!(store.insert(row1).is_ok(), "First insert should succeed");

        let row2 = Row::new(
            2,
            vec![
                Value::String("a".into()),
                Value::String("b\")|String(\"c".into()),
                Value::String("Name2".into()),
            ],
        );
        assert!(
            store.insert(row2).is_ok(),
            "Defect 1: Different composite keys should NOT collide"
        );
    }

    #[test]
    fn test_composite_pk_type_confusion_defect() {
        // Test that Int32 and Int64 with same numeric value don't collide
        // Int32(42) and Int64(42) should produce different keys
        let mut store = RowStore::new(test_schema_composite_pk_int_int());

        // Row with (Int32(42), Int64(100))
        let row1 = Row::new(
            1,
            vec![
                Value::Int32(42),
                Value::Int64(100),
                Value::String("Name1".into()),
            ],
        );
        assert!(store.insert(row1).is_ok(), "First insert should succeed");

        // Row with (Int32(42), Int64(100)) - same composite key, should fail
        let row2 = Row::new(
            2,
            vec![
                Value::Int32(42),
                Value::Int64(100),
                Value::String("Name2".into()),
            ],
        );
        assert!(
            store.insert(row2).is_err(),
            "Same composite key should fail"
        );

        // Row with (Int32(100), Int64(42)) - different composite key, should succeed
        let row3 = Row::new(
            3,
            vec![
                Value::Int32(100),
                Value::Int64(42),
                Value::String("Name3".into()),
            ],
        );
        assert!(
            store.insert(row3).is_ok(),
            "Different composite key should succeed"
        );
    }
}
