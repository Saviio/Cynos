//! Journal for tracking changes in Cynos database.
//!
//! This module provides the `Journal` struct for recording and managing
//! database changes within a transaction.

use crate::cache::TableCache;
use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::{Error, Result, Row, RowId};

/// A single journal entry representing a change.
#[derive(Clone, Debug)]
pub enum JournalEntry {
    /// A row was inserted.
    Insert {
        table: String,
        row_id: RowId,
        row: Row,
    },
    /// A row was updated.
    Update {
        table: String,
        row_id: RowId,
        old: Row,
        new: Row,
    },
    /// A row was deleted.
    Delete {
        table: String,
        row_id: RowId,
        row: Row,
    },
}

impl JournalEntry {
    /// Returns the table name for this entry.
    pub fn table(&self) -> &str {
        match self {
            JournalEntry::Insert { table, .. } => table,
            JournalEntry::Update { table, .. } => table,
            JournalEntry::Delete { table, .. } => table,
        }
    }

    /// Returns the row ID for this entry.
    pub fn row_id(&self) -> RowId {
        match self {
            JournalEntry::Insert { row_id, .. } => *row_id,
            JournalEntry::Update { row_id, .. } => *row_id,
            JournalEntry::Delete { row_id, .. } => *row_id,
        }
    }
}

/// Table diff tracking changes for a single table.
#[derive(Clone, Debug, Default)]
pub struct TableDiff {
    /// Table name.
    table_name: String,
    /// Added rows (row_id → row).
    added: BTreeMap<RowId, Row>,
    /// Modified rows (row_id → (old, new)).
    modified: BTreeMap<RowId, (Row, Row)>,
    /// Deleted rows (row_id → row).
    deleted: BTreeMap<RowId, Row>,
}

impl TableDiff {
    /// Creates a new table diff.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            added: BTreeMap::new(),
            modified: BTreeMap::new(),
            deleted: BTreeMap::new(),
        }
    }

    /// Returns the table name.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Records an addition.
    pub fn add(&mut self, row: Row) {
        let row_id = row.id();
        // If this row was previously deleted, convert to modify
        if let Some(old_row) = self.deleted.remove(&row_id) {
            self.modified.insert(row_id, (old_row, row));
        } else {
            self.added.insert(row_id, row);
        }
    }

    /// Records a modification.
    pub fn modify(&mut self, old: Row, new: Row) {
        let row_id = old.id();
        // If this row was added in this diff, keep it as add with new value
        if self.added.contains_key(&row_id) {
            self.added.insert(row_id, new);
        } else if let Some((original_old, _)) = self.modified.get(&row_id) {
            // Keep original old value
            let original = original_old.clone();
            self.modified.insert(row_id, (original, new));
        } else {
            self.modified.insert(row_id, (old, new));
        }
    }

    /// Records a deletion.
    pub fn delete(&mut self, row: Row) {
        let row_id = row.id();
        // If this row was added in this diff, just remove from added
        if self.added.remove(&row_id).is_some() {
            return;
        }
        // If this row was modified, use the original old value
        if let Some((old_row, _)) = self.modified.remove(&row_id) {
            self.deleted.insert(row_id, old_row);
        } else {
            self.deleted.insert(row_id, row);
        }
    }

    /// Returns added rows.
    pub fn get_added(&self) -> &BTreeMap<RowId, Row> {
        &self.added
    }

    /// Returns modified rows.
    pub fn get_modified(&self) -> &BTreeMap<RowId, (Row, Row)> {
        &self.modified
    }

    /// Returns deleted rows.
    pub fn get_deleted(&self) -> &BTreeMap<RowId, Row> {
        &self.deleted
    }

    /// Returns true if there are no changes.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.modified.is_empty() && self.deleted.is_empty()
    }

    /// Returns the reverse of this diff (for rollback).
    pub fn get_reverse(&self) -> Self {
        let mut reverse = Self::new(&self.table_name);

        // Added becomes deleted
        for (row_id, row) in &self.added {
            reverse.deleted.insert(*row_id, row.clone());
        }

        // Modified is reversed
        for (row_id, (old, new)) in &self.modified {
            reverse.modified.insert(*row_id, (new.clone(), old.clone()));
        }

        // Deleted becomes added
        for (row_id, row) in &self.deleted {
            reverse.added.insert(*row_id, row.clone());
        }

        reverse
    }

    /// Converts to a list of modifications (for IVM).
    pub fn get_as_modifications(&self) -> Vec<(Option<Row>, Option<Row>)> {
        let mut mods = Vec::new();

        for row in self.added.values() {
            mods.push((None, Some(row.clone())));
        }

        for (old, new) in self.modified.values() {
            mods.push((Some(old.clone()), Some(new.clone())));
        }

        for row in self.deleted.values() {
            mods.push((Some(row.clone()), None));
        }

        mods
    }
}

/// Journal for tracking changes within a transaction.
pub struct Journal {
    /// Table diffs (table name → diff).
    table_diffs: BTreeMap<String, TableDiff>,
    /// Ordered list of entries for replay.
    entries: Vec<JournalEntry>,
}

impl Journal {
    /// Creates a new empty journal.
    pub fn new() -> Self {
        Self {
            table_diffs: BTreeMap::new(),
            entries: Vec::new(),
        }
    }

    /// Records an insert operation.
    pub fn record_insert(&mut self, table: &str, row: Row) {
        let row_id = row.id();

        self.get_or_create_diff(table).add(row.clone());

        self.entries.push(JournalEntry::Insert {
            table: table.into(),
            row_id,
            row,
        });
    }

    /// Records an update operation.
    pub fn record_update(&mut self, table: &str, old: Row, new: Row) {
        let row_id = old.id();

        self.get_or_create_diff(table)
            .modify(old.clone(), new.clone());

        self.entries.push(JournalEntry::Update {
            table: table.into(),
            row_id,
            old,
            new,
        });
    }

    /// Records a delete operation.
    pub fn record_delete(&mut self, table: &str, row: Row) {
        let row_id = row.id();

        self.get_or_create_diff(table).delete(row.clone());

        self.entries.push(JournalEntry::Delete {
            table: table.into(),
            row_id,
            row,
        });
    }

    /// Gets or creates a table diff.
    fn get_or_create_diff(&mut self, table: &str) -> &mut TableDiff {
        if !self.table_diffs.contains_key(table) {
            self.table_diffs.insert(table.into(), TableDiff::new(table));
        }
        self.table_diffs.get_mut(table).unwrap()
    }

    /// Returns all journal entries.
    pub fn get_entries(&self) -> &[JournalEntry] {
        &self.entries
    }

    /// Returns the table diff for a table.
    pub fn get_table_diff(&self, table: &str) -> Option<&TableDiff> {
        self.table_diffs.get(table)
    }

    /// Returns all table diffs.
    pub fn get_all_diffs(&self) -> &BTreeMap<String, TableDiff> {
        &self.table_diffs
    }

    /// Returns true if the journal is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Commits the journal changes to the cache.
    /// Note: Changes are already applied during record_* calls.
    /// This method is for finalizing the transaction.
    pub fn commit(&mut self) -> Vec<JournalEntry> {
        let entries = core::mem::take(&mut self.entries);
        self.table_diffs.clear();
        entries
    }

    /// Rolls back the journal changes.
    pub fn rollback(&mut self, cache: &mut TableCache) -> Result<()> {
        // Apply changes in reverse order
        for entry in self.entries.iter().rev() {
            match entry {
                JournalEntry::Insert { table, row_id, .. } => {
                    if let Some(store) = cache.get_table_mut(table) {
                        let _ = store.delete(*row_id);
                    }
                }
                JournalEntry::Update {
                    table,
                    row_id,
                    old,
                    new,
                } => {
                    if let Some(store) = cache.get_table_mut(table) {
                        // Restore old values but keep version incrementing to maintain monotonicity
                        let rollback_version = new.version().checked_add(1).ok_or_else(|| {
                            Error::invalid_operation(
                                "Cannot rollback update: row version would overflow",
                            )
                        })?;
                        let rollback_row = Row::new_with_version(
                            old.id(),
                            rollback_version,
                            old.values().to_vec(),
                        );
                        let _ = store.update(*row_id, rollback_row);
                    }
                }
                JournalEntry::Delete { table, row, .. } => {
                    if let Some(store) = cache.get_table_mut(table) {
                        let _ = store.insert(row.clone());
                    }
                }
            }
        }

        self.entries.clear();
        self.table_diffs.clear();
        Ok(())
    }

    /// Clears the journal without applying changes.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.table_diffs.clear();
    }
}

impl Default for Journal {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, Error, Value};

    fn test_schema() -> cynos_core::schema::Table {
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

    #[test]
    fn test_journal_insert() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        let mut journal = Journal::new();
        let row = Row::new(1, vec![Value::Int64(1), Value::String("test".into())]);

        // Insert into cache first
        cache
            .get_table_mut("test")
            .unwrap()
            .insert(row.clone())
            .unwrap();
        journal.record_insert("test", row);

        assert_eq!(journal.get_entries().len(), 1);
        assert_eq!(cache.get_table("test").unwrap().len(), 1);
    }

    #[test]
    fn test_journal_rollback() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        // Insert initial row
        let row1 = Row::new(1, vec![Value::Int64(1), Value::String("initial".into())]);
        cache.get_table_mut("test").unwrap().insert(row1).unwrap();
        assert_eq!(cache.get_table("test").unwrap().len(), 1);

        // Start journal and insert second row
        let mut journal = Journal::new();
        let row2 = Row::new(2, vec![Value::Int64(2), Value::String("second".into())]);
        cache
            .get_table_mut("test")
            .unwrap()
            .insert(row2.clone())
            .unwrap();
        journal.record_insert("test", row2);

        assert_eq!(cache.get_table("test").unwrap().len(), 2);

        // Rollback
        journal.rollback(&mut cache).unwrap();

        // Should only have the first row
        assert_eq!(cache.get_table("test").unwrap().len(), 1);
    }

    #[test]
    fn test_journal_rollback_update_rejects_version_overflow() {
        let mut cache = TableCache::new();
        cache.create_table(test_schema()).unwrap();

        let old_row = Row::new_with_version(
            1,
            u64::MAX - 1,
            vec![Value::Int64(1), Value::String("old".into())],
        );
        let new_row = Row::new_with_version(
            1,
            u64::MAX,
            vec![Value::Int64(1), Value::String("new".into())],
        );

        cache
            .get_table_mut("test")
            .unwrap()
            .insert(new_row.clone())
            .unwrap();

        let mut journal = Journal::new();
        journal.record_update("test", old_row, new_row);

        let error = journal.rollback(&mut cache).unwrap_err();
        assert!(matches!(error, Error::InvalidOperation { .. }));

        let stored = cache.get_table("test").unwrap().get(1).unwrap();
        assert_eq!(stored.version(), u64::MAX);
        assert_eq!(stored.get(1), Some(&Value::String("new".into())));
    }

    #[test]
    fn test_table_diff_add_delete() {
        let mut diff = TableDiff::new("test");

        let row = Row::new(1, vec![Value::Int64(1)]);
        diff.add(row.clone());
        assert_eq!(diff.get_added().len(), 1);

        diff.delete(row);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_table_diff_modify() {
        let mut diff = TableDiff::new("test");

        let old = Row::new(1, vec![Value::Int64(1)]);
        let new = Row::new(1, vec![Value::Int64(2)]);
        diff.modify(old, new);

        assert_eq!(diff.get_modified().len(), 1);
    }

    #[test]
    fn test_table_diff_reverse() {
        let mut diff = TableDiff::new("test");

        let row = Row::new(1, vec![Value::Int64(1)]);
        diff.add(row);

        let reverse = diff.get_reverse();
        assert_eq!(reverse.get_deleted().len(), 1);
        assert!(reverse.get_added().is_empty());
    }

    #[test]
    fn test_table_diff_get_as_modifications() {
        let mut diff = TableDiff::new("test");

        let row1 = Row::new(1, vec![Value::Int64(1)]);
        let row2_old = Row::new(2, vec![Value::Int64(2)]);
        let row2_new = Row::new(2, vec![Value::Int64(20)]);
        let row3 = Row::new(3, vec![Value::Int64(3)]);

        diff.add(row1);
        diff.modify(row2_old, row2_new);
        diff.delete(row3);

        let mods = diff.get_as_modifications();
        assert_eq!(mods.len(), 3);
    }
}
