//! Relation and RelationEntry types for query execution.

use alloc::rc::Rc;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use cynos_core::{join_row_id, left_join_null_row_id, right_join_null_row_id, Row, RowId, Value};

/// Shared table names to avoid repeated cloning during joins.
pub type SharedTables = Arc<[String]>;

/// Internal storage for table names - either owned or shared.
#[derive(Clone, Debug)]
enum TablesStorage {
    /// Owned vector (for single-table entries).
    Owned(Vec<String>),
    /// Shared via Arc (for join results).
    Shared(SharedTables),
}

impl TablesStorage {
    #[inline]
    fn as_slice(&self) -> &[String] {
        match self {
            TablesStorage::Owned(v) => v,
            TablesStorage::Shared(arc) => arc,
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn to_vec(&self) -> Vec<String> {
        match self {
            TablesStorage::Owned(v) => v.clone(),
            TablesStorage::Shared(arc) => arc.to_vec(),
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn len(&self) -> usize {
        match self {
            TablesStorage::Owned(v) => v.len(),
            TablesStorage::Shared(arc) => arc.len(),
        }
    }
}

/// A relation entry wraps a row with table context.
#[derive(Clone, Debug)]
pub struct RelationEntry {
    /// The underlying row (reference counted for efficient sharing).
    pub row: Rc<Row>,
    /// Whether this entry is from a joined relation.
    pub is_combined: bool,
    /// Table names this entry belongs to.
    tables: TablesStorage,
}

impl RelationEntry {
    /// Creates a relation entry with shared tables (avoids cloning for each row).
    #[inline]
    pub fn new_shared(row: Rc<Row>, shared_tables: SharedTables) -> Self {
        Self {
            row,
            is_combined: shared_tables.len() > 1,
            tables: TablesStorage::Shared(shared_tables),
        }
    }

    /// Creates a new relation entry.
    pub fn new(row: Rc<Row>, tables: Vec<String>) -> Self {
        Self {
            row,
            is_combined: tables.len() > 1,
            tables: TablesStorage::Owned(tables),
        }
    }

    /// Creates a new relation entry from an owned Row.
    pub fn new_owned(row: Row, tables: Vec<String>) -> Self {
        Self {
            row: Rc::new(row),
            is_combined: tables.len() > 1,
            tables: TablesStorage::Owned(tables),
        }
    }

    /// Creates a combined relation entry with shared tables (for join results).
    /// This avoids cloning the tables vector for each result row.
    #[inline]
    pub fn new_combined(row: Rc<Row>, shared_tables: SharedTables) -> Self {
        Self {
            row,
            is_combined: true,
            tables: TablesStorage::Shared(shared_tables),
        }
    }

    /// Creates a relation entry from a single table.
    pub fn from_row(row: Rc<Row>, table: impl Into<String>) -> Self {
        Self {
            row,
            is_combined: false,
            tables: TablesStorage::Owned(alloc::vec![table.into()]),
        }
    }

    /// Creates a relation entry from an owned Row and a single table.
    pub fn from_row_owned(row: Row, table: impl Into<String>) -> Self {
        Self {
            row: Rc::new(row),
            is_combined: false,
            tables: TablesStorage::Owned(alloc::vec![table.into()]),
        }
    }

    /// Returns the row ID.
    pub fn id(&self) -> RowId {
        self.row.id()
    }

    /// Returns the tables this entry belongs to.
    pub fn tables(&self) -> &[String] {
        self.tables.as_slice()
    }

    /// Gets a field value by column index.
    pub fn get_field(&self, index: usize) -> Option<&Value> {
        self.row.get(index)
    }

    /// Combines two entries into a joined entry.
    /// The combined row's version is the sum of both source row versions.
    pub fn combine(
        left: &RelationEntry,
        left_tables: &[String],
        right: &RelationEntry,
        right_tables: &[String],
    ) -> Self {
        let left_values = left.row.values();
        let right_values = right.row.values();
        let total_len = left_values.len() + right_values.len();

        let mut values = Vec::with_capacity(total_len);
        values.extend(left_values.iter().cloned());
        values.extend(right_values.iter().cloned());

        let mut tables = Vec::with_capacity(left_tables.len() + right_tables.len());
        tables.extend(left_tables.iter().cloned());
        tables.extend(right_tables.iter().cloned());

        // Sum version for JOIN result
        let combined_version = left.row.version().wrapping_add(right.row.version());

        Self {
            row: Rc::new(Row::new_with_version(
                join_row_id(left.row.id(), right.row.id()),
                combined_version,
                values,
            )),
            is_combined: true,
            tables: TablesStorage::Owned(tables),
        }
    }

    /// Creates a combined entry with null values for the right side (for outer joins).
    /// The combined row's version is the left row's version (right side is NULL).
    pub fn combine_with_null(
        left: &RelationEntry,
        left_tables: &[String],
        right_column_count: usize,
        right_tables: &[String],
    ) -> Self {
        let left_values = left.row.values();
        let total_len = left_values.len() + right_column_count;

        let mut values = Vec::with_capacity(total_len);
        values.extend(left_values.iter().cloned());
        values.resize(total_len, Value::Null);

        let mut tables = Vec::with_capacity(left_tables.len() + right_tables.len());
        tables.extend(left_tables.iter().cloned());
        tables.extend(right_tables.iter().cloned());

        // For outer join unmatched rows, use left's version
        let combined_version = left.row.version();

        Self {
            row: Rc::new(Row::new_with_version(
                left_join_null_row_id(left.row.id()),
                combined_version,
                values,
            )),
            is_combined: true,
            tables: TablesStorage::Owned(tables),
        }
    }

    /// Fast combine that reuses pre-computed tables (avoids repeated table name cloning).
    /// The combined row's version is the sum of both source row versions.
    #[inline]
    pub fn combine_fast(
        left: &RelationEntry,
        right: &RelationEntry,
        combined_tables: SharedTables,
    ) -> Self {
        let left_values = left.row.values();
        let right_values = right.row.values();
        let total_len = left_values.len() + right_values.len();

        let mut values = Vec::with_capacity(total_len);
        values.extend(left_values.iter().cloned());
        values.extend(right_values.iter().cloned());

        // Sum version for JOIN result
        let combined_version = left.row.version().wrapping_add(right.row.version());

        Self {
            row: Rc::new(Row::new_with_version(
                join_row_id(left.row.id(), right.row.id()),
                combined_version,
                values,
            )),
            is_combined: true,
            tables: TablesStorage::Shared(combined_tables),
        }
    }

    /// Fast combine with null that reuses pre-computed tables.
    /// The combined row's version is the left row's version (right side is NULL).
    #[inline]
    pub fn combine_with_null_fast(
        left: &RelationEntry,
        right_column_count: usize,
        combined_tables: SharedTables,
    ) -> Self {
        let left_values = left.row.values();
        let total_len = left_values.len() + right_column_count;

        let mut values = Vec::with_capacity(total_len);
        values.extend(left_values.iter().cloned());
        values.resize(total_len, Value::Null);

        // For outer join unmatched rows, use left's version
        let combined_version = left.row.version();

        Self {
            row: Rc::new(Row::new_with_version(
                left_join_null_row_id(left.row.id()),
                combined_version,
                values,
            )),
            is_combined: true,
            tables: TablesStorage::Shared(combined_tables),
        }
    }

    /// Fast combine with nulls on the left that reuses pre-computed tables.
    /// The combined row's version is the right row's version (left side is NULL).
    #[inline]
    pub fn combine_null_with_fast(
        left_column_count: usize,
        right: &RelationEntry,
        combined_tables: SharedTables,
    ) -> Self {
        let right_values = right.row.values();
        let total_len = left_column_count + right_values.len();

        let mut values = Vec::with_capacity(total_len);
        values.resize(left_column_count, Value::Null);
        values.extend(right_values.iter().cloned());

        let combined_version = right.row.version();

        Self {
            row: Rc::new(Row::new_with_version(
                right_join_null_row_id(right.row.id()),
                combined_version,
                values,
            )),
            is_combined: true,
            tables: TablesStorage::Shared(combined_tables),
        }
    }

    /// Creates a combined entry with null values for the left side (for right outer joins).
    /// The combined row's version is the right row's version (left side is NULL).
    pub fn combine_null_with(
        left_column_count: usize,
        left_tables: &[String],
        right: &RelationEntry,
        right_tables: &[String],
    ) -> Self {
        let right_values = right.row.values();
        let total_len = left_column_count + right_values.len();

        let mut values = Vec::with_capacity(total_len);
        values.resize(left_column_count, Value::Null);
        values.extend(right_values.iter().cloned());

        let mut tables = Vec::with_capacity(left_tables.len() + right_tables.len());
        tables.extend(left_tables.iter().cloned());
        tables.extend(right_tables.iter().cloned());

        // For right outer join unmatched rows, use right's version
        let combined_version = right.row.version();

        Self {
            row: Rc::new(Row::new_with_version(
                right_join_null_row_id(right.row.id()),
                combined_version,
                values,
            )),
            is_combined: true,
            tables: TablesStorage::Owned(tables),
        }
    }
}

/// A relation is a collection of entries with table context.
#[derive(Clone, Debug)]
pub struct Relation {
    /// The entries in this relation.
    pub entries: Vec<RelationEntry>,
    /// Table names in this relation.
    pub tables: Vec<String>,
    /// Column counts for each table (used for computing offsets in joined relations).
    /// The i-th element is the number of columns in the i-th table.
    pub table_column_counts: Vec<usize>,
}

impl Relation {
    /// Creates a new empty relation.
    pub fn new(tables: Vec<String>) -> Self {
        let table_count = tables.len();
        Self {
            entries: Vec::new(),
            tables,
            table_column_counts: alloc::vec![0; table_count],
        }
    }

    /// Creates a new empty relation with column counts.
    pub fn new_with_column_counts(tables: Vec<String>, column_counts: Vec<usize>) -> Self {
        Self {
            entries: Vec::new(),
            tables,
            table_column_counts: column_counts,
        }
    }

    /// Creates an empty relation.
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
            tables: Vec::new(),
            table_column_counts: Vec::new(),
        }
    }

    /// Creates a relation from Rc<Row>s.
    /// Uses shared tables to avoid cloning for each row.
    pub fn from_rows(rows: Vec<Rc<Row>>, tables: Vec<String>) -> Self {
        let shared_tables: SharedTables = Arc::from(tables.as_slice());
        // Infer column count from first row
        let column_count = rows.first().map(|r| r.len()).unwrap_or(0);
        let table_column_counts = if tables.len() == 1 {
            alloc::vec![column_count]
        } else {
            alloc::vec![0; tables.len()]
        };
        let entries = rows
            .into_iter()
            .map(|row| RelationEntry::new_shared(row, shared_tables.clone()))
            .collect();
        Self {
            entries,
            tables,
            table_column_counts,
        }
    }

    /// Creates a relation from Rc<Row>s with explicit column count.
    pub fn from_rows_with_column_count(
        rows: Vec<Rc<Row>>,
        tables: Vec<String>,
        column_count: usize,
    ) -> Self {
        let shared_tables: SharedTables = Arc::from(tables.as_slice());
        let table_column_counts = alloc::vec![column_count];
        let entries = rows
            .into_iter()
            .map(|row| RelationEntry::new_shared(row, shared_tables.clone()))
            .collect();
        Self {
            entries,
            tables,
            table_column_counts,
        }
    }

    /// Creates a relation from owned Rows.
    /// Uses shared tables to avoid cloning for each row.
    pub fn from_rows_owned(rows: Vec<Row>, tables: Vec<String>) -> Self {
        let shared_tables: SharedTables = Arc::from(tables.as_slice());
        // Infer column count from first row
        let column_count = rows.first().map(|r| r.len()).unwrap_or(0);
        let table_column_counts = if tables.len() == 1 {
            alloc::vec![column_count]
        } else {
            alloc::vec![0; tables.len()]
        };
        let entries = rows
            .into_iter()
            .map(|row| RelationEntry {
                row: Rc::new(row),
                is_combined: shared_tables.len() > 1,
                tables: TablesStorage::Shared(shared_tables.clone()),
            })
            .collect();
        Self {
            entries,
            tables,
            table_column_counts,
        }
    }

    /// Creates a relation from existing entries with metadata.
    pub fn from_entries(
        entries: Vec<RelationEntry>,
        tables: Vec<String>,
        table_column_counts: Vec<usize>,
    ) -> Self {
        Self {
            entries,
            tables,
            table_column_counts,
        }
    }

    /// Returns the tables in this relation.
    pub fn tables(&self) -> &[String] {
        &self.tables
    }

    /// Returns the column counts for each table.
    pub fn table_column_counts(&self) -> &[usize] {
        &self.table_column_counts
    }

    /// Computes the column offset for a given table name.
    /// Returns None if the table is not found.
    pub fn get_table_offset(&self, table_name: &str) -> Option<usize> {
        let mut offset = 0;
        for (i, t) in self.tables.iter().enumerate() {
            if t == table_name {
                return Some(offset);
            }
            offset += self.table_column_counts.get(i).copied().unwrap_or(0);
        }
        None
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the relation is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Adds an entry to the relation.
    pub fn push(&mut self, entry: RelationEntry) {
        self.entries.push(entry);
    }

    /// Returns an iterator over the entries.
    pub fn iter(&self) -> impl Iterator<Item = &RelationEntry> {
        self.entries.iter()
    }
}

impl IntoIterator for Relation {
    type Item = RelationEntry;
    type IntoIter = alloc::vec::IntoIter<RelationEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_relation_entry() {
        let row = Rc::new(Row::new(
            1,
            vec![Value::Int64(42), Value::String("test".into())],
        ));
        let entry = RelationEntry::from_row(row, "users");

        assert_eq!(entry.id(), 1);
        assert_eq!(entry.tables(), &["users"]);
        assert_eq!(entry.get_field(0), Some(&Value::Int64(42)));
        assert!(!entry.is_combined);
    }

    #[test]
    fn test_relation_entry_combine() {
        let left_row = Rc::new(Row::new(1, vec![Value::Int64(1)]));
        let right_row = Rc::new(Row::new(2, vec![Value::Int64(2)]));

        let left_entry = RelationEntry::from_row(left_row, "a");
        let right_entry = RelationEntry::from_row(right_row, "b");

        let combined =
            RelationEntry::combine(&left_entry, &["a".into()], &right_entry, &["b".into()]);

        assert!(combined.is_combined);
        assert_eq!(combined.tables(), &["a", "b"]);
        assert_eq!(combined.get_field(0), Some(&Value::Int64(1)));
        assert_eq!(combined.get_field(1), Some(&Value::Int64(2)));
    }

    #[test]
    fn test_relation_from_rows() {
        let rows = vec![
            Rc::new(Row::new(1, vec![Value::Int64(1)])),
            Rc::new(Row::new(2, vec![Value::Int64(2)])),
        ];
        let relation = Relation::from_rows(rows, vec!["users".into()]);

        assert_eq!(relation.len(), 2);
        assert_eq!(relation.tables(), &["users"]);
    }
}
