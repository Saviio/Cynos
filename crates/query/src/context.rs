//! Execution context for query execution.

use alloc::string::String;
use alloc::vec::Vec;

/// Index type enumeration for query optimization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum QueryIndexType {
    /// Hash index - O(1) point lookups.
    Hash,
    /// B+Tree index - O(log n) range queries.
    #[default]
    BTree,
    /// GIN (Generalized Inverted Index) - for JSONB containment queries.
    Gin,
}

/// Statistics about a table for query optimization.
#[derive(Clone, Debug, Default)]
pub struct TableStats {
    /// Number of rows in the table.
    pub row_count: usize,
    /// Whether the table is sorted by primary key.
    pub is_sorted: bool,
    /// Available indexes on this table.
    pub indexes: Vec<IndexInfo>,
}

/// Information about an index.
#[derive(Clone, Debug)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Column names in the index.
    pub columns: Vec<String>,
    /// Whether this is a unique index.
    pub is_unique: bool,
    /// Index type (BTree or GIN).
    pub index_type: QueryIndexType,
    /// Optional normalized JSON paths covered by a GIN index.
    pub gin_paths: Option<Vec<String>>,
}

impl IndexInfo {
    /// Creates a new index info with default BTree type.
    pub fn new(name: impl Into<String>, columns: Vec<String>, is_unique: bool) -> Self {
        Self {
            name: name.into(),
            columns,
            is_unique,
            index_type: QueryIndexType::BTree,
            gin_paths: None,
        }
    }

    /// Creates a new GIN index info.
    pub fn new_gin(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            columns,
            is_unique: false, // GIN indexes are never unique
            index_type: QueryIndexType::Gin,
            gin_paths: None,
        }
    }

    /// Sets the index type.
    pub fn with_type(mut self, index_type: QueryIndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Restricts a GIN index to a set of normalized JSON paths.
    pub fn with_gin_paths(mut self, gin_paths: Vec<String>) -> Self {
        self.gin_paths = if gin_paths.is_empty() {
            None
        } else {
            Some(gin_paths)
        };
        self
    }

    /// Returns true if this is a GIN index.
    pub fn is_gin(&self) -> bool {
        self.index_type == QueryIndexType::Gin
    }

    /// Returns true if this is a hash index.
    pub fn is_hash(&self) -> bool {
        self.index_type == QueryIndexType::Hash
    }

    /// Returns true if this index can satisfy point lookups.
    pub fn supports_point_lookup(&self) -> bool {
        !self.is_gin()
    }

    /// Returns true if this index can satisfy ordered/range scans.
    pub fn supports_range(&self) -> bool {
        self.index_type == QueryIndexType::BTree
    }

    /// Returns true if this index preserves key order.
    pub fn supports_ordering(&self) -> bool {
        self.supports_range()
    }

    /// Returns whether this GIN index supports the given normalized path.
    pub fn supports_gin_path(&self, path: &str) -> bool {
        self.is_gin()
            && self.gin_paths.as_ref().map_or(true, |paths| {
                paths.iter().any(|candidate| candidate == path)
            })
    }
}

/// Execution context providing access to table metadata and statistics.
#[derive(Clone, Debug, Default)]
pub struct ExecutionContext {
    /// Table statistics for optimization.
    table_stats: alloc::collections::BTreeMap<String, TableStats>,
}

impl ExecutionContext {
    /// Creates a new empty execution context.
    pub fn new() -> Self {
        Self {
            table_stats: alloc::collections::BTreeMap::new(),
        }
    }

    /// Registers table statistics.
    pub fn register_table(&mut self, table: impl Into<String>, stats: TableStats) {
        self.table_stats.insert(table.into(), stats);
    }

    /// Gets statistics for a table.
    pub fn get_stats(&self, table: &str) -> Option<&TableStats> {
        self.table_stats.get(table)
    }

    /// Gets the row count for a table.
    pub fn row_count(&self, table: &str) -> usize {
        self.table_stats
            .get(table)
            .map(|s| s.row_count)
            .unwrap_or(0)
    }

    /// Checks if a table has an index on the given columns.
    pub fn has_index(&self, table: &str, columns: &[&str]) -> bool {
        self.table_stats
            .get(table)
            .map(|s| {
                s.indexes.iter().any(|idx| {
                    idx.columns.len() == columns.len()
                        && idx.columns.iter().zip(columns.iter()).all(|(a, b)| a == *b)
                })
            })
            .unwrap_or(false)
    }

    /// Finds an index whose key columns exactly match the requested columns.
    pub fn find_index(&self, table: &str, columns: &[&str]) -> Option<&IndexInfo> {
        self.table_stats.get(table).and_then(|s| {
            s.indexes.iter().find(|idx| {
                idx.columns.len() == columns.len()
                    && idx.columns.iter().zip(columns.iter()).all(|(a, b)| a == *b)
            })
        })
    }

    /// Finds an index whose leading columns match the requested columns.
    pub fn find_index_prefix(&self, table: &str, columns: &[&str]) -> Option<&IndexInfo> {
        self.table_stats.get(table).and_then(|s| {
            s.indexes.iter().find(|idx| {
                idx.supports_ordering()
                    && idx.columns.len() >= columns.len()
                    && idx.columns.iter().zip(columns.iter()).all(|(a, b)| a == *b)
            })
        })
    }

    /// Finds an index by its name.
    pub fn find_index_by_name(&self, table: &str, index_name: &str) -> Option<&IndexInfo> {
        self.table_stats
            .get(table)
            .and_then(|s| s.indexes.iter().find(|idx| idx.name == index_name))
    }

    /// Finds a GIN index for the given column.
    pub fn find_gin_index(&self, table: &str, column: &str) -> Option<&IndexInfo> {
        self.table_stats.get(table).and_then(|s| {
            s.indexes
                .iter()
                .find(|idx| idx.is_gin() && idx.columns.iter().any(|c| c == column))
        })
    }

    /// Finds the most specific GIN index for the given column/path pair.
    pub fn find_gin_index_for_path(
        &self,
        table: &str,
        column: &str,
        path: &str,
    ) -> Option<&IndexInfo> {
        let stats = self.table_stats.get(table)?;

        stats
            .indexes
            .iter()
            .find(|idx| {
                idx.is_gin()
                    && idx.columns.iter().any(|c| c == column)
                    && idx
                        .gin_paths
                        .as_ref()
                        .is_some_and(|paths| paths.iter().any(|candidate| candidate == path))
            })
            .or_else(|| {
                stats.indexes.iter().find(|idx| {
                    idx.is_gin()
                        && idx.columns.iter().any(|c| c == column)
                        && idx.gin_paths.is_none()
                })
            })
    }

    /// Finds the primary key index (unique BTree index) for a table.
    /// Returns the first unique BTree index found, which is typically the primary key.
    pub fn find_primary_index(&self, table: &str) -> Option<&IndexInfo> {
        self.table_stats.get(table).and_then(|s| {
            s.indexes
                .iter()
                .find(|idx| idx.is_unique && idx.supports_ordering())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_context() {
        let mut ctx = ExecutionContext::new();

        let stats = TableStats {
            row_count: 1000,
            is_sorted: true,
            indexes: alloc::vec![IndexInfo::new("idx_id", alloc::vec!["id".into()], true)],
        };

        ctx.register_table("users", stats);

        assert_eq!(ctx.row_count("users"), 1000);
        assert!(ctx.has_index("users", &["id"]));
        assert!(!ctx.has_index("users", &["name"]));
    }

    #[test]
    fn test_find_index() {
        let mut ctx = ExecutionContext::new();

        let stats = TableStats {
            row_count: 100,
            is_sorted: false,
            indexes: alloc::vec![
                IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                IndexInfo::new(
                    "idx_name_age",
                    alloc::vec!["name".into(), "age".into()],
                    false
                ),
            ],
        };

        ctx.register_table("users", stats);

        let idx = ctx.find_index("users", &["id"]);
        assert!(idx.is_some());
        assert_eq!(idx.unwrap().name, "idx_id");

        let idx = ctx.find_index("users", &["name"]);
        assert!(idx.is_none());

        let idx = ctx.find_index_prefix("users", &["name"]);
        assert!(idx.is_some());
        assert_eq!(idx.unwrap().name, "idx_name_age");

        let idx = ctx.find_index("users", &["email"]);
        assert!(idx.is_none());
    }

    #[test]
    fn test_find_gin_index_for_path() {
        let mut ctx = ExecutionContext::new();

        let stats = TableStats {
            row_count: 100,
            is_sorted: false,
            indexes: alloc::vec![
                IndexInfo::new_gin("idx_metadata_tier", alloc::vec!["metadata".into()])
                    .with_gin_paths(alloc::vec!["customer.tier".into()]),
                IndexInfo::new_gin("idx_metadata_full", alloc::vec!["metadata".into()]),
            ],
        };

        ctx.register_table("issues", stats);

        let idx = ctx
            .find_gin_index_for_path("issues", "metadata", "customer.tier")
            .unwrap();
        assert_eq!(idx.name, "idx_metadata_tier");

        let fallback = ctx
            .find_gin_index_for_path("issues", "metadata", "risk.bucket")
            .unwrap();
        assert_eq!(fallback.name, "idx_metadata_full");
    }
}
