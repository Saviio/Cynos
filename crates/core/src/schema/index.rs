//! Index definition for Cynos database schema.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

/// Index type enumeration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IndexType {
    /// Hash index - O(1) point lookups.
    Hash,
    /// B+Tree index - O(log n) range queries.
    BTree,
    /// GIN (Generalized Inverted Index) - for JSONB containment queries.
    Gin,
}

/// Sort order for index columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Order {
    /// Ascending order.
    #[default]
    Asc,
    /// Descending order.
    Desc,
}

/// A column reference within an index definition.
#[derive(Clone, Debug)]
pub struct IndexedColumn {
    /// Column name.
    pub name: String,
    /// Sort order for this column in the index.
    pub order: Order,
    /// Whether this column auto-increments (only valid for primary key).
    pub auto_increment: bool,
}

impl IndexedColumn {
    /// Creates a new indexed column with default ascending order.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            order: Order::Asc,
            auto_increment: false,
        }
    }

    /// Sets the sort order.
    pub fn order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }

    /// Sets auto-increment flag.
    pub fn auto_increment(mut self, auto_increment: bool) -> Self {
        self.auto_increment = auto_increment;
        self
    }
}

/// An index definition in a table schema.
#[derive(Clone, Debug)]
pub struct IndexDef {
    /// Index name.
    name: String,
    /// Table name this index belongs to.
    table_name: String,
    /// Columns included in this index.
    columns: Vec<IndexedColumn>,
    /// Whether this index enforces uniqueness.
    unique: bool,
    /// Index type.
    index_type: IndexType,
    /// Optional normalized JSON paths covered by a GIN index.
    ///
    /// `None` means the index covers the full JSON tree.
    gin_paths: Option<Vec<String>>,
}

impl IndexDef {
    /// Creates a new index definition.
    pub fn new(
        name: impl Into<String>,
        table_name: impl Into<String>,
        columns: Vec<IndexedColumn>,
    ) -> Self {
        Self {
            name: name.into(),
            table_name: table_name.into(),
            columns,
            unique: false,
            index_type: IndexType::BTree,
            gin_paths: None,
        }
    }

    /// Sets whether this index is unique.
    pub fn unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    /// Sets the index type.
    pub fn index_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Restricts a GIN index to a set of normalized JSON paths.
    pub fn with_gin_paths(mut self, paths: Vec<String>) -> Self {
        self.gin_paths = if paths.is_empty() { None } else { Some(paths) };
        self
    }

    /// Returns the index name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the table name.
    #[inline]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns the normalized name (table.index).
    pub fn normalized_name(&self) -> String {
        format!("{}.{}", self.table_name, self.name)
    }

    /// Returns the indexed columns.
    #[inline]
    pub fn columns(&self) -> &[IndexedColumn] {
        &self.columns
    }

    /// Returns whether this index is unique.
    #[inline]
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// Returns the index type.
    #[inline]
    pub fn get_index_type(&self) -> IndexType {
        self.index_type
    }

    /// Returns the normalized JSON paths covered by this GIN index.
    #[inline]
    pub fn gin_paths(&self) -> Option<&[String]> {
        self.gin_paths.as_deref()
    }

    /// Returns whether this GIN index can answer lookups for the given path.
    #[inline]
    pub fn supports_gin_path(&self, path: &str) -> bool {
        if self.index_type != IndexType::Gin {
            return false;
        }

        self.gin_paths.as_ref().map_or(true, |paths| {
            paths.iter().any(|candidate| candidate == path)
        })
    }

    /// Returns whether this is a single-column index.
    #[inline]
    pub fn is_single_column(&self) -> bool {
        self.columns.len() == 1
    }

    /// Returns whether any column has auto-increment.
    pub fn has_auto_increment(&self) -> bool {
        self.columns.iter().any(|c| c.auto_increment)
    }
}

impl PartialEq for IndexDef {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.table_name == other.table_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec;

    #[test]
    fn test_indexed_column() {
        let col = IndexedColumn::new("id")
            .order(Order::Desc)
            .auto_increment(true);

        assert_eq!(col.name, "id");
        assert_eq!(col.order, Order::Desc);
        assert!(col.auto_increment);
    }

    #[test]
    fn test_index_def() {
        let idx = IndexDef::new("idx_user_email", "users", vec![IndexedColumn::new("email")])
            .unique(true)
            .index_type(IndexType::Hash);

        assert_eq!(idx.name(), "idx_user_email");
        assert_eq!(idx.table_name(), "users");
        assert_eq!(idx.normalized_name(), "users.idx_user_email");
        assert!(idx.is_unique());
        assert_eq!(idx.get_index_type(), IndexType::Hash);
        assert!(idx.is_single_column());
    }

    #[test]
    fn test_composite_index() {
        let idx = IndexDef::new(
            "idx_name_age",
            "users",
            vec![
                IndexedColumn::new("last_name"),
                IndexedColumn::new("first_name"),
            ],
        );

        assert!(!idx.is_single_column());
        assert_eq!(idx.columns().len(), 2);
    }

    #[test]
    fn test_gin_index_paths() {
        let idx = IndexDef::new("idx_profile", "users", vec![IndexedColumn::new("profile")])
            .index_type(IndexType::Gin)
            .with_gin_paths(vec!["customer.tier".into(), "risk.bucket".into()]);

        assert!(idx.supports_gin_path("customer.tier"));
        assert!(!idx.supports_gin_path("flags.strategic"));
        assert_eq!(
            idx.gin_paths().unwrap(),
            ["customer.tier".to_string(), "risk.bucket".to_string()]
        );
    }
}
