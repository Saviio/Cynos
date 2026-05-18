//! Table definition for Cynos database schema.

use super::column::Column;
use super::constraint::{Constraints, ForeignKey};
use super::index::{IndexDef, IndexType, IndexedColumn};
use crate::error::{Error, Result};
use crate::types::DataType;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;

/// A table definition in the database schema.
#[derive(Clone, Debug)]
pub struct Table {
    /// Table name.
    name: String,
    /// Column definitions.
    columns: Vec<Column>,
    /// Index definitions.
    indices: Vec<IndexDef>,
    /// Table constraints.
    constraints: Constraints,
    /// Whether to persist indices.
    persistent_index: bool,
}

impl Table {
    /// Creates a new table with the given name and columns.
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        let name = name.into();
        let columns: Vec<Column> = columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| c.with_index(i))
            .collect();

        Self {
            name,
            columns,
            indices: Vec::new(),
            constraints: Constraints::new(),
            persistent_index: false,
        }
    }

    /// Returns the table name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the columns.
    #[inline]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Returns the indices.
    #[inline]
    pub fn indices(&self) -> &[IndexDef] {
        &self.indices
    }

    /// Returns the constraints.
    #[inline]
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    /// Returns whether indices should be persisted.
    #[inline]
    pub fn persistent_index(&self) -> bool {
        self.persistent_index
    }

    /// Gets a column by name.
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name() == name)
    }

    /// Gets a column index by name.
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name() == name)
    }

    /// Gets an index by name.
    pub fn get_index(&self, name: &str) -> Option<&IndexDef> {
        self.indices.iter().find(|i| i.name() == name)
    }

    /// Returns the primary key index if defined.
    pub fn primary_key(&self) -> Option<&IndexDef> {
        self.constraints.get_primary_key()
    }
}

/// Builder for creating table definitions.
pub struct TableBuilder {
    name: String,
    columns: Vec<Column>,
    indices: Vec<IndexDef>,
    pk_name: Option<String>,
    pk_columns: Vec<IndexedColumn>,
    unique_columns: Vec<String>,
    foreign_keys: Vec<ForeignKey>,
    persistent_index: bool,
}

impl TableBuilder {
    /// Creates a new table builder.
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        Self::check_naming_rules(&name)?;
        Ok(Self {
            name,
            columns: Vec::new(),
            indices: Vec::new(),
            pk_name: None,
            pk_columns: Vec::new(),
            unique_columns: Vec::new(),
            foreign_keys: Vec::new(),
            persistent_index: false,
        })
    }

    /// Validates a name follows naming rules.
    fn check_naming_rules(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(Error::InvalidSchema {
                message: "Name cannot be empty".into(),
            });
        }
        let first = name.chars().next().unwrap();
        if !first.is_ascii_alphabetic() && first != '_' {
            return Err(Error::InvalidSchema {
                message: format!("Name must start with letter or underscore: {}", name),
            });
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(Error::InvalidSchema {
                message: format!("Name contains invalid characters: {}", name),
            });
        }
        Ok(())
    }

    /// Adds a column to the table.
    pub fn add_column(mut self, name: impl Into<String>, data_type: DataType) -> Result<Self> {
        let name = name.into();
        Self::check_naming_rules(&name)?;
        if self.columns.iter().any(|c| c.name() == name) {
            return Err(Error::InvalidSchema {
                message: format!("Column already exists: {}", name),
            });
        }
        self.columns.push(Column::new(name, data_type));
        Ok(self)
    }

    /// Adds a nullable column.
    pub fn add_nullable(mut self, columns: &[&str]) -> Self {
        for name in columns {
            if let Some(col) = self.columns.iter_mut().find(|c| c.name() == *name) {
                *col = col.clone().nullable(true);
            }
        }
        self
    }

    /// Sets the primary key.
    pub fn add_primary_key(mut self, columns: &[&str], auto_increment: bool) -> Result<Self> {
        let pk_name = format!("pk{}", capitalize(&self.name));
        Self::check_naming_rules(&pk_name)?;

        let indexed_cols: Vec<IndexedColumn> = columns
            .iter()
            .map(|name| {
                IndexedColumn::new(*name).auto_increment(auto_increment && columns.len() == 1)
            })
            .collect();

        // Validate columns exist and are indexable
        for col in &indexed_cols {
            let column = self.columns.iter().find(|c| c.name() == col.name);
            match column {
                None => {
                    return Err(Error::InvalidSchema {
                        message: format!("Column not found: {}", col.name),
                    })
                }
                Some(c) if !c.is_indexable() => {
                    return Err(Error::InvalidSchema {
                        message: format!("Column is not indexable: {}", col.name),
                    })
                }
                Some(c)
                    if col.auto_increment
                        && c.data_type() != DataType::Int32
                        && c.data_type() != DataType::Int64 =>
                {
                    return Err(Error::InvalidSchema {
                        message: "Auto-increment requires integer type".into(),
                    })
                }
                _ => {}
            }
        }

        if columns.len() == 1 {
            self.unique_columns.push(columns[0].to_string());
        }

        self.pk_name = Some(pk_name);
        self.pk_columns = indexed_cols;
        Ok(self)
    }

    /// Adds a unique constraint.
    pub fn add_unique(mut self, name: impl Into<String>, columns: &[&str]) -> Result<Self> {
        let name = name.into();
        Self::check_naming_rules(&name)?;

        let indexed_cols: Vec<IndexedColumn> =
            columns.iter().map(|n| IndexedColumn::new(*n)).collect();

        // Validate columns
        for col in &indexed_cols {
            if !self.columns.iter().any(|c| c.name() == col.name) {
                return Err(Error::InvalidSchema {
                    message: format!("Column not found: {}", col.name),
                });
            }
        }

        if columns.len() == 1 {
            self.unique_columns.push(columns[0].to_string());
        }

        let idx = IndexDef::new(name, &self.name, indexed_cols).unique(true);
        self.indices.push(idx);
        Ok(self)
    }

    /// Adds an index.
    /// For JSONB columns, automatically uses GIN index type.
    pub fn add_index(
        mut self,
        name: impl Into<String>,
        columns: &[&str],
        unique: bool,
    ) -> Result<Self> {
        let name = name.into();
        Self::check_naming_rules(&name)?;

        let indexed_cols: Vec<IndexedColumn> =
            columns.iter().map(|n| IndexedColumn::new(*n)).collect();

        // Validate columns and determine index type
        let mut use_gin = false;
        for col in &indexed_cols {
            let column = self.columns.iter().find(|c| c.name() == col.name);
            match column {
                None => {
                    return Err(Error::InvalidSchema {
                        message: format!("Column not found: {}", col.name),
                    })
                }
                Some(c) if c.data_type() == DataType::Jsonb => {
                    // JSONB columns use GIN index
                    use_gin = true;
                }
                Some(c) if !c.is_indexable() => {
                    return Err(Error::InvalidSchema {
                        message: format!("Column is not indexable: {}", col.name),
                    })
                }
                _ => {}
            }
        }

        let mut idx = IndexDef::new(name, &self.name, indexed_cols).unique(unique);
        if use_gin {
            idx = idx.index_type(IndexType::Gin);
        }
        self.indices.push(idx);
        Ok(self)
    }

    /// Adds a path-restricted GIN index for a JSONB column.
    pub fn add_jsonb_index(
        mut self,
        name: impl Into<String>,
        column: &str,
        paths: &[&str],
    ) -> Result<Self> {
        let name = name.into();
        Self::check_naming_rules(&name)?;

        let column_def = self
            .columns
            .iter()
            .find(|c| c.name() == column)
            .ok_or_else(|| Error::InvalidSchema {
                message: format!("Column not found: {}", column),
            })?;

        if column_def.data_type() != DataType::Jsonb {
            return Err(Error::InvalidSchema {
                message: format!("JSONB index requires Jsonb column: {}", column),
            });
        }

        let normalized_paths = paths
            .iter()
            .filter(|path| !path.is_empty())
            .map(|path| (*path).to_string())
            .collect();

        let idx = IndexDef::new(name, &self.name, vec![IndexedColumn::new(column)])
            .index_type(IndexType::Gin)
            .with_gin_paths(normalized_paths);
        self.indices.push(idx);
        Ok(self)
    }

    /// Adds a hash index for point-lookups on scalar columns.
    pub fn add_hash_index(
        mut self,
        name: impl Into<String>,
        columns: &[&str],
        unique: bool,
    ) -> Result<Self> {
        let name = name.into();
        Self::check_naming_rules(&name)?;

        let indexed_cols: Vec<IndexedColumn> =
            columns.iter().map(|n| IndexedColumn::new(*n)).collect();

        for col in &indexed_cols {
            let column = self.columns.iter().find(|c| c.name() == col.name);
            match column {
                None => {
                    return Err(Error::InvalidSchema {
                        message: format!("Column not found: {}", col.name),
                    })
                }
                Some(c) if c.data_type() == DataType::Jsonb => {
                    return Err(Error::InvalidSchema {
                        message: format!(
                            "Hash index is not supported for JSONB column: {}",
                            col.name
                        ),
                    })
                }
                Some(c) if !c.is_indexable() => {
                    return Err(Error::InvalidSchema {
                        message: format!("Column is not indexable: {}", col.name),
                    })
                }
                _ => {}
            }
        }

        let idx = IndexDef::new(name, &self.name, indexed_cols)
            .unique(unique)
            .index_type(IndexType::Hash);
        self.indices.push(idx);
        Ok(self)
    }

    /// Adds a foreign key constraint.
    pub fn add_foreign_key(
        self,
        name: impl Into<String>,
        child_column: &str,
        parent_table: &str,
        parent_column: &str,
    ) -> Result<Self> {
        self.add_foreign_key_with_graphql_names(
            name,
            child_column,
            parent_table,
            parent_column,
            None::<String>,
            None::<String>,
        )
    }

    /// Adds a foreign key constraint with optional GraphQL relation field names.
    pub fn add_foreign_key_with_graphql_names(
        mut self,
        name: impl Into<String>,
        child_column: &str,
        parent_table: &str,
        parent_column: &str,
        graphql_forward_field: Option<impl Into<String>>,
        graphql_reverse_field: Option<impl Into<String>>,
    ) -> Result<Self> {
        let name = name.into();
        Self::check_naming_rules(&name)?;

        if !self.columns.iter().any(|c| c.name() == child_column) {
            return Err(Error::InvalidSchema {
                message: format!("Column not found: {}", child_column),
            });
        }

        let fk = ForeignKey::new(&name, &self.name, child_column, parent_table, parent_column)
            .graphql_fields(graphql_forward_field, graphql_reverse_field);
        self.foreign_keys.push(fk);

        // Add index for foreign key column
        let is_unique = self.unique_columns.contains(&child_column.to_string());
        self = self.add_index(&name, &[child_column], is_unique)?;
        Ok(self)
    }

    /// Sets whether to persist indices.
    pub fn persistent_index(mut self, value: bool) -> Self {
        self.persistent_index = value;
        self
    }

    /// Builds the table definition.
    pub fn build(self) -> Result<Table> {
        // Build constraints
        let mut constraints = Constraints::new();

        // Collect all indices (including primary key)
        let mut all_indices = self.indices;

        // Add primary key
        if let Some(pk_name) = &self.pk_name {
            let pk = IndexDef::new(pk_name, &self.name, self.pk_columns.clone()).unique(true);
            // Add primary key to indices list so it's available for query optimization
            all_indices.push(pk.clone());
            constraints = constraints.primary_key(pk);
        }

        // Add not-nullable columns
        let not_nullable: Vec<String> = self
            .columns
            .iter()
            .filter(|c| !c.is_nullable())
            .map(|c| c.name().to_string())
            .collect();
        constraints = constraints.not_nullable(not_nullable);

        // Add foreign keys
        for fk in self.foreign_keys {
            constraints = constraints.add_foreign_key(fk);
        }

        // Build columns with indices
        let columns: Vec<Column> = self
            .columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                let is_unique = self.unique_columns.contains(&c.name().to_string());
                c.unique(is_unique).with_index(i)
            })
            .collect();

        Ok(Table {
            name: self.name,
            columns,
            indices: all_indices,
            constraints,
            persistent_index: self.persistent_index,
        })
    }
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().chain(chars).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_builder() {
        let table = TableBuilder::new("users")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_column("email", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], true)
            .unwrap()
            .add_unique("uq_email", &["email"])
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(table.name(), "users");
        assert_eq!(table.columns().len(), 3);
        assert!(table.primary_key().is_some());
    }

    #[test]
    fn test_table_get_column() {
        let table = TableBuilder::new("test")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .build()
            .unwrap();

        assert!(table.get_column("id").is_some());
        assert!(table.get_column("name").is_some());
        assert!(table.get_column("unknown").is_none());
    }

    #[test]
    fn test_invalid_column_name() {
        let result = TableBuilder::new("test")
            .unwrap()
            .add_column("123invalid", DataType::Int32);

        assert!(result.is_err());
    }

    #[test]
    fn test_duplicate_column() {
        let result = TableBuilder::new("test")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("id", DataType::Int64);

        assert!(result.is_err());
    }

    #[test]
    fn test_add_hash_index() {
        let table = TableBuilder::new("users")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("email", DataType::String)
            .unwrap()
            .add_hash_index("idx_email_hash", &["email"], true)
            .unwrap()
            .build()
            .unwrap();

        let index = table.get_index("idx_email_hash").unwrap();
        assert_eq!(index.get_index_type(), IndexType::Hash);
        assert!(index.is_unique());
    }

    #[test]
    fn test_add_jsonb_index_paths() {
        let table = TableBuilder::new("issues")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("metadata", DataType::Jsonb)
            .unwrap()
            .add_jsonb_index(
                "idx_issues_metadata",
                "metadata",
                &["customer.tier", "risk.bucket"],
            )
            .unwrap()
            .build()
            .unwrap();

        let index = table.get_index("idx_issues_metadata").unwrap();
        assert_eq!(index.get_index_type(), IndexType::Gin);
        assert_eq!(
            index.gin_paths().unwrap(),
            ["customer.tier".to_string(), "risk.bucket".to_string()]
        );
    }
}

#[test]
fn test_primary_key_in_indices() {
    let table = TableBuilder::new("users")
        .unwrap()
        .add_column("id", DataType::Int64)
        .unwrap()
        .add_column("name", DataType::String)
        .unwrap()
        .add_primary_key(&["id"], true)
        .unwrap()
        .build()
        .unwrap();

    // Primary key should be in indices list
    let indices = table.indices();

    assert!(
        indices.iter().any(|idx| idx.name() == "pkUsers"),
        "Primary key index 'pkUsers' should be in indices list"
    );
    assert!(
        indices
            .iter()
            .any(|idx| idx.columns().iter().any(|c| c.name == "id")),
        "Primary key index should contain 'id' column"
    );
}
