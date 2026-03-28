//! Table and TableBuilder for schema definition.
//!
//! This module provides the JavaScript API for creating and managing tables.

use crate::expr::Column;
use crate::JsDataType;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use cynos_core::schema::{Table, TableBuilder};
use cynos_core::DataType;
use cynos_jsonb::JsonPath;
use wasm_bindgen::prelude::*;

/// Column options for table creation.
#[wasm_bindgen]
#[derive(Clone, Debug, Default)]
pub struct ColumnOptions {
    pub primary_key: bool,
    pub nullable: bool,
    pub unique: bool,
    pub auto_increment: bool,
}

/// Foreign-key options for GraphQL relation naming.
#[wasm_bindgen]
#[derive(Clone, Debug, Default)]
pub struct ForeignKeyOptions {
    field_name: Option<String>,
    reverse_field_name: Option<String>,
}

#[wasm_bindgen]
impl ColumnOptions {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    #[wasm_bindgen(js_name = primaryKey)]
    pub fn set_primary_key(mut self, value: bool) -> Self {
        self.primary_key = value;
        self
    }

    #[wasm_bindgen(js_name = setNullable)]
    pub fn set_nullable(mut self, value: bool) -> Self {
        self.nullable = value;
        self
    }

    #[wasm_bindgen(js_name = setUnique)]
    pub fn set_unique(mut self, value: bool) -> Self {
        self.unique = value;
        self
    }

    #[wasm_bindgen(js_name = setAutoIncrement)]
    pub fn set_auto_increment(mut self, value: bool) -> Self {
        self.auto_increment = value;
        self
    }
}

#[wasm_bindgen]
impl ForeignKeyOptions {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    #[wasm_bindgen(js_name = fieldName)]
    pub fn set_field_name(mut self, value: &str) -> Self {
        self.field_name = Some(value.to_string());
        self
    }

    #[wasm_bindgen(js_name = reverseFieldName)]
    pub fn set_reverse_field_name(mut self, value: &str) -> Self {
        self.reverse_field_name = Some(value.to_string());
        self
    }
}

/// JavaScript-friendly table builder.
#[wasm_bindgen]
pub struct JsTableBuilder {
    name: String,
    columns: Vec<ColumnDef>,
    primary_key: Option<Vec<String>>,
    indices: Vec<IndexDef>,
    foreign_keys: Vec<ForeignKeyDef>,
    auto_increment: bool,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct ColumnDef {
    name: String,
    data_type: DataType,
    nullable: bool,
    unique: bool,
}

#[derive(Clone, Debug)]
struct IndexDef {
    name: String,
    columns: Vec<String>,
    unique: bool,
    gin_paths: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
struct ForeignKeyDef {
    name: String,
    child_column: String,
    parent_table: String,
    parent_column: String,
    field_name: Option<String>,
    reverse_field_name: Option<String>,
}

#[wasm_bindgen]
impl JsTableBuilder {
    /// Creates a new table builder.
    #[wasm_bindgen(constructor)]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            columns: Vec::new(),
            primary_key: None,
            indices: Vec::new(),
            foreign_keys: Vec::new(),
            auto_increment: false,
        }
    }

    /// Builds the table schema and returns a JsTable.
    pub fn build(&self) -> Result<JsTable, JsValue> {
        let schema = self.build_internal()?;
        Ok(JsTable::new(schema))
    }

    /// Adds a column to the table.
    pub fn column(
        mut self,
        name: &str,
        data_type: JsDataType,
        options: Option<ColumnOptions>,
    ) -> Self {
        let opts = options.unwrap_or_default();

        self.columns.push(ColumnDef {
            name: name.to_string(),
            data_type: data_type.into(),
            nullable: opts.nullable,
            unique: opts.unique || opts.primary_key,
        });

        if opts.primary_key {
            self.primary_key = Some(alloc::vec![name.to_string()]);
            self.auto_increment = opts.auto_increment;
        }

        self
    }

    /// Sets the primary key columns.
    #[wasm_bindgen(js_name = primaryKey)]
    pub fn primary_key(mut self, columns: &JsValue) -> Self {
        if let Some(arr) = columns.dyn_ref::<js_sys::Array>() {
            let cols: Vec<String> = arr.iter().filter_map(|v| v.as_string()).collect();
            self.primary_key = Some(cols);
        } else if let Some(s) = columns.as_string() {
            self.primary_key = Some(alloc::vec![s]);
        }
        self
    }

    /// Adds an index to the table.
    pub fn index(mut self, name: &str, columns: &JsValue) -> Self {
        let cols = if let Some(arr) = columns.dyn_ref::<js_sys::Array>() {
            arr.iter().filter_map(|v| v.as_string()).collect()
        } else if let Some(s) = columns.as_string() {
            alloc::vec![s]
        } else {
            return self;
        };

        self.indices.push(IndexDef {
            name: name.to_string(),
            columns: cols,
            unique: false,
            gin_paths: None,
        });
        self
    }

    /// Adds a unique index to the table.
    #[wasm_bindgen(js_name = uniqueIndex)]
    pub fn unique_index(mut self, name: &str, columns: &JsValue) -> Self {
        let cols = if let Some(arr) = columns.dyn_ref::<js_sys::Array>() {
            arr.iter().filter_map(|v| v.as_string()).collect()
        } else if let Some(s) = columns.as_string() {
            alloc::vec![s]
        } else {
            return self;
        };

        self.indices.push(IndexDef {
            name: name.to_string(),
            columns: cols,
            unique: true,
            gin_paths: None,
        });
        self
    }

    /// Adds a JSONB index for specific paths.
    #[wasm_bindgen(js_name = jsonbIndex)]
    pub fn jsonb_index(mut self, column: &str, paths: &JsValue) -> Self {
        let gin_paths = if paths.is_null() || paths.is_undefined() {
            None
        } else {
            let normalized = Self::parse_jsonb_index_paths(paths);
            if normalized.is_empty() {
                return self;
            }
            Some(normalized)
        };
        let name = alloc::format!("idx_jsonb_{}", column);
        self.indices.push(IndexDef {
            name,
            columns: alloc::vec![column.to_string()],
            unique: false,
            gin_paths,
        });
        self
    }

    /// Adds a foreign key and optional GraphQL relation names.
    #[wasm_bindgen(js_name = foreignKey)]
    pub fn foreign_key(
        mut self,
        name: &str,
        child_column: &str,
        parent_table: &str,
        parent_column: &str,
        options: Option<ForeignKeyOptions>,
    ) -> Self {
        let options = options.unwrap_or_default();
        self.foreign_keys.push(ForeignKeyDef {
            name: name.to_string(),
            child_column: child_column.to_string(),
            parent_table: parent_table.to_string(),
            parent_column: parent_column.to_string(),
            field_name: options.field_name,
            reverse_field_name: options.reverse_field_name,
        });
        self
    }

    /// Builds the table schema (internal use).
    pub(crate) fn build_internal(&self) -> Result<Table, JsValue> {
        let mut builder = TableBuilder::new(&self.name)
            .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

        // Add columns
        for col in &self.columns {
            builder = builder
                .add_column(&col.name, col.data_type)
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

            if col.nullable {
                builder = builder.add_nullable(&[col.name.as_str()]);
            }
        }

        // Add primary key
        if let Some(pk_cols) = &self.primary_key {
            let pk_refs: Vec<&str> = pk_cols.iter().map(|s| s.as_str()).collect();
            builder = builder
                .add_primary_key(&pk_refs, self.auto_increment)
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
        }

        // Add indices
        for idx in &self.indices {
            let col_refs: Vec<&str> = idx.columns.iter().map(|s| s.as_str()).collect();
            builder = if let Some(paths) = &idx.gin_paths {
                let path_refs: Vec<&str> = paths.iter().map(|path| path.as_str()).collect();
                builder
                    .add_jsonb_index(&idx.name, &idx.columns[0], &path_refs)
                    .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?
            } else {
                builder
                    .add_index(&idx.name, &col_refs, idx.unique)
                    .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?
            };
        }

        // Add foreign keys
        for fk in &self.foreign_keys {
            builder = builder
                .add_foreign_key_with_graphql_names(
                    &fk.name,
                    &fk.child_column,
                    &fk.parent_table,
                    &fk.parent_column,
                    fk.field_name.clone(),
                    fk.reverse_field_name.clone(),
                )
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
        }

        builder
            .build()
            .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))
    }

    /// Returns the table name.
    #[wasm_bindgen(getter)]
    pub fn name(&self) -> String {
        self.name.clone()
    }
}

impl JsTableBuilder {
    fn parse_jsonb_index_paths(paths: &JsValue) -> Vec<String> {
        if let Some(arr) = paths.dyn_ref::<js_sys::Array>() {
            arr.iter()
                .filter_map(|value| value.as_string())
                .filter_map(|path| Self::normalize_jsonb_index_path(&path))
                .collect()
        } else if let Some(path) = paths.as_string() {
            Self::normalize_jsonb_index_path(&path)
                .map(|normalized| alloc::vec![normalized])
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    fn normalize_jsonb_index_path(path: &str) -> Option<String> {
        let parsed = JsonPath::parse(path).ok()?;
        let mut segments = Vec::new();
        if !Self::collect_jsonb_index_segments(&parsed, &mut segments) || segments.is_empty() {
            return None;
        }

        let mut normalized = String::new();
        for segment in segments {
            if !normalized.is_empty() {
                normalized.push('.');
            }
            normalized.push_str(&segment);
        }
        Some(normalized)
    }

    fn collect_jsonb_index_segments(path: &JsonPath, segments: &mut Vec<String>) -> bool {
        match path {
            JsonPath::Root => true,
            JsonPath::Field(parent, field) => {
                if !Self::collect_jsonb_index_segments(parent, segments) {
                    return false;
                }
                segments.push(field.clone());
                true
            }
            JsonPath::Index(parent, index) => {
                if !Self::collect_jsonb_index_segments(parent, segments) {
                    return false;
                }
                segments.push(index.to_string());
                true
            }
            JsonPath::Slice(_, _, _)
            | JsonPath::RecursiveField(_, _)
            | JsonPath::Wildcard(_)
            | JsonPath::Filter(_, _) => false,
        }
    }
}

/// JavaScript-friendly table reference.
#[wasm_bindgen]
pub struct JsTable {
    schema: Table,
}

impl JsTable {
    pub fn new(schema: Table) -> Self {
        Self { schema }
    }

    pub fn schema(&self) -> &Table {
        &self.schema
    }
}

#[wasm_bindgen]
impl JsTable {
    /// Returns the table name.
    #[wasm_bindgen(getter)]
    pub fn name(&self) -> String {
        self.schema.name().to_string()
    }

    /// Returns a column reference.
    pub fn col(&self, name: &str) -> Option<Column> {
        self.schema
            .get_column(name)
            .map(|c| Column::new(self.schema.name(), c.name()).with_index(c.index()))
    }

    /// Returns the column names.
    #[wasm_bindgen(js_name = columnNames)]
    pub fn column_names(&self) -> js_sys::Array {
        let arr = js_sys::Array::new();
        for col in self.schema.columns() {
            arr.push(&JsValue::from_str(col.name()));
        }
        arr
    }

    /// Returns the number of columns.
    #[wasm_bindgen(js_name = columnCount)]
    pub fn column_count(&self) -> usize {
        self.schema.columns().len()
    }

    /// Returns the column data type.
    #[wasm_bindgen(js_name = getColumnType)]
    pub fn get_column_type(&self, name: &str) -> Option<JsDataType> {
        self.schema.get_column(name).map(|c| c.data_type().into())
    }

    /// Returns whether a column is nullable.
    #[wasm_bindgen(js_name = isColumnNullable)]
    pub fn is_column_nullable(&self, name: &str) -> bool {
        self.schema
            .get_column(name)
            .map(|c| c.is_nullable())
            .unwrap_or(false)
    }

    /// Returns the primary key column names.
    #[wasm_bindgen(js_name = primaryKeyColumns)]
    pub fn primary_key_columns(&self) -> js_sys::Array {
        let arr = js_sys::Array::new();
        if let Some(pk) = self.schema.primary_key() {
            for col in pk.columns() {
                arr.push(&JsValue::from_str(&col.name));
            }
        }
        arr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn test_table_builder_basic() {
        let builder = JsTableBuilder::new("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None)
            .column("age", JsDataType::Int32, None);

        let table = builder.build_internal().unwrap();
        assert_eq!(table.name(), "users");
        assert_eq!(table.columns().len(), 3);
    }

    #[wasm_bindgen_test]
    fn test_table_builder_with_index() {
        let builder = JsTableBuilder::new("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("email", JsDataType::String, None)
            .unique_index("idx_email", &JsValue::from_str("email"));

        let table = builder.build_internal().unwrap();
        assert!(table.indices().iter().any(|i| i.name() == "idx_email"));
    }

    #[wasm_bindgen_test]
    fn test_table_builder_nullable() {
        let builder = JsTableBuilder::new("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column(
                "bio",
                JsDataType::String,
                Some(ColumnOptions::new().set_nullable(true)),
            );

        let table = builder.build_internal().unwrap();
        let bio_col = table.get_column("bio").unwrap();
        assert!(bio_col.is_nullable());
    }

    #[wasm_bindgen_test]
    fn test_js_table_col() {
        let builder = JsTableBuilder::new("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None);

        let schema = builder.build_internal().unwrap();
        let table = JsTable::new(schema);

        let col = table.col("name").unwrap();
        assert_eq!(col.name(), "name");
    }

    #[wasm_bindgen_test]
    fn test_js_table_column_names() {
        let builder = JsTableBuilder::new("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None)
            .column("age", JsDataType::Int32, None);

        let schema = builder.build_internal().unwrap();
        let table = JsTable::new(schema);

        let names = table.column_names();
        assert_eq!(names.length(), 3);
    }
}
