//! Cynos API - WASM bindings and JavaScript API for Cynos database.
//!
//! This crate provides the public API for the Cynos in-memory database,
//! including WASM bindings for use in JavaScript/TypeScript applications.
//!
//! # Core Components
//!
//! - `Database`: Main entry point for database operations
//! - `TableBuilder`: Builder for creating table schemas
//! - `SelectBuilder`: Query builder for SELECT statements
//! - `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder`: DML builders
//! - `JsObservableQuery`: Observable query with subscription support
//!
//! # Example (JavaScript)
//!
//! ```javascript
//! import { Database, DataType, col } from 'cynos';
//!
//! const db = await Database.create('mydb');
//!
//! db.createTable('users')
//!   .column('id', DataType.Int64, { primaryKey: true })
//!   .column('name', DataType.String)
//!   .column('age', DataType.Int32)
//!   .build();
//!
//! await db.insert('users').values([
//!   { id: 1, name: 'Alice', age: 25 },
//!   { id: 2, name: 'Bob', age: 30 },
//! ]).exec();
//!
//! const results = await db.select()
//!   .from('users')
//!   .where(col('age').gt(25))
//!   .exec();
//! ```

extern crate alloc;

pub mod binary_protocol;
pub mod convert;
pub mod database;
pub mod dataflow_compiler;
pub mod expr;
pub mod live_runtime;
mod profiling;
pub mod query_builder;
pub mod query_engine;
pub mod reactive_bridge;
pub mod table;
pub mod transaction;

pub use binary_protocol::{BinaryResult, SchemaLayout};
pub use convert::{js_to_row, js_to_value, row_to_js, value_to_js};
pub use database::{Database, PreparedGraphqlQuery};
pub use expr::{Column, Expr};
pub use query_builder::{
    DeleteBuilder, InsertBuilder, PreparedSelectQuery, SelectBuilder, UpdateBuilder,
};
pub use reactive_bridge::{
    JsChangesStream, JsGraphqlSubscription, JsIvmObservableQuery, JsObservableQuery,
};
pub use table::{ForeignKeyOptions, JsTable, JsTableBuilder};
pub use transaction::JsTransaction;

use wasm_bindgen::prelude::*;

/// Initialize the WASM module.
#[wasm_bindgen(start)]
pub fn init() {
    // Panic hook initialization removed - feature not configured
}

/// Data types supported by Cynos.
#[wasm_bindgen]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsDataType {
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Float64 = 3,
    String = 4,
    DateTime = 5,
    Bytes = 6,
    Jsonb = 7,
}

impl From<JsDataType> for cynos_core::DataType {
    fn from(dt: JsDataType) -> Self {
        match dt {
            JsDataType::Boolean => cynos_core::DataType::Boolean,
            JsDataType::Int32 => cynos_core::DataType::Int32,
            JsDataType::Int64 => cynos_core::DataType::Int64,
            JsDataType::Float64 => cynos_core::DataType::Float64,
            JsDataType::String => cynos_core::DataType::String,
            JsDataType::DateTime => cynos_core::DataType::DateTime,
            JsDataType::Bytes => cynos_core::DataType::Bytes,
            JsDataType::Jsonb => cynos_core::DataType::Jsonb,
        }
    }
}

impl From<cynos_core::DataType> for JsDataType {
    fn from(dt: cynos_core::DataType) -> Self {
        match dt {
            cynos_core::DataType::Boolean => JsDataType::Boolean,
            cynos_core::DataType::Int32 => JsDataType::Int32,
            cynos_core::DataType::Int64 => JsDataType::Int64,
            cynos_core::DataType::Float64 => JsDataType::Float64,
            cynos_core::DataType::String => JsDataType::String,
            cynos_core::DataType::DateTime => JsDataType::DateTime,
            cynos_core::DataType::Bytes => JsDataType::Bytes,
            cynos_core::DataType::Jsonb => JsDataType::Jsonb,
        }
    }
}

/// Sort order for ORDER BY clauses.
#[wasm_bindgen]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsSortOrder {
    Asc = 0,
    Desc = 1,
}

impl From<JsSortOrder> for cynos_query::ast::SortOrder {
    fn from(order: JsSortOrder) -> Self {
        match order {
            JsSortOrder::Asc => cynos_query::ast::SortOrder::Asc,
            JsSortOrder::Desc => cynos_query::ast::SortOrder::Desc,
        }
    }
}

/// Helper function to create a column reference.
#[wasm_bindgen]
pub fn col(name: &str) -> Column {
    Column::new_simple(name)
}
