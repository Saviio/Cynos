//! Cynos Core - Core types and schema definitions for Cynos database.
//!
//! This crate provides the foundational types for the Cynos in-memory database:
//!
//! - `DataType`: Supported data types (Boolean, Int32, Int64, Float64, String, DateTime, Bytes, Jsonb)
//! - `Value`: Runtime values that can be stored in the database
//! - `Row`: A row of values with a unique identifier
//! - `schema`: Schema definitions (Column, Table, Index, Constraints)
//! - `Error`: Error types for database operations
//!
//! # Example
//!
//! ```rust
//! use cynos_core::{DataType, Value, Row};
//! use cynos_core::schema::{TableBuilder, Column};
//!
//! // Create a table schema
//! let table = TableBuilder::new("users")
//!     .unwrap()
//!     .add_column("id", DataType::Int64)
//!     .unwrap()
//!     .add_column("name", DataType::String)
//!     .unwrap()
//!     .add_primary_key(&["id"], true)
//!     .unwrap()
//!     .build()
//!     .unwrap();
//!
//! // Create a row
//! let row = Row::new(1, vec![
//!     Value::Int64(1),
//!     Value::String("Alice".into()),
//! ]);
//!
//! assert_eq!(row.id(), 1);
//! assert_eq!(row.get(1), Some(&Value::String("Alice".into())));
//! ```

#![no_std]

extern crate alloc;

mod error;
pub mod pattern_match;
mod row;
pub mod schema;
mod types;
mod value;

pub use error::{Error, Result};
pub use row::{
    aggregate_group_row_id, join_row_id, left_join_null_row_id, next_row_id, reserve_row_ids,
    right_join_null_row_id, set_next_row_id, set_next_row_id_if_greater, Row, RowId, DUMMY_ROW_ID,
};
pub use types::DataType;
pub use value::{JsonbValue, Value};
