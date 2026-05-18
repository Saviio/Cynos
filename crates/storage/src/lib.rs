//! Cynos Storage - Storage layer for Cynos in-memory database.
//!
//! This crate provides the storage layer including:
//!
//! - `RowStore`: Row storage with index maintenance
//! - `TableCache`: Multi-table cache management
//! - `Journal`: Change tracking for transactions
//! - `Transaction`: Transaction management with rollback support
//! - `ConstraintChecker`: Constraint validation
//! - `LockManager`: Concurrent access control
//!
//! # Example
//!
//! ```rust
//! use cynos_storage::{TableCache, Transaction};
//! use cynos_core::schema::TableBuilder;
//! use cynos_core::{DataType, Row, Value};
//!
//! // Create a cache and table
//! let mut cache = TableCache::new();
//! let schema = TableBuilder::new("users")
//!     .unwrap()
//!     .add_column("id", DataType::Int64)
//!     .unwrap()
//!     .add_column("name", DataType::String)
//!     .unwrap()
//!     .add_primary_key(&["id"], false)
//!     .unwrap()
//!     .build()
//!     .unwrap();
//! cache.create_table(schema).unwrap();
//!
//! // Use a transaction
//! let mut tx = Transaction::begin();
//! let row = Row::new(1, vec![Value::Int64(1), Value::String("Alice".into())]);
//! tx.insert(&mut cache, "users", row).unwrap();
//! tx.commit().unwrap();
//!
//! assert_eq!(cache.get_table("users").unwrap().len(), 1);
//! ```

#![no_std]

extern crate alloc;

pub mod cache;
pub mod constraint;
pub mod journal;
pub mod lock;
#[cfg(feature = "benchmark")]
pub mod profiling;
pub mod row_store;
pub mod transaction;

pub use cache::TableCache;
pub use constraint::ConstraintChecker;
pub use journal::{Journal, JournalEntry, TableDiff};
pub use lock::{LockManager, LockType};
#[cfg(feature = "benchmark")]
pub use profiling::{StorageGinInsertProfile, StorageInsertProfile};
pub use row_store::{BTreeIndexStore, HashIndexStore, IndexStore, RowStore};
pub use transaction::{Transaction, TransactionId, TransactionState};
