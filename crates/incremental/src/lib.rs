//! Cynos Incremental - Incremental View Maintenance (IVM) for Cynos database.
//!
//! This crate implements Incremental View Maintenance based on Differential Dataflow
//! concepts. It allows query results to be updated incrementally when underlying
//! data changes, rather than recomputing from scratch.
//!
//! # Core Concepts
//!
//! - `Delta<T>`: Represents a change to data (+1 for insert, -1 for delete)
//! - `DiffCollection<T>`: A collection that tracks both snapshot and pending changes
//! - `DataflowNode`: Nodes in a dataflow graph representing query operations
//! - `MaterializedView`: A cached query result that updates incrementally
//!
//! # Incremental Operators
//!
//! - `filter_incremental`: Filters deltas based on a predicate
//! - `map_incremental`: Transforms deltas using a mapper function
//! - `project_incremental`: Projects specific columns from row deltas
//! - `IncrementalHashJoin`: Maintains join results incrementally
//! - `IncrementalCount/Sum/Avg/Min/Max`: Incremental aggregate functions
//!
//! # Example
//!
//! ```ignore
//! use cynos_incremental::{Delta, MaterializedView, DataflowNode};
//! use cynos_core::{Row, Value};
//!
//! // Create a dataflow: Source -> Filter(age > 18)
//! let dataflow = DataflowNode::filter(
//!     DataflowNode::source(1),
//!     |row| row.get(1).and_then(|v| v.as_i64()).map(|age| age > 18).unwrap_or(false)
//! );
//!
//! let mut view = MaterializedView::new(dataflow);
//!
//! // Insert a row that passes the filter
//! let deltas = vec![Delta::insert(Row::new(1, vec![Value::Int64(1), Value::Int64(25)]))];
//! let output = view.on_table_change(1, deltas);
//!
//! assert_eq!(output.len(), 1);
//! assert_eq!(view.len(), 1);
//! ```

#![no_std]

extern crate alloc;

pub mod collection;
pub mod dataflow;
pub mod delta;
pub mod materialize;
pub mod operators;
pub mod trace;

pub use collection::{ConsolidatedCollection, DiffCollection};
pub use dataflow::{
    AggregateType, ColumnId, DataflowGraph, DataflowNode, JoinKeySpec, JoinType, KeyExtractorFn,
    NodeId, TableId,
};
pub use delta::{Delta, DeltaBatch, DeltaBatchExt};
pub use materialize::{
    AggregateState, BootstrapExecutionProfile, CompiledBootstrapPlan, CompiledIvmPlan,
    GroupAggregateState, JoinState, MaterializedView, MaterializedViewBuilder,
};
pub use operators::{
    filter_incremental, map_incremental, project_incremental, IncrementalAvg, IncrementalCount,
    IncrementalHashJoin, IncrementalMax, IncrementalMin, IncrementalSum,
};
pub use trace::{TraceDeltaBatch, TraceTupleArena, TraceTupleHandle, VisibleResultStore};
