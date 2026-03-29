//! Dataflow graph for Incremental View Maintenance.
//!
//! This module provides the dataflow graph abstraction for propagating
//! incremental changes through a query plan.

mod graph;
pub mod node;

pub use graph::{DataflowGraph, NodeId};
pub use node::{
    AggregateType, ColumnId, DataflowNode, JoinKeySpec, JoinType, KeyExtractorFn, TableId,
};
