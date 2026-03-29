//! Cynos Index - Index implementations for Cynos database.
//!
//! This crate provides various index implementations:
//!
//! - `HashIndex`: O(1) point queries using hash map
//! - `BTreeIndex`: Efficient range queries using B+Tree
//! - `GinIndex`: Inverted index for JSONB and composite types
//!
//! # Example
//!
//! ```rust
//! use cynos_index::{BTreeIndex, HashIndex, Index, RangeIndex, KeyRange};
//!
//! // Create a B+Tree index
//! let mut btree: BTreeIndex<i32> = BTreeIndex::new(64, true);
//! btree.add(10, 100).unwrap();
//! btree.add(20, 200).unwrap();
//! btree.add(5, 50).unwrap();
//!
//! // Point query
//! assert_eq!(btree.get(&10), vec![100]);
//!
//! // Range query
//! let range = KeyRange::lower_bound(10, false);
//! let results = btree.get_range(Some(&range), false, None, 0);
//! assert_eq!(results, vec![100, 200]);
//!
//! // Create a Hash index
//! let mut hash: HashIndex<i32> = HashIndex::new(true);
//! hash.add(10, 100).unwrap();
//! assert_eq!(hash.get(&10), vec![100]);
//! ```

#![no_std]

extern crate alloc;

pub mod btree;
pub mod comparator;
pub mod gin;
pub mod hash;
pub mod nullable;
pub mod stats;
pub mod traits;

pub use btree::BTreeIndex;
pub use comparator::{
    Comparator, MultiKeyComparator, MultiKeyComparatorWithNull, Order, SimpleComparator,
};
pub use gin::{
    contains_trigram_key, contains_trigram_pairs, contains_trigrams, GinBulkBuilder, GinIndex,
    PostingList,
};
pub use hash::HashIndex;
pub use nullable::NullableIndex;
pub use stats::IndexStats;
pub use traits::{Index, IndexError, KeyRange, RangeIndex};
