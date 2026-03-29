//! Hash Join implementation.

use crate::executor::{Relation, RelationEntry, SharedTables, SqlValueRef};
use alloc::rc::Rc;
use alloc::sync::Arc;
use alloc::vec::Vec;
use cynos_core::{join_row_id, left_join_null_row_id, Row, Value};
use hashbrown::HashMap;

/// Hash Join executor.
///
/// Implements the classic hash join algorithm:
/// 1. Build phase: Create a hash table from the smaller relation
/// 2. Probe phase: Scan the larger relation and probe the hash table
pub struct HashJoin {
    /// Column index for the left (build) relation.
    left_key_index: usize,
    /// Column index for the right (probe) relation.
    right_key_index: usize,
    /// Whether this is an outer join.
    is_outer_join: bool,
}

impl HashJoin {
    /// Creates a new hash join executor.
    pub fn new(left_key_index: usize, right_key_index: usize, is_outer_join: bool) -> Self {
        Self {
            left_key_index,
            right_key_index,
            is_outer_join,
        }
    }

    /// Creates an inner hash join.
    pub fn inner(left_key_index: usize, right_key_index: usize) -> Self {
        Self::new(left_key_index, right_key_index, false)
    }

    /// Creates a left outer hash join.
    pub fn left_outer(left_key_index: usize, right_key_index: usize) -> Self {
        Self::new(left_key_index, right_key_index, true)
    }

    /// Executes the hash join.
    pub fn execute(&self, left: Relation, right: Relation) -> Relation {
        // Determine which relation to use for build vs probe
        // For outer join, we must use right for build (to preserve all left rows)
        let (build_rel, probe_rel, build_key_idx, probe_key_idx, swap) = if self.is_outer_join {
            (
                &right,
                &left,
                self.right_key_index,
                self.left_key_index,
                true,
            )
        } else if left.len() <= right.len() {
            (
                &left,
                &right,
                self.left_key_index,
                self.right_key_index,
                false,
            )
        } else {
            (
                &right,
                &left,
                self.right_key_index,
                self.left_key_index,
                true,
            )
        };

        // Build phase: create hash table mapping key values to entry indices
        let mut hash_table: HashMap<SqlValueRef<'_>, Vec<u32>> =
            HashMap::with_capacity(build_rel.len());

        for (idx, entry) in build_rel.entries.iter().enumerate() {
            if let Some(key_value) = entry.get_field(build_key_idx) {
                if !key_value.is_null() {
                    hash_table
                        .entry(SqlValueRef::new(key_value))
                        .or_default()
                        .push(idx as u32);
                }
            }
        }

        // Probe phase
        let build_col_count = build_rel.entries.first().map(|e| e.row.len()).unwrap_or(0);
        let probe_col_count = probe_rel.entries.first().map(|e| e.row.len()).unwrap_or(0);
        let total_col_count = if swap {
            probe_col_count + build_col_count
        } else {
            build_col_count + probe_col_count
        };

        // Pre-compute combined tables once (shared via Arc)
        let combined_tables: SharedTables = if swap {
            let mut t = probe_rel.tables.clone();
            t.extend(build_rel.tables.iter().cloned());
            Arc::from(t)
        } else {
            let mut t = build_rel.tables.clone();
            t.extend(probe_rel.tables.iter().cloned());
            Arc::from(t)
        };

        // Estimate result size for pre-allocation
        let avg_matches_per_key = if !hash_table.is_empty() {
            build_rel.len() / hash_table.len()
        } else {
            1
        };
        let estimated_matches = probe_rel.len() * avg_matches_per_key;
        let mut result_entries = Vec::with_capacity(estimated_matches);

        for probe_entry in probe_rel.entries.iter() {
            let key_value = probe_entry.get_field(probe_key_idx);
            let mut matched = false;

            if let Some(kv) = key_value {
                if !kv.is_null() {
                    if let Some(build_indices) = hash_table.get(&SqlValueRef::new(kv)) {
                        matched = true;
                        for &build_idx in build_indices {
                            let build_entry = &build_rel.entries[build_idx as usize];

                            // Inline combine to avoid function call overhead
                            let mut values = Vec::with_capacity(total_col_count);
                            // Compute sum version for JOIN result
                            let combined_version = if swap {
                                values.extend(probe_entry.row.values().iter().cloned());
                                values.extend(build_entry.row.values().iter().cloned());
                                probe_entry
                                    .row
                                    .version()
                                    .wrapping_add(build_entry.row.version())
                            } else {
                                values.extend(build_entry.row.values().iter().cloned());
                                values.extend(probe_entry.row.values().iter().cloned());
                                build_entry
                                    .row
                                    .version()
                                    .wrapping_add(probe_entry.row.version())
                            };

                            result_entries.push(RelationEntry::new_combined(
                                Rc::new(Row::new_with_version(
                                    if swap {
                                        join_row_id(probe_entry.row.id(), build_entry.row.id())
                                    } else {
                                        join_row_id(build_entry.row.id(), probe_entry.row.id())
                                    },
                                    combined_version,
                                    values,
                                )),
                                Arc::clone(&combined_tables),
                            ));
                        }
                    }
                }
            }

            // For outer join, add unmatched probe entries with nulls
            if self.is_outer_join && !matched {
                let mut values = Vec::with_capacity(total_col_count);
                values.extend(probe_entry.row.values().iter().cloned());
                values.resize(total_col_count, Value::Null);
                // For unmatched rows, use probe's version (the other side is NULL)
                let combined_version = probe_entry.row.version();

                result_entries.push(RelationEntry::new_combined(
                    Rc::new(Row::new_with_version(
                        left_join_null_row_id(probe_entry.row.id()),
                        combined_version,
                        values,
                    )),
                    Arc::clone(&combined_tables),
                ));
            }
        }

        // Compute combined table column counts
        let combined_column_counts: Vec<usize> = if swap {
            let mut counts = probe_rel.table_column_counts.clone();
            counts.extend(build_rel.table_column_counts.iter().cloned());
            counts
        } else {
            let mut counts = build_rel.table_column_counts.clone();
            counts.extend(probe_rel.table_column_counts.iter().cloned());
            counts
        };

        Relation {
            entries: result_entries,
            tables: combined_tables.to_vec(),
            table_column_counts: combined_column_counts,
        }
    }
}

/// Performs a hash join using key extraction functions.
#[allow(dead_code)]
pub fn hash_join<L, R, K, O, LK, RK, OF>(
    left: &[L],
    right: &[R],
    left_key: LK,
    right_key: RK,
    output_fn: OF,
) -> Vec<O>
where
    K: Eq + core::hash::Hash + Clone,
    LK: Fn(&L) -> K,
    RK: Fn(&R) -> K,
    OF: Fn(&L, &R) -> O,
{
    // Build phase
    let mut hash_table: HashMap<K, Vec<&L>> = HashMap::new();
    for item in left {
        let key = left_key(item);
        hash_table.entry(key).or_default().push(item);
    }

    // Probe phase
    let mut results = Vec::new();
    for item in right {
        let key = right_key(item);
        if let Some(matches) = hash_table.get(&key) {
            for left_item in matches {
                results.push(output_fn(left_item, item));
            }
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use cynos_core::Row;

    #[test]
    fn test_hash_join_inner() {
        let left_rows = vec![
            Row::new(0, vec![Value::Int64(1), Value::String("A".into())]),
            Row::new(1, vec![Value::Int64(2), Value::String("B".into())]),
            Row::new(2, vec![Value::Int64(3), Value::String("C".into())]),
        ];
        let right_rows = vec![
            Row::new(10, vec![Value::Int64(1), Value::String("X".into())]),
            Row::new(11, vec![Value::Int64(2), Value::String("Y".into())]),
            Row::new(12, vec![Value::Int64(4), Value::String("Z".into())]),
        ];

        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let join = HashJoin::inner(0, 0);
        let result = join.execute(left, right);

        // Should match on keys 1 and 2
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_hash_join_left_outer() {
        let left_rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Int64(2)]),
            Row::new(2, vec![Value::Int64(3)]),
        ];
        let right_rows = vec![
            Row::new(10, vec![Value::Int64(1)]),
            Row::new(11, vec![Value::Int64(4)]),
        ];

        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let join = HashJoin::left_outer(0, 0);
        let result = join.execute(left, right);

        // Should have 3 rows: 1 match + 2 unmatched left rows with nulls
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_hash_join_function() {
        let left = vec![(1, "A"), (2, "B"), (3, "C")];
        let right = vec![(1, "X"), (2, "Y"), (4, "Z")];

        let result = hash_join(&left, &right, |l| l.0, |r| r.0, |l, r| (l.1, r.1));

        assert_eq!(result.len(), 2);
        assert!(result.contains(&("A", "X")));
        assert!(result.contains(&("B", "Y")));
    }

    #[test]
    fn test_hash_join_with_nulls() {
        let left_rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Null]),
        ];
        let right_rows = vec![
            Row::new(10, vec![Value::Int64(1)]),
            Row::new(11, vec![Value::Null]),
        ];

        let left = Relation::from_rows_owned(left_rows, vec!["left".into()]);
        let right = Relation::from_rows_owned(right_rows, vec!["right".into()]);

        let join = HashJoin::inner(0, 0);
        let result = join.execute(left, right);

        // NULL values should not match
        assert_eq!(result.len(), 1);
    }
}
