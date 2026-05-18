//! Materialized view for Incremental View Maintenance.
//!
//! Based on DBSP theory: each relational operator is lifted to work on Z-sets
//! (multisets with integer multiplicities). The materialized view maintains
//! the current result and propagates deltas through the dataflow graph.

use crate::dataflow::node::JoinType;
use crate::dataflow::{AggregateType, ColumnId, DataflowNode, JoinKeySpec, TableId};
use crate::delta::Delta;
use crate::operators::{filter_incremental, map_incremental, project_incremental};
use crate::trace::{TraceDeltaBatch, TraceTupleArena, TraceTupleHandle, VisibleResultStore};
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::rc::Rc;
use alloc::vec;
use alloc::vec::Vec;
use core::mem;
use cynos_core::{aggregate_group_row_id, Row, RowId, Value};
use hashbrown::HashMap;

// Tiny flushes are common for live updates; scan them linearly to avoid HashMap allocation.
const JOIN_NORMALIZE_LINEAR_SCAN_LIMIT: usize = 16;

// ---------------------------------------------------------------------------
// JoinState — supports Inner, Left, Right, Full Outer joins via DBSP
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum JoinKey {
    Empty,
    One(Value),
    Many(Vec<Value>),
}

impl JoinKey {
    fn from_vec(mut values: Vec<Value>) -> Self {
        match values.len() {
            0 => Self::Empty,
            1 => Self::One(values.pop().unwrap()),
            _ => Self::Many(values),
        }
    }
}

#[derive(Clone, Debug)]
struct JoinSlot {
    row: TraceTupleHandle,
    key: JoinKey,
    bucket_pos: usize,
    match_count: usize,
}

#[derive(Default)]
struct JoinSideState {
    buckets: HashMap<JoinKey, Vec<usize>>,
    row_to_slot: HashMap<RowId, usize>,
    slots: Vec<Option<JoinSlot>>,
    free_list: Vec<usize>,
    col_count: usize,
}

impl JoinSideState {
    fn with_col_count(col_count: usize) -> Self {
        Self {
            col_count,
            ..Self::default()
        }
    }

    #[inline]
    fn ensure_col_count(&mut self, row: &TraceTupleHandle) {
        if self.col_count == 0 {
            self.col_count = row.len();
        }
    }

    fn insert(&mut self, row: TraceTupleHandle, key: JoinKey, match_count: usize) -> usize {
        self.ensure_col_count(&row);
        let row_id = row.row_id();
        let bucket = self.buckets.entry(key.clone()).or_default();
        let bucket_pos = bucket.len();

        let slot = JoinSlot {
            row,
            key,
            bucket_pos,
            match_count,
        };

        let slot_id = if let Some(slot_id) = self.free_list.pop() {
            self.slots[slot_id] = Some(slot);
            slot_id
        } else {
            self.slots.push(Some(slot));
            self.slots.len() - 1
        };

        bucket.push(slot_id);
        self.row_to_slot.insert(row_id, slot_id);
        slot_id
    }

    fn remove_by_row_id(&mut self, row_id: RowId) -> Option<JoinSlot> {
        let slot_id = self.row_to_slot.remove(&row_id)?;
        let (bucket_key, expected_pos) = {
            let slot = self.slots.get(slot_id)?.as_ref()?;
            (slot.key.clone(), slot.bucket_pos)
        };

        let mut bucket_should_remove = false;
        let mut actual_pos = None;
        if let Some(bucket) = self.buckets.get_mut(&bucket_key) {
            actual_pos = if expected_pos < bucket.len()
                && bucket.get(expected_pos).copied() == Some(slot_id)
            {
                Some(expected_pos)
            } else {
                repair_bucket_positions(bucket, &mut self.slots, &bucket_key);
                bucket.iter().position(|candidate| *candidate == slot_id)
            };
        }

        let slot = self.slots.get_mut(slot_id)?.take()?;

        if let Some(bucket) = self.buckets.get_mut(&bucket_key) {
            let remove_pos = actual_pos.unwrap_or_else(|| {
                repair_bucket_positions(bucket, &mut self.slots, &bucket_key);
                bucket
                    .iter()
                    .position(|candidate| *candidate == slot_id)
                    .unwrap_or(bucket.len())
            });
            if remove_pos >= bucket.len() {
                bucket_should_remove = bucket.is_empty();
            } else {
                let swapped = bucket.swap_remove(remove_pos);
                if swapped != slot_id {
                    if let Some(moved_slot) = self.slots.get_mut(swapped).and_then(Option::as_mut) {
                        moved_slot.bucket_pos = remove_pos;
                    } else {
                        repair_bucket_positions(bucket, &mut self.slots, &bucket_key);
                    }
                }
                bucket_should_remove = bucket.is_empty();
            }
        }

        if bucket_should_remove {
            self.buckets.remove(&bucket_key);
        }

        self.free_list.push(slot_id);
        Some(slot)
    }

    fn collect_match_ids(&self, key: &JoinKey, out: &mut Vec<usize>) {
        out.clear();
        if let Some(bucket) = self.buckets.get(key) {
            out.extend(bucket.iter().copied());
        }
    }

    #[inline]
    fn slot(&self, slot_id: usize) -> &JoinSlot {
        self.slots[slot_id]
            .as_ref()
            .expect("join slot should exist while referenced")
    }

    #[inline]
    fn slot_mut(&mut self, slot_id: usize) -> &mut JoinSlot {
        self.slots[slot_id]
            .as_mut()
            .expect("join slot should exist while referenced")
    }

    #[inline]
    fn len(&self) -> usize {
        self.row_to_slot.len()
    }
}

fn repair_bucket_positions(bucket: &mut Vec<usize>, slots: &mut [Option<JoinSlot>], key: &JoinKey) {
    let mut write = 0usize;
    for read in 0..bucket.len() {
        let slot_id = bucket[read];
        let Some(slot) = slots.get_mut(slot_id).and_then(Option::as_mut) else {
            continue;
        };
        if slot.key != *key || slot.bucket_pos == usize::MAX {
            continue;
        }
        slot.bucket_pos = usize::MAX;
        bucket[write] = slot_id;
        write += 1;
    }
    bucket.truncate(write);

    for (bucket_pos, slot_id) in bucket.iter().copied().enumerate() {
        if let Some(slot) = slots.get_mut(slot_id).and_then(Option::as_mut) {
            slot.bucket_pos = bucket_pos;
        }
    }
}

/// State for incremental join operations.
/// Maintains per-side slot arenas and key buckets for O(1) delete bookkeeping.
pub struct JoinState {
    left: JoinSideState,
    right: JoinSideState,
    match_scratch: Vec<usize>,
    normalize_scratch: JoinNormalizeScratch,
}

impl JoinState {
    pub fn new() -> Self {
        Self {
            left: JoinSideState::default(),
            right: JoinSideState::default(),
            match_scratch: Vec::new(),
            normalize_scratch: JoinNormalizeScratch::default(),
        }
    }

    /// Creates a new join state with known column counts.
    /// Required for outer joins to correctly pad NULL columns.
    pub fn with_col_counts(left_col_count: usize, right_col_count: usize) -> Self {
        Self {
            left: JoinSideState::with_col_count(left_col_count),
            right: JoinSideState::with_col_count(right_col_count),
            match_scratch: Vec::new(),
            normalize_scratch: JoinNormalizeScratch::default(),
        }
    }

    pub fn on_left_insert(&mut self, row: Row, key: Vec<Value>) -> Vec<Row> {
        let arena = TraceTupleArena;
        let mut deltas = Vec::new();
        self.process_left_insert(
            arena.owned(row),
            JoinKey::from_vec(key),
            JoinType::Inner,
            &arena,
            &mut deltas,
        );
        deltas
            .into_iter()
            .filter(|delta| delta.is_insert())
            .map(|delta| arena.materialize_row(&delta.data))
            .collect()
    }

    pub fn on_left_delete(&mut self, row: &Row, _key: Vec<Value>) -> Vec<Row> {
        let arena = TraceTupleArena;
        let mut deltas = Vec::new();
        self.process_left_delete(row.id(), JoinType::Inner, &arena, &mut deltas);
        deltas
            .into_iter()
            .filter(|delta| delta.is_delete())
            .map(|delta| arena.materialize_row(&delta.data))
            .collect()
    }

    pub fn on_right_insert(&mut self, row: Row, key: Vec<Value>) -> Vec<Row> {
        let arena = TraceTupleArena;
        let mut deltas = Vec::new();
        self.process_right_insert(
            arena.owned(row),
            JoinKey::from_vec(key),
            JoinType::Inner,
            &arena,
            &mut deltas,
        );
        deltas
            .into_iter()
            .filter(|delta| delta.is_insert())
            .map(|delta| arena.materialize_row(&delta.data))
            .collect()
    }

    pub fn on_right_delete(&mut self, row: &Row, _key: Vec<Value>) -> Vec<Row> {
        let arena = TraceTupleArena;
        let mut deltas = Vec::new();
        self.process_right_delete(row.id(), JoinType::Inner, &arena, &mut deltas);
        deltas
            .into_iter()
            .filter(|delta| delta.is_delete())
            .map(|delta| arena.materialize_row(&delta.data))
            .collect()
    }

    pub fn left_count(&self) -> usize {
        self.left.len()
    }

    pub fn right_count(&self) -> usize {
        self.right.len()
    }

    fn process_left_insert(
        &mut self,
        row: TraceTupleHandle,
        key: JoinKey,
        join_type: JoinType,
        arena: &TraceTupleArena,
        output: &mut Vec<Delta<TraceTupleHandle>>,
    ) {
        self.right.collect_match_ids(&key, &mut self.match_scratch);
        let match_count = self.match_scratch.len();
        let slot_id = self.left.insert(row, key, match_count);

        if match_count == 0 {
            if emits_unmatched_left(join_type) {
                let left_row = &self.left.slot(slot_id).row;
                output.push(Delta::insert(
                    arena.null_pad_right(left_row.clone(), self.right.col_count),
                ));
            }
            return;
        }

        for right_id in self.match_scratch.iter().copied() {
            let emit_unmatched_delete = {
                let left_row = &self.left.slot(slot_id).row;
                let right_slot = self.right.slot(right_id);
                output.push(Delta::insert(arena.concat(
                    left_row.clone(),
                    right_slot.row.clone(),
                    self.left.col_count,
                )));
                emits_unmatched_right(join_type) && right_slot.match_count == 0
            };

            if emit_unmatched_delete {
                let right_row = &self.right.slot(right_id).row;
                output.push(Delta::delete(
                    arena.null_pad_left(right_row.clone(), self.left.col_count),
                ));
            }

            self.right.slot_mut(right_id).match_count += 1;
        }
    }

    fn process_left_delete(
        &mut self,
        row_id: RowId,
        join_type: JoinType,
        arena: &TraceTupleArena,
        output: &mut Vec<Delta<TraceTupleHandle>>,
    ) {
        let Some(slot) = self.left.remove_by_row_id(row_id) else {
            return;
        };

        if slot.match_count == 0 {
            if emits_unmatched_left(join_type) {
                output.push(Delta::delete(
                    arena.null_pad_right(slot.row.clone(), self.right.col_count),
                ));
            }
            return;
        }

        self.right
            .collect_match_ids(&slot.key, &mut self.match_scratch);
        for right_id in self.match_scratch.iter().copied() {
            let emit_unmatched_insert = {
                let right_slot = self.right.slot(right_id);
                output.push(Delta::delete(arena.concat(
                    slot.row.clone(),
                    right_slot.row.clone(),
                    self.left.col_count,
                )));
                emits_unmatched_right(join_type) && right_slot.match_count == 1
            };

            {
                let right_slot = self.right.slot_mut(right_id);
                right_slot.match_count = right_slot.match_count.saturating_sub(1);
            }

            if emit_unmatched_insert {
                let right_row = &self.right.slot(right_id).row;
                output.push(Delta::insert(
                    arena.null_pad_left(right_row.clone(), self.left.col_count),
                ));
            }
        }
    }

    fn process_left_update_same_key(
        &mut self,
        old_row: &TraceTupleHandle,
        new_row: TraceTupleHandle,
        key: JoinKey,
        join_type: JoinType,
        arena: &TraceTupleArena,
        output: &mut Vec<Delta<TraceTupleHandle>>,
    ) {
        let Some(&slot_id) = self.left.row_to_slot.get(&old_row.row_id()) else {
            self.process_left_insert(new_row, key, join_type, arena, output);
            return;
        };

        let match_count = self.left.slot(slot_id).match_count;
        if match_count == 0 {
            if emits_unmatched_left(join_type) {
                output.push(Delta::delete(
                    arena.null_pad_right(old_row.clone(), self.right.col_count),
                ));
                output.push(Delta::insert(
                    arena.null_pad_right(new_row.clone(), self.right.col_count),
                ));
            }
            self.left.slot_mut(slot_id).row = new_row;
            return;
        }

        self.right.collect_match_ids(&key, &mut self.match_scratch);
        for right_id in self.match_scratch.iter().copied() {
            let right_row = &self.right.slot(right_id).row;
            output.push(Delta::delete(arena.concat(
                old_row.clone(),
                right_row.clone(),
                self.left.col_count,
            )));
            output.push(Delta::insert(arena.concat(
                new_row.clone(),
                right_row.clone(),
                self.left.col_count,
            )));
        }

        self.left.slot_mut(slot_id).row = new_row;
    }

    fn process_right_insert(
        &mut self,
        row: TraceTupleHandle,
        key: JoinKey,
        join_type: JoinType,
        arena: &TraceTupleArena,
        output: &mut Vec<Delta<TraceTupleHandle>>,
    ) {
        self.left.collect_match_ids(&key, &mut self.match_scratch);
        let match_count = self.match_scratch.len();
        let slot_id = self.right.insert(row, key, match_count);

        if match_count == 0 {
            if emits_unmatched_right(join_type) {
                let right_row = &self.right.slot(slot_id).row;
                output.push(Delta::insert(
                    arena.null_pad_left(right_row.clone(), self.left.col_count),
                ));
            }
            return;
        }

        for left_id in self.match_scratch.iter().copied() {
            let emit_unmatched_delete = {
                let left_slot = self.left.slot(left_id);
                let right_row = &self.right.slot(slot_id).row;
                output.push(Delta::insert(arena.concat(
                    left_slot.row.clone(),
                    right_row.clone(),
                    self.left.col_count,
                )));
                emits_unmatched_left(join_type) && left_slot.match_count == 0
            };

            if emit_unmatched_delete {
                let left_row = &self.left.slot(left_id).row;
                output.push(Delta::delete(
                    arena.null_pad_right(left_row.clone(), self.right.col_count),
                ));
            }

            self.left.slot_mut(left_id).match_count += 1;
        }
    }

    fn process_right_delete(
        &mut self,
        row_id: RowId,
        join_type: JoinType,
        arena: &TraceTupleArena,
        output: &mut Vec<Delta<TraceTupleHandle>>,
    ) {
        let Some(slot) = self.right.remove_by_row_id(row_id) else {
            return;
        };

        if slot.match_count == 0 {
            if emits_unmatched_right(join_type) {
                output.push(Delta::delete(
                    arena.null_pad_left(slot.row.clone(), self.left.col_count),
                ));
            }
            return;
        }

        self.left
            .collect_match_ids(&slot.key, &mut self.match_scratch);
        for left_id in self.match_scratch.iter().copied() {
            let emit_unmatched_insert = {
                let left_slot = self.left.slot(left_id);
                output.push(Delta::delete(arena.concat(
                    left_slot.row.clone(),
                    slot.row.clone(),
                    self.left.col_count,
                )));
                emits_unmatched_left(join_type) && left_slot.match_count == 1
            };

            {
                let left_slot = self.left.slot_mut(left_id);
                left_slot.match_count = left_slot.match_count.saturating_sub(1);
            }

            if emit_unmatched_insert {
                let left_row = &self.left.slot(left_id).row;
                output.push(Delta::insert(
                    arena.null_pad_right(left_row.clone(), self.right.col_count),
                ));
            }
        }
    }

    fn process_right_update_same_key(
        &mut self,
        old_row: &TraceTupleHandle,
        new_row: TraceTupleHandle,
        key: JoinKey,
        join_type: JoinType,
        arena: &TraceTupleArena,
        output: &mut Vec<Delta<TraceTupleHandle>>,
    ) {
        let Some(&slot_id) = self.right.row_to_slot.get(&old_row.row_id()) else {
            self.process_right_insert(new_row, key, join_type, arena, output);
            return;
        };

        let match_count = self.right.slot(slot_id).match_count;
        if match_count == 0 {
            if emits_unmatched_right(join_type) {
                output.push(Delta::delete(
                    arena.null_pad_left(old_row.clone(), self.left.col_count),
                ));
                output.push(Delta::insert(
                    arena.null_pad_left(new_row.clone(), self.left.col_count),
                ));
            }
            self.right.slot_mut(slot_id).row = new_row;
            return;
        }

        self.left.collect_match_ids(&key, &mut self.match_scratch);
        for left_id in self.match_scratch.iter().copied() {
            let left_row = &self.left.slot(left_id).row;
            output.push(Delta::delete(arena.concat(
                left_row.clone(),
                old_row.clone(),
                self.left.col_count,
            )));
            output.push(Delta::insert(arena.concat(
                left_row.clone(),
                new_row.clone(),
                self.left.col_count,
            )));
        }

        self.right.slot_mut(slot_id).row = new_row;
    }

    fn on_left_insert_outer(
        &mut self,
        row: TraceTupleHandle,
        key: JoinKey,
        join_type: JoinType,
        arena: &TraceTupleArena,
        output: &mut Vec<Delta<TraceTupleHandle>>,
    ) {
        self.process_left_insert(row, key, join_type, arena, output);
    }

    fn on_left_delete_outer(
        &mut self,
        row: &TraceTupleHandle,
        _key: JoinKey,
        join_type: JoinType,
        arena: &TraceTupleArena,
        output: &mut Vec<Delta<TraceTupleHandle>>,
    ) {
        self.process_left_delete(row.row_id(), join_type, arena, output);
    }

    fn on_right_insert_outer(
        &mut self,
        row: TraceTupleHandle,
        key: JoinKey,
        join_type: JoinType,
        arena: &TraceTupleArena,
        output: &mut Vec<Delta<TraceTupleHandle>>,
    ) {
        self.process_right_insert(row, key, join_type, arena, output);
    }

    fn on_right_delete_outer(
        &mut self,
        row: &TraceTupleHandle,
        _key: JoinKey,
        join_type: JoinType,
        arena: &TraceTupleArena,
        output: &mut Vec<Delta<TraceTupleHandle>>,
    ) {
        self.process_right_delete(row.row_id(), join_type, arena, output);
    }

    fn finalize_bootstrap_match_counts(&mut self) {
        for slot in self.left.slots.iter_mut().filter_map(Option::as_mut) {
            slot.match_count = 0;
        }
        for slot in self.right.slots.iter_mut().filter_map(Option::as_mut) {
            slot.match_count = 0;
        }

        let (left_slots, left_buckets) = (&mut self.left.slots, &self.left.buckets);
        let (right_slots, right_buckets) = (&mut self.right.slots, &self.right.buckets);

        for (key, left_bucket) in left_buckets.iter() {
            let Some(right_bucket) = right_buckets.get(key) else {
                continue;
            };
            let right_matches = right_bucket.len();
            let left_matches = left_bucket.len();
            for &left_id in left_bucket {
                if let Some(slot) = left_slots.get_mut(left_id).and_then(Option::as_mut) {
                    slot.match_count = right_matches;
                }
            }

            for &right_id in right_bucket {
                if let Some(slot) = right_slots.get_mut(right_id).and_then(Option::as_mut) {
                    slot.match_count = left_matches;
                }
            }
        }
    }

    fn emit_bootstrap_rows<F>(&self, join_type: JoinType, arena: &TraceTupleArena, emit: &mut F)
    where
        F: FnMut(TraceTupleHandle) + ?Sized,
    {
        for left_slot in self.left.slots.iter().filter_map(Option::as_ref) {
            if let Some(right_bucket) = self.right.buckets.get(&left_slot.key) {
                for &right_id in right_bucket {
                    let right_slot = self.right.slot(right_id);
                    emit(arena.concat(
                        left_slot.row.clone(),
                        right_slot.row.clone(),
                        self.left.col_count,
                    ));
                }
            } else if emits_unmatched_left(join_type) {
                emit(arena.null_pad_right(left_slot.row.clone(), self.right.col_count));
            }
        }

        if emits_unmatched_right(join_type) {
            for right_slot in self.right.slots.iter().filter_map(Option::as_ref) {
                if right_slot.match_count == 0 {
                    emit(arena.null_pad_left(right_slot.row.clone(), self.left.col_count));
                }
            }
        }
    }
}

impl Default for JoinState {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
/// Test/compat helper: materializes a full joined row from two concrete rows.
fn merge_rows(left: &Row, right: &Row) -> Row {
    let mut values = Vec::with_capacity(left.len().saturating_add(right.len()));
    values.extend(left.values().iter().cloned());
    values.extend(right.values().iter().cloned());
    Row::new_with_version(
        cynos_core::join_row_id(left.id(), right.id()),
        left.version().wrapping_add(right.version()),
        values,
    )
}

#[allow(dead_code)]
/// Test/compat helper: materializes a left row with NULLs on the right side.
fn merge_rows_null_right(left: &Row, right_col_count: usize) -> Row {
    let mut values = Vec::with_capacity(left.len().saturating_add(right_col_count));
    values.extend(left.values().iter().cloned());
    values.resize(values.len().saturating_add(right_col_count), Value::Null);
    Row::new_with_version(
        cynos_core::left_join_null_row_id(left.id()),
        left.version(),
        values,
    )
}

#[allow(dead_code)]
/// Test/compat helper: materializes a right row with NULLs on the left side.
fn merge_rows_null_left(right: &Row, left_col_count: usize) -> Row {
    let mut values = Vec::with_capacity(left_col_count.saturating_add(right.len()));
    values.resize(left_col_count, Value::Null);
    values.extend(right.values().iter().cloned());
    Row::new_with_version(
        cynos_core::right_join_null_row_id(right.id()),
        right.version(),
        values,
    )
}

#[inline]
fn emits_unmatched_left(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::LeftOuter | JoinType::FullOuter)
}

#[inline]
fn emits_unmatched_right(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::RightOuter | JoinType::FullOuter)
}

// ---------------------------------------------------------------------------
// AggregateState — DBSP-based incremental aggregation per group
// ---------------------------------------------------------------------------

/// Per-function aggregate state. Uses DBSP Z-set approach:
/// - COUNT/SUM/AVG: maintain running totals, O(1) per delta
/// - MIN/MAX: maintain ordered multiset (BTreeMap), O(log n) per delta
///   This eliminates the `needs_recompute` fallback entirely.
pub enum AggregateState {
    Count {
        count: i64,
    },
    Sum {
        sum: f64,
        count: i64,
    },
    Avg {
        sum: f64,
        count: i64,
    },
    /// BTreeMap<Value, multiplicity> — ordered multiset for O(log n) min on delete
    Min {
        values: BTreeMap<Value, i32>,
    },
    /// BTreeMap<Value, multiplicity> — ordered multiset for O(log n) max on delete
    Max {
        values: BTreeMap<Value, i32>,
    },
}

impl AggregateState {
    pub fn new(agg_type: AggregateType) -> Self {
        match agg_type {
            AggregateType::Count => AggregateState::Count { count: 0 },
            AggregateType::Sum => AggregateState::Sum { sum: 0.0, count: 0 },
            AggregateType::Avg => AggregateState::Avg { sum: 0.0, count: 0 },
            AggregateType::Min => AggregateState::Min {
                values: BTreeMap::new(),
            },
            AggregateType::Max => AggregateState::Max {
                values: BTreeMap::new(),
            },
        }
    }

    /// Apply a single delta to this aggregate state.
    pub fn apply(&mut self, value: &Value, diff: i32) {
        match self {
            AggregateState::Count { count } => {
                *count += diff as i64;
            }
            AggregateState::Sum { sum, count } => {
                *sum += extract_numeric(value) * diff as f64;
                *count += diff as i64;
            }
            AggregateState::Avg { sum, count } => {
                *sum += extract_numeric(value) * diff as f64;
                *count += diff as i64;
            }
            AggregateState::Min { values } | AggregateState::Max { values } => {
                let entry = values.entry(value.clone()).or_insert(0);
                *entry += diff;
                if *entry <= 0 {
                    values.remove(value);
                }
            }
        }
    }

    /// Get the current aggregate value.
    pub fn get_value(&self) -> Value {
        match self {
            AggregateState::Count { count } => Value::Int64(*count),
            AggregateState::Sum { sum, .. } => Value::Float64(*sum),
            AggregateState::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Float64(*sum / *count as f64)
                }
            }
            AggregateState::Min { values } => values.keys().next().cloned().unwrap_or(Value::Null),
            AggregateState::Max { values } => {
                values.keys().next_back().cloned().unwrap_or(Value::Null)
            }
        }
    }

    /// Returns true if this aggregate group is empty (count dropped to 0).
    pub fn is_empty(&self) -> bool {
        match self {
            AggregateState::Count { count } => *count == 0,
            AggregateState::Avg { count, .. } => *count == 0,
            AggregateState::Sum { count, .. } => *count == 0,
            AggregateState::Min { values } | AggregateState::Max { values } => values.is_empty(),
        }
    }
}

/// State for incremental GROUP BY aggregation.
/// Maps group_key -> (per-function states, current output row).
pub struct GroupAggregateState {
    /// group_key_values -> Vec<AggregateState> (one per aggregate function)
    groups: HashMap<Vec<Value>, Vec<AggregateState>>,
    /// The aggregate function types (for creating new states)
    functions: Vec<(ColumnId, AggregateType)>,
    /// The group-by column indices
    group_by: Vec<ColumnId>,
    /// Track the last emitted row ID per group key, so deletes use the correct ID
    last_row_ids: HashMap<Vec<Value>, RowId>,
}

impl GroupAggregateState {
    pub fn new(group_by: Vec<ColumnId>, functions: Vec<(ColumnId, AggregateType)>) -> Self {
        Self {
            groups: HashMap::new(),
            functions,
            group_by,
            last_row_ids: HashMap::new(),
        }
    }

    /// Process a batch of input deltas and produce output deltas.
    /// For each affected group:
    ///   1. Emit delete(old_aggregate_row) if group existed
    ///   2. Update aggregate states
    ///   3. Emit insert(new_aggregate_row) if group still has data
    pub fn process_deltas(&mut self, deltas: &[Delta<Row>]) -> Vec<Delta<Row>> {
        // Collect deltas by group key
        let mut grouped: HashMap<Vec<Value>, Vec<(&Row, i32)>> = HashMap::new();
        for d in deltas {
            let key: Vec<Value> = self
                .group_by
                .iter()
                .map(|&col| d.data.get(col).cloned().unwrap_or(Value::Null))
                .collect();
            grouped.entry(key).or_default().push((&d.data, d.diff));
        }

        let mut output = Vec::new();

        for (key, rows) in grouped {
            let existed = self.groups.contains_key(&key);

            // Snapshot old value before update
            let old_row = if existed {
                Some(self.build_output_row(&key))
            } else {
                None
            };

            // Get or create group state
            let states = self.groups.entry(key.clone()).or_insert_with(|| {
                self.functions
                    .iter()
                    .map(|(_, agg_type)| AggregateState::new(*agg_type))
                    .collect()
            });

            // Apply all deltas for this group
            for (row, diff) in &rows {
                for (i, (col, _)) in self.functions.iter().enumerate() {
                    let value = row.get(*col).cloned().unwrap_or(Value::Null);
                    states[i].apply(&value, *diff);
                }
            }

            // Check if group is now empty
            let is_empty = states.iter().all(|s| s.is_empty());

            // Emit old row deletion if group existed (use tracked row ID)
            if let Some(&old_id) = self.last_row_ids.get(&key) {
                if let Some(old) = old_row {
                    let mut old_with_id = old;
                    old_with_id.set_id(old_id);
                    output.push(Delta::delete(old_with_id));
                }
            }

            // Emit new row insertion if group still has data
            if !is_empty {
                let new_row = self.build_output_row(&key);
                self.last_row_ids.insert(key.clone(), new_row.id());
                output.push(Delta::insert(new_row));
            } else {
                self.groups.remove(&key);
                self.last_row_ids.remove(&key);
            }
        }

        output
    }

    pub fn process_trace_deltas(
        &mut self,
        arena: &TraceTupleArena,
        deltas: &[Delta<TraceTupleHandle>],
    ) -> Vec<Delta<TraceTupleHandle>> {
        let mut grouped: HashMap<Vec<Value>, Vec<(&TraceTupleHandle, i32)>> = HashMap::new();
        for delta in deltas {
            let key = self
                .group_by
                .iter()
                .map(|&col| arena.value_at(&delta.data, col).unwrap_or(Value::Null))
                .collect::<Vec<_>>();
            grouped
                .entry(key)
                .or_default()
                .push((&delta.data, delta.diff));
        }

        let mut output = Vec::new();
        for (key, rows) in grouped {
            let existed = self.groups.contains_key(&key);
            let old_row = if existed {
                Some(self.build_output_row(&key))
            } else {
                None
            };

            let states = self.groups.entry(key.clone()).or_insert_with(|| {
                self.functions
                    .iter()
                    .map(|(_, agg_type)| AggregateState::new(*agg_type))
                    .collect()
            });

            for (handle, diff) in &rows {
                for (index, (col, _)) in self.functions.iter().enumerate() {
                    let value = arena.value_at(handle, *col).unwrap_or(Value::Null);
                    states[index].apply(&value, *diff);
                }
            }

            let is_empty = states.iter().all(|state| state.is_empty());

            if let Some(&old_id) = self.last_row_ids.get(&key) {
                if let Some(old) = old_row {
                    let mut old_with_id = old;
                    old_with_id.set_id(old_id);
                    output.push(Delta::delete(arena.owned(old_with_id)));
                }
            }

            if !is_empty {
                let new_row = self.build_output_row(&key);
                self.last_row_ids.insert(key.clone(), new_row.id());
                output.push(Delta::insert(arena.owned(new_row)));
            } else {
                self.groups.remove(&key);
                self.last_row_ids.remove(&key);
            }
        }

        output
    }

    fn apply_trace_bootstrap_handle(&mut self, arena: &TraceTupleArena, handle: &TraceTupleHandle) {
        let key = self
            .group_by
            .iter()
            .map(|&col| arena.value_at(handle, col).unwrap_or(Value::Null))
            .collect::<Vec<_>>();

        let states = self.groups.entry(key).or_insert_with(|| {
            self.functions
                .iter()
                .map(|(_, agg_type)| AggregateState::new(*agg_type))
                .collect()
        });

        for (index, (col, _)) in self.functions.iter().enumerate() {
            let value = arena.value_at(handle, *col).unwrap_or(Value::Null);
            states[index].apply(&value, 1);
        }
    }

    fn emit_bootstrap_rows<F>(&mut self, arena: &TraceTupleArena, emit: &mut F)
    where
        F: FnMut(TraceTupleHandle) + ?Sized,
    {
        let group_keys = self.groups.keys().cloned().collect::<Vec<_>>();
        for key in group_keys {
            let row = self.build_output_row(&key);
            self.last_row_ids.insert(key, row.id());
            emit(arena.owned(row));
        }
    }

    /// Build an output row from group key + aggregate values.
    fn build_output_row(&mut self, key: &[Value]) -> Row {
        let states = self.groups.get(key).unwrap();
        let mut values: Vec<Value> = key.to_vec();
        for state in states {
            values.push(state.get_value());
        }
        Row::new(aggregate_group_row_id(key), values)
    }
}

#[inline]
fn left_child_state_id(state_id: usize) -> usize {
    state_id.saturating_mul(2).saturating_add(1)
}

#[inline]
fn right_child_state_id(state_id: usize) -> usize {
    state_id.saturating_mul(2).saturating_add(2)
}

fn extract_join_key_handle(
    spec: &crate::dataflow::JoinKeySpec,
    arena: &TraceTupleArena,
    handle: &TraceTupleHandle,
) -> JoinKey {
    match spec {
        crate::dataflow::JoinKeySpec::Columns(indices) => match indices.as_slice() {
            [] => JoinKey::Empty,
            [index] => JoinKey::One(arena.value_at(handle, *index).unwrap_or(Value::Null)),
            _ => JoinKey::Many(
                indices
                    .iter()
                    .map(|&index| arena.value_at(handle, index).unwrap_or(Value::Null))
                    .collect(),
            ),
        },
        crate::dataflow::JoinKeySpec::Constant(values) => JoinKey::from_vec(values.clone()),
        crate::dataflow::JoinKeySpec::Dynamic(extractor) => {
            JoinKey::from_vec(extractor(&arena.materialize_rc(handle)))
        }
    }
}

#[allow(dead_code)]
#[derive(Clone)]
struct CompiledIvmNode {
    sources: Box<[TableId]>,
    kind: CompiledIvmNodeKind,
}

pub struct CompiledIvmPlan {
    node: CompiledIvmNode,
    program: Option<CompiledTraceProgram>,
}

type TraceSlotId = usize;

struct CompiledTraceProgram {
    instructions: Box<[TraceInstruction]>,
    slot_count: usize,
    root_slot: TraceSlotId,
    scratch_slots: Vec<Vec<Delta<TraceTupleHandle>>>,
}

#[derive(Clone)]
enum TraceInstruction {
    Source {
        table_id: TableId,
        output_slot: TraceSlotId,
    },
    Unary {
        input_slot: TraceSlotId,
        output_slot: TraceSlotId,
        block: UnaryFusionBlock,
    },
    Join {
        node_index: usize,
        state_id: usize,
        left_width: usize,
        right_width: usize,
        left_slot: TraceSlotId,
        right_slot: TraceSlotId,
        output_slot: TraceSlotId,
    },
    Aggregate {
        state_id: usize,
        input_slot: TraceSlotId,
        output_slot: TraceSlotId,
        group_by: Vec<ColumnId>,
        functions: Vec<(ColumnId, AggregateType)>,
    },
}

#[derive(Clone)]
struct UnaryFusionBlock {
    ops: Vec<TraceUnaryOp>,
    accepts_append: bool,
}

#[derive(Clone)]
enum TraceUnaryOp {
    Filter {
        node_index: usize,
        tuple_native: bool,
    },
    Project {
        columns: Rc<[usize]>,
    },
    Map {
        node_index: usize,
        tuple_native: bool,
    },
}

enum TraceRuntimeNode<'a> {
    Filter {
        predicate: &'a (dyn Fn(&Row) -> bool + Send + Sync),
        trace_predicate:
            Option<&'a (dyn Fn(&TraceTupleArena, &TraceTupleHandle) -> bool + Send + Sync)>,
    },
    Map {
        mapper: &'a (dyn Fn(&Row) -> Row + Send + Sync),
        trace_mapper:
            Option<&'a (dyn Fn(&TraceTupleArena, &TraceTupleHandle) -> Row + Send + Sync)>,
    },
    Join {
        left_key: &'a JoinKeySpec,
        right_key: &'a JoinKeySpec,
        join_type: JoinType,
    },
    Marker,
}

#[derive(Clone, Debug, Default)]
pub struct TraceUpdateProfile {
    pub source_dispatch_ms: f64,
    pub unary_execute_ms: f64,
    pub join_execute_ms: f64,
    pub aggregate_execute_ms: f64,
    pub result_apply_ms: f64,
}

#[derive(Clone, Debug, Default)]
pub struct BootstrapExecutionProfile {
    pub filter_bootstrap_ms: f64,
    pub project_bootstrap_ms: f64,
    pub map_bootstrap_ms: f64,
    pub join_bootstrap_ms: f64,
    pub aggregate_bootstrap_ms: f64,
    pub root_sink_ms: f64,
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct CompiledBootstrapPlan {
    legacy_node: CompiledBootstrapNode,
    program: CompiledBootstrapProgram,
}

#[derive(Clone)]
struct CompiledBootstrapProgram {
    instructions: Box<[BootstrapInstruction]>,
    slot_count: usize,
}

type BootstrapSlotId = usize;

#[derive(Clone)]
enum BootstrapInstruction {
    Source {
        node_index: usize,
        source_index: usize,
        output_slot: BootstrapSlotId,
    },
    Filter {
        node_index: usize,
        input_slot: BootstrapSlotId,
        output_slot: BootstrapSlotId,
        covered_source_index: Option<usize>,
    },
    Project {
        input_slot: BootstrapSlotId,
        output_slot: BootstrapSlotId,
        columns: Rc<[usize]>,
    },
    Map {
        node_index: usize,
        input_slot: BootstrapSlotId,
        output_slot: BootstrapSlotId,
    },
    Join {
        node_index: usize,
        state_id: usize,
        left_width: usize,
        right_width: usize,
        left_slot: BootstrapSlotId,
        right_slot: BootstrapSlotId,
        output_slot: Option<BootstrapSlotId>,
    },
    Aggregate {
        state_id: usize,
        input_slot: BootstrapSlotId,
        output_slot: Option<BootstrapSlotId>,
        group_by: Vec<ColumnId>,
        functions: Vec<(ColumnId, AggregateType)>,
    },
}

struct BootstrapCompileOutput {
    output_slot: Option<BootstrapSlotId>,
    direct_source_index: Option<usize>,
}

enum BootstrapRuntimeNode<'a> {
    Source {
        table_id: TableId,
    },
    Filter {
        predicate: &'a (dyn Fn(&Row) -> bool + Send + Sync),
        trace_predicate:
            Option<&'a (dyn Fn(&TraceTupleArena, &TraceTupleHandle) -> bool + Send + Sync)>,
    },
    Map {
        mapper: &'a (dyn Fn(&Row) -> Row + Send + Sync),
        trace_mapper:
            Option<&'a (dyn Fn(&TraceTupleArena, &TraceTupleHandle) -> Row + Send + Sync)>,
    },
    Join {
        left_key: &'a JoinKeySpec,
        right_key: &'a JoinKeySpec,
        join_type: JoinType,
    },
}

#[allow(dead_code)]
#[derive(Clone)]
struct CompiledBootstrapNode {
    kind: CompiledBootstrapNodeKind,
}

#[allow(dead_code)]
#[derive(Clone)]
enum CompiledBootstrapNodeKind {
    Source {
        source_index: usize,
    },
    Filter {
        input: Box<CompiledBootstrapNode>,
    },
    Project {
        input: Box<CompiledBootstrapNode>,
        columns: Rc<[usize]>,
    },
    Map {
        input: Box<CompiledBootstrapNode>,
    },
    Join {
        state_id: usize,
        left_width: usize,
        right_width: usize,
        left: Box<CompiledBootstrapNode>,
        right: Box<CompiledBootstrapNode>,
    },
    Aggregate {
        state_id: usize,
        input: Box<CompiledBootstrapNode>,
    },
}

#[allow(dead_code)]
#[derive(Clone)]
enum CompiledIvmNodeKind {
    Source,
    Filter {
        input: Box<CompiledIvmNode>,
    },
    Project {
        input: Box<CompiledIvmNode>,
        columns: Rc<[usize]>,
    },
    Map {
        input: Box<CompiledIvmNode>,
    },
    Join {
        state_id: usize,
        left: Box<CompiledIvmNode>,
        right: Box<CompiledIvmNode>,
    },
    Aggregate {
        state_id: usize,
        input: Box<CompiledIvmNode>,
    },
}

impl CompiledIvmPlan {
    pub fn compile(node: &DataflowNode) -> Self {
        Self {
            node: compile_ivm_node(node, 0),
            program: None,
        }
    }

    pub fn compile_with_trace_program(node: &DataflowNode) -> Self {
        let mut plan = Self::compile(node);
        plan.ensure_trace_program(node);
        plan
    }

    fn ensure_trace_program(&mut self, node: &DataflowNode) -> &mut CompiledTraceProgram {
        self.program
            .get_or_insert_with(|| compile_trace_program(node))
    }

    #[cfg(test)]
    fn trace_program(&self) -> Option<&CompiledTraceProgram> {
        self.program.as_ref()
    }

    #[inline]
    pub fn sources(&self) -> &[TableId] {
        &self.node.sources
    }

    #[inline]
    pub fn depends_on(&self, table_id: TableId) -> bool {
        self.node.sources.binary_search(&table_id).is_ok()
    }
}

fn compile_trace_program(node: &DataflowNode) -> CompiledTraceProgram {
    let mut next_trace_node_index = 0usize;
    let mut next_trace_slot = 0usize;
    let mut trace_instructions = Vec::new();
    let root_slot = compile_trace_program_node(
        node,
        0,
        &mut next_trace_node_index,
        &mut next_trace_slot,
        &mut trace_instructions,
    );
    CompiledTraceProgram {
        instructions: trace_instructions.into_boxed_slice(),
        slot_count: next_trace_slot,
        root_slot,
        scratch_slots: vec![Vec::new(); next_trace_slot],
    }
}

impl UnaryFusionBlock {
    fn new(op: TraceUnaryOp) -> Self {
        let accepts_append = op.can_append();
        Self {
            ops: vec![op],
            accepts_append,
        }
    }

    fn push(&mut self, op: TraceUnaryOp) {
        self.accepts_append = op.can_append();
        self.ops.push(op);
    }
}

impl TraceUnaryOp {
    #[inline]
    fn can_append(&self) -> bool {
        match self {
            Self::Project { .. } => true,
            Self::Filter { tuple_native, .. } | Self::Map { tuple_native, .. } => *tuple_native,
        }
    }
}

impl CompiledBootstrapPlan {
    pub fn compile(node: &DataflowNode) -> Self {
        let mut next_source_index = 0usize;
        let legacy_node = compile_bootstrap_node(node, 0, &mut next_source_index);
        let mut program_source_index = 0usize;
        let mut next_node_index = 0usize;
        let mut next_slot = 0usize;
        let mut instructions = Vec::new();
        compile_bootstrap_program_node(
            node,
            false,
            0,
            &mut next_node_index,
            &mut program_source_index,
            &mut next_slot,
            &mut instructions,
        );
        Self {
            legacy_node,
            program: CompiledBootstrapProgram {
                instructions: instructions.into_boxed_slice(),
                slot_count: next_slot,
            },
        }
    }
}

#[allow(dead_code)]
impl CompiledIvmNode {
    #[inline]
    fn sources(&self) -> &[TableId] {
        &self.sources
    }

    #[inline]
    fn depends_on(&self, table_id: TableId) -> bool {
        self.sources.binary_search(&table_id).is_ok()
    }
}

fn compile_ivm_node(node: &DataflowNode, state_id: usize) -> CompiledIvmNode {
    match node {
        DataflowNode::Source { table_id } => CompiledIvmNode {
            sources: alloc::vec![*table_id].into_boxed_slice(),
            kind: CompiledIvmNodeKind::Source,
        },
        DataflowNode::Filter { input, .. } => {
            let input = compile_ivm_node(input, state_id);
            CompiledIvmNode {
                sources: input.sources.clone(),
                kind: CompiledIvmNodeKind::Filter {
                    input: Box::new(input),
                },
            }
        }
        DataflowNode::Project { input, columns } => {
            let input = compile_ivm_node(input, state_id);
            CompiledIvmNode {
                sources: input.sources.clone(),
                kind: CompiledIvmNodeKind::Project {
                    input: Box::new(input),
                    columns: Rc::<[usize]>::from(columns.clone().into_boxed_slice()),
                },
            }
        }
        DataflowNode::Map { input, .. } => {
            let input = compile_ivm_node(input, state_id);
            CompiledIvmNode {
                sources: input.sources.clone(),
                kind: CompiledIvmNodeKind::Map {
                    input: Box::new(input),
                },
            }
        }
        DataflowNode::Join { left, right, .. } => {
            let left = compile_ivm_node(left, left_child_state_id(state_id));
            let right = compile_ivm_node(right, right_child_state_id(state_id));
            CompiledIvmNode {
                sources: merge_compiled_sources(left.sources(), right.sources()),
                kind: CompiledIvmNodeKind::Join {
                    state_id,
                    left: Box::new(left),
                    right: Box::new(right),
                },
            }
        }
        DataflowNode::Aggregate { input, .. } => {
            let input = compile_ivm_node(input, left_child_state_id(state_id));
            CompiledIvmNode {
                sources: input.sources.clone(),
                kind: CompiledIvmNodeKind::Aggregate {
                    state_id,
                    input: Box::new(input),
                },
            }
        }
    }
}

fn alloc_trace_slot(next_slot: &mut usize) -> TraceSlotId {
    let slot = *next_slot;
    *next_slot = next_slot.saturating_add(1);
    slot
}

fn append_trace_unary_op(
    input_slot: TraceSlotId,
    next_slot: &mut usize,
    instructions: &mut Vec<TraceInstruction>,
    op: TraceUnaryOp,
) -> TraceSlotId {
    if let Some(TraceInstruction::Unary {
        output_slot, block, ..
    }) = instructions.last_mut()
    {
        if *output_slot == input_slot && block.accepts_append {
            block.push(op);
            return *output_slot;
        }
    }

    let output_slot = alloc_trace_slot(next_slot);
    instructions.push(TraceInstruction::Unary {
        input_slot,
        output_slot,
        block: UnaryFusionBlock::new(op),
    });
    output_slot
}

fn compile_trace_program_node(
    node: &DataflowNode,
    state_id: usize,
    next_node_index: &mut usize,
    next_slot: &mut usize,
    instructions: &mut Vec<TraceInstruction>,
) -> TraceSlotId {
    let node_index = *next_node_index;
    *next_node_index = next_node_index.saturating_add(1);

    match node {
        DataflowNode::Source { table_id } => {
            let output_slot = alloc_trace_slot(next_slot);
            instructions.push(TraceInstruction::Source {
                table_id: *table_id,
                output_slot,
            });
            output_slot
        }
        DataflowNode::Filter {
            input,
            trace_predicate,
            ..
        } => {
            let input_slot = compile_trace_program_node(
                input,
                state_id,
                next_node_index,
                next_slot,
                instructions,
            );
            append_trace_unary_op(
                input_slot,
                next_slot,
                instructions,
                TraceUnaryOp::Filter {
                    node_index,
                    tuple_native: trace_predicate.is_some(),
                },
            )
        }
        DataflowNode::Project { input, columns } => {
            let input_slot = compile_trace_program_node(
                input,
                state_id,
                next_node_index,
                next_slot,
                instructions,
            );
            append_trace_unary_op(
                input_slot,
                next_slot,
                instructions,
                TraceUnaryOp::Project {
                    columns: Rc::<[usize]>::from(columns.clone().into_boxed_slice()),
                },
            )
        }
        DataflowNode::Map {
            input,
            trace_mapper,
            ..
        } => {
            let input_slot = compile_trace_program_node(
                input,
                state_id,
                next_node_index,
                next_slot,
                instructions,
            );
            append_trace_unary_op(
                input_slot,
                next_slot,
                instructions,
                TraceUnaryOp::Map {
                    node_index,
                    tuple_native: trace_mapper.is_some(),
                },
            )
        }
        DataflowNode::Join {
            left,
            right,
            left_width,
            right_width,
            ..
        } => {
            let left_slot = compile_trace_program_node(
                left,
                left_child_state_id(state_id),
                next_node_index,
                next_slot,
                instructions,
            );
            let right_slot = compile_trace_program_node(
                right,
                right_child_state_id(state_id),
                next_node_index,
                next_slot,
                instructions,
            );
            let output_slot = alloc_trace_slot(next_slot);
            instructions.push(TraceInstruction::Join {
                node_index,
                state_id,
                left_width: *left_width,
                right_width: *right_width,
                left_slot,
                right_slot,
                output_slot,
            });
            output_slot
        }
        DataflowNode::Aggregate {
            input,
            group_by,
            functions,
        } => {
            let input_slot = compile_trace_program_node(
                input,
                left_child_state_id(state_id),
                next_node_index,
                next_slot,
                instructions,
            );
            let output_slot = alloc_trace_slot(next_slot);
            instructions.push(TraceInstruction::Aggregate {
                state_id,
                input_slot,
                output_slot,
                group_by: group_by.clone(),
                functions: functions.clone(),
            });
            output_slot
        }
    }
}

fn compile_bootstrap_node(
    node: &DataflowNode,
    state_id: usize,
    next_source_index: &mut usize,
) -> CompiledBootstrapNode {
    let kind = match node {
        DataflowNode::Source { .. } => {
            let source_index = *next_source_index;
            *next_source_index = next_source_index.saturating_add(1);
            CompiledBootstrapNodeKind::Source { source_index }
        }
        DataflowNode::Filter { input, .. } => CompiledBootstrapNodeKind::Filter {
            input: Box::new(compile_bootstrap_node(input, state_id, next_source_index)),
        },
        DataflowNode::Project { input, columns } => CompiledBootstrapNodeKind::Project {
            input: Box::new(compile_bootstrap_node(input, state_id, next_source_index)),
            columns: Rc::<[usize]>::from(columns.clone().into_boxed_slice()),
        },
        DataflowNode::Map { input, .. } => CompiledBootstrapNodeKind::Map {
            input: Box::new(compile_bootstrap_node(input, state_id, next_source_index)),
        },
        DataflowNode::Join {
            left,
            right,
            left_width,
            right_width,
            ..
        } => CompiledBootstrapNodeKind::Join {
            state_id,
            left_width: *left_width,
            right_width: *right_width,
            left: Box::new(compile_bootstrap_node(
                left,
                left_child_state_id(state_id),
                next_source_index,
            )),
            right: Box::new(compile_bootstrap_node(
                right,
                right_child_state_id(state_id),
                next_source_index,
            )),
        },
        DataflowNode::Aggregate { input, .. } => CompiledBootstrapNodeKind::Aggregate {
            state_id,
            input: Box::new(compile_bootstrap_node(
                input,
                left_child_state_id(state_id),
                next_source_index,
            )),
        },
    };
    CompiledBootstrapNode { kind }
}

fn alloc_bootstrap_slot(next_slot: &mut usize) -> BootstrapSlotId {
    let slot = *next_slot;
    *next_slot = next_slot.saturating_add(1);
    slot
}

fn compile_bootstrap_program_node(
    node: &DataflowNode,
    emit_to_parent: bool,
    state_id: usize,
    next_node_index: &mut usize,
    next_source_index: &mut usize,
    next_slot: &mut usize,
    instructions: &mut Vec<BootstrapInstruction>,
) -> BootstrapCompileOutput {
    let node_index = *next_node_index;
    *next_node_index = next_node_index.saturating_add(1);

    match node {
        DataflowNode::Source { .. } => {
            let source_index = *next_source_index;
            *next_source_index = next_source_index.saturating_add(1);
            if !emit_to_parent {
                return BootstrapCompileOutput {
                    output_slot: None,
                    direct_source_index: Some(source_index),
                };
            }

            let output_slot = alloc_bootstrap_slot(next_slot);
            instructions.push(BootstrapInstruction::Source {
                node_index,
                source_index,
                output_slot,
            });
            BootstrapCompileOutput {
                output_slot: Some(output_slot),
                direct_source_index: Some(source_index),
            }
        }
        DataflowNode::Filter { input, .. } => {
            if !emit_to_parent {
                compile_bootstrap_program_node(
                    input,
                    false,
                    state_id,
                    next_node_index,
                    next_source_index,
                    next_slot,
                    instructions,
                );
                return BootstrapCompileOutput {
                    output_slot: None,
                    direct_source_index: None,
                };
            }

            let input = compile_bootstrap_program_node(
                input,
                true,
                state_id,
                next_node_index,
                next_source_index,
                next_slot,
                instructions,
            );
            let Some(input_slot) = input.output_slot else {
                return BootstrapCompileOutput {
                    output_slot: None,
                    direct_source_index: None,
                };
            };
            let output_slot = alloc_bootstrap_slot(next_slot);
            instructions.push(BootstrapInstruction::Filter {
                node_index,
                input_slot,
                output_slot,
                covered_source_index: input.direct_source_index,
            });
            BootstrapCompileOutput {
                output_slot: Some(output_slot),
                direct_source_index: None,
            }
        }
        DataflowNode::Project { input, columns } => {
            if !emit_to_parent {
                compile_bootstrap_program_node(
                    input,
                    false,
                    state_id,
                    next_node_index,
                    next_source_index,
                    next_slot,
                    instructions,
                );
                return BootstrapCompileOutput {
                    output_slot: None,
                    direct_source_index: None,
                };
            }

            let input = compile_bootstrap_program_node(
                input,
                true,
                state_id,
                next_node_index,
                next_source_index,
                next_slot,
                instructions,
            );
            let Some(input_slot) = input.output_slot else {
                return BootstrapCompileOutput {
                    output_slot: None,
                    direct_source_index: None,
                };
            };
            let output_slot = alloc_bootstrap_slot(next_slot);
            instructions.push(BootstrapInstruction::Project {
                input_slot,
                output_slot,
                columns: Rc::<[usize]>::from(columns.clone().into_boxed_slice()),
            });
            BootstrapCompileOutput {
                output_slot: Some(output_slot),
                direct_source_index: None,
            }
        }
        DataflowNode::Map { input, .. } => {
            if !emit_to_parent {
                compile_bootstrap_program_node(
                    input,
                    false,
                    state_id,
                    next_node_index,
                    next_source_index,
                    next_slot,
                    instructions,
                );
                return BootstrapCompileOutput {
                    output_slot: None,
                    direct_source_index: None,
                };
            }

            let input = compile_bootstrap_program_node(
                input,
                true,
                state_id,
                next_node_index,
                next_source_index,
                next_slot,
                instructions,
            );
            let Some(input_slot) = input.output_slot else {
                return BootstrapCompileOutput {
                    output_slot: None,
                    direct_source_index: None,
                };
            };
            let output_slot = alloc_bootstrap_slot(next_slot);
            instructions.push(BootstrapInstruction::Map {
                node_index,
                input_slot,
                output_slot,
            });
            BootstrapCompileOutput {
                output_slot: Some(output_slot),
                direct_source_index: None,
            }
        }
        DataflowNode::Join {
            left,
            right,
            left_width,
            right_width,
            ..
        } => {
            let left = compile_bootstrap_program_node(
                left,
                true,
                left_child_state_id(state_id),
                next_node_index,
                next_source_index,
                next_slot,
                instructions,
            );
            let Some(left_slot) = left.output_slot else {
                return BootstrapCompileOutput {
                    output_slot: None,
                    direct_source_index: None,
                };
            };
            let right = compile_bootstrap_program_node(
                right,
                true,
                right_child_state_id(state_id),
                next_node_index,
                next_source_index,
                next_slot,
                instructions,
            );
            let Some(right_slot) = right.output_slot else {
                return BootstrapCompileOutput {
                    output_slot: None,
                    direct_source_index: None,
                };
            };
            let output_slot = emit_to_parent.then(|| alloc_bootstrap_slot(next_slot));
            instructions.push(BootstrapInstruction::Join {
                node_index,
                state_id,
                left_width: *left_width,
                right_width: *right_width,
                left_slot,
                right_slot,
                output_slot,
            });
            BootstrapCompileOutput {
                output_slot,
                direct_source_index: None,
            }
        }
        DataflowNode::Aggregate {
            input,
            group_by,
            functions,
        } => {
            let input = compile_bootstrap_program_node(
                input,
                true,
                left_child_state_id(state_id),
                next_node_index,
                next_source_index,
                next_slot,
                instructions,
            );
            let Some(input_slot) = input.output_slot else {
                return BootstrapCompileOutput {
                    output_slot: None,
                    direct_source_index: None,
                };
            };
            let output_slot = emit_to_parent.then(|| alloc_bootstrap_slot(next_slot));
            instructions.push(BootstrapInstruction::Aggregate {
                state_id,
                input_slot,
                output_slot,
                group_by: group_by.clone(),
                functions: functions.clone(),
            });
            BootstrapCompileOutput {
                output_slot,
                direct_source_index: None,
            }
        }
    }
}

fn collect_bootstrap_runtime_nodes<'a>(
    node: &'a DataflowNode,
    runtime_nodes: &mut Vec<BootstrapRuntimeNode<'a>>,
) {
    match node {
        DataflowNode::Source { table_id } => {
            runtime_nodes.push(BootstrapRuntimeNode::Source {
                table_id: *table_id,
            });
        }
        DataflowNode::Filter {
            input,
            predicate,
            trace_predicate,
        } => {
            runtime_nodes.push(BootstrapRuntimeNode::Filter {
                predicate: predicate.as_ref(),
                trace_predicate: trace_predicate.as_deref(),
            });
            collect_bootstrap_runtime_nodes(input, runtime_nodes);
        }
        DataflowNode::Project { input, .. } => {
            runtime_nodes.push(BootstrapRuntimeNode::Source { table_id: u32::MAX });
            collect_bootstrap_runtime_nodes(input, runtime_nodes);
        }
        DataflowNode::Map {
            input,
            mapper,
            trace_mapper,
        } => {
            runtime_nodes.push(BootstrapRuntimeNode::Map {
                mapper: mapper.as_ref(),
                trace_mapper: trace_mapper.as_deref(),
            });
            collect_bootstrap_runtime_nodes(input, runtime_nodes);
        }
        DataflowNode::Join {
            left,
            right,
            left_key,
            right_key,
            join_type,
            ..
        } => {
            runtime_nodes.push(BootstrapRuntimeNode::Join {
                left_key,
                right_key,
                join_type: *join_type,
            });
            collect_bootstrap_runtime_nodes(left, runtime_nodes);
            collect_bootstrap_runtime_nodes(right, runtime_nodes);
        }
        DataflowNode::Aggregate { input, .. } => {
            runtime_nodes.push(BootstrapRuntimeNode::Source { table_id: u32::MAX });
            collect_bootstrap_runtime_nodes(input, runtime_nodes);
        }
    }
}

fn merge_compiled_sources(left: &[TableId], right: &[TableId]) -> Box<[TableId]> {
    let mut merged = Vec::with_capacity(left.len().saturating_add(right.len()));
    let mut left_index = 0usize;
    let mut right_index = 0usize;

    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            core::cmp::Ordering::Less => {
                merged.push(left[left_index]);
                left_index += 1;
            }
            core::cmp::Ordering::Greater => {
                merged.push(right[right_index]);
                right_index += 1;
            }
            core::cmp::Ordering::Equal => {
                merged.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }

    merged.extend_from_slice(&left[left_index..]);
    merged.extend_from_slice(&right[right_index..]);
    merged.into_boxed_slice()
}

fn collect_trace_runtime_nodes<'a>(
    node: &'a DataflowNode,
    runtime_nodes: &mut Vec<TraceRuntimeNode<'a>>,
) {
    match node {
        DataflowNode::Source { .. } => runtime_nodes.push(TraceRuntimeNode::Marker),
        DataflowNode::Filter {
            input,
            predicate,
            trace_predicate,
        } => {
            runtime_nodes.push(TraceRuntimeNode::Filter {
                predicate: predicate.as_ref(),
                trace_predicate: trace_predicate.as_deref(),
            });
            collect_trace_runtime_nodes(input, runtime_nodes);
        }
        DataflowNode::Project { input, .. } => {
            runtime_nodes.push(TraceRuntimeNode::Marker);
            collect_trace_runtime_nodes(input, runtime_nodes);
        }
        DataflowNode::Map {
            input,
            mapper,
            trace_mapper,
        } => {
            runtime_nodes.push(TraceRuntimeNode::Map {
                mapper: mapper.as_ref(),
                trace_mapper: trace_mapper.as_deref(),
            });
            collect_trace_runtime_nodes(input, runtime_nodes);
        }
        DataflowNode::Join {
            left,
            right,
            left_key,
            right_key,
            join_type,
            ..
        } => {
            runtime_nodes.push(TraceRuntimeNode::Join {
                left_key,
                right_key,
                join_type: *join_type,
            });
            collect_trace_runtime_nodes(left, runtime_nodes);
            collect_trace_runtime_nodes(right, runtime_nodes);
        }
        DataflowNode::Aggregate { input, .. } => {
            runtime_nodes.push(TraceRuntimeNode::Marker);
            collect_trace_runtime_nodes(input, runtime_nodes);
        }
    }
}

fn keep_trace_handle(
    arena: &TraceTupleArena,
    handle: &TraceTupleHandle,
    predicate: &(dyn Fn(&Row) -> bool + Send + Sync),
    trace_predicate: Option<&(dyn Fn(&TraceTupleArena, &TraceTupleHandle) -> bool + Send + Sync)>,
) -> bool {
    if let Some(trace_predicate) = trace_predicate.filter(|_| !arena.has_materialized_row(handle)) {
        trace_predicate(arena, handle)
    } else {
        let row = arena.materialize_rc(handle);
        predicate(&row)
    }
}

fn map_trace_handle(
    arena: &TraceTupleArena,
    handle: &TraceTupleHandle,
    mapper: &(dyn Fn(&Row) -> Row + Send + Sync),
    trace_mapper: Option<&(dyn Fn(&TraceTupleArena, &TraceTupleHandle) -> Row + Send + Sync)>,
) -> Row {
    if let Some(trace_mapper) = trace_mapper.filter(|_| !arena.has_materialized_row(handle)) {
        trace_mapper(arena, handle)
    } else {
        let row = arena.materialize_rc(handle);
        mapper(&row)
    }
}

fn execute_unary_fusion_block(
    block: &UnaryFusionBlock,
    runtime_nodes: &[TraceRuntimeNode<'_>],
    arena: &TraceTupleArena,
    input: &mut Vec<Delta<TraceTupleHandle>>,
    output: &mut Vec<Delta<TraceTupleHandle>>,
) {
    output.clear();
    output.reserve(input.len());

    for delta in input.drain(..) {
        let mut handle = delta.data;
        let diff = delta.diff;
        let mut keep = true;

        for op in block.ops.iter() {
            match op {
                TraceUnaryOp::Filter { node_index, .. } => {
                    let (predicate, trace_predicate) = match runtime_nodes.get(*node_index) {
                        Some(TraceRuntimeNode::Filter {
                            predicate,
                            trace_predicate,
                        }) => (*predicate, *trace_predicate),
                        _ => unreachable!("trace filter op must map to filter runtime node"),
                    };
                    if !keep_trace_handle(arena, &handle, predicate, trace_predicate) {
                        keep = false;
                        break;
                    }
                }
                TraceUnaryOp::Project { columns } => {
                    handle = arena.project(handle, columns.clone());
                }
                TraceUnaryOp::Map { node_index, .. } => {
                    let (mapper, trace_mapper) = match runtime_nodes.get(*node_index) {
                        Some(TraceRuntimeNode::Map {
                            mapper,
                            trace_mapper,
                        }) => (*mapper, *trace_mapper),
                        _ => unreachable!("trace map op must map to map runtime node"),
                    };
                    let row_id = handle.row_id();
                    let version = handle.version();
                    let mut mapped = map_trace_handle(arena, &handle, mapper, trace_mapper);
                    mapped.set_id(row_id);
                    mapped.set_version(version);
                    handle = arena.owned(mapped);
                }
            }
        }

        if keep {
            output.push(Delta::new(handle, diff));
        }
    }
}

fn recycle_slot_buffer(
    slots: &mut [Vec<Delta<TraceTupleHandle>>],
    slot_id: TraceSlotId,
    mut buffer: Vec<Delta<TraceTupleHandle>>,
) {
    buffer.clear();
    mem::swap(&mut slots[slot_id], &mut buffer);
}

fn execute_compiled_trace_program(
    dataflow: &DataflowNode,
    program: &mut CompiledTraceProgram,
    join_states: &mut HashMap<usize, JoinState>,
    aggregate_states: &mut HashMap<usize, GroupAggregateState>,
    source_table: TableId,
    deltas: &TraceDeltaBatch,
    now_fn: Option<fn() -> f64>,
) -> (TraceDeltaBatch, TraceUpdateProfile) {
    let arena = deltas.arena().clone();
    let mut runtime_nodes = Vec::new();
    collect_trace_runtime_nodes(dataflow, &mut runtime_nodes);
    let slots = &mut program.scratch_slots;
    debug_assert_eq!(slots.len(), program.slot_count);
    let mut profile = TraceUpdateProfile::default();

    for instruction in program.instructions.iter() {
        match instruction {
            TraceInstruction::Source {
                table_id,
                output_slot,
            } => {
                let output = &mut slots[*output_slot];
                output.clear();
                if *table_id != source_table {
                    continue;
                }
                output.reserve(deltas.deltas().len());
                timed_block(now_fn, &mut profile.source_dispatch_ms, || {
                    output.extend(deltas.deltas().iter().cloned());
                });
            }
            TraceInstruction::Unary {
                input_slot,
                output_slot,
                block,
            } => {
                let mut input = Vec::new();
                mem::swap(&mut slots[*input_slot], &mut input);
                timed_block(now_fn, &mut profile.unary_execute_ms, || {
                    execute_unary_fusion_block(
                        block,
                        &runtime_nodes,
                        &arena,
                        &mut input,
                        &mut slots[*output_slot],
                    );
                });
                recycle_slot_buffer(slots, *input_slot, input);
            }
            TraceInstruction::Join {
                node_index,
                state_id,
                left_width,
                right_width,
                left_slot,
                right_slot,
                output_slot,
            } => {
                let (left_key, right_key, join_type) = match runtime_nodes.get(*node_index) {
                    Some(TraceRuntimeNode::Join {
                        left_key,
                        right_key,
                        join_type,
                    }) => (*left_key, *right_key, *join_type),
                    _ => unreachable!("trace join instruction must map to join runtime node"),
                };
                let mut left_input = Vec::new();
                let mut right_input = Vec::new();
                mem::swap(&mut slots[*left_slot], &mut left_input);
                mem::swap(&mut slots[*right_slot], &mut right_input);
                let output = &mut slots[*output_slot];
                output.clear();
                timed_block(now_fn, &mut profile.join_execute_ms, || {
                    process_join_trace_deltas(
                        join_states,
                        *state_id,
                        *left_width,
                        *right_width,
                        left_key,
                        right_key,
                        join_type,
                        &arena,
                        &left_input,
                        &right_input,
                        output,
                    );
                });
                recycle_slot_buffer(slots, *left_slot, left_input);
                recycle_slot_buffer(slots, *right_slot, right_input);
            }
            TraceInstruction::Aggregate {
                state_id,
                input_slot,
                output_slot,
                group_by,
                functions,
            } => {
                let mut input = Vec::new();
                mem::swap(&mut slots[*input_slot], &mut input);
                let output = &mut slots[*output_slot];
                output.clear();
                timed_block(now_fn, &mut profile.aggregate_execute_ms, || {
                    process_aggregate_trace_deltas(
                        aggregate_states,
                        *state_id,
                        group_by,
                        functions,
                        &arena,
                        &input,
                        output,
                    );
                });
                recycle_slot_buffer(slots, *input_slot, input);
            }
        }
    }

    let mut output = Vec::new();
    mem::swap(&mut slots[program.root_slot], &mut output);
    (TraceDeltaBatch::new(arena, output), profile)
}

fn process_left_join_trace_deltas(
    join_state: &mut JoinState,
    left_key: &JoinKeySpec,
    join_type: JoinType,
    arena: &TraceTupleArena,
    deltas: &[Delta<TraceTupleHandle>],
    output: &mut Vec<Delta<TraceTupleHandle>>,
) {
    let mut normalize_scratch = mem::take(&mut join_state.normalize_scratch);
    normalize_join_side_deltas_into(deltas, left_key, arena, &mut normalize_scratch);

    for action in normalize_scratch.actions.drain(..) {
        match action {
            NormalizedJoinSideAction::SameKeyUpdate(update) => {
                let SameKeyTraceUpdate {
                    old_row,
                    new_row,
                    key,
                } = update;
                join_state
                    .process_left_update_same_key(&old_row, new_row, key, join_type, arena, output);
            }
            NormalizedJoinSideAction::Delta(delta) => {
                let key = extract_join_key_handle(left_key, arena, &delta.data);
                if join_type == JoinType::Inner {
                    if delta.is_insert() {
                        join_state.process_left_insert(
                            delta.data,
                            key,
                            JoinType::Inner,
                            arena,
                            output,
                        );
                    } else if delta.is_delete() {
                        join_state.process_left_delete(
                            delta.data.row_id(),
                            JoinType::Inner,
                            arena,
                            output,
                        );
                    }
                } else if delta.is_insert() {
                    join_state.on_left_insert_outer(delta.data, key, join_type, arena, output);
                } else if delta.is_delete() {
                    join_state.on_left_delete_outer(&delta.data, key, join_type, arena, output);
                }
            }
        }
    }

    join_state.normalize_scratch = normalize_scratch;
}

struct SameKeyTraceUpdate {
    old_row: TraceTupleHandle,
    new_row: TraceTupleHandle,
    key: JoinKey,
}

enum NormalizedJoinSideAction {
    SameKeyUpdate(SameKeyTraceUpdate),
    Delta(Delta<TraceTupleHandle>),
}

#[derive(Default)]
struct JoinNormalizeScratch {
    pending_deletes: HashMap<RowId, usize>,
    updates_by_delete: Vec<Option<SameKeyTraceUpdate>>,
    skip_indices: Vec<usize>,
    pending_delete_pairs: Vec<(RowId, usize)>,
    actions: Vec<NormalizedJoinSideAction>,
}

impl JoinNormalizeScratch {
    fn clear_working_sets(&mut self) {
        self.pending_deletes.clear();
        self.updates_by_delete.clear();
        self.skip_indices.clear();
        self.pending_delete_pairs.clear();
        self.actions.clear();
    }
}

fn normalize_join_side_deltas_into(
    deltas: &[Delta<TraceTupleHandle>],
    key_spec: &JoinKeySpec,
    arena: &TraceTupleArena,
    scratch: &mut JoinNormalizeScratch,
) {
    scratch.clear_working_sets();
    if deltas.len() < 2 {
        scratch
            .actions
            .extend(deltas.iter().cloned().map(NormalizedJoinSideAction::Delta));
        return;
    }

    let mut has_delete = false;
    let mut has_insert = false;
    for delta in deltas {
        has_delete |= delta.is_delete();
        has_insert |= delta.is_insert();
        if has_delete && has_insert {
            break;
        }
    }
    if !has_delete || !has_insert {
        scratch
            .actions
            .extend(deltas.iter().cloned().map(NormalizedJoinSideAction::Delta));
        return;
    }

    if deltas.len() <= JOIN_NORMALIZE_LINEAR_SCAN_LIMIT {
        normalize_join_side_deltas_linear(deltas, key_spec, arena, scratch);
        return;
    }

    for (index, delta) in deltas.iter().enumerate() {
        if delta.is_delete() {
            scratch.pending_deletes.insert(delta.data.row_id(), index);
            continue;
        }
        if !delta.is_insert() {
            continue;
        }

        let Some(delete_index) = scratch.pending_deletes.remove(&delta.data.row_id()) else {
            continue;
        };
        let old_delta = &deltas[delete_index];
        let old_key = extract_join_key_handle(key_spec, arena, &old_delta.data);
        let new_key = extract_join_key_handle(key_spec, arena, &delta.data);
        if old_key != new_key {
            continue;
        }
        if scratch.updates_by_delete.len() < deltas.len() {
            scratch.updates_by_delete.resize_with(deltas.len(), || None);
        }
        scratch.updates_by_delete[delete_index] = Some(SameKeyTraceUpdate {
            old_row: old_delta.data.clone(),
            new_row: delta.data.clone(),
            key: old_key,
        });
        scratch.skip_indices.push(index);
    }

    if scratch.skip_indices.is_empty() {
        scratch
            .actions
            .extend(deltas.iter().cloned().map(NormalizedJoinSideAction::Delta));
        scratch.pending_deletes.clear();
        scratch.updates_by_delete.clear();
        return;
    }

    if scratch.skip_indices.len() > 1 {
        scratch.skip_indices.sort_unstable();
    }

    let mut skip_offset = 0usize;
    for (index, delta) in deltas.iter().enumerate() {
        if let Some(update) = scratch.updates_by_delete[index].take() {
            scratch
                .actions
                .push(NormalizedJoinSideAction::SameKeyUpdate(update));
            continue;
        }
        if scratch
            .skip_indices
            .get(skip_offset)
            .copied()
            .is_some_and(|skip_index| skip_index == index)
        {
            skip_offset += 1;
            continue;
        }
        scratch
            .actions
            .push(NormalizedJoinSideAction::Delta(delta.clone()));
    }
    scratch.pending_deletes.clear();
    scratch.updates_by_delete.clear();
    scratch.skip_indices.clear();
}

fn normalize_join_side_deltas_linear(
    deltas: &[Delta<TraceTupleHandle>],
    key_spec: &JoinKeySpec,
    arena: &TraceTupleArena,
    scratch: &mut JoinNormalizeScratch,
) {
    scratch.updates_by_delete.resize_with(deltas.len(), || None);

    for (index, delta) in deltas.iter().enumerate() {
        if delta.is_delete() {
            let row_id = delta.data.row_id();
            if let Some((_, delete_index)) = scratch
                .pending_delete_pairs
                .iter_mut()
                .find(|(pending_row_id, _)| *pending_row_id == row_id)
            {
                *delete_index = index;
            } else {
                scratch.pending_delete_pairs.push((row_id, index));
            }
            continue;
        }
        if !delta.is_insert() {
            continue;
        }

        let row_id = delta.data.row_id();
        let Some(pending_index) = scratch
            .pending_delete_pairs
            .iter()
            .position(|(pending_row_id, _)| *pending_row_id == row_id)
        else {
            continue;
        };
        let (_, delete_index) = scratch.pending_delete_pairs.swap_remove(pending_index);
        let old_delta = &deltas[delete_index];
        let old_key = extract_join_key_handle(key_spec, arena, &old_delta.data);
        let new_key = extract_join_key_handle(key_spec, arena, &delta.data);
        if old_key != new_key {
            continue;
        }

        scratch.updates_by_delete[delete_index] = Some(SameKeyTraceUpdate {
            old_row: old_delta.data.clone(),
            new_row: delta.data.clone(),
            key: old_key,
        });
        scratch.skip_indices.push(index);
    }

    if scratch.skip_indices.is_empty() {
        scratch
            .actions
            .extend(deltas.iter().cloned().map(NormalizedJoinSideAction::Delta));
        scratch.updates_by_delete.clear();
        scratch.pending_delete_pairs.clear();
        return;
    }

    for (index, delta) in deltas.iter().enumerate() {
        if let Some(update) = scratch.updates_by_delete[index].take() {
            scratch
                .actions
                .push(NormalizedJoinSideAction::SameKeyUpdate(update));
        } else if !scratch.skip_indices.contains(&index) {
            scratch
                .actions
                .push(NormalizedJoinSideAction::Delta(delta.clone()));
        }
    }
    scratch.updates_by_delete.clear();
    scratch.skip_indices.clear();
    scratch.pending_delete_pairs.clear();
}

fn process_right_join_trace_deltas(
    join_state: &mut JoinState,
    right_key: &JoinKeySpec,
    join_type: JoinType,
    arena: &TraceTupleArena,
    deltas: &[Delta<TraceTupleHandle>],
    output: &mut Vec<Delta<TraceTupleHandle>>,
) {
    let mut normalize_scratch = mem::take(&mut join_state.normalize_scratch);
    normalize_join_side_deltas_into(deltas, right_key, arena, &mut normalize_scratch);

    for action in normalize_scratch.actions.drain(..) {
        match action {
            NormalizedJoinSideAction::SameKeyUpdate(update) => {
                let SameKeyTraceUpdate {
                    old_row,
                    new_row,
                    key,
                } = update;
                join_state.process_right_update_same_key(
                    &old_row, new_row, key, join_type, arena, output,
                );
            }
            NormalizedJoinSideAction::Delta(delta) => {
                let key = extract_join_key_handle(right_key, arena, &delta.data);
                if join_type == JoinType::Inner {
                    if delta.is_insert() {
                        join_state.process_right_insert(
                            delta.data,
                            key,
                            JoinType::Inner,
                            arena,
                            output,
                        );
                    } else if delta.is_delete() {
                        join_state.process_right_delete(
                            delta.data.row_id(),
                            JoinType::Inner,
                            arena,
                            output,
                        );
                    }
                } else if delta.is_insert() {
                    join_state.on_right_insert_outer(delta.data, key, join_type, arena, output);
                } else if delta.is_delete() {
                    join_state.on_right_delete_outer(&delta.data, key, join_type, arena, output);
                }
            }
        }
    }

    join_state.normalize_scratch = normalize_scratch;
}

fn process_join_trace_deltas(
    join_states: &mut HashMap<usize, JoinState>,
    state_id: usize,
    left_width: usize,
    right_width: usize,
    left_key: &JoinKeySpec,
    right_key: &JoinKeySpec,
    join_type: JoinType,
    arena: &TraceTupleArena,
    left_deltas: &[Delta<TraceTupleHandle>],
    right_deltas: &[Delta<TraceTupleHandle>],
    output: &mut Vec<Delta<TraceTupleHandle>>,
) {
    if left_deltas.is_empty() && right_deltas.is_empty() {
        output.clear();
        return;
    }

    let join_state = join_states
        .entry(state_id)
        .or_insert_with(|| JoinState::with_col_counts(left_width, right_width));
    output.clear();

    if !left_deltas.is_empty() {
        process_left_join_trace_deltas(join_state, left_key, join_type, arena, left_deltas, output);
    }
    if !right_deltas.is_empty() {
        process_right_join_trace_deltas(
            join_state,
            right_key,
            join_type,
            arena,
            right_deltas,
            output,
        );
    }
}

fn process_aggregate_trace_deltas(
    aggregate_states: &mut HashMap<usize, GroupAggregateState>,
    state_id: usize,
    group_by: &[ColumnId],
    functions: &[(ColumnId, AggregateType)],
    arena: &TraceTupleArena,
    input_deltas: &[Delta<TraceTupleHandle>],
    output: &mut Vec<Delta<TraceTupleHandle>>,
) {
    if input_deltas.is_empty() {
        output.clear();
        return;
    }

    let agg_state = aggregate_states
        .entry(state_id)
        .or_insert_with(|| GroupAggregateState::new(group_by.to_vec(), functions.to_vec()));
    *output = agg_state.process_trace_deltas(arena, input_deltas);
}

#[allow(dead_code)]
fn bootstrap_node_stream_with_source_visitor<F>(
    node: &DataflowNode,
    meta: &CompiledBootstrapNode,
    emit_to_parent: bool,
    visit_source_rows: &mut F,
    join_states: &mut HashMap<usize, JoinState>,
    aggregate_states: &mut HashMap<usize, GroupAggregateState>,
    emit: &mut dyn FnMut(TraceTupleHandle),
) where
    F: FnMut(TableId, usize, &mut dyn FnMut(Rc<Row>)),
{
    let arena = TraceTupleArena;
    match (node, &meta.kind) {
        (DataflowNode::Source { table_id }, CompiledBootstrapNodeKind::Source { source_index }) => {
            if !emit_to_parent {
                return;
            }
            let mut emit_source_row = |row: Rc<Row>| emit(arena.base_rc(row));
            visit_source_rows(*table_id, *source_index, &mut emit_source_row);
        }

        (
            DataflowNode::Filter {
                input,
                predicate,
                trace_predicate,
            },
            CompiledBootstrapNodeKind::Filter { input: input_meta },
        ) => bootstrap_node_stream_with_source_visitor(
            input,
            input_meta,
            emit_to_parent,
            visit_source_rows,
            join_states,
            aggregate_states,
            &mut |handle| {
                let keep = if let Some(trace_predicate) = trace_predicate
                    .as_ref()
                    .filter(|_| !arena.has_materialized_row(&handle))
                {
                    trace_predicate(&arena, &handle)
                } else {
                    let row = arena.materialize_rc(&handle);
                    predicate(&row)
                };
                if keep {
                    emit(handle);
                }
            },
        ),

        (
            DataflowNode::Project { input, .. },
            CompiledBootstrapNodeKind::Project {
                input: input_meta,
                columns,
            },
        ) => {
            bootstrap_node_stream_with_source_visitor(
                input,
                input_meta,
                emit_to_parent,
                visit_source_rows,
                join_states,
                aggregate_states,
                &mut |handle| emit(arena.project(handle, columns.clone())),
            );
        }

        (
            DataflowNode::Map {
                input,
                mapper,
                trace_mapper,
            },
            CompiledBootstrapNodeKind::Map { input: input_meta },
        ) => bootstrap_node_stream_with_source_visitor(
            input,
            input_meta,
            emit_to_parent,
            visit_source_rows,
            join_states,
            aggregate_states,
            &mut |handle| {
                let mut mapped = if let Some(trace_mapper) = trace_mapper
                    .as_ref()
                    .filter(|_| !arena.has_materialized_row(&handle))
                {
                    trace_mapper(&arena, &handle)
                } else {
                    let row = arena.materialize_rc(&handle);
                    mapper(&row)
                };
                mapped.set_id(handle.row_id());
                mapped.set_version(handle.version());
                emit(arena.owned(mapped));
            },
        ),

        (
            DataflowNode::Join {
                left,
                right,
                left_key,
                right_key,
                join_type,
                ..
            },
            CompiledBootstrapNodeKind::Join {
                state_id,
                left_width,
                right_width,
                left: left_meta,
                right: right_meta,
            },
        ) => {
            let mut join_state = JoinState::with_col_counts(*left_width, *right_width);

            bootstrap_node_stream_with_source_visitor(
                left,
                left_meta,
                true,
                visit_source_rows,
                join_states,
                aggregate_states,
                &mut |handle| {
                    let key = extract_join_key_handle(left_key, &arena, &handle);
                    join_state.left.insert(handle, key, 0);
                },
            );

            bootstrap_node_stream_with_source_visitor(
                right,
                right_meta,
                true,
                visit_source_rows,
                join_states,
                aggregate_states,
                &mut |handle| {
                    let key = extract_join_key_handle(right_key, &arena, &handle);
                    join_state.right.insert(handle, key, 0);
                },
            );

            join_state.finalize_bootstrap_match_counts();
            if emit_to_parent {
                join_state.emit_bootstrap_rows(*join_type, &arena, emit);
            }
            join_states.insert(*state_id, join_state);
        }

        (
            DataflowNode::Aggregate {
                input,
                group_by,
                functions,
            },
            CompiledBootstrapNodeKind::Aggregate {
                state_id,
                input: input_meta,
            },
        ) => {
            let mut aggregate_state = GroupAggregateState::new(group_by.clone(), functions.clone());
            bootstrap_node_stream_with_source_visitor(
                input,
                input_meta,
                true,
                visit_source_rows,
                join_states,
                aggregate_states,
                &mut |handle| aggregate_state.apply_trace_bootstrap_handle(&arena, &handle),
            );
            if emit_to_parent {
                aggregate_state.emit_bootstrap_rows(&arena, emit);
            }
            aggregate_states.insert(*state_id, aggregate_state);
        }

        _ => unreachable!("compiled bootstrap metadata must mirror the dataflow shape"),
    }
}

fn timed_block(now_fn: Option<fn() -> f64>, total_ms: &mut f64, run: impl FnOnce()) {
    if let Some(now_fn) = now_fn {
        let started_at = now_fn();
        run();
        *total_ms += now_fn() - started_at;
    } else {
        run();
    }
}

fn execute_compiled_bootstrap_program_with_source_visitor<F>(
    dataflow: &DataflowNode,
    plan: &CompiledBootstrapPlan,
    visit_source_rows: &mut F,
    source_filter_coverage: Option<&[bool]>,
    join_states: &mut HashMap<usize, JoinState>,
    aggregate_states: &mut HashMap<usize, GroupAggregateState>,
    now_fn: Option<fn() -> f64>,
) -> BootstrapExecutionProfile
where
    F: FnMut(TableId, usize, &mut dyn FnMut(Rc<Row>)),
{
    let arena = TraceTupleArena;
    let mut runtime_nodes = Vec::new();
    collect_bootstrap_runtime_nodes(dataflow, &mut runtime_nodes);
    let mut slots = vec![Vec::<TraceTupleHandle>::new(); plan.program.slot_count];
    let mut profile = BootstrapExecutionProfile::default();

    for instruction in plan.program.instructions.iter() {
        match instruction {
            BootstrapInstruction::Source {
                node_index,
                source_index,
                output_slot,
            } => {
                let table_id = match runtime_nodes.get(*node_index) {
                    Some(BootstrapRuntimeNode::Source { table_id }) => *table_id,
                    _ => {
                        unreachable!("bootstrap source instruction must map to source runtime node")
                    }
                };
                let slot = &mut slots[*output_slot];
                slot.clear();
                let mut emit_source_row = |row: Rc<Row>| slot.push(arena.base_rc(row));
                visit_source_rows(table_id, *source_index, &mut emit_source_row);
            }
            BootstrapInstruction::Filter {
                node_index,
                input_slot,
                output_slot,
                covered_source_index,
            } => {
                let (predicate, trace_predicate) = match runtime_nodes.get(*node_index) {
                    Some(BootstrapRuntimeNode::Filter {
                        predicate,
                        trace_predicate,
                    }) => (*predicate, *trace_predicate),
                    _ => {
                        unreachable!("bootstrap filter instruction must map to filter runtime node")
                    }
                };
                let mut input = mem::take(&mut slots[*input_slot]);
                let skip_filter = covered_source_index
                    .and_then(|source_index| {
                        source_filter_coverage
                            .and_then(|coverages| coverages.get(source_index))
                            .copied()
                    })
                    .unwrap_or(false);
                if skip_filter {
                    recycle_bootstrap_slot_buffer(&mut slots, *output_slot, input);
                    continue;
                }
                timed_block(now_fn, &mut profile.filter_bootstrap_ms, || {
                    input.retain(|handle| {
                        let keep = if let Some(trace_predicate) =
                            trace_predicate.filter(|_| !arena.has_materialized_row(handle))
                        {
                            trace_predicate(&arena, handle)
                        } else {
                            let row = arena.materialize_rc(handle);
                            predicate(&row)
                        };
                        keep
                    });
                });
                recycle_bootstrap_slot_buffer(&mut slots, *output_slot, input);
            }
            BootstrapInstruction::Project {
                input_slot,
                output_slot,
                columns,
            } => {
                let mut input = mem::take(&mut slots[*input_slot]);
                timed_block(now_fn, &mut profile.project_bootstrap_ms, || {
                    for handle in input.iter_mut() {
                        *handle = arena.project(handle.clone(), columns.clone());
                    }
                });
                recycle_bootstrap_slot_buffer(&mut slots, *output_slot, input);
            }
            BootstrapInstruction::Map {
                node_index,
                input_slot,
                output_slot,
            } => {
                let (mapper, trace_mapper) = match runtime_nodes.get(*node_index) {
                    Some(BootstrapRuntimeNode::Map {
                        mapper,
                        trace_mapper,
                    }) => (*mapper, *trace_mapper),
                    _ => unreachable!("bootstrap map instruction must map to map runtime node"),
                };
                let mut input = mem::take(&mut slots[*input_slot]);
                timed_block(now_fn, &mut profile.map_bootstrap_ms, || {
                    for handle in input.iter_mut() {
                        let source = handle.clone();
                        let mut mapped = if let Some(trace_mapper) =
                            trace_mapper.filter(|_| !arena.has_materialized_row(&source))
                        {
                            trace_mapper(&arena, &source)
                        } else {
                            let row = arena.materialize_rc(&source);
                            mapper(&row)
                        };
                        mapped.set_id(source.row_id());
                        mapped.set_version(source.version());
                        *handle = arena.owned(mapped);
                    }
                });
                recycle_bootstrap_slot_buffer(&mut slots, *output_slot, input);
            }
            BootstrapInstruction::Join {
                node_index,
                state_id,
                left_width,
                right_width,
                left_slot,
                right_slot,
                output_slot,
            } => {
                let (left_key, right_key, join_type) = match runtime_nodes.get(*node_index) {
                    Some(BootstrapRuntimeNode::Join {
                        left_key,
                        right_key,
                        join_type,
                    }) => (*left_key, *right_key, *join_type),
                    _ => unreachable!("bootstrap join instruction must map to join runtime node"),
                };
                let left_input = mem::take(&mut slots[*left_slot]);
                let right_input = mem::take(&mut slots[*right_slot]);
                let mut join_state = JoinState::with_col_counts(*left_width, *right_width);
                timed_block(now_fn, &mut profile.join_bootstrap_ms, || {
                    for handle in left_input {
                        let key = extract_join_key_handle(left_key, &arena, &handle);
                        join_state.left.insert(handle, key, 0);
                    }
                    for handle in right_input {
                        let key = extract_join_key_handle(right_key, &arena, &handle);
                        join_state.right.insert(handle, key, 0);
                    }
                    join_state.finalize_bootstrap_match_counts();
                });

                if let Some(output_slot) = output_slot {
                    let output = &mut slots[*output_slot];
                    output.clear();
                    timed_block(now_fn, &mut profile.join_bootstrap_ms, || {
                        join_state.emit_bootstrap_rows(join_type, &arena, &mut |handle| {
                            output.push(handle);
                        });
                    });
                } else {
                    // Root join is the visible sink in trace bootstrap; visible rows are already installed.
                    timed_block(now_fn, &mut profile.root_sink_ms, || {});
                }

                join_states.insert(*state_id, join_state);
            }
            BootstrapInstruction::Aggregate {
                state_id,
                input_slot,
                output_slot,
                group_by,
                functions,
            } => {
                let input = mem::take(&mut slots[*input_slot]);
                let mut aggregate_state =
                    GroupAggregateState::new(group_by.clone(), functions.clone());
                timed_block(now_fn, &mut profile.aggregate_bootstrap_ms, || {
                    for handle in &input {
                        aggregate_state.apply_trace_bootstrap_handle(&arena, handle);
                    }
                });

                if let Some(output_slot) = output_slot {
                    let output = &mut slots[*output_slot];
                    output.clear();
                    timed_block(now_fn, &mut profile.aggregate_bootstrap_ms, || {
                        aggregate_state.emit_bootstrap_rows(&arena, &mut |handle| {
                            output.push(handle);
                        });
                    });
                } else {
                    timed_block(now_fn, &mut profile.root_sink_ms, || {});
                }

                aggregate_states.insert(*state_id, aggregate_state);
            }
        }
    }

    profile
}

fn recycle_bootstrap_slot_buffer(
    slots: &mut [Vec<TraceTupleHandle>],
    slot_id: BootstrapSlotId,
    mut buffer: Vec<TraceTupleHandle>,
) {
    let target = &mut slots[slot_id];
    target.clear();
    mem::swap(target, &mut buffer);
}

fn extract_numeric(value: &Value) -> f64 {
    match value {
        Value::Int32(v) => *v as f64,
        Value::Int64(v) => *v as f64,
        Value::Float64(v) => *v,
        _ => 0.0,
    }
}

// ---------------------------------------------------------------------------
// MaterializedView — the core DBSP dataflow executor
// ---------------------------------------------------------------------------

/// A materialized view that maintains query results incrementally.
pub struct MaterializedView {
    dataflow: DataflowNode,
    compiled_plan: CompiledIvmPlan,
    visible_rows: VisibleResultStore,
    dependencies: Vec<TableId>,
    join_states: HashMap<usize, JoinState>,
    aggregate_states: HashMap<usize, GroupAggregateState>,
}

impl MaterializedView {
    pub fn new(dataflow: DataflowNode) -> Self {
        let compiled_plan = CompiledIvmPlan::compile(&dataflow);
        let dependencies = compiled_plan.sources().to_vec();
        Self {
            dataflow,
            compiled_plan,
            visible_rows: VisibleResultStore::default(),
            dependencies,
            join_states: HashMap::new(),
            aggregate_states: HashMap::new(),
        }
    }

    pub fn with_initial(dataflow: DataflowNode, initial: Vec<Row>) -> Self {
        let compiled_plan = CompiledIvmPlan::compile(&dataflow);
        Self::with_compiled_initial(dataflow, compiled_plan, initial)
    }

    pub fn with_compiled_initial(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        initial: Vec<Row>,
    ) -> Self {
        Self::with_compiled_initial_rows_only(dataflow, compiled_plan, initial)
    }

    pub fn with_compiled_initial_rc(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        initial: Vec<Rc<Row>>,
    ) -> Self {
        Self::with_compiled_initial_only(dataflow, compiled_plan, initial)
    }

    fn with_compiled_initial_only(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        initial: Vec<Rc<Row>>,
    ) -> Self {
        let dependencies = compiled_plan.sources().to_vec();
        Self {
            dataflow,
            compiled_plan,
            visible_rows: VisibleResultStore::from_rc_rows(initial),
            dependencies,
            join_states: HashMap::new(),
            aggregate_states: HashMap::new(),
        }
    }

    fn with_compiled_initial_rows_only(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        initial: Vec<Row>,
    ) -> Self {
        let dependencies = compiled_plan.sources().to_vec();
        Self {
            dataflow,
            compiled_plan,
            visible_rows: VisibleResultStore::from_rows(initial),
            dependencies,
            join_states: HashMap::new(),
            aggregate_states: HashMap::new(),
        }
    }

    pub fn with_compiled_initial_and_bootstrap(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        _compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial: Vec<Rc<Row>>,
    ) -> Self {
        Self::with_compiled_initial_only(dataflow, compiled_plan, initial)
    }

    pub fn with_sources(
        dataflow: DataflowNode,
        initial: Vec<Row>,
        source_rows: &HashMap<TableId, Vec<Row>>,
    ) -> Self {
        let compiled_plan = CompiledIvmPlan::compile(&dataflow);
        let compiled_bootstrap_plan = CompiledBootstrapPlan::compile(&dataflow);
        Self::with_compiled_sources_and_bootstrap(
            dataflow,
            compiled_plan,
            compiled_bootstrap_plan,
            initial.into_iter().map(Rc::new).collect(),
            source_rows,
        )
    }

    pub fn with_compiled_sources(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        initial: Vec<Row>,
        source_rows: &HashMap<TableId, Vec<Row>>,
    ) -> Self {
        let compiled_bootstrap_plan = CompiledBootstrapPlan::compile(&dataflow);
        Self::with_compiled_sources_and_bootstrap(
            dataflow,
            compiled_plan,
            compiled_bootstrap_plan,
            initial.into_iter().map(Rc::new).collect(),
            source_rows,
        )
    }

    pub fn with_compiled_sources_and_bootstrap(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial: Vec<Rc<Row>>,
        source_rows: &HashMap<TableId, Vec<Row>>,
    ) -> Self {
        let dependencies = compiled_plan.sources().to_vec();

        let mut join_states = HashMap::new();
        let mut aggregate_states = HashMap::new();
        execute_compiled_bootstrap_program_with_source_visitor(
            &dataflow,
            &compiled_bootstrap_plan,
            &mut |table_id, _source_index, emit| {
                if let Some(rows) = source_rows.get(&table_id) {
                    for row in rows {
                        emit(Rc::new(row.clone()));
                    }
                }
            },
            None,
            &mut join_states,
            &mut aggregate_states,
            None,
        );

        Self {
            dataflow,
            compiled_plan,
            visible_rows: VisibleResultStore::from_rc_rows(initial),
            dependencies,
            join_states,
            aggregate_states,
        }
    }

    pub fn with_compiled_loader<F>(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        initial: Vec<Rc<Row>>,
        load_source_rows: F,
    ) -> Self
    where
        F: FnMut(TableId) -> Vec<Rc<Row>>,
    {
        let compiled_bootstrap_plan = CompiledBootstrapPlan::compile(&dataflow);
        Self::with_compiled_loader_and_bootstrap(
            dataflow,
            compiled_plan,
            compiled_bootstrap_plan,
            initial,
            load_source_rows,
        )
    }

    pub fn with_compiled_loader_and_bootstrap<F>(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial: Vec<Rc<Row>>,
        mut load_source_rows: F,
    ) -> Self
    where
        F: FnMut(TableId) -> Vec<Rc<Row>>,
    {
        Self::with_compiled_source_visitor_and_bootstrap(
            dataflow,
            compiled_plan,
            compiled_bootstrap_plan,
            initial,
            move |table_id, _source_index, emit| {
                for row in load_source_rows(table_id) {
                    emit(row);
                }
            },
        )
    }

    pub fn with_compiled_source_visitor_and_bootstrap<F>(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial: Vec<Rc<Row>>,
        visit_source_rows: F,
    ) -> Self
    where
        F: FnMut(TableId, usize, &mut dyn FnMut(Rc<Row>)),
    {
        Self::with_compiled_source_visitor_and_bootstrap_profiled(
            dataflow,
            compiled_plan,
            compiled_bootstrap_plan,
            initial,
            visit_source_rows,
            None,
        )
        .0
    }

    pub fn with_compiled_source_visitor_and_bootstrap_profiled<F>(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial: Vec<Rc<Row>>,
        visit_source_rows: F,
        now_fn: Option<fn() -> f64>,
    ) -> (Self, BootstrapExecutionProfile)
    where
        F: FnMut(TableId, usize, &mut dyn FnMut(Rc<Row>)),
    {
        Self::with_compiled_source_visitor_and_bootstrap_profiled_with_filter_coverage(
            dataflow,
            compiled_plan,
            compiled_bootstrap_plan,
            initial,
            visit_source_rows,
            None,
            now_fn,
        )
    }

    pub fn with_compiled_source_visitor_and_bootstrap_profiled_with_filter_coverage<F>(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        compiled_bootstrap_plan: CompiledBootstrapPlan,
        initial: Vec<Rc<Row>>,
        mut visit_source_rows: F,
        source_filter_coverage: Option<Vec<bool>>,
        now_fn: Option<fn() -> f64>,
    ) -> (Self, BootstrapExecutionProfile)
    where
        F: FnMut(TableId, usize, &mut dyn FnMut(Rc<Row>)),
    {
        let dependencies = compiled_plan.sources().to_vec();

        let mut join_states = HashMap::new();
        let mut aggregate_states = HashMap::new();
        let bootstrap_profile = execute_compiled_bootstrap_program_with_source_visitor(
            &dataflow,
            &compiled_bootstrap_plan,
            &mut visit_source_rows,
            source_filter_coverage.as_deref(),
            &mut join_states,
            &mut aggregate_states,
            now_fn,
        );

        (
            Self {
                dataflow,
                compiled_plan,
                visible_rows: VisibleResultStore::from_rc_rows(initial),
                dependencies,
                join_states,
                aggregate_states,
            },
            bootstrap_profile,
        )
    }

    pub fn initialize_join_state(
        &mut self,
        left_rows: &[Row],
        right_rows: &[Row],
        left_key_fn: impl Fn(&Row) -> Vec<Value>,
        right_key_fn: impl Fn(&Row) -> Vec<Value>,
    ) {
        let join_state = self.join_states.entry(0).or_insert_with(JoinState::new);
        let arena = TraceTupleArena;
        for row in left_rows {
            join_state.left.insert(
                arena.owned(row.clone()),
                JoinKey::from_vec(left_key_fn(row)),
                0,
            );
        }
        for row in right_rows {
            join_state.right.insert(
                arena.owned(row.clone()),
                JoinKey::from_vec(right_key_fn(row)),
                0,
            );
        }
    }

    #[inline]
    pub fn result(&self) -> Vec<Row> {
        self.visible_rows.rows()
    }

    #[inline]
    pub fn result_rc(&self) -> Vec<Rc<Row>> {
        self.visible_rows.rc_rows()
    }

    #[inline]
    pub fn result_row_refs(&self) -> impl Iterator<Item = &Rc<Row>> + '_ {
        self.visible_rows.row_refs()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.visible_rows.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.visible_rows.is_empty()
    }

    #[inline]
    pub fn dependencies(&self) -> &[TableId] {
        &self.dependencies
    }

    pub fn depends_on(&self, table_id: TableId) -> bool {
        self.compiled_plan.depends_on(table_id)
    }

    /// Handles changes to a source table.
    /// Propagates deltas through the dataflow and updates the result.
    pub fn on_table_change(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
    ) -> Vec<Delta<Row>> {
        self.on_table_change_profiled(table_id, deltas, None).0
    }

    pub fn on_table_change_profiled(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
        now_fn: Option<fn() -> f64>,
    ) -> (Vec<Delta<Row>>, TraceUpdateProfile) {
        // Keep the row-returning API as a thin boundary adapter over the
        // handle-native executor. This avoids Row -> Handle -> Row round trips
        // at every join level while preserving the public `Vec<Delta<Row>>`
        // return type.
        let (output_batch, profile) = self.on_table_change_batch_profiled(table_id, deltas, now_fn);
        (output_batch.materialize_rows(), profile)
    }

    pub fn on_table_change_batch(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
    ) -> TraceDeltaBatch {
        self.on_table_change_batch_profiled(table_id, deltas, None)
            .0
    }

    pub fn on_table_change_batch_profiled(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
        now_fn: Option<fn() -> f64>,
    ) -> (TraceDeltaBatch, TraceUpdateProfile) {
        if !self.depends_on(table_id) {
            return (TraceDeltaBatch::empty(), TraceUpdateProfile::default());
        }

        let input_batch = TraceDeltaBatch::from_row_deltas(deltas);
        let program = self.compiled_plan.ensure_trace_program(&self.dataflow);
        let (output_deltas, mut profile) = execute_compiled_trace_program(
            &self.dataflow,
            program,
            &mut self.join_states,
            &mut self.aggregate_states,
            table_id,
            &input_batch,
            now_fn,
        );

        timed_block(now_fn, &mut profile.result_apply_ms, || {
            self.visible_rows.apply(&output_deltas);
        });

        (output_deltas, profile)
    }

    pub fn clear(&mut self) {
        self.visible_rows.clear();
    }

    pub fn set_result(&mut self, rows: Vec<Row>) {
        self.visible_rows.replace_rows(rows);
    }

    pub fn set_result_rc(&mut self, rows: Vec<Rc<Row>>) {
        self.visible_rows.replace_rc_rows(rows);
    }
}

#[allow(dead_code)]
fn propagate_row_deltas(
    node: &DataflowNode,
    meta: &CompiledIvmNode,
    join_states: &mut HashMap<usize, JoinState>,
    aggregate_states: &mut HashMap<usize, GroupAggregateState>,
    source_table: TableId,
    deltas: Vec<Delta<Row>>,
    now_fn: Option<fn() -> f64>,
    profile: &mut TraceUpdateProfile,
) -> Vec<Delta<Row>> {
    match (node, &meta.kind) {
        (DataflowNode::Source { table_id }, CompiledIvmNodeKind::Source) => {
            if *table_id == source_table {
                deltas
            } else {
                Vec::new()
            }
        }
        (
            DataflowNode::Filter {
                input, predicate, ..
            },
            CompiledIvmNodeKind::Filter { input: input_meta },
        ) => {
            let input_deltas = propagate_row_deltas(
                input,
                input_meta,
                join_states,
                aggregate_states,
                source_table,
                deltas,
                now_fn,
                profile,
            );
            let mut output = Vec::new();
            timed_block(now_fn, &mut profile.unary_execute_ms, || {
                output = filter_incremental(&input_deltas, |row| predicate(row));
            });
            output
        }
        (
            DataflowNode::Project { input, columns },
            CompiledIvmNodeKind::Project {
                input: input_meta, ..
            },
        ) => {
            let input_deltas = propagate_row_deltas(
                input,
                input_meta,
                join_states,
                aggregate_states,
                source_table,
                deltas,
                now_fn,
                profile,
            );
            let mut output = Vec::new();
            timed_block(now_fn, &mut profile.unary_execute_ms, || {
                output = project_incremental(&input_deltas, columns);
            });
            output
        }
        (
            DataflowNode::Map { input, mapper, .. },
            CompiledIvmNodeKind::Map { input: input_meta },
        ) => {
            let input_deltas = propagate_row_deltas(
                input,
                input_meta,
                join_states,
                aggregate_states,
                source_table,
                deltas,
                now_fn,
                profile,
            );
            let mut output = Vec::new();
            timed_block(now_fn, &mut profile.unary_execute_ms, || {
                output = map_incremental(&input_deltas, |row| mapper(row));
            });
            output
        }
        (
            DataflowNode::Join {
                left,
                right,
                left_key,
                right_key,
                join_type,
                left_width,
                right_width,
            },
            CompiledIvmNodeKind::Join {
                state_id,
                left: left_meta,
                right: right_meta,
            },
        ) => {
            if !join_states.contains_key(state_id) {
                join_states.insert(
                    *state_id,
                    JoinState::with_col_counts(*left_width, *right_width),
                );
            }

            let is_left_side = left_meta.depends_on(source_table);
            let is_right_side = right_meta.depends_on(source_table);

            let left_deltas = if is_left_side {
                propagate_row_deltas(
                    left,
                    left_meta,
                    join_states,
                    aggregate_states,
                    source_table,
                    deltas.clone(),
                    now_fn,
                    profile,
                )
            } else {
                Vec::new()
            };

            let right_deltas = if is_right_side {
                propagate_row_deltas(
                    right,
                    right_meta,
                    join_states,
                    aggregate_states,
                    source_table,
                    deltas,
                    now_fn,
                    profile,
                )
            } else {
                Vec::new()
            };

            let mut output_deltas = Vec::new();
            timed_block(now_fn, &mut profile.join_execute_ms, || {
                let join_state = join_states
                    .get_mut(state_id)
                    .expect("join state should exist after initialization");
                let arena = TraceTupleArena;
                let left_trace = left_deltas
                    .into_iter()
                    .map(|delta| Delta::new(arena.owned(delta.data), delta.diff))
                    .collect::<Vec<_>>();
                let right_trace = right_deltas
                    .into_iter()
                    .map(|delta| Delta::new(arena.owned(delta.data), delta.diff))
                    .collect::<Vec<_>>();
                let mut trace_output = Vec::new();

                process_left_join_trace_deltas(
                    join_state,
                    left_key,
                    *join_type,
                    &arena,
                    &left_trace,
                    &mut trace_output,
                );
                process_right_join_trace_deltas(
                    join_state,
                    right_key,
                    *join_type,
                    &arena,
                    &right_trace,
                    &mut trace_output,
                );

                output_deltas = trace_output
                    .into_iter()
                    .map(|delta| Delta::new(arena.materialize_row(&delta.data), delta.diff))
                    .collect();
            });

            output_deltas
        }
        (
            DataflowNode::Aggregate {
                input,
                group_by,
                functions,
            },
            CompiledIvmNodeKind::Aggregate {
                state_id,
                input: input_meta,
            },
        ) => {
            let input_deltas = propagate_row_deltas(
                input,
                input_meta,
                join_states,
                aggregate_states,
                source_table,
                deltas,
                now_fn,
                profile,
            );

            if input_deltas.is_empty() {
                return Vec::new();
            }

            if !aggregate_states.contains_key(state_id) {
                aggregate_states.insert(
                    *state_id,
                    GroupAggregateState::new(group_by.clone(), functions.clone()),
                );
            }

            let mut output = Vec::new();
            timed_block(now_fn, &mut profile.aggregate_execute_ms, || {
                output = aggregate_states
                    .get_mut(state_id)
                    .expect("aggregate state should exist after initialization")
                    .process_deltas(&input_deltas);
            });
            output
        }
        _ => Vec::new(),
    }
}

/// Propagates trace deltas through a dataflow node without eagerly materializing rows.
#[allow(dead_code)]
fn propagate_trace_deltas(
    node: &DataflowNode,
    meta: &CompiledIvmNode,
    join_states: &mut HashMap<usize, JoinState>,
    aggregate_states: &mut HashMap<usize, GroupAggregateState>,
    source_table: TableId,
    deltas: &TraceDeltaBatch,
) -> TraceDeltaBatch {
    let arena = deltas.arena().clone();
    match (node, &meta.kind) {
        (DataflowNode::Source { table_id }, CompiledIvmNodeKind::Source) => {
            if *table_id == source_table {
                TraceDeltaBatch::new(arena, deltas.deltas().to_vec())
            } else {
                TraceDeltaBatch::empty()
            }
        }

        (
            DataflowNode::Filter {
                input,
                predicate,
                trace_predicate,
            },
            CompiledIvmNodeKind::Filter { input: input_meta },
        ) => {
            let input_deltas = propagate_trace_deltas(
                input,
                input_meta,
                join_states,
                aggregate_states,
                source_table,
                deltas,
            );
            if input_deltas.is_empty() {
                return input_deltas;
            }
            let mut filtered = Vec::with_capacity(input_deltas.deltas().len());
            for delta in input_deltas.deltas() {
                if keep_trace_handle(
                    &arena,
                    &delta.data,
                    predicate.as_ref(),
                    trace_predicate.as_deref(),
                ) {
                    filtered.push(delta.clone());
                }
            }
            TraceDeltaBatch::new(arena, filtered)
        }

        (
            DataflowNode::Project { input, .. },
            CompiledIvmNodeKind::Project {
                input: input_meta,
                columns,
            },
        ) => {
            let input_deltas = propagate_trace_deltas(
                input,
                input_meta,
                join_states,
                aggregate_states,
                source_table,
                deltas,
            );
            if input_deltas.is_empty() {
                return input_deltas;
            }
            let projected = input_deltas
                .deltas()
                .iter()
                .map(|delta| {
                    Delta::new(
                        arena.project(delta.data.clone(), columns.clone()),
                        delta.diff,
                    )
                })
                .collect();
            TraceDeltaBatch::new(arena, projected)
        }

        (
            DataflowNode::Map {
                input,
                mapper,
                trace_mapper,
            },
            CompiledIvmNodeKind::Map { input: input_meta },
        ) => {
            let input_deltas = propagate_trace_deltas(
                input,
                input_meta,
                join_states,
                aggregate_states,
                source_table,
                deltas,
            );
            if input_deltas.is_empty() {
                return input_deltas;
            }
            let mapped = input_deltas
                .deltas()
                .iter()
                .map(|delta| {
                    let mut mapped = map_trace_handle(
                        &arena,
                        &delta.data,
                        mapper.as_ref(),
                        trace_mapper.as_deref(),
                    );
                    mapped.set_id(delta.data.row_id());
                    mapped.set_version(delta.data.version());
                    Delta::new(arena.owned(mapped), delta.diff)
                })
                .collect();
            TraceDeltaBatch::new(arena, mapped)
        }

        (
            DataflowNode::Join {
                left,
                right,
                left_key,
                right_key,
                join_type,
                left_width,
                right_width,
            },
            CompiledIvmNodeKind::Join {
                state_id: current_join_id,
                left: left_meta,
                right: right_meta,
            },
        ) => {
            if !join_states.contains_key(current_join_id) {
                join_states.insert(
                    *current_join_id,
                    JoinState::with_col_counts(*left_width, *right_width),
                );
            }

            let is_left_side = left_meta.depends_on(source_table);
            let is_right_side = right_meta.depends_on(source_table);
            let jt = *join_type;

            let mut output_deltas = Vec::new();
            let left_deltas = if is_left_side {
                propagate_trace_deltas(
                    left,
                    left_meta,
                    join_states,
                    aggregate_states,
                    source_table,
                    deltas,
                )
            } else {
                TraceDeltaBatch::empty()
            };
            let right_deltas = if is_right_side {
                propagate_trace_deltas(
                    right,
                    right_meta,
                    join_states,
                    aggregate_states,
                    source_table,
                    deltas,
                )
            } else {
                TraceDeltaBatch::empty()
            };
            process_join_trace_deltas(
                join_states,
                *current_join_id,
                *left_width,
                *right_width,
                left_key,
                right_key,
                jt,
                &arena,
                left_deltas.deltas(),
                right_deltas.deltas(),
                &mut output_deltas,
            );

            TraceDeltaBatch::new(arena, output_deltas)
        }

        (
            DataflowNode::Aggregate {
                input,
                group_by,
                functions,
            },
            CompiledIvmNodeKind::Aggregate {
                state_id: current_agg_id,
                input: input_meta,
            },
        ) => {
            let input_deltas = propagate_trace_deltas(
                input,
                input_meta,
                join_states,
                aggregate_states,
                source_table,
                deltas,
            );

            if input_deltas.is_empty() {
                return input_deltas;
            }

            let mut output = Vec::new();
            process_aggregate_trace_deltas(
                aggregate_states,
                *current_agg_id,
                group_by,
                functions,
                &arena,
                input_deltas.deltas(),
                &mut output,
            );
            TraceDeltaBatch::new(arena.clone(), output)
        }

        _ => unreachable!("compiled IVM metadata must mirror the dataflow shape"),
    }
}

/// Builder for creating materialized views.
pub struct MaterializedViewBuilder {
    dataflow: Option<DataflowNode>,
    initial: Vec<Row>,
}

impl Default for MaterializedViewBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MaterializedViewBuilder {
    pub fn new() -> Self {
        Self {
            dataflow: None,
            initial: Vec::new(),
        }
    }

    pub fn dataflow(mut self, dataflow: DataflowNode) -> Self {
        self.dataflow = Some(dataflow);
        self
    }

    pub fn initial(mut self, rows: Vec<Row>) -> Self {
        self.initial = rows;
        self
    }

    pub fn build(self) -> Option<MaterializedView> {
        self.dataflow.map(|df| {
            if self.initial.is_empty() {
                MaterializedView::new(df)
            } else {
                MaterializedView::with_initial(df, self.initial)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataflow::JoinKeySpec;
    use alloc::boxed::Box;
    use alloc::rc::Rc;
    use alloc::vec;
    use cynos_core::Value;

    fn make_row(id: u64, age: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(age)])
    }

    #[test]
    fn test_materialized_view_new() {
        let dataflow = DataflowNode::source(1);
        let view = MaterializedView::new(dataflow);
        assert!(view.is_empty());
        assert_eq!(view.dependencies(), &[1]);
    }

    #[test]
    fn test_materialized_view_source_propagation() {
        let dataflow = DataflowNode::source(1);
        let mut view = MaterializedView::new(dataflow);
        let deltas = vec![
            Delta::insert(make_row(1, 25)),
            Delta::insert(make_row(2, 30)),
        ];
        let output = view.on_table_change(1, deltas);
        assert_eq!(output.len(), 2);
        assert_eq!(view.len(), 2);
    }

    #[test]
    fn test_materialized_view_filter_propagation() {
        let dataflow = DataflowNode::filter(DataflowNode::source(1), |row| {
            row.get(1)
                .and_then(|v| v.as_i64())
                .map(|age| age > 18)
                .unwrap_or(false)
        });
        let mut view = MaterializedView::new(dataflow);
        let deltas = vec![
            Delta::insert(make_row(1, 25)),
            Delta::insert(make_row(2, 15)),
            Delta::insert(make_row(3, 30)),
        ];
        let output = view.on_table_change(1, deltas);
        assert_eq!(output.len(), 2);
        assert_eq!(view.len(), 2);
    }

    #[test]
    fn test_materialized_view_delete() {
        let dataflow = DataflowNode::source(1);
        let mut view = MaterializedView::new(dataflow);
        view.on_table_change(1, vec![Delta::insert(make_row(1, 25))]);
        assert_eq!(view.len(), 1);
        view.on_table_change(1, vec![Delta::delete(make_row(1, 25))]);
        assert_eq!(view.len(), 0);
    }

    #[test]
    fn test_materialized_view_wrong_table() {
        let dataflow = DataflowNode::source(1);
        let mut view = MaterializedView::new(dataflow);
        let output = view.on_table_change(2, vec![Delta::insert(make_row(1, 25))]);
        assert!(output.is_empty());
        assert!(view.is_empty());
    }

    fn make_employee(id: u64, name_hash: i64, dept_id: i64) -> Row {
        Row::new(
            id,
            vec![
                Value::Int64(id as i64),
                Value::Int64(name_hash),
                Value::Int64(dept_id),
            ],
        )
    }

    fn make_department(id: u64, name_hash: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(name_hash)])
    }

    fn make_employee_department_inner_join() -> DataflowNode {
        DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: JoinKeySpec::Columns(vec![2]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::Inner,
            left_width: 3,
            right_width: 2,
        }
    }

    fn make_employee_department_left_outer_join() -> DataflowNode {
        DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: JoinKeySpec::Columns(vec![2]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::LeftOuter,
            left_width: 3,
            right_width: 2,
        }
    }

    fn make_sum_aggregate() -> DataflowNode {
        DataflowNode::Aggregate {
            input: Box::new(DataflowNode::source(1)),
            group_by: vec![0],
            functions: vec![(1, AggregateType::Sum)],
        }
    }

    fn make_self_join() -> DataflowNode {
        DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(1)),
            left_key: JoinKeySpec::Columns(vec![1]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::Inner,
            left_width: 2,
            right_width: 2,
        }
    }

    fn normalize_rows(rows: &[Row]) -> Vec<alloc::string::String> {
        let mut normalized: Vec<_> = rows
            .iter()
            .map(|row| alloc::format!("{:?}:{}:{}", row.values(), row.id(), row.version()))
            .collect();
        normalized.sort();
        normalized
    }

    fn normalize_deltas(deltas: &[Delta<Row>]) -> Vec<alloc::string::String> {
        let mut normalized: Vec<_> = deltas
            .iter()
            .map(|delta| {
                alloc::format!(
                    "{}:{:?}:{}:{}",
                    if delta.is_insert() {
                        "insert"
                    } else {
                        "delete"
                    },
                    delta.data.values(),
                    delta.data.id(),
                    delta.data.version()
                )
            })
            .collect();
        normalized.sort();
        normalized
    }

    fn bootstrap_view_with_legacy_executor(
        dataflow: DataflowNode,
        initial: Vec<Row>,
        source_rows: &HashMap<(TableId, usize), Vec<Row>>,
    ) -> MaterializedView {
        let compiled_plan = CompiledIvmPlan::compile(&dataflow);
        let compiled_bootstrap_plan = CompiledBootstrapPlan::compile(&dataflow);
        let dependencies = compiled_plan.sources().to_vec();
        let mut join_states = HashMap::new();
        let mut aggregate_states = HashMap::new();
        bootstrap_node_stream_with_source_visitor(
            &dataflow,
            &compiled_bootstrap_plan.legacy_node,
            false,
            &mut |table_id, source_index, emit| {
                if let Some(rows) = source_rows.get(&(table_id, source_index)) {
                    for row in rows {
                        emit(Rc::new(row.clone()));
                    }
                }
            },
            &mut join_states,
            &mut aggregate_states,
            &mut |_handle| {},
        );
        MaterializedView {
            dataflow,
            compiled_plan,
            visible_rows: VisibleResultStore::from_rc_rows(
                initial.into_iter().map(Rc::new).collect(),
            ),
            dependencies,
            join_states,
            aggregate_states,
        }
    }

    fn bootstrap_view_with_compiled_executor(
        dataflow: DataflowNode,
        initial: Vec<Row>,
        source_rows: &HashMap<(TableId, usize), Vec<Row>>,
    ) -> MaterializedView {
        bootstrap_view_with_compiled_executor_with_filter_coverage(
            dataflow,
            initial,
            source_rows,
            None,
        )
    }

    fn bootstrap_view_with_compiled_executor_with_filter_coverage(
        dataflow: DataflowNode,
        initial: Vec<Row>,
        source_rows: &HashMap<(TableId, usize), Vec<Row>>,
        source_filter_coverage: Option<Vec<bool>>,
    ) -> MaterializedView {
        let compiled_plan = CompiledIvmPlan::compile(&dataflow);
        let compiled_bootstrap_plan = CompiledBootstrapPlan::compile(&dataflow);
        MaterializedView::with_compiled_source_visitor_and_bootstrap_profiled_with_filter_coverage(
            dataflow,
            compiled_plan,
            compiled_bootstrap_plan,
            initial.into_iter().map(Rc::new).collect(),
            |table_id, source_index, emit| {
                if let Some(rows) = source_rows.get(&(table_id, source_index)) {
                    for row in rows {
                        emit(Rc::new(row.clone()));
                    }
                }
            },
            source_filter_coverage,
            None,
        )
        .0
    }

    fn unary_block_op_counts(plan: &CompiledIvmPlan) -> Vec<usize> {
        plan.trace_program()
            .expect("trace program should be compiled for unary block tests")
            .instructions
            .iter()
            .filter_map(|instruction| match instruction {
                TraceInstruction::Unary { block, .. } => Some(block.ops.len()),
                _ => None,
            })
            .collect()
    }

    fn apply_legacy_update(
        view: &mut MaterializedView,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
    ) -> Vec<Delta<Row>> {
        if !view.depends_on(table_id) {
            return Vec::new();
        }
        let input_batch = TraceDeltaBatch::from_row_deltas(deltas);
        let output = propagate_trace_deltas(
            &view.dataflow,
            &view.compiled_plan.node,
            &mut view.join_states,
            &mut view.aggregate_states,
            table_id,
            &input_batch,
        );
        view.visible_rows.apply(&output);
        output.materialize_rows()
    }

    fn make_trace_unary_fusion_dataflow() -> DataflowNode {
        DataflowNode::Map {
            input: Box::new(DataflowNode::Project {
                input: Box::new(DataflowNode::Filter {
                    input: Box::new(DataflowNode::source(1)),
                    predicate: Box::new(|row| {
                        row.get(1)
                            .and_then(Value::as_i64)
                            .map(|value| value >= 18)
                            .unwrap_or(false)
                    }),
                    trace_predicate: Some(Box::new(|arena, handle| {
                        arena
                            .value_at(handle, 1)
                            .and_then(|value| value.as_i64())
                            .map(|value| value >= 18)
                            .unwrap_or(false)
                    })),
                }),
                columns: vec![0, 1],
            }),
            mapper: Box::new(|row| {
                Row::new(row.id(), vec![row.get(0).cloned().unwrap_or(Value::Null)])
            }),
            trace_mapper: Some(Box::new(|arena, handle| {
                Row::new(
                    arena.row_id(handle),
                    vec![arena.value_at(handle, 0).unwrap_or(Value::Null)],
                )
            })),
        }
    }

    fn make_trace_filter_project_chain() -> DataflowNode {
        DataflowNode::Project {
            input: Box::new(DataflowNode::Project {
                input: Box::new(DataflowNode::Filter {
                    input: Box::new(DataflowNode::Filter {
                        input: Box::new(DataflowNode::source(1)),
                        predicate: Box::new(|row| {
                            row.get(1)
                                .and_then(Value::as_i64)
                                .map(|value| value >= 18)
                                .unwrap_or(false)
                        }),
                        trace_predicate: Some(Box::new(|arena, handle| {
                            arena
                                .value_at(handle, 1)
                                .and_then(|value| value.as_i64())
                                .map(|value| value >= 18)
                                .unwrap_or(false)
                        })),
                    }),
                    predicate: Box::new(|row| {
                        row.get(1)
                            .and_then(Value::as_i64)
                            .map(|value| value % 2 == 0)
                            .unwrap_or(false)
                    }),
                    trace_predicate: Some(Box::new(|arena, handle| {
                        arena
                            .value_at(handle, 1)
                            .and_then(|value| value.as_i64())
                            .map(|value| value % 2 == 0)
                            .unwrap_or(false)
                    })),
                }),
                columns: vec![0, 1],
            }),
            columns: vec![1],
        }
    }

    fn make_dynamic_map_barrier_chain() -> DataflowNode {
        DataflowNode::Project {
            input: Box::new(DataflowNode::Map {
                input: Box::new(DataflowNode::Project {
                    input: Box::new(DataflowNode::source(1)),
                    columns: vec![0, 1],
                }),
                mapper: Box::new(|row| {
                    Row::new(
                        row.id(),
                        vec![
                            row.get(0).cloned().unwrap_or(Value::Null),
                            row.get(1).cloned().unwrap_or(Value::Null),
                            Value::Int64(row.len() as i64),
                        ],
                    )
                }),
                trace_mapper: None,
            }),
            columns: vec![2],
        }
    }

    #[test]
    fn test_inner_join() {
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: JoinKeySpec::Columns(vec![2]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::Inner,
            left_width: 3,
            right_width: 2,
        };
        let mut view = MaterializedView::new(dataflow);

        view.on_table_change(2, vec![Delta::insert(make_department(10, 100))]);
        let output = view.on_table_change(1, vec![Delta::insert(make_employee(1, 200, 10))]);
        assert_eq!(output.len(), 1);
        assert_eq!(view.len(), 1);
    }

    #[test]
    fn test_left_outer_join_no_match() {
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: JoinKeySpec::Columns(vec![2]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::LeftOuter,
            left_width: 3,
            right_width: 2,
        };
        let mut view = MaterializedView::new(dataflow);

        // Insert a department first so JoinState learns right_col_count
        view.on_table_change(2, vec![Delta::insert(make_department(10, 100))]);
        view.on_table_change(2, vec![Delta::delete(make_department(10, 100))]);

        // Insert employee with no matching department
        let output = view.on_table_change(1, vec![Delta::insert(make_employee(1, 200, 99))]);
        // Should get antijoin row: employee + NULLs
        assert_eq!(output.len(), 1);
        assert!(output[0].is_insert());
        let row = &output[0].data;
        // 3 employee cols + 2 NULL cols = 5
        assert_eq!(row.len(), 5);
        assert_eq!(row.get(3), Some(&Value::Null));
        assert_eq!(row.get(4), Some(&Value::Null));
    }

    #[test]
    fn test_left_outer_join_match_then_unmatch() {
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: JoinKeySpec::Columns(vec![2]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::LeftOuter,
            left_width: 3,
            right_width: 2,
        };
        let mut view = MaterializedView::new(dataflow);

        // Insert employee (no dept yet → antijoin)
        // Need to set right_col_count first by inserting a dept
        view.on_table_change(2, vec![Delta::insert(make_department(10, 100))]);
        view.on_table_change(1, vec![Delta::insert(make_employee(1, 200, 10))]);
        // Should have inner join result
        assert_eq!(view.len(), 1);

        // Delete department → employee becomes unmatched
        let output = view.on_table_change(2, vec![Delta::delete(make_department(10, 100))]);
        // Should delete inner join row and insert antijoin row
        let inserts: Vec<_> = output.iter().filter(|d| d.is_insert()).collect();
        let deletes: Vec<_> = output.iter().filter(|d| d.is_delete()).collect();
        assert_eq!(deletes.len(), 1); // remove inner join
        assert_eq!(inserts.len(), 1); // add antijoin
                                      // Antijoin row should have NULLs for right side
        assert_eq!(inserts[0].data.get(3), Some(&Value::Null));
    }

    #[test]
    fn test_left_outer_join_same_key_update_does_not_emit_unmatched_transition() {
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: JoinKeySpec::Columns(vec![2]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::LeftOuter,
            left_width: 3,
            right_width: 2,
        };
        let mut view = MaterializedView::new(dataflow);

        let employee = make_employee(1, 200, 10);
        let department_v1 = make_department(10, 100);
        let department_v2 = Row::new_with_version(10, 1, vec![Value::Int64(10), Value::Int64(101)]);

        view.on_table_change(2, vec![Delta::insert(department_v1.clone())]);
        view.on_table_change(1, vec![Delta::insert(employee.clone())]);

        let output = view.on_table_change(
            2,
            vec![
                Delta::delete(department_v1.clone()),
                Delta::insert(department_v2.clone()),
            ],
        );

        let inserts: Vec<_> = output.iter().filter(|delta| delta.is_insert()).collect();
        let deletes: Vec<_> = output.iter().filter(|delta| delta.is_delete()).collect();
        assert_eq!(inserts.len(), 1);
        assert_eq!(deletes.len(), 1);
        assert_eq!(inserts[0].data.get(0), Some(&Value::Int64(1)));
        assert_eq!(inserts[0].data.get(3), Some(&Value::Int64(10)));
        assert_eq!(inserts[0].data.get(4), Some(&Value::Int64(101)));
        assert_eq!(deletes[0].data.get(4), Some(&Value::Int64(100)));
        assert_eq!(view.len(), 1);
    }

    #[test]
    fn test_aggregate_count_sum() {
        // GROUP BY column 0, COUNT(*) and SUM(column 1)
        let dataflow = DataflowNode::Aggregate {
            input: Box::new(DataflowNode::source(1)),
            group_by: vec![0],
            functions: vec![(0, AggregateType::Count), (1, AggregateType::Sum)],
        };
        let mut view = MaterializedView::new(dataflow);

        // Insert rows with group key = 1
        let output = view.on_table_change(
            1,
            vec![
                Delta::insert(Row::new(1, vec![Value::Int64(1), Value::Int64(10)])),
                Delta::insert(Row::new(2, vec![Value::Int64(1), Value::Int64(20)])),
            ],
        );

        // Should have one group with count=2, sum=30
        let inserts: Vec<_> = output.iter().filter(|d| d.is_insert()).collect();
        assert!(!inserts.is_empty());
        let last_insert = inserts.last().unwrap();
        // group_key(1), count(2), sum(30)
        assert_eq!(last_insert.data.get(0), Some(&Value::Int64(1)));
        assert_eq!(last_insert.data.get(1), Some(&Value::Int64(2)));
        assert_eq!(last_insert.data.get(2), Some(&Value::Float64(30.0)));
    }

    #[test]
    fn test_aggregate_min_max_delete() {
        // GROUP BY column 0, MIN(column 1), MAX(column 1)
        let dataflow = DataflowNode::Aggregate {
            input: Box::new(DataflowNode::source(1)),
            group_by: vec![0],
            functions: vec![(1, AggregateType::Min), (1, AggregateType::Max)],
        };
        let mut view = MaterializedView::new(dataflow);

        // Insert 3 rows
        view.on_table_change(
            1,
            vec![
                Delta::insert(Row::new(1, vec![Value::Int64(1), Value::Int64(10)])),
                Delta::insert(Row::new(2, vec![Value::Int64(1), Value::Int64(30)])),
                Delta::insert(Row::new(3, vec![Value::Int64(1), Value::Int64(20)])),
            ],
        );

        // Delete the min value (10) — should NOT need recompute, BTreeMap handles it
        let output = view.on_table_change(
            1,
            vec![Delta::delete(Row::new(
                1,
                vec![Value::Int64(1), Value::Int64(10)],
            ))],
        );

        // Should have delete(old) + insert(new)
        let inserts: Vec<_> = output.iter().filter(|d| d.is_insert()).collect();
        assert_eq!(inserts.len(), 1);
        // New min should be 20, max still 30
        assert_eq!(inserts[0].data.get(1), Some(&Value::Int64(20)));
        assert_eq!(inserts[0].data.get(2), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_builder() {
        let view = MaterializedViewBuilder::new()
            .dataflow(DataflowNode::source(1))
            .initial(vec![make_row(1, 25)])
            .build()
            .unwrap();
        assert_eq!(view.len(), 1);
    }

    #[test]
    fn test_materialized_view_with_sources_bootstraps_inner_join_state() {
        let employee = make_employee(1, 200, 10);
        let department = make_department(10, 100);
        let initial = vec![merge_rows(&employee, &department)];
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: JoinKeySpec::Columns(vec![2]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::Inner,
            left_width: 3,
            right_width: 2,
        };

        let mut source_rows = HashMap::new();
        source_rows.insert(1, vec![employee]);
        source_rows.insert(2, vec![department]);

        let mut view = MaterializedView::with_sources(dataflow, initial, &source_rows);
        assert_eq!(view.len(), 1);

        let output = view.on_table_change(1, vec![Delta::insert(make_employee(2, 201, 10))]);
        assert_eq!(output.len(), 1);
        assert_eq!(view.len(), 2);
    }

    #[test]
    fn test_materialized_view_with_sources_bootstraps_left_outer_join_widths() {
        let employee = make_employee(1, 200, 99);
        let initial = vec![merge_rows_null_right(&employee, 2)];
        let dataflow = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: JoinKeySpec::Columns(vec![2]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::LeftOuter,
            left_width: 3,
            right_width: 2,
        };

        let mut source_rows = HashMap::new();
        source_rows.insert(1, vec![employee]);
        source_rows.insert(2, Vec::new());

        let mut view = MaterializedView::with_sources(dataflow, initial, &source_rows);
        let output = view.on_table_change(2, vec![Delta::insert(make_department(99, 300))]);

        let inserts: Vec<_> = output.iter().filter(|delta| delta.is_insert()).collect();
        let deletes: Vec<_> = output.iter().filter(|delta| delta.is_delete()).collect();
        assert_eq!(inserts.len(), 1);
        assert_eq!(deletes.len(), 1);
        assert_eq!(inserts[0].data.len(), 5);
        assert_eq!(view.len(), 1);
    }

    #[test]
    fn test_materialized_view_with_sources_bootstraps_aggregate_state() {
        let dataflow = DataflowNode::Aggregate {
            input: Box::new(DataflowNode::source(1)),
            group_by: vec![0],
            functions: vec![(1, AggregateType::Sum)],
        };

        let mut source_rows = HashMap::new();
        source_rows.insert(
            1,
            vec![
                Row::new(1, vec![Value::Int64(1), Value::Int64(10)]),
                Row::new(2, vec![Value::Int64(1), Value::Int64(20)]),
            ],
        );

        let initial = vec![Row::new(
            aggregate_group_row_id(&[Value::Int64(1)]),
            vec![Value::Int64(1), Value::Float64(30.0)],
        )];
        let mut view = MaterializedView::with_sources(dataflow, initial, &source_rows);
        let output = view.on_table_change(
            1,
            vec![Delta::insert(Row::new(
                3,
                vec![Value::Int64(1), Value::Int64(5)],
            ))],
        );

        let inserts: Vec<_> = output.iter().filter(|delta| delta.is_insert()).collect();
        assert_eq!(inserts.len(), 1);
        assert_eq!(inserts[0].data.get(1), Some(&Value::Float64(35.0)));
        assert_eq!(view.result()[0].get(1), Some(&Value::Float64(35.0)));
    }

    #[test]
    fn test_compiled_bootstrap_skip_covered_source_filter_matches_reference_aggregate() {
        let dataflow = DataflowNode::Aggregate {
            input: Box::new(DataflowNode::Filter {
                input: Box::new(DataflowNode::source(1)),
                predicate: Box::new(|row| {
                    row.get(1)
                        .and_then(Value::as_i64)
                        .map(|value| value > 10)
                        .unwrap_or(false)
                }),
                trace_predicate: None,
            }),
            group_by: vec![0],
            functions: vec![(1, AggregateType::Sum)],
        };

        let initial = vec![Row::new(
            aggregate_group_row_id(&[Value::Int64(1)]),
            vec![Value::Int64(1), Value::Float64(50.0)],
        )];
        let mut source_rows = HashMap::new();
        source_rows.insert(
            (1, 0),
            vec![
                Row::new(1, vec![Value::Int64(1), Value::Int64(20)]),
                Row::new(2, vec![Value::Int64(1), Value::Int64(30)]),
            ],
        );

        let mut reference_view =
            bootstrap_view_with_compiled_executor(dataflow, initial.clone(), &source_rows);
        let mut skip_view = bootstrap_view_with_compiled_executor_with_filter_coverage(
            DataflowNode::Aggregate {
                input: Box::new(DataflowNode::Filter {
                    input: Box::new(DataflowNode::source(1)),
                    predicate: Box::new(|row| {
                        row.get(1)
                            .and_then(Value::as_i64)
                            .map(|value| value > 10)
                            .unwrap_or(false)
                    }),
                    trace_predicate: None,
                }),
                group_by: vec![0],
                functions: vec![(1, AggregateType::Sum)],
            },
            initial,
            &source_rows,
            Some(vec![true]),
        );

        let follow_up = vec![
            Delta::insert(Row::new(3, vec![Value::Int64(1), Value::Int64(5)])),
            Delta::insert(Row::new(4, vec![Value::Int64(1), Value::Int64(25)])),
        ];
        let reference_output = reference_view.on_table_change(1, follow_up.clone());
        let skip_output = skip_view.on_table_change(1, follow_up);

        assert_eq!(
            normalize_deltas(&reference_output),
            normalize_deltas(&skip_output)
        );
        assert_eq!(
            normalize_rows(&reference_view.result()),
            normalize_rows(&skip_view.result())
        );
    }

    #[test]
    fn test_compiled_bootstrap_skip_covered_source_filter_matches_reference_join_state() {
        let employee = make_employee(1, 200, 10);
        let department = make_department(10, 100);
        let initial = vec![merge_rows(&employee, &department)];
        let mut source_rows = HashMap::new();
        source_rows.insert((1, 0), vec![employee]);
        source_rows.insert((2, 1), vec![department]);

        let filtered_join = || DataflowNode::Join {
            left: Box::new(DataflowNode::Filter {
                input: Box::new(DataflowNode::source(1)),
                predicate: Box::new(|row| {
                    row.get(2)
                        .and_then(Value::as_i64)
                        .map(|dept_id| dept_id == 10)
                        .unwrap_or(false)
                }),
                trace_predicate: None,
            }),
            right: Box::new(DataflowNode::source(2)),
            left_key: JoinKeySpec::Columns(vec![2]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::Inner,
            left_width: 3,
            right_width: 2,
        };

        let mut reference_view =
            bootstrap_view_with_compiled_executor(filtered_join(), initial.clone(), &source_rows);
        let mut skip_view = bootstrap_view_with_compiled_executor_with_filter_coverage(
            filtered_join(),
            initial,
            &source_rows,
            Some(vec![true, false]),
        );

        let follow_up = vec![Delta::insert(make_employee(2, 201, 10))];
        let reference_output = reference_view.on_table_change(1, follow_up.clone());
        let skip_output = skip_view.on_table_change(1, follow_up);

        assert_eq!(
            normalize_deltas(&reference_output),
            normalize_deltas(&skip_output)
        );
        assert_eq!(
            normalize_rows(&reference_view.result()),
            normalize_rows(&skip_view.result())
        );
    }

    #[test]
    fn test_compiled_bootstrap_matches_legacy_inner_join_followup_delta() {
        let employee = make_employee(1, 200, 10);
        let department = make_department(10, 100);
        let initial = vec![merge_rows(&employee, &department)];

        let mut source_rows = HashMap::new();
        source_rows.insert((1, 0), vec![employee]);
        source_rows.insert((2, 1), vec![department]);

        let mut legacy_view = bootstrap_view_with_legacy_executor(
            make_employee_department_inner_join(),
            initial.clone(),
            &source_rows,
        );
        let mut compiled_view = bootstrap_view_with_compiled_executor(
            make_employee_department_inner_join(),
            initial,
            &source_rows,
        );

        let follow_up = vec![Delta::insert(make_employee(2, 201, 10))];
        let legacy_output = legacy_view.on_table_change(1, follow_up.clone());
        let compiled_output = compiled_view.on_table_change(1, follow_up);

        assert_eq!(
            normalize_deltas(&legacy_output),
            normalize_deltas(&compiled_output)
        );
        assert_eq!(
            normalize_rows(&legacy_view.result()),
            normalize_rows(&compiled_view.result())
        );
    }

    #[test]
    fn test_compiled_bootstrap_matches_legacy_left_outer_join_followup_delta() {
        let employee = make_employee(1, 200, 99);
        let initial = vec![merge_rows_null_right(&employee, 2)];

        let mut source_rows = HashMap::new();
        source_rows.insert((1, 0), vec![employee]);
        source_rows.insert((2, 1), Vec::new());

        let mut legacy_view = bootstrap_view_with_legacy_executor(
            make_employee_department_left_outer_join(),
            initial.clone(),
            &source_rows,
        );
        let mut compiled_view = bootstrap_view_with_compiled_executor(
            make_employee_department_left_outer_join(),
            initial,
            &source_rows,
        );

        let follow_up = vec![Delta::insert(make_department(99, 300))];
        let legacy_output = legacy_view.on_table_change(2, follow_up.clone());
        let compiled_output = compiled_view.on_table_change(2, follow_up);

        assert_eq!(
            normalize_deltas(&legacy_output),
            normalize_deltas(&compiled_output)
        );
        assert_eq!(
            normalize_rows(&legacy_view.result()),
            normalize_rows(&compiled_view.result())
        );
    }

    #[test]
    fn test_compiled_bootstrap_matches_legacy_aggregate_followup_delta() {
        let initial = vec![Row::new(
            aggregate_group_row_id(&[Value::Int64(1)]),
            vec![Value::Int64(1), Value::Float64(30.0)],
        )];
        let mut source_rows = HashMap::new();
        source_rows.insert(
            (1, 0),
            vec![
                Row::new(1, vec![Value::Int64(1), Value::Int64(10)]),
                Row::new(2, vec![Value::Int64(1), Value::Int64(20)]),
            ],
        );

        let mut legacy_view = bootstrap_view_with_legacy_executor(
            make_sum_aggregate(),
            initial.clone(),
            &source_rows,
        );
        let mut compiled_view =
            bootstrap_view_with_compiled_executor(make_sum_aggregate(), initial, &source_rows);

        let follow_up = vec![Delta::insert(Row::new(
            3,
            vec![Value::Int64(1), Value::Int64(5)],
        ))];
        let legacy_output = legacy_view.on_table_change(1, follow_up.clone());
        let compiled_output = compiled_view.on_table_change(1, follow_up);

        assert_eq!(
            normalize_deltas(&legacy_output),
            normalize_deltas(&compiled_output)
        );
        assert_eq!(
            normalize_rows(&legacy_view.result()),
            normalize_rows(&compiled_view.result())
        );
    }

    #[test]
    fn test_compiled_bootstrap_uses_source_index_for_self_join_sources() {
        let left = Row::new(1, vec![Value::Int64(1), Value::Int64(10)]);
        let right = Row::new(2, vec![Value::Int64(10), Value::Int64(99)]);
        let initial = vec![merge_rows(&left, &right)];

        let mut source_rows = HashMap::new();
        source_rows.insert((1, 0), vec![left]);
        source_rows.insert((1, 1), vec![right]);

        let legacy_view =
            bootstrap_view_with_legacy_executor(make_self_join(), initial.clone(), &source_rows);
        let compiled_view =
            bootstrap_view_with_compiled_executor(make_self_join(), initial, &source_rows);

        let legacy_state = legacy_view.join_states.values().next().unwrap();
        let compiled_state = compiled_view.join_states.values().next().unwrap();
        assert_eq!(legacy_state.left.len(), 1);
        assert_eq!(legacy_state.right.len(), 1);
        assert_eq!(compiled_state.left.len(), 1);
        assert_eq!(compiled_state.right.len(), 1);
        assert_eq!(legacy_view.len(), compiled_view.len());
    }

    #[test]
    fn test_compiled_trace_program_fuses_filter_project_trace_map_chain() {
        let plan = CompiledIvmPlan::compile_with_trace_program(&make_trace_unary_fusion_dataflow());
        assert_eq!(unary_block_op_counts(&plan), vec![3]);
    }

    #[test]
    fn test_compiled_trace_program_fuses_consecutive_filters_and_projects() {
        let plan = CompiledIvmPlan::compile_with_trace_program(&make_trace_filter_project_chain());
        assert_eq!(unary_block_op_counts(&plan), vec![4]);
    }

    #[test]
    fn test_compiled_trace_program_keeps_dynamic_map_barrier_split() {
        let plan = CompiledIvmPlan::compile_with_trace_program(&make_dynamic_map_barrier_chain());
        assert_eq!(unary_block_op_counts(&plan), vec![2, 1]);
    }

    #[test]
    fn test_row_change_api_uses_compiled_trace_program() {
        let mut view = MaterializedView::new(make_trace_filter_project_chain());
        assert!(view.compiled_plan.trace_program().is_none());

        let output = view.on_table_change(1, vec![Delta::insert(make_row(1, 22))]);

        assert!(!output.is_empty());
        assert!(view.compiled_plan.trace_program().is_some());
    }

    #[test]
    fn test_compiled_unary_block_matches_legacy_recursive_path() {
        let mut compiled_view = MaterializedView::new(make_trace_unary_fusion_dataflow());
        let mut legacy_view = MaterializedView::new(make_trace_unary_fusion_dataflow());
        let deltas = vec![
            Delta::insert(make_row(1, 18)),
            Delta::insert(make_row(2, 17)),
            Delta::insert(make_row(3, 26)),
        ];

        let compiled_output = compiled_view.on_table_change(1, deltas.clone());
        let legacy_output = apply_legacy_update(&mut legacy_view, 1, deltas);

        assert_eq!(
            normalize_deltas(&compiled_output),
            normalize_deltas(&legacy_output)
        );
        assert_eq!(
            normalize_rows(&compiled_view.result()),
            normalize_rows(&legacy_view.result())
        );
    }

    #[test]
    fn test_compiled_unary_filter_project_chain_matches_legacy_recursive_path() {
        let mut compiled_view = MaterializedView::new(make_trace_filter_project_chain());
        let mut legacy_view = MaterializedView::new(make_trace_filter_project_chain());
        let deltas = vec![
            Delta::insert(make_row(1, 18)),
            Delta::insert(make_row(2, 21)),
            Delta::insert(make_row(3, 26)),
        ];

        let compiled_output = compiled_view.on_table_change(1, deltas.clone());
        let legacy_output = apply_legacy_update(&mut legacy_view, 1, deltas);

        assert_eq!(
            normalize_deltas(&compiled_output),
            normalize_deltas(&legacy_output)
        );
        assert_eq!(
            normalize_rows(&compiled_view.result()),
            normalize_rows(&legacy_view.result())
        );
    }

    #[test]
    fn test_dynamic_map_barrier_matches_legacy_recursive_path() {
        let mut compiled_view = MaterializedView::new(make_dynamic_map_barrier_chain());
        let mut legacy_view = MaterializedView::new(make_dynamic_map_barrier_chain());
        let deltas = vec![
            Delta::insert(make_row(1, 18)),
            Delta::insert(make_row(2, 21)),
        ];

        let compiled_output = compiled_view.on_table_change(1, deltas.clone());
        let legacy_output = apply_legacy_update(&mut legacy_view, 1, deltas);

        assert_eq!(
            normalize_deltas(&compiled_output),
            normalize_deltas(&legacy_output)
        );
        assert_eq!(
            normalize_rows(&compiled_view.result()),
            normalize_rows(&legacy_view.result())
        );
    }

    #[test]
    fn test_join_same_key_update_normalizes_non_adjacent_pair() {
        let dataflow = make_employee_department_left_outer_join();
        let mut view = MaterializedView::new(dataflow);

        let employee = make_employee(1, 200, 10);
        let department_v1 = make_department(10, 100);
        let department_v2 = Row::new_with_version(10, 1, vec![Value::Int64(10), Value::Int64(101)]);
        let unrelated = make_department(20, 999);

        view.on_table_change(2, vec![Delta::insert(department_v1.clone())]);
        view.on_table_change(1, vec![Delta::insert(employee.clone())]);

        let output = view.on_table_change(
            2,
            vec![
                Delta::delete(department_v1),
                Delta::insert(unrelated),
                Delta::insert(department_v2),
            ],
        );

        let inserts: Vec<_> = output.iter().filter(|delta| delta.is_insert()).collect();
        let deletes: Vec<_> = output.iter().filter(|delta| delta.is_delete()).collect();
        assert_eq!(inserts.len(), 1);
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].data.get(4), Some(&Value::Int64(100)));
        assert_eq!(inserts[0].data.get(4), Some(&Value::Int64(101)));
        assert_eq!(view.len(), 1);
    }

    #[test]
    fn test_self_join_update_matches_legacy_recursive_path() {
        let mut compiled_view = MaterializedView::new(make_self_join());
        let mut legacy_view = MaterializedView::new(make_self_join());
        let deltas = vec![
            Delta::insert(Row::new(1, vec![Value::Int64(1), Value::Int64(10)])),
            Delta::insert(Row::new(2, vec![Value::Int64(10), Value::Int64(99)])),
        ];

        let compiled_output = compiled_view.on_table_change(1, deltas.clone());
        let legacy_output = apply_legacy_update(&mut legacy_view, 1, deltas);

        assert_eq!(
            normalize_deltas(&compiled_output),
            normalize_deltas(&legacy_output)
        );
        assert_eq!(
            normalize_rows(&compiled_view.result()),
            normalize_rows(&legacy_view.result())
        );
    }

    // ==================== Bug 2 Test: Sum is_empty() incorrect logic ====================
    // This test demonstrates Bug 2: Sum uses sum == 0.0 to check if group is empty,
    // which is incorrect when values sum to zero (e.g., +5 and -5).

    #[test]
    fn test_aggregate_sum_zero_bug() {
        // GROUP BY column 0, SUM(column 1)
        let dataflow = DataflowNode::Aggregate {
            input: Box::new(DataflowNode::source(1)),
            group_by: vec![0],
            functions: vec![(1, AggregateType::Sum)],
        };
        let mut view = MaterializedView::new(dataflow);

        // Insert two rows with values that sum to zero: +5 and -5
        let output = view.on_table_change(
            1,
            vec![
                Delta::insert(Row::new(1, vec![Value::Int64(1), Value::Int64(5)])),
                Delta::insert(Row::new(2, vec![Value::Int64(1), Value::Int64(-5)])),
            ],
        );

        // The group should still exist with sum=0, not be deleted
        // BUG: The group is incorrectly deleted because sum == 0.0 is used to check emptiness
        let final_inserts: Vec<_> = output.iter().filter(|d| d.is_insert()).collect();
        let final_deletes: Vec<_> = output.iter().filter(|d| d.is_delete()).collect();

        // After processing both inserts, we should have:
        // - One group with key=1 and sum=0.0
        // The group should NOT be deleted just because sum is zero
        assert!(
            final_inserts.len() > final_deletes.len() || view.len() == 1,
            "Group with sum=0 should still exist, but it was incorrectly deleted"
        );

        // Verify the group exists with sum=0
        let result = view.result();
        assert_eq!(result.len(), 1, "Group should exist even when sum is zero");
        assert_eq!(
            result[0].get(1),
            Some(&Value::Float64(0.0)),
            "Sum should be 0.0"
        );
    }

    #[test]
    fn test_aggregate_sum_is_empty_direct() {
        // Direct test of AggregateState::is_empty() for Sum
        use crate::dataflow::AggregateType;

        let mut state = AggregateState::new(AggregateType::Sum);

        // Apply two values that sum to zero
        state.apply(&Value::Int64(5), 1); // +5
        state.apply(&Value::Int64(-5), 1); // -5

        // BUG: is_empty() returns true because sum == 0.0
        // But the group has 2 rows, so it should NOT be empty
        assert!(
            !state.is_empty(),
            "Sum group with 2 rows should NOT be empty, even if sum is 0.0"
        );
    }

    #[test]
    fn test_left_join_fanout_project_update_across_downstream_joins() {
        let join_issues_projects = DataflowNode::Join {
            left: Box::new(DataflowNode::source(1)),
            right: Box::new(DataflowNode::source(2)),
            left_key: JoinKeySpec::Columns(vec![1]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::LeftOuter,
            left_width: 2,
            right_width: 2,
        };
        let join_with_counters = DataflowNode::Join {
            left: Box::new(join_issues_projects),
            right: Box::new(DataflowNode::source(3)),
            left_key: JoinKeySpec::Columns(vec![2]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::LeftOuter,
            left_width: 4,
            right_width: 2,
        };
        let join_with_snapshots = DataflowNode::Join {
            left: Box::new(join_with_counters),
            right: Box::new(DataflowNode::source(4)),
            left_key: JoinKeySpec::Columns(vec![2]),
            right_key: JoinKeySpec::Columns(vec![0]),
            join_type: JoinType::LeftOuter,
            left_width: 6,
            right_width: 2,
        };
        let dataflow = DataflowNode::filter(join_with_snapshots, |row| {
            row.get(3)
                .and_then(Value::as_i64)
                .map(|health| health >= 45)
                .unwrap_or(false)
                && row
                    .get(5)
                    .and_then(Value::as_i64)
                    .map(|count| count >= 5)
                    .unwrap_or(false)
                && row
                    .get(7)
                    .and_then(Value::as_i64)
                    .map(|velocity| velocity >= 18)
                    .unwrap_or(false)
        });
        let mut view = MaterializedView::new(dataflow);

        let counter = Row::new(100, vec![Value::Int64(100), Value::Int64(7)]);
        let snapshot = Row::new(100, vec![Value::Int64(100), Value::Int64(35)]);
        let project_v1 = Row::new(100, vec![Value::Int64(100), Value::Int64(61)]);
        let project_v2 = Row::new_with_version(100, 1, vec![Value::Int64(100), Value::Int64(12)]);
        let issue_a = Row::new(1, vec![Value::Int64(1), Value::Int64(100)]);
        let issue_b = Row::new(2, vec![Value::Int64(2), Value::Int64(100)]);

        view.on_table_change(3, vec![Delta::insert(counter)]);
        view.on_table_change(4, vec![Delta::insert(snapshot)]);
        view.on_table_change(2, vec![Delta::insert(project_v1.clone())]);

        let initial = view.on_table_change(
            1,
            vec![
                Delta::insert(issue_a.clone()),
                Delta::insert(issue_b.clone()),
            ],
        );
        assert_eq!(initial.len(), 2);
        assert!(initial.iter().all(Delta::is_insert));
        assert_eq!(view.len(), 2);

        let update = view.on_table_change(
            2,
            vec![Delta::delete(project_v1), Delta::insert(project_v2)],
        );
        assert_eq!(update.len(), 2);
        assert!(update.iter().all(Delta::is_delete));
        assert_eq!(view.len(), 0);
    }
}
