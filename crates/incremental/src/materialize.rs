//! Materialized view for Incremental View Maintenance.
//!
//! Based on DBSP theory: each relational operator is lifted to work on Z-sets
//! (multisets with integer multiplicities). The materialized view maintains
//! the current result and propagates deltas through the dataflow graph.

use crate::dataflow::node::JoinType;
use crate::dataflow::{AggregateType, ColumnId, DataflowNode, TableId};
use crate::delta::Delta;
use crate::trace::{TraceDeltaBatch, TraceTupleArena, TraceTupleHandle, VisibleResultStore};
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::rc::Rc;
use alloc::vec::Vec;
use cynos_core::{aggregate_group_row_id, Row, RowId, Value};
use hashbrown::HashMap;

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
    bucket.retain(|slot_id| {
        slots
            .get(*slot_id)
            .and_then(Option::as_ref)
            .map(|slot| slot.key == *key)
            .unwrap_or(false)
    });
    bucket.sort_unstable();
    bucket.dedup();

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
}

impl JoinState {
    pub fn new() -> Self {
        Self {
            left: JoinSideState::default(),
            right: JoinSideState::default(),
            match_scratch: Vec::new(),
        }
    }

    /// Creates a new join state with known column counts.
    /// Required for outer joins to correctly pad NULL columns.
    pub fn with_col_counts(left_col_count: usize, right_col_count: usize) -> Self {
        Self {
            left: JoinSideState::with_col_count(left_col_count),
            right: JoinSideState::with_col_count(right_col_count),
            match_scratch: Vec::new(),
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
                output.push(Delta::insert(arena.null_pad_right(
                    left_row.clone(),
                    self.right.col_count,
                )));
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
                output.push(Delta::delete(arena.null_pad_left(
                    right_row.clone(),
                    self.left.col_count,
                )));
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
                output.push(Delta::delete(arena.null_pad_right(
                    slot.row.clone(),
                    self.right.col_count,
                )));
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
                output.push(Delta::insert(arena.null_pad_left(
                    right_row.clone(),
                    self.left.col_count,
                )));
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
                output.push(Delta::delete(arena.null_pad_right(
                    old_row.clone(),
                    self.right.col_count,
                )));
                output.push(Delta::insert(arena.null_pad_right(
                    new_row.clone(),
                    self.right.col_count,
                )));
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
                output.push(Delta::insert(arena.null_pad_left(
                    right_row.clone(),
                    self.left.col_count,
                )));
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
                output.push(Delta::delete(arena.null_pad_right(
                    left_row.clone(),
                    self.right.col_count,
                )));
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
                output.push(Delta::delete(arena.null_pad_left(
                    slot.row.clone(),
                    self.left.col_count,
                )));
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
                output.push(Delta::insert(arena.null_pad_right(
                    left_row.clone(),
                    self.right.col_count,
                )));
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
                output.push(Delta::delete(arena.null_pad_left(
                    old_row.clone(),
                    self.left.col_count,
                )));
                output.push(Delta::insert(arena.null_pad_left(
                    new_row.clone(),
                    self.left.col_count,
                )));
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

        let bucket_pairs = self
            .left
            .buckets
            .iter()
            .filter_map(|(key, left_bucket)| {
                self.right
                    .buckets
                    .get(key)
                    .map(|right_bucket| (left_bucket.clone(), right_bucket.clone()))
            })
            .collect::<Vec<_>>();

        for (left_bucket, right_bucket) in bucket_pairs {
            let right_matches = right_bucket.len();
            let left_matches = left_bucket.len();
            for left_id in left_bucket {
                self.left.slot_mut(left_id).match_count = right_matches;
            }

            for right_id in right_bucket {
                self.right.slot_mut(right_id).match_count = left_matches;
            }
        }
    }

    fn emit_bootstrap_rows<F>(
        &self,
        join_type: JoinType,
        arena: &TraceTupleArena,
        emit: &mut F,
    ) where
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
                emit(arena.null_pad_right(
                    left_slot.row.clone(),
                    self.right.col_count,
                ));
            }
        }

        if emits_unmatched_right(join_type) {
            for right_slot in self.right.slots.iter().filter_map(Option::as_ref) {
                if right_slot.match_count == 0 {
                    emit(arena.null_pad_left(
                        right_slot.row.clone(),
                        self.left.col_count,
                    ));
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
    Row::new_with_version(cynos_core::left_join_null_row_id(left.id()), left.version(), values)
}

#[allow(dead_code)]
/// Test/compat helper: materializes a right row with NULLs on the left side.
fn merge_rows_null_left(right: &Row, left_col_count: usize) -> Row {
    let mut values = Vec::with_capacity(left_col_count.saturating_add(right.len()));
    values.resize(left_col_count, Value::Null);
    values.extend(right.values().iter().cloned());
    Row::new_with_version(cynos_core::right_join_null_row_id(right.id()), right.version(), values)
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
            grouped.entry(key).or_default().push((&delta.data, delta.diff));
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

    fn apply_trace_bootstrap_handle(
        &mut self,
        arena: &TraceTupleArena,
        handle: &TraceTupleHandle,
    ) {
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

#[derive(Clone)]
struct CompiledIvmNode {
    sources: Box<[TableId]>,
    kind: CompiledIvmNodeKind,
}

#[derive(Clone)]
pub struct CompiledIvmPlan {
    node: CompiledIvmNode,
}

#[derive(Clone)]
pub struct CompiledBootstrapPlan {
    node: CompiledBootstrapNode,
}

#[derive(Clone)]
struct CompiledBootstrapNode {
    kind: CompiledBootstrapNodeKind,
}

#[derive(Clone)]
enum CompiledBootstrapNodeKind {
    Source,
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
        }
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

impl CompiledBootstrapPlan {
    pub fn compile(node: &DataflowNode) -> Self {
        Self {
            node: compile_bootstrap_node(node, 0),
        }
    }
}

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

fn compile_bootstrap_node(node: &DataflowNode, state_id: usize) -> CompiledBootstrapNode {
    let kind = match node {
        DataflowNode::Source { .. } => CompiledBootstrapNodeKind::Source,
        DataflowNode::Filter { input, .. } => CompiledBootstrapNodeKind::Filter {
            input: Box::new(compile_bootstrap_node(input, state_id)),
        },
        DataflowNode::Project { input, columns } => CompiledBootstrapNodeKind::Project {
            input: Box::new(compile_bootstrap_node(input, state_id)),
            columns: Rc::<[usize]>::from(columns.clone().into_boxed_slice()),
        },
        DataflowNode::Map { input, .. } => CompiledBootstrapNodeKind::Map {
            input: Box::new(compile_bootstrap_node(input, state_id)),
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
            left: Box::new(compile_bootstrap_node(left, left_child_state_id(state_id))),
            right: Box::new(compile_bootstrap_node(right, right_child_state_id(state_id))),
        },
        DataflowNode::Aggregate { input, .. } => CompiledBootstrapNodeKind::Aggregate {
            state_id,
            input: Box::new(compile_bootstrap_node(input, left_child_state_id(state_id))),
        },
    };
    CompiledBootstrapNode { kind }
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

fn bootstrap_node_stream_with_source_visitor<F>(
    node: &DataflowNode,
    meta: &CompiledBootstrapNode,
    emit_to_parent: bool,
    visit_source_rows: &mut F,
    join_states: &mut HashMap<usize, JoinState>,
    aggregate_states: &mut HashMap<usize, GroupAggregateState>,
    emit: &mut dyn FnMut(TraceTupleHandle),
) where
    F: FnMut(TableId, &mut dyn FnMut(Rc<Row>)),
{
    let arena = TraceTupleArena;
    match (node, &meta.kind) {
        (DataflowNode::Source { table_id }, CompiledBootstrapNodeKind::Source) => {
            if !emit_to_parent {
                return;
            }
            let mut emit_source_row = |row: Rc<Row>| emit(arena.base_rc(row));
            visit_source_rows(*table_id, &mut emit_source_row);
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
                let keep = if let Some(trace_predicate) = trace_predicate.as_ref().filter(|_| {
                    !arena.has_materialized_row(&handle)
                }) {
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
                let mut mapped = if let Some(trace_mapper) = trace_mapper.as_ref().filter(|_| {
                    !arena.has_materialized_row(&handle)
                }) {
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
        let compiled_bootstrap_plan = CompiledBootstrapPlan::compile(&dataflow);
        Self::with_compiled_initial_and_bootstrap(
            dataflow,
            compiled_plan,
            compiled_bootstrap_plan,
            initial.into_iter().map(Rc::new).collect(),
        )
    }

    pub fn with_compiled_initial_rc(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        initial: Vec<Rc<Row>>,
    ) -> Self {
        let compiled_bootstrap_plan = CompiledBootstrapPlan::compile(&dataflow);
        Self::with_compiled_initial_and_bootstrap(
            dataflow,
            compiled_plan,
            compiled_bootstrap_plan,
            initial,
        )
    }

    pub fn with_compiled_initial_and_bootstrap(
        dataflow: DataflowNode,
        compiled_plan: CompiledIvmPlan,
        _compiled_bootstrap_plan: CompiledBootstrapPlan,
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
        bootstrap_node_stream_with_source_visitor(
            &dataflow,
            &compiled_bootstrap_plan.node,
            false,
            &mut |table_id, emit| {
                if let Some(rows) = source_rows.get(&table_id) {
                    for row in rows {
                        emit(Rc::new(row.clone()));
                    }
                }
            },
            &mut join_states,
            &mut aggregate_states,
            &mut |_| {},
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
            move |table_id, emit| {
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
        mut visit_source_rows: F,
    ) -> Self
    where
        F: FnMut(TableId, &mut dyn FnMut(Rc<Row>)),
    {
        let dependencies = compiled_plan.sources().to_vec();

        let mut join_states = HashMap::new();
        let mut aggregate_states = HashMap::new();
        bootstrap_node_stream_with_source_visitor(
            &dataflow,
            &compiled_bootstrap_plan.node,
            false,
            &mut visit_source_rows,
            &mut join_states,
            &mut aggregate_states,
            &mut |_| {},
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
            join_state
                .left
                .insert(arena.owned(row.clone()), JoinKey::from_vec(left_key_fn(row)), 0);
        }
        for row in right_rows {
            join_state
                .right
                .insert(arena.owned(row.clone()), JoinKey::from_vec(right_key_fn(row)), 0);
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
        self.on_table_change_batch(table_id, deltas).materialize_rows()
    }

    pub fn on_table_change_batch(
        &mut self,
        table_id: TableId,
        deltas: Vec<Delta<Row>>,
    ) -> TraceDeltaBatch {
        if !self.depends_on(table_id) {
            return TraceDeltaBatch::empty();
        }

        let input_batch = TraceDeltaBatch::from_row_deltas(deltas);
        let output_deltas = propagate_trace_deltas(
            &self.dataflow,
            &self.compiled_plan.node,
            &mut self.join_states,
            &mut self.aggregate_states,
            table_id,
            &input_batch,
        );

        self.visible_rows.apply(&output_deltas);

        output_deltas
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

/// Propagates trace deltas through a dataflow node without eagerly materializing rows.
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
                let keep = if let Some(trace_predicate) = trace_predicate.as_ref().filter(|_| {
                    !arena.has_materialized_row(&delta.data)
                }) {
                    trace_predicate(&arena, &delta.data)
                } else {
                    let row = arena.materialize_rc(&delta.data);
                    predicate(&row)
                };
                if keep {
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
                .map(|delta| Delta::new(arena.project(delta.data.clone(), columns.clone()), delta.diff))
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
                    let mut mapped = if let Some(trace_mapper) = trace_mapper.as_ref().filter(|_| {
                        !arena.has_materialized_row(&delta.data)
                    }) {
                        trace_mapper(&arena, &delta.data)
                    } else {
                        let row = arena.materialize_rc(&delta.data);
                        mapper(&row)
                    };
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

            if is_left_side {
                let left_deltas = propagate_trace_deltas(
                    left,
                    left_meta,
                    join_states,
                    aggregate_states,
                    source_table,
                    deltas,
                );

                let join_state = join_states.get_mut(current_join_id).unwrap();
                let mut deltas_iter = left_deltas.deltas().iter().cloned().peekable();
                while let Some(delta) = deltas_iter.next() {
                    if delta.is_delete() {
                        let maybe_new_key = deltas_iter.peek().and_then(|next_delta| {
                            if next_delta.is_insert()
                                && next_delta.data.row_id() == delta.data.row_id()
                            {
                                Some(extract_join_key_handle(left_key, &arena, &next_delta.data))
                            } else {
                                None
                            }
                        });
                        let old_key = extract_join_key_handle(left_key, &arena, &delta.data);
                        if let Some(new_key) = maybe_new_key {
                            if old_key == new_key {
                                let new_delta = deltas_iter.next().unwrap();
                                join_state.process_left_update_same_key(
                                    &delta.data,
                                    new_delta.data,
                                    old_key,
                                    jt,
                                    &arena,
                                    &mut output_deltas,
                                );
                                continue;
                            }
                        }
                    }

                    let key = extract_join_key_handle(left_key, &arena, &delta.data);
                    if jt == JoinType::Inner {
                        if delta.is_insert() {
                            join_state.process_left_insert(
                                delta.data,
                                key,
                                JoinType::Inner,
                                &arena,
                                &mut output_deltas,
                            );
                        } else if delta.is_delete() {
                            join_state.process_left_delete(
                                delta.data.row_id(),
                                JoinType::Inner,
                                &arena,
                                &mut output_deltas,
                            );
                        }
                    } else if delta.is_insert() {
                        join_state.on_left_insert_outer(
                            delta.data,
                            key,
                            jt,
                            &arena,
                            &mut output_deltas,
                        );
                    } else if delta.is_delete() {
                        join_state.on_left_delete_outer(
                            &delta.data,
                            key,
                            jt,
                            &arena,
                            &mut output_deltas,
                        );
                    }
                }
            }

            if is_right_side {
                let right_deltas = propagate_trace_deltas(
                    right,
                    right_meta,
                    join_states,
                    aggregate_states,
                    source_table,
                    deltas,
                );

                let join_state = join_states.get_mut(current_join_id).unwrap();
                let mut deltas_iter = right_deltas.deltas().iter().cloned().peekable();
                while let Some(delta) = deltas_iter.next() {
                    if delta.is_delete() {
                        let maybe_new_key = deltas_iter.peek().and_then(|next_delta| {
                            if next_delta.is_insert()
                                && next_delta.data.row_id() == delta.data.row_id()
                            {
                                Some(extract_join_key_handle(right_key, &arena, &next_delta.data))
                            } else {
                                None
                            }
                        });
                        let old_key = extract_join_key_handle(right_key, &arena, &delta.data);
                        if let Some(new_key) = maybe_new_key {
                            if old_key == new_key {
                                let new_delta = deltas_iter.next().unwrap();
                                join_state.process_right_update_same_key(
                                    &delta.data,
                                    new_delta.data,
                                    old_key,
                                    jt,
                                    &arena,
                                    &mut output_deltas,
                                );
                                continue;
                            }
                        }
                    }

                    let key = extract_join_key_handle(right_key, &arena, &delta.data);
                    if jt == JoinType::Inner {
                        if delta.is_insert() {
                            join_state.process_right_insert(
                                delta.data,
                                key,
                                JoinType::Inner,
                                &arena,
                                &mut output_deltas,
                            );
                        } else if delta.is_delete() {
                            join_state.process_right_delete(
                                delta.data.row_id(),
                                JoinType::Inner,
                                &arena,
                                &mut output_deltas,
                            );
                        }
                    } else if delta.is_insert() {
                        join_state.on_right_insert_outer(
                            delta.data,
                            key,
                            jt,
                            &arena,
                            &mut output_deltas,
                        );
                    } else if delta.is_delete() {
                        join_state.on_right_delete_outer(
                            &delta.data,
                            key,
                            jt,
                            &arena,
                            &mut output_deltas,
                        );
                    }
                }
            }

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

            // Get or create aggregate state
            if !aggregate_states.contains_key(current_agg_id) {
                aggregate_states.insert(
                    *current_agg_id,
                    GroupAggregateState::new(group_by.clone(), functions.clone()),
                );
            }

            let agg_state = aggregate_states.get_mut(current_agg_id).unwrap();
            TraceDeltaBatch::new(
                arena.clone(),
                agg_state.process_trace_deltas(&arena, input_deltas.deltas()),
            )
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
