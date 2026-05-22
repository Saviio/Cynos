//! Internal late-materialization primitives for trace()/IVM.

use crate::delta::Delta;
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::{OnceCell, RefCell};
use cynos_core::{join_row_id, left_join_null_row_id, right_join_null_row_id, Row, RowId, Value};
use hashbrown::HashMap;

#[derive(Clone, Debug)]
pub struct TraceTupleHandle(Rc<TraceTupleNode>);

#[derive(Debug)]
struct TraceTupleNode {
    layout: TraceTupleLayout,
    materialized: OnceCell<Rc<Row>>,
}

#[derive(Debug)]
struct TraceTupleLayout {
    len: usize,
    row_id: RowId,
    version: u64,
    segments: TraceTupleSegments,
}

#[derive(Debug)]
struct TraceTupleSegment {
    output_start: usize,
    len: usize,
    kind: TraceTupleSegmentKind,
}

#[derive(Debug)]
enum TraceTupleSegmentKind {
    Values { row: Rc<Row>, row_start: usize },
    Nulls,
    Missing,
}

#[derive(Debug)]
enum TraceTupleSegments {
    Empty,
    One(TraceTupleSegment),
    Heap(Box<[TraceTupleSegment]>),
}

impl TraceTupleSegments {
    fn from_vec(mut segments: Vec<TraceTupleSegment>) -> Self {
        match segments.len() {
            0 => Self::Empty,
            1 => Self::One(segments.pop().expect("single segment")),
            _ => Self::Heap(segments.into_boxed_slice()),
        }
    }

    fn one(segment: TraceTupleSegment) -> Self {
        if segment.len == 0 {
            Self::Empty
        } else {
            Self::One(segment)
        }
    }

    fn as_slice(&self) -> &[TraceTupleSegment] {
        match self {
            Self::Empty => &[],
            Self::One(segment) => core::slice::from_ref(segment),
            Self::Heap(segments) => segments.as_ref(),
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn iter(&self) -> core::slice::Iter<'_, TraceTupleSegment> {
        self.as_slice().iter()
    }
}

impl TraceTupleHandle {
    fn new(layout: TraceTupleLayout, materialized: Option<Rc<Row>>) -> Self {
        let materialized_cell = OnceCell::new();
        if let Some(row) = materialized {
            let _ = materialized_cell.set(row);
        }
        Self(Rc::new(TraceTupleNode {
            layout,
            materialized: materialized_cell,
        }))
    }

    pub fn len(&self) -> usize {
        self.0.layout.len
    }

    pub fn row_id(&self) -> RowId {
        self.0.layout.row_id
    }

    pub fn version(&self) -> u64 {
        self.0.layout.version
    }

    pub fn value_at(&self, index: usize) -> Option<Value> {
        if index >= self.0.layout.len {
            return None;
        }
        let segment = self.0.layout.segment_at(index)?;
        let offset = index.saturating_sub(segment.output_start);
        match &segment.kind {
            TraceTupleSegmentKind::Values { row, row_start } => {
                row.get(row_start.saturating_add(offset)).cloned()
            }
            TraceTupleSegmentKind::Nulls => Some(Value::Null),
            TraceTupleSegmentKind::Missing => None,
        }
    }

    pub fn materialize_rc(&self) -> Rc<Row> {
        self.0
            .materialized
            .get_or_init(|| self.0.layout.materialize_row())
            .clone()
    }

    pub fn materialize_row(&self) -> Row {
        (*self.materialize_rc()).clone()
    }

    pub fn has_materialized_row(&self) -> bool {
        self.0.materialized.get().is_some()
    }

    #[cfg(test)]
    fn segment_count(&self) -> usize {
        self.0.layout.segments.len()
    }
}

impl TraceTupleLayout {
    fn from_row(row: Rc<Row>) -> Self {
        let len = row.len();
        Self {
            len,
            row_id: row.id(),
            version: row.version(),
            segments: TraceTupleSegments::one(TraceTupleSegment {
                output_start: 0,
                len,
                kind: TraceTupleSegmentKind::Values { row, row_start: 0 },
            }),
        }
    }

    fn concat(left: &TraceTupleHandle, right: &TraceTupleHandle, left_width: usize) -> Self {
        let mut segments = Vec::new();
        append_layout_range(&mut segments, &left.0.layout, 0, left_width, 0);
        append_layout_range(&mut segments, &right.0.layout, 0, right.len(), left_width);
        Self {
            len: left_width.saturating_add(right.len()),
            row_id: join_row_id(left.row_id(), right.row_id()),
            version: left.version().wrapping_add(right.version()),
            segments: TraceTupleSegments::from_vec(segments),
        }
    }

    fn null_pad_left(right: &TraceTupleHandle, left_width: usize) -> Self {
        let mut segments = Vec::new();
        push_segment(
            &mut segments,
            TraceTupleSegment {
                output_start: 0,
                len: left_width,
                kind: TraceTupleSegmentKind::Nulls,
            },
        );
        append_layout_range(&mut segments, &right.0.layout, 0, right.len(), left_width);
        Self {
            len: left_width.saturating_add(right.len()),
            row_id: right_join_null_row_id(right.row_id()),
            version: right.version(),
            segments: TraceTupleSegments::from_vec(segments),
        }
    }

    fn null_pad_right(left: &TraceTupleHandle, right_width: usize) -> Self {
        let mut segments = Vec::new();
        append_layout_range(&mut segments, &left.0.layout, 0, left.len(), 0);
        push_segment(
            &mut segments,
            TraceTupleSegment {
                output_start: left.len(),
                len: right_width,
                kind: TraceTupleSegmentKind::Nulls,
            },
        );
        Self {
            len: left.len().saturating_add(right_width),
            row_id: left_join_null_row_id(left.row_id()),
            version: left.version(),
            segments: TraceTupleSegments::from_vec(segments),
        }
    }

    fn project(input: &TraceTupleHandle, columns: Rc<[usize]>) -> Self {
        let mut segments = Vec::new();
        for (output_index, source_index) in columns.iter().copied().enumerate() {
            append_layout_range(
                &mut segments,
                &input.0.layout,
                source_index,
                1,
                output_index,
            );
        }
        Self {
            len: columns.len(),
            row_id: input.row_id(),
            version: input.version(),
            segments: TraceTupleSegments::from_vec(segments),
        }
    }

    fn segment_at(&self, index: usize) -> Option<&TraceTupleSegment> {
        let TraceTupleSegments::Heap(segments) = &self.segments else {
            let segment = match &self.segments {
                TraceTupleSegments::One(segment) => segment,
                TraceTupleSegments::Empty | TraceTupleSegments::Heap(_) => return None,
            };
            return (index >= segment.output_start
                && index < segment.output_start.saturating_add(segment.len))
            .then_some(segment);
        };

        let segments = segments.as_ref();
        let mut low = 0usize;
        let mut high = segments.len();
        while low < high {
            let mid = low + ((high - low) / 2);
            let segment = &segments[mid];
            if index < segment.output_start {
                high = mid;
            } else if index >= segment.output_start.saturating_add(segment.len) {
                low = mid + 1;
            } else {
                return Some(segment);
            }
        }
        None
    }

    fn materialize_row(&self) -> Rc<Row> {
        let mut values = Vec::with_capacity(self.len);
        for segment in self.segments.iter() {
            match &segment.kind {
                TraceTupleSegmentKind::Values { row, row_start } => {
                    for offset in 0..segment.len {
                        if let Some(value) = row.get(row_start.saturating_add(offset)).cloned() {
                            values.push(value);
                        }
                    }
                }
                TraceTupleSegmentKind::Nulls => {
                    values.resize(values.len().saturating_add(segment.len), Value::Null);
                }
                TraceTupleSegmentKind::Missing => {}
            }
        }
        Rc::new(Row::new_with_version(self.row_id, self.version, values))
    }
}

fn append_layout_range(
    segments: &mut Vec<TraceTupleSegment>,
    layout: &TraceTupleLayout,
    source_start: usize,
    len: usize,
    output_start: usize,
) {
    if len == 0 {
        return;
    }

    let source_end = source_start.saturating_add(len).min(layout.len);
    if source_start >= source_end {
        push_segment(
            segments,
            TraceTupleSegment {
                output_start,
                len,
                kind: TraceTupleSegmentKind::Missing,
            },
        );
        return;
    }

    for segment in layout.segments.iter() {
        let segment_start = segment.output_start;
        let segment_end = segment.output_start.saturating_add(segment.len);
        let overlap_start = segment_start.max(source_start);
        let overlap_end = segment_end.min(source_end);
        if overlap_start >= overlap_end {
            continue;
        }

        let overlap_len = overlap_end - overlap_start;
        let segment_offset = overlap_start - segment_start;
        let projected_output_start = output_start + (overlap_start - source_start);
        let kind = match &segment.kind {
            TraceTupleSegmentKind::Values { row, row_start } => TraceTupleSegmentKind::Values {
                row: row.clone(),
                row_start: row_start.saturating_add(segment_offset),
            },
            TraceTupleSegmentKind::Nulls => TraceTupleSegmentKind::Nulls,
            TraceTupleSegmentKind::Missing => TraceTupleSegmentKind::Missing,
        };
        push_segment(
            segments,
            TraceTupleSegment {
                output_start: projected_output_start,
                len: overlap_len,
                kind,
            },
        );
    }

    if source_end < source_start.saturating_add(len) {
        push_segment(
            segments,
            TraceTupleSegment {
                output_start: output_start + (source_end - source_start),
                len: source_start.saturating_add(len) - source_end,
                kind: TraceTupleSegmentKind::Missing,
            },
        );
    }
}

fn push_segment(segments: &mut Vec<TraceTupleSegment>, segment: TraceTupleSegment) {
    if segment.len == 0 {
        return;
    }

    if let Some(previous) = segments.last_mut() {
        let adjacent_output =
            previous.output_start.saturating_add(previous.len) == segment.output_start;
        if adjacent_output {
            match (&mut previous.kind, &segment.kind) {
                (TraceTupleSegmentKind::Nulls, TraceTupleSegmentKind::Nulls)
                | (TraceTupleSegmentKind::Missing, TraceTupleSegmentKind::Missing) => {
                    previous.len = previous.len.saturating_add(segment.len);
                    return;
                }
                (
                    TraceTupleSegmentKind::Values {
                        row: previous_row,
                        row_start: previous_row_start,
                    },
                    TraceTupleSegmentKind::Values { row, row_start },
                ) if Rc::ptr_eq(previous_row, row)
                    && previous_row_start.saturating_add(previous.len) == *row_start =>
                {
                    previous.len = previous.len.saturating_add(segment.len);
                    return;
                }
                _ => {}
            }
        }
    }

    segments.push(segment);
}

#[derive(Clone, Debug, Default)]
pub struct TraceTupleArena;

impl TraceTupleArena {
    pub fn base_rc(&self, row: Rc<Row>) -> TraceTupleHandle {
        TraceTupleHandle::new(TraceTupleLayout::from_row(row.clone()), Some(row))
    }

    pub fn owned(&self, row: Row) -> TraceTupleHandle {
        let row = Rc::new(row);
        TraceTupleHandle::new(TraceTupleLayout::from_row(row.clone()), Some(row))
    }

    pub fn owned_rc(&self, row: Rc<Row>) -> TraceTupleHandle {
        TraceTupleHandle::new(TraceTupleLayout::from_row(row.clone()), Some(row))
    }

    pub fn concat(
        &self,
        left: TraceTupleHandle,
        right: TraceTupleHandle,
        left_width: usize,
    ) -> TraceTupleHandle {
        TraceTupleHandle::new(TraceTupleLayout::concat(&left, &right, left_width), None)
    }

    pub fn null_pad_left(&self, right: TraceTupleHandle, left_width: usize) -> TraceTupleHandle {
        TraceTupleHandle::new(TraceTupleLayout::null_pad_left(&right, left_width), None)
    }

    pub fn null_pad_right(&self, left: TraceTupleHandle, right_width: usize) -> TraceTupleHandle {
        TraceTupleHandle::new(TraceTupleLayout::null_pad_right(&left, right_width), None)
    }

    pub fn project(&self, input: TraceTupleHandle, columns: Rc<[usize]>) -> TraceTupleHandle {
        TraceTupleHandle::new(TraceTupleLayout::project(&input, columns), None)
    }

    #[inline]
    pub fn len(&self, handle: &TraceTupleHandle) -> usize {
        handle.len()
    }

    #[inline]
    pub fn row_id(&self, handle: &TraceTupleHandle) -> RowId {
        handle.row_id()
    }

    #[inline]
    pub fn version(&self, handle: &TraceTupleHandle) -> u64 {
        handle.version()
    }

    #[inline]
    pub fn value_at(&self, handle: &TraceTupleHandle, index: usize) -> Option<Value> {
        handle.value_at(index)
    }

    #[inline]
    pub fn materialize_rc(&self, handle: &TraceTupleHandle) -> Rc<Row> {
        handle.materialize_rc()
    }

    #[inline]
    pub fn materialize_row(&self, handle: &TraceTupleHandle) -> Row {
        handle.materialize_row()
    }

    #[inline]
    pub fn has_materialized_row(&self, handle: &TraceTupleHandle) -> bool {
        handle.has_materialized_row()
    }
}

#[derive(Clone, Debug)]
pub struct TraceDeltaBatch {
    arena: TraceTupleArena,
    deltas: Vec<Delta<TraceTupleHandle>>,
    insert_count: usize,
    delete_count: usize,
    materialized_rows: RefCell<Option<Vec<Delta<Row>>>>,
}

impl TraceDeltaBatch {
    pub fn empty() -> Self {
        Self::new(TraceTupleArena, Vec::new())
    }

    pub fn new(arena: TraceTupleArena, deltas: Vec<Delta<TraceTupleHandle>>) -> Self {
        let insert_count = deltas.iter().filter(|delta| delta.is_insert()).count();
        let delete_count = deltas.iter().filter(|delta| delta.is_delete()).count();
        Self {
            arena,
            deltas,
            insert_count,
            delete_count,
            materialized_rows: RefCell::new(None),
        }
    }

    pub fn from_row_deltas(deltas: Vec<Delta<Row>>) -> Self {
        let arena = TraceTupleArena;
        let handles = deltas
            .into_iter()
            .map(|delta| Delta::new(arena.owned(delta.data), delta.diff))
            .collect();
        Self::new(arena, handles)
    }

    #[inline]
    pub fn arena(&self) -> &TraceTupleArena {
        &self.arena
    }

    #[inline]
    pub fn deltas(&self) -> &[Delta<TraceTupleHandle>] {
        &self.deltas
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    #[inline]
    pub fn insert_count(&self) -> usize {
        self.insert_count
    }

    #[inline]
    pub fn delete_count(&self) -> usize {
        self.delete_count
    }

    pub fn materialize_rows(&self) -> Vec<Delta<Row>> {
        if let Some(rows) = self.materialized_rows.borrow().as_ref() {
            return rows.clone();
        }

        let rows = self
            .deltas
            .iter()
            .map(|delta| Delta::new(self.arena.materialize_row(&delta.data), delta.diff))
            .collect::<Vec<_>>();
        *self.materialized_rows.borrow_mut() = Some(rows.clone());
        rows
    }
}

#[derive(Clone, Debug)]
pub struct VisibleResultStore {
    storage: VisibleResultStoreStorage,
}

#[derive(Clone, Debug)]
enum VisibleResultStoreStorage {
    Owned(OwnedVisibleRows),
    Shared(SharedVisibleRows),
}

#[derive(Clone, Debug, Default)]
struct OwnedVisibleRows {
    slots: Vec<Option<Row>>,
    row_to_slot: HashMap<RowId, usize>,
    free_list: Vec<usize>,
    rc_shadow: OnceCell<Vec<Option<Rc<Row>>>>,
}

#[derive(Clone, Debug, Default)]
struct SharedVisibleRows {
    slots: Vec<Option<Rc<Row>>>,
    row_to_slot: HashMap<RowId, usize>,
    free_list: Vec<usize>,
}

pub struct VisibleResultStoreRowRefs<'a> {
    slots: &'a [Option<Rc<Row>>],
    index: usize,
}

pub struct VisibleResultStoreRows<'a> {
    storage: VisibleResultStoreRowsStorage<'a>,
}

enum VisibleResultStoreRowsStorage<'a> {
    Owned {
        slots: &'a [Option<Row>],
        index: usize,
    },
    Shared {
        slots: &'a [Option<Rc<Row>>],
        index: usize,
    },
}

impl Default for VisibleResultStore {
    fn default() -> Self {
        Self {
            storage: VisibleResultStoreStorage::Owned(OwnedVisibleRows::default()),
        }
    }
}

impl<'a> Iterator for VisibleResultStoreRowRefs<'a> {
    type Item = &'a Rc<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.slots.len() {
            let index = self.index;
            self.index += 1;
            if let Some(row) = self.slots[index].as_ref() {
                return Some(row);
            }
        }

        None
    }
}

impl<'a> Iterator for VisibleResultStoreRows<'a> {
    type Item = &'a Row;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.storage {
            VisibleResultStoreRowsStorage::Owned { slots, index } => {
                while *index < slots.len() {
                    let current = *index;
                    *index += 1;
                    if let Some(row) = slots[current].as_ref() {
                        return Some(row);
                    }
                }
            }
            VisibleResultStoreRowsStorage::Shared { slots, index } => {
                while *index < slots.len() {
                    let current = *index;
                    *index += 1;
                    if let Some(row) = slots[current].as_ref() {
                        return Some(row.as_ref());
                    }
                }
            }
        }

        None
    }
}

impl OwnedVisibleRows {
    fn from_rows(rows: Vec<Row>) -> Self {
        let mut slots = Vec::with_capacity(rows.len());
        let mut row_to_slot = HashMap::with_capacity(rows.len());
        let mut free_list = Vec::new();
        for row in rows {
            let row_id = row.id();
            let slot_id = slots.len();
            slots.push(Some(row));
            if let Some(previous_slot) = row_to_slot.insert(row_id, slot_id) {
                slots[previous_slot] = None;
                free_list.push(previous_slot);
            }
        }
        Self {
            slots,
            row_to_slot,
            free_list,
            rc_shadow: OnceCell::new(),
        }
    }

    fn apply_rows(&mut self, deltas: &[Delta<Row>]) {
        for delta in deltas {
            if delta.is_insert() {
                self.upsert(delta.data.clone());
            } else if delta.is_delete() {
                self.remove(delta.data.id());
            }
        }
    }

    fn replace_rows(&mut self, rows: Vec<Row>) {
        *self = Self::from_rows(rows);
    }

    fn rows(&self) -> Vec<Row> {
        self.slots
            .iter()
            .filter_map(|row| row.as_ref().cloned())
            .collect()
    }

    fn row_iter(&self) -> VisibleResultStoreRows<'_> {
        VisibleResultStoreRows {
            storage: VisibleResultStoreRowsStorage::Owned {
                slots: self.slots.as_slice(),
                index: 0,
            },
        }
    }

    fn rc_rows(&self) -> Vec<Rc<Row>> {
        self.rc_shadow_slots()
            .iter()
            .filter_map(|row| row.as_ref().cloned())
            .collect()
    }

    fn row_refs(&self) -> VisibleResultStoreRowRefs<'_> {
        VisibleResultStoreRowRefs {
            slots: self.rc_shadow_slots().as_slice(),
            index: 0,
        }
    }

    fn visit_rc_rows<F>(&self, mut visitor: F)
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row in self.rc_shadow_slots().iter().filter_map(|row| row.as_ref()) {
            if !visitor(row) {
                break;
            }
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.row_to_slot.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.row_to_slot.is_empty()
    }

    fn clear(&mut self) {
        self.slots.clear();
        self.row_to_slot.clear();
        self.free_list.clear();
        self.rc_shadow = OnceCell::new();
    }

    fn upsert(&mut self, row: Row) {
        let row_id = row.id();
        let shadow_row = self.rc_shadow.get_mut().map(|_| Rc::new(row.clone()));
        if let Some(slot_id) = self.row_to_slot.get(&row_id).copied() {
            self.slots[slot_id] = Some(row);
            if let Some(shadow) = self.rc_shadow.get_mut() {
                shadow[slot_id] = shadow_row;
            }
            return;
        }

        let slot_id = if let Some(slot_id) = self.free_list.pop() {
            self.slots[slot_id] = Some(row);
            if let Some(shadow) = self.rc_shadow.get_mut() {
                shadow[slot_id] = shadow_row;
            }
            slot_id
        } else {
            self.slots.push(Some(row));
            if let Some(shadow) = self.rc_shadow.get_mut() {
                shadow.push(shadow_row);
            }
            self.slots.len().saturating_sub(1)
        };
        self.row_to_slot.insert(row_id, slot_id);
    }

    fn remove(&mut self, row_id: RowId) {
        if let Some(slot_id) = self.row_to_slot.remove(&row_id) {
            self.slots[slot_id] = None;
            if let Some(shadow) = self.rc_shadow.get_mut() {
                shadow[slot_id] = None;
            }
            self.free_list.push(slot_id);
        }
    }

    fn rc_shadow_slots(&self) -> &Vec<Option<Rc<Row>>> {
        self.rc_shadow.get_or_init(|| {
            self.slots
                .iter()
                .map(|row| row.as_ref().map(|row| Rc::new(row.clone())))
                .collect()
        })
    }
}

impl SharedVisibleRows {
    fn from_rc_rows(rows: Vec<Rc<Row>>) -> Self {
        let mut slots = Vec::with_capacity(rows.len());
        let mut row_to_slot = HashMap::with_capacity(rows.len());
        let mut free_list = Vec::new();
        for row in rows {
            let row_id = row.id();
            let slot_id = slots.len();
            slots.push(Some(row));
            if let Some(previous_slot) = row_to_slot.insert(row_id, slot_id) {
                slots[previous_slot] = None;
                free_list.push(previous_slot);
            }
        }
        Self {
            slots,
            row_to_slot,
            free_list,
        }
    }

    fn apply(&mut self, batch: &TraceDeltaBatch) {
        for delta in batch.deltas() {
            let row_id = batch.arena().row_id(&delta.data);
            if delta.is_insert() {
                self.upsert(batch.arena().materialize_rc(&delta.data));
            } else if delta.is_delete() {
                self.remove(row_id);
            }
        }
    }

    fn apply_rows(&mut self, deltas: &[Delta<Row>]) {
        for delta in deltas {
            if delta.is_insert() {
                self.upsert(Rc::new(delta.data.clone()));
            } else if delta.is_delete() {
                self.remove(delta.data.id());
            }
        }
    }

    fn replace_rc_rows(&mut self, rows: Vec<Rc<Row>>) {
        *self = Self::from_rc_rows(rows);
    }

    fn rows(&self) -> Vec<Row> {
        self.slots
            .iter()
            .filter_map(|row| row.as_ref().map(|row| (**row).clone()))
            .collect()
    }

    fn row_iter(&self) -> VisibleResultStoreRows<'_> {
        VisibleResultStoreRows {
            storage: VisibleResultStoreRowsStorage::Shared {
                slots: self.slots.as_slice(),
                index: 0,
            },
        }
    }

    fn rc_rows(&self) -> Vec<Rc<Row>> {
        self.slots
            .iter()
            .filter_map(|row| row.as_ref().cloned())
            .collect()
    }

    fn row_refs(&self) -> VisibleResultStoreRowRefs<'_> {
        VisibleResultStoreRowRefs {
            slots: self.slots.as_slice(),
            index: 0,
        }
    }

    fn visit_rc_rows<F>(&self, mut visitor: F)
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row in self.slots.iter().filter_map(|row| row.as_ref()) {
            if !visitor(row) {
                break;
            }
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.row_to_slot.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.row_to_slot.is_empty()
    }

    fn clear(&mut self) {
        self.slots.clear();
        self.row_to_slot.clear();
        self.free_list.clear();
    }

    fn upsert(&mut self, row: Rc<Row>) {
        let row_id = row.id();
        if let Some(slot_id) = self.row_to_slot.get(&row_id).copied() {
            self.slots[slot_id] = Some(row);
            return;
        }

        let slot_id = if let Some(slot_id) = self.free_list.pop() {
            self.slots[slot_id] = Some(row);
            slot_id
        } else {
            self.slots.push(Some(row));
            self.slots.len().saturating_sub(1)
        };
        self.row_to_slot.insert(row_id, slot_id);
    }

    fn remove(&mut self, row_id: RowId) {
        if let Some(slot_id) = self.row_to_slot.remove(&row_id) {
            self.slots[slot_id] = None;
            self.free_list.push(slot_id);
        }
    }
}

impl VisibleResultStore {
    pub fn from_rows(rows: Vec<Row>) -> Self {
        Self {
            storage: VisibleResultStoreStorage::Owned(OwnedVisibleRows::from_rows(rows)),
        }
    }

    pub fn from_rc_rows(rows: Vec<Rc<Row>>) -> Self {
        Self {
            storage: VisibleResultStoreStorage::Shared(SharedVisibleRows::from_rc_rows(rows)),
        }
    }

    pub fn apply(&mut self, batch: &TraceDeltaBatch) {
        match &mut self.storage {
            VisibleResultStoreStorage::Owned(rows) => {
                for delta in batch.deltas() {
                    let row_id = batch.arena().row_id(&delta.data);
                    if delta.is_insert() {
                        rows.upsert(batch.arena().materialize_row(&delta.data));
                    } else if delta.is_delete() {
                        rows.remove(row_id);
                    }
                }
            }
            VisibleResultStoreStorage::Shared(rows) => rows.apply(batch),
        }
    }

    pub fn apply_rows(&mut self, deltas: &[Delta<Row>]) {
        match &mut self.storage {
            VisibleResultStoreStorage::Owned(rows) => rows.apply_rows(deltas),
            VisibleResultStoreStorage::Shared(rows) => rows.apply_rows(deltas),
        }
    }

    pub fn replace_rows(&mut self, rows: Vec<Row>) {
        match &mut self.storage {
            VisibleResultStoreStorage::Owned(store) => store.replace_rows(rows),
            VisibleResultStoreStorage::Shared(_) => {
                self.storage = VisibleResultStoreStorage::Owned(OwnedVisibleRows::from_rows(rows));
            }
        }
    }

    pub fn replace_rc_rows(&mut self, rows: Vec<Rc<Row>>) {
        match &mut self.storage {
            VisibleResultStoreStorage::Shared(store) => store.replace_rc_rows(rows),
            VisibleResultStoreStorage::Owned(_) => {
                self.storage =
                    VisibleResultStoreStorage::Shared(SharedVisibleRows::from_rc_rows(rows));
            }
        }
    }

    pub fn rows(&self) -> Vec<Row> {
        match &self.storage {
            VisibleResultStoreStorage::Owned(rows) => rows.rows(),
            VisibleResultStoreStorage::Shared(rows) => rows.rows(),
        }
    }

    pub fn row_iter(&self) -> VisibleResultStoreRows<'_> {
        match &self.storage {
            VisibleResultStoreStorage::Owned(rows) => rows.row_iter(),
            VisibleResultStoreStorage::Shared(rows) => rows.row_iter(),
        }
    }

    pub fn rc_rows(&self) -> Vec<Rc<Row>> {
        match &self.storage {
            VisibleResultStoreStorage::Owned(rows) => rows.rc_rows(),
            VisibleResultStoreStorage::Shared(rows) => rows.rc_rows(),
        }
    }

    pub fn row_refs(&self) -> VisibleResultStoreRowRefs<'_> {
        match &self.storage {
            VisibleResultStoreStorage::Owned(rows) => rows.row_refs(),
            VisibleResultStoreStorage::Shared(rows) => rows.row_refs(),
        }
    }

    pub fn visit_rc_rows<F>(&self, visitor: F)
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        match &self.storage {
            VisibleResultStoreStorage::Owned(rows) => rows.visit_rc_rows(visitor),
            VisibleResultStoreStorage::Shared(rows) => rows.visit_rc_rows(visitor),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match &self.storage {
            VisibleResultStoreStorage::Owned(rows) => rows.len(),
            VisibleResultStoreStorage::Shared(rows) => rows.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match &self.storage {
            VisibleResultStoreStorage::Owned(rows) => rows.is_empty(),
            VisibleResultStoreStorage::Shared(rows) => rows.is_empty(),
        }
    }

    pub fn clear(&mut self) {
        match &mut self.storage {
            VisibleResultStoreStorage::Owned(rows) => rows.clear(),
            VisibleResultStoreStorage::Shared(rows) => rows.clear(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use cynos_core::Value;

    fn make_row(id: u64, value: i64) -> Row {
        Row::new(id, vec![Value::Int64(id as i64), Value::Int64(value)])
    }

    #[test]
    fn trace_tuple_value_at_uses_flat_layout_without_materializing() {
        let arena = TraceTupleArena;
        let left = arena.owned(make_row(1, 10));
        let middle = arena.owned(Row::new(
            2,
            vec![Value::Int64(2), Value::String("middle".into())],
        ));
        let right = arena.owned(make_row(3, 30));

        let joined = arena.concat(
            arena.concat(left.clone(), middle.clone(), left.len()),
            right.clone(),
            left.len().saturating_add(middle.len()),
        );

        assert_eq!(joined.segment_count(), 3);
        assert!(!joined.has_materialized_row());
        assert_eq!(joined.value_at(0), Some(Value::Int64(1)));
        assert_eq!(joined.value_at(3), Some(Value::String("middle".into())));
        assert_eq!(joined.value_at(5), Some(Value::Int64(30)));
        assert!(!joined.has_materialized_row());

        let row = joined.materialize_rc();
        assert_eq!(row.len(), 6);
        assert!(joined.has_materialized_row());
    }

    #[test]
    fn trace_tuple_project_and_null_padding_preserve_existing_semantics() {
        let arena = TraceTupleArena;
        let left = arena.owned(make_row(1, 10));
        let right = arena.owned(Row::new(
            2,
            vec![Value::Int64(2), Value::String("right".into())],
        ));
        let joined = arena.concat(left.clone(), right.clone(), left.len());
        let projected = arena.project(joined, Rc::from([3usize, 0, 99]));
        let padded = arena.null_pad_left(projected.clone(), 2);

        assert_eq!(projected.len(), 3);
        assert_eq!(projected.value_at(0), Some(Value::String("right".into())));
        assert_eq!(projected.value_at(1), Some(Value::Int64(1)));
        assert_eq!(projected.value_at(2), None);
        assert!(!projected.has_materialized_row());

        assert_eq!(padded.value_at(0), Some(Value::Null));
        assert_eq!(padded.value_at(1), Some(Value::Null));
        assert_eq!(padded.value_at(2), Some(Value::String("right".into())));
        assert_eq!(padded.value_at(3), Some(Value::Int64(1)));
        assert_eq!(padded.value_at(4), None);

        let projected_row = projected.materialize_rc();
        assert_eq!(
            projected_row.values(),
            &[Value::String("right".into()), Value::Int64(1)]
        );
    }

    #[test]
    fn visible_result_store_keeps_owned_rows_until_rc_view_needed() {
        let store = VisibleResultStore::from_rows(vec![make_row(1, 10), make_row(2, 20)]);

        match &store.storage {
            VisibleResultStoreStorage::Owned(rows) => {
                assert!(rows.rc_shadow.get().is_none());
            }
            VisibleResultStoreStorage::Shared(_) => panic!("expected owned storage"),
        }

        let ids = store.row_refs().map(|row| row.id()).collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 2]);

        match &store.storage {
            VisibleResultStoreStorage::Owned(rows) => {
                assert!(rows.rc_shadow.get().is_some());
            }
            VisibleResultStoreStorage::Shared(_) => panic!("expected owned storage"),
        }
    }

    #[test]
    fn visible_result_store_row_iter_does_not_build_owned_rc_shadow() {
        let store = VisibleResultStore::from_rows(vec![make_row(1, 10), make_row(2, 20)]);

        let ids = store.row_iter().map(|row| row.id()).collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 2]);

        match &store.storage {
            VisibleResultStoreStorage::Owned(rows) => {
                assert!(rows.rc_shadow.get().is_none());
            }
            VisibleResultStoreStorage::Shared(_) => panic!("expected owned storage"),
        }
    }

    #[test]
    fn visible_result_store_updates_rc_shadow_after_owned_mutations() {
        let mut store = VisibleResultStore::from_rows(vec![make_row(1, 10), make_row(2, 20)]);
        let _ = store.row_refs().collect::<Vec<_>>();

        store.apply_rows(&[
            Delta::delete(make_row(1, 10)),
            Delta::insert(make_row(3, 30)),
            Delta::insert(make_row(2, 200)),
        ]);

        let rows = store
            .rows()
            .into_iter()
            .map(|row| {
                (
                    row.id(),
                    row.get(1)
                        .and_then(|value| value.as_i64())
                        .unwrap_or_default(),
                )
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(rows.get(&2), Some(&200));
        assert_eq!(rows.get(&3), Some(&30));
        assert!(!rows.contains_key(&1));

        let refs = store
            .row_refs()
            .map(|row| {
                (
                    row.id(),
                    row.get(1)
                        .and_then(|value| value.as_i64())
                        .unwrap_or_default(),
                )
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(refs.get(&2), Some(&200));
        assert_eq!(refs.get(&3), Some(&30));
        assert!(!refs.contains_key(&1));
    }
}
