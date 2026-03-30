//! Internal late-materialization primitives for trace()/IVM.

use crate::delta::Delta;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::{join_row_id, left_join_null_row_id, right_join_null_row_id, Row, RowId, Value};
use hashbrown::HashMap;

#[derive(Clone, Debug)]
pub struct TraceTupleHandle(Rc<TraceTupleNode>);

#[derive(Debug)]
struct TraceTupleNode {
    kind: TraceTupleKind,
    materialized: RefCell<Option<Rc<Row>>>,
}

#[derive(Clone, Debug)]
enum TraceTupleKind {
    Base(Rc<Row>),
    Concat {
        left: TraceTupleHandle,
        right: TraceTupleHandle,
        left_width: usize,
    },
    NullPadLeft {
        right: TraceTupleHandle,
        left_width: usize,
    },
    NullPadRight {
        left: TraceTupleHandle,
        right_width: usize,
    },
    Project {
        input: TraceTupleHandle,
        columns: Rc<[usize]>,
    },
    Owned(Rc<Row>),
}

impl TraceTupleHandle {
    fn new(kind: TraceTupleKind, materialized: Option<Rc<Row>>) -> Self {
        Self(Rc::new(TraceTupleNode {
            kind,
            materialized: RefCell::new(materialized),
        }))
    }

    pub fn len(&self) -> usize {
        match &self.0.kind {
            TraceTupleKind::Base(row) | TraceTupleKind::Owned(row) => row.len(),
            TraceTupleKind::Concat {
                left_width, right, ..
            } => left_width.saturating_add(right.len()),
            TraceTupleKind::NullPadLeft { left_width, right } => {
                left_width.saturating_add(right.len())
            }
            TraceTupleKind::NullPadRight { left, right_width } => {
                left.len().saturating_add(*right_width)
            }
            TraceTupleKind::Project { columns, .. } => columns.len(),
        }
    }

    pub fn row_id(&self) -> RowId {
        match &self.0.kind {
            TraceTupleKind::Base(row) | TraceTupleKind::Owned(row) => row.id(),
            TraceTupleKind::Concat { left, right, .. } => {
                join_row_id(left.row_id(), right.row_id())
            }
            TraceTupleKind::NullPadLeft { right, .. } => right_join_null_row_id(right.row_id()),
            TraceTupleKind::NullPadRight { left, .. } => left_join_null_row_id(left.row_id()),
            TraceTupleKind::Project { input, .. } => input.row_id(),
        }
    }

    pub fn version(&self) -> u64 {
        match &self.0.kind {
            TraceTupleKind::Base(row) | TraceTupleKind::Owned(row) => row.version(),
            TraceTupleKind::Concat { left, right, .. } => {
                left.version().wrapping_add(right.version())
            }
            TraceTupleKind::NullPadLeft { right, .. } => right.version(),
            TraceTupleKind::NullPadRight { left, .. } => left.version(),
            TraceTupleKind::Project { input, .. } => input.version(),
        }
    }

    pub fn value_at(&self, index: usize) -> Option<Value> {
        match &self.0.kind {
            TraceTupleKind::Base(row) | TraceTupleKind::Owned(row) => row.get(index).cloned(),
            TraceTupleKind::Concat {
                left,
                right,
                left_width,
            } => {
                if index < *left_width {
                    left.value_at(index)
                } else {
                    right.value_at(index.saturating_sub(*left_width))
                }
            }
            TraceTupleKind::NullPadLeft { right, left_width } => {
                if index < *left_width {
                    Some(Value::Null)
                } else {
                    right.value_at(index.saturating_sub(*left_width))
                }
            }
            TraceTupleKind::NullPadRight { left, right_width } => {
                if index < left.len() {
                    left.value_at(index)
                } else if index < left.len().saturating_add(*right_width) {
                    Some(Value::Null)
                } else {
                    None
                }
            }
            TraceTupleKind::Project { input, columns } => columns
                .get(index)
                .and_then(|source_index| input.value_at(*source_index)),
        }
    }

    pub fn materialize_rc(&self) -> Rc<Row> {
        if let Some(row) = self.0.materialized.borrow().as_ref() {
            return row.clone();
        }

        let row = match &self.0.kind {
            TraceTupleKind::Base(row) | TraceTupleKind::Owned(row) => row.clone(),
            TraceTupleKind::Concat { left, right, .. } => {
                let left_row = left.materialize_rc();
                let right_row = right.materialize_rc();
                let mut values = Vec::with_capacity(left_row.len().saturating_add(right_row.len()));
                values.extend(left_row.values().iter().cloned());
                values.extend(right_row.values().iter().cloned());
                Rc::new(Row::new_with_version(self.row_id(), self.version(), values))
            }
            TraceTupleKind::NullPadLeft { right, left_width } => {
                let right_row = right.materialize_rc();
                let mut values = Vec::with_capacity(left_width.saturating_add(right_row.len()));
                values.resize(*left_width, Value::Null);
                values.extend(right_row.values().iter().cloned());
                Rc::new(Row::new_with_version(self.row_id(), self.version(), values))
            }
            TraceTupleKind::NullPadRight { left, right_width } => {
                let left_row = left.materialize_rc();
                let mut values = Vec::with_capacity(left_row.len().saturating_add(*right_width));
                values.extend(left_row.values().iter().cloned());
                values.resize(values.len().saturating_add(*right_width), Value::Null);
                Rc::new(Row::new_with_version(self.row_id(), self.version(), values))
            }
            TraceTupleKind::Project { input, columns } => {
                let input_row = input.materialize_rc();
                let values = columns
                    .iter()
                    .filter_map(|index| input_row.get(*index).cloned())
                    .collect();
                Rc::new(Row::new_with_version(self.row_id(), self.version(), values))
            }
        };

        *self.0.materialized.borrow_mut() = Some(row.clone());
        row
    }

    pub fn materialize_row(&self) -> Row {
        (*self.materialize_rc()).clone()
    }

    pub fn has_materialized_row(&self) -> bool {
        self.0.materialized.borrow().is_some()
    }
}

#[derive(Clone, Debug, Default)]
pub struct TraceTupleArena;

impl TraceTupleArena {
    pub fn base_rc(&self, row: Rc<Row>) -> TraceTupleHandle {
        TraceTupleHandle::new(TraceTupleKind::Base(row.clone()), Some(row))
    }

    pub fn owned(&self, row: Row) -> TraceTupleHandle {
        let row = Rc::new(row);
        TraceTupleHandle::new(TraceTupleKind::Owned(row.clone()), Some(row))
    }

    pub fn owned_rc(&self, row: Rc<Row>) -> TraceTupleHandle {
        TraceTupleHandle::new(TraceTupleKind::Owned(row.clone()), Some(row))
    }

    pub fn concat(
        &self,
        left: TraceTupleHandle,
        right: TraceTupleHandle,
        left_width: usize,
    ) -> TraceTupleHandle {
        TraceTupleHandle::new(
            TraceTupleKind::Concat {
                left,
                right,
                left_width,
            },
            None,
        )
    }

    pub fn null_pad_left(&self, right: TraceTupleHandle, left_width: usize) -> TraceTupleHandle {
        TraceTupleHandle::new(TraceTupleKind::NullPadLeft { right, left_width }, None)
    }

    pub fn null_pad_right(&self, left: TraceTupleHandle, right_width: usize) -> TraceTupleHandle {
        TraceTupleHandle::new(TraceTupleKind::NullPadRight { left, right_width }, None)
    }

    pub fn project(&self, input: TraceTupleHandle, columns: Rc<[usize]>) -> TraceTupleHandle {
        TraceTupleHandle::new(TraceTupleKind::Project { input, columns }, None)
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

#[derive(Clone, Debug, Default)]
pub struct VisibleResultStore {
    slots: Vec<Option<Rc<Row>>>,
    row_to_slot: HashMap<RowId, usize>,
    free_list: Vec<usize>,
}

impl VisibleResultStore {
    pub fn from_rows(rows: Vec<Row>) -> Self {
        let mut store = Self::default();
        store.replace_rows(rows);
        store
    }

    pub fn from_rc_rows(rows: Vec<Rc<Row>>) -> Self {
        let mut store = Self::default();
        store.replace_rc_rows(rows);
        store
    }

    pub fn apply(&mut self, batch: &TraceDeltaBatch) {
        for delta in batch.deltas() {
            let row_id = batch.arena().row_id(&delta.data);
            if delta.is_insert() {
                self.upsert_rc(batch.arena().materialize_rc(&delta.data));
            } else if delta.is_delete() {
                self.remove(row_id);
            }
        }
    }

    pub fn apply_rows(&mut self, deltas: &[Delta<Row>]) {
        for delta in deltas {
            if delta.is_insert() {
                self.upsert_rc(Rc::new(delta.data.clone()));
            } else if delta.is_delete() {
                self.remove(delta.data.id());
            }
        }
    }

    pub fn replace_rows(&mut self, rows: Vec<Row>) {
        self.clear();
        self.slots.reserve(rows.len());
        self.row_to_slot.reserve(rows.len());
        for row in rows {
            let row = Rc::new(row);
            if let Some(slot_id) = self.row_to_slot.get(&row.id()).copied() {
                self.slots[slot_id] = Some(row);
            } else {
                let slot_id = self.slots.len();
                self.slots.push(Some(row.clone()));
                self.row_to_slot.insert(row.id(), slot_id);
            }
        }
    }

    pub fn replace_rc_rows(&mut self, rows: Vec<Rc<Row>>) {
        self.clear();
        self.slots.reserve(rows.len());
        self.row_to_slot.reserve(rows.len());
        for row in rows {
            if let Some(slot_id) = self.row_to_slot.get(&row.id()).copied() {
                self.slots[slot_id] = Some(row);
            } else {
                let slot_id = self.slots.len();
                self.slots.push(Some(row.clone()));
                self.row_to_slot.insert(row.id(), slot_id);
            }
        }
    }

    pub fn rows(&self) -> Vec<Row> {
        self.slots
            .iter()
            .filter_map(|row| row.as_ref().map(|row| (**row).clone()))
            .collect()
    }

    pub fn rc_rows(&self) -> Vec<Rc<Row>> {
        self.slots
            .iter()
            .filter_map(|row| row.as_ref().cloned())
            .collect()
    }

    pub fn row_refs(&self) -> impl Iterator<Item = &Rc<Row>> + '_ {
        self.slots.iter().filter_map(|row| row.as_ref())
    }

    pub fn visit_rc_rows<F>(&self, mut visitor: F)
    where
        F: FnMut(&Rc<Row>) -> bool,
    {
        for row in self.row_refs() {
            if !visitor(row) {
                break;
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.row_to_slot.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.row_to_slot.is_empty()
    }

    pub fn clear(&mut self) {
        self.slots.clear();
        self.row_to_slot.clear();
        self.free_list.clear();
    }

    fn upsert_rc(&mut self, row: Rc<Row>) {
        if let Some(slot_id) = self.row_to_slot.get(&row.id()).copied() {
            self.slots[slot_id] = Some(row);
            return;
        }

        let slot_id = if let Some(slot_id) = self.free_list.pop() {
            self.slots[slot_id] = Some(row.clone());
            slot_id
        } else {
            self.slots.push(Some(row.clone()));
            self.slots.len().saturating_sub(1)
        };
        self.row_to_slot.insert(row.id(), slot_id);
    }

    fn remove(&mut self, row_id: RowId) {
        if let Some(slot_id) = self.row_to_slot.remove(&row_id) {
            self.slots[slot_id] = None;
            self.free_list.push(slot_id);
        }
    }
}
