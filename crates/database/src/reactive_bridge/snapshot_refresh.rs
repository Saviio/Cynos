use crate::live_runtime::{
    RowsSnapshotLookupPrimitive, RowsSnapshotOrderKey, RowsSnapshotPartialRefreshMetadata,
    RowsSnapshotPartialRefreshState, RowsSnapshotRootSubsetMetadata, RowsSnapshotRootSubsetPlan,
};
use crate::query_engine::{
    CompiledPhysicalPlan, RootSubsetPlanVariant, RootSubsetRefreshCostInput,
    RootSubsetRefreshDecision, SnapshotRefreshCostModel,
};
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cmp::Ordering;
use cynos_core::{Row, Value};
use cynos_incremental::Delta;
use cynos_storage::{RowStore, TableCache};
use hashbrown::{HashMap, HashSet};

#[derive(Clone)]
pub(super) struct PartialRefreshCandidateRow {
    pub(super) row: Rc<Row>,
    pub(super) root_row_id: u64,
}

pub(super) struct SnapshotPartialRefreshRuntime {
    pub(super) metadata: RowsSnapshotPartialRefreshMetadata,
    pub(super) candidate_rows: Vec<PartialRefreshCandidateRow>,
}

impl SnapshotPartialRefreshRuntime {
    pub(super) fn new(cache: &TableCache, state: RowsSnapshotPartialRefreshState) -> Self {
        let candidate_rows = build_partial_refresh_candidate_rows(
            cache,
            &state.metadata,
            &state.initial_candidate_rows,
        )
        .expect("partial refresh candidate rows should be resolvable");
        Self {
            metadata: state.metadata,
            candidate_rows,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum SnapshotRootKey {
    Empty,
    One(Value),
    Many(Vec<Value>),
}

impl SnapshotRootKey {
    fn from_values(mut values: Vec<Value>) -> Self {
        match values.len() {
            0 => Self::Empty,
            1 => Self::One(values.pop().unwrap()),
            _ => Self::Many(values),
        }
    }

    fn from_output_row(row: &Row, indices: &[usize]) -> Option<Self> {
        let mut values = Vec::with_capacity(indices.len());
        for &index in indices {
            values.push(row.get(index)?.clone());
        }
        Some(Self::from_values(values))
    }

    fn from_store_row(row: &Row, indices: &[usize]) -> Option<Self> {
        let mut values = Vec::with_capacity(indices.len());
        for &column_index in indices {
            values.push(row.get(column_index)?.clone());
        }
        Some(Self::from_values(values))
    }
}

pub(super) struct RootSubsetRefreshRuntime {
    compiled_plans: crate::live_runtime::RowsSnapshotRootSubsetVariants,
    pub(super) metadata: RowsSnapshotRootSubsetMetadata,
    row_id_to_root_key: HashMap<u64, SnapshotRootKey>,
    root_ordinals: HashMap<SnapshotRootKey, usize>,
    next_root_ordinal: usize,
    root_rows: HashMap<SnapshotRootKey, Vec<Rc<Row>>>,
    visible_root_order: Vec<SnapshotRootKey>,
}

pub(super) struct SnapshotRootRefreshOutcome {
    pub(super) changed: bool,
    pub(super) dirty_root_rows: HashSet<u64>,
}

impl RootSubsetRefreshRuntime {
    pub(super) fn new(
        cache: &TableCache,
        plan: RowsSnapshotRootSubsetPlan,
        initial_rows: &[Rc<Row>],
    ) -> Option<Self> {
        let RowsSnapshotRootSubsetPlan {
            metadata,
            compiled_plans,
        } = plan;
        let store = cache.get_table(&metadata.root_table)?;

        let mut row_id_to_root_key = HashMap::new();
        let mut root_ordinals = HashMap::new();
        let mut next_root_ordinal = 0usize;
        store.visit_rows(|row| {
            if let Some(root_key) =
                SnapshotRootKey::from_store_row(row, &metadata.root_pk_store_indices)
            {
                row_id_to_root_key.insert(row.id(), root_key.clone());
                if !root_ordinals.contains_key(&root_key) {
                    root_ordinals.insert(root_key, next_root_ordinal);
                    next_root_ordinal += 1;
                }
            }
            true
        });

        let mut root_rows = HashMap::new();
        let mut visible_root_order = Vec::new();
        let mut seen_roots = HashSet::new();
        let mut last_root_key: Option<SnapshotRootKey> = None;

        for row in initial_rows {
            let root_key =
                SnapshotRootKey::from_output_row(row.as_ref(), &metadata.root_pk_output_indices)?;
            if last_root_key.as_ref() != Some(&root_key) && seen_roots.contains(&root_key) {
                return None;
            }
            if seen_roots.insert(root_key.clone()) {
                visible_root_order.push(root_key.clone());
            }
            root_rows
                .entry(root_key.clone())
                .or_insert_with(Vec::new)
                .push(row.clone());
            last_root_key = Some(root_key);
        }

        Some(Self {
            compiled_plans,
            metadata,
            row_id_to_root_key,
            root_ordinals,
            next_root_ordinal,
            root_rows,
            visible_root_order,
        })
    }

    fn select_decision(
        &self,
        cache: &TableCache,
        affected_root_ids: &HashSet<u64>,
    ) -> RootSubsetRefreshDecision {
        let table_row_count = cache
            .get_table(&self.metadata.root_table)
            .map(|store| store.len())
            .unwrap_or(0);
        let input = RootSubsetRefreshCostInput::new(affected_root_ids.len(), table_row_count);
        SnapshotRefreshCostModel::DEFAULT.decide_root_subset_refresh_for(input)
    }

    fn select_variant(
        &self,
        cache: &TableCache,
        affected_root_ids: &HashSet<u64>,
    ) -> RootSubsetPlanVariant {
        self.select_decision(cache, affected_root_ids).variant
    }

    pub(super) fn select_compiled_plan<'a>(
        &'a self,
        cache: &TableCache,
        affected_root_ids: &HashSet<u64>,
    ) -> &'a CompiledPhysicalPlan {
        self.compiled_plans
            .select(self.select_variant(cache, affected_root_ids))
    }

    pub(super) fn collect_affected_root_keys(
        &mut self,
        cache: &TableCache,
        affected_root_ids: &HashSet<u64>,
        refresh_existing: bool,
    ) -> Option<HashSet<SnapshotRootKey>> {
        let store = cache.get_table(&self.metadata.root_table)?;
        let mut affected_keys = HashSet::new();

        for &row_id in affected_root_ids {
            if let Some(existing) = self.row_id_to_root_key.get(&row_id).cloned() {
                affected_keys.insert(existing);
                if !refresh_existing {
                    continue;
                }
            }

            if let Some(row) = store.get(row_id) {
                let root_key = SnapshotRootKey::from_store_row(
                    row.as_ref(),
                    &self.metadata.root_pk_store_indices,
                )?;
                affected_keys.insert(root_key.clone());
                if !self.root_ordinals.contains_key(&root_key) {
                    self.root_ordinals
                        .insert(root_key.clone(), self.next_root_ordinal);
                    self.next_root_ordinal += 1;
                }
                self.row_id_to_root_key.insert(row_id, root_key);
            } else {
                self.row_id_to_root_key.remove(&row_id);
            }
        }

        Some(affected_keys)
    }

    pub(super) fn apply_subset_rows(
        &mut self,
        affected_root_keys: &HashSet<SnapshotRootKey>,
        rows: Vec<Rc<Row>>,
    ) -> Option<Vec<Rc<Row>>> {
        let mut recomputed_groups: HashMap<SnapshotRootKey, Vec<Rc<Row>>> = HashMap::new();
        for row in rows {
            let root_key = SnapshotRootKey::from_output_row(
                row.as_ref(),
                &self.metadata.root_pk_output_indices,
            )?;
            recomputed_groups
                .entry(root_key)
                .or_insert_with(Vec::new)
                .push(row);
        }

        let mut next_visible_root_order = Vec::with_capacity(
            self.visible_root_order
                .len()
                .saturating_add(recomputed_groups.len()),
        );
        for root_key in affected_root_keys {
            if !recomputed_groups.contains_key(root_key) {
                self.root_rows.remove(root_key);
            }
        }

        for root_key in self.visible_root_order.drain(..) {
            if affected_root_keys.contains(&root_key) {
                if let Some(group) = recomputed_groups.remove(&root_key) {
                    self.root_rows.insert(root_key.clone(), group);
                    next_visible_root_order.push(root_key);
                }
                continue;
            }

            next_visible_root_order.push(root_key);
        }

        let mut new_root_entries: Vec<_> = recomputed_groups
            .into_iter()
            .map(|(root_key, group)| {
                let ordinal = self
                    .root_ordinals
                    .get(&root_key)
                    .copied()
                    .unwrap_or(usize::MAX);
                (root_key, group, ordinal)
            })
            .collect();
        new_root_entries.sort_by_key(|(_, _, ordinal)| *ordinal);

        if new_root_entries.is_empty() {
            self.visible_root_order = next_visible_root_order;
        } else {
            let mut merged_order =
                Vec::with_capacity(next_visible_root_order.len() + new_root_entries.len());
            let mut existing_order = next_visible_root_order.into_iter().peekable();

            for (root_key, group, ordinal) in new_root_entries {
                while existing_order.peek().is_some_and(|candidate| {
                    self.root_ordinals
                        .get(candidate)
                        .copied()
                        .unwrap_or(usize::MAX)
                        <= ordinal
                }) {
                    if let Some(existing_root_key) = existing_order.next() {
                        merged_order.push(existing_root_key);
                    }
                }

                self.root_rows.insert(root_key.clone(), group);
                merged_order.push(root_key);
            }

            merged_order.extend(existing_order);
            self.visible_root_order = merged_order;
        }

        let total_rows = self
            .visible_root_order
            .iter()
            .filter_map(|root_key| self.root_rows.get(root_key).map(Vec::len))
            .sum();
        let mut result = Vec::with_capacity(total_rows);
        for root_key in &self.visible_root_order {
            if let Some(group) = self.root_rows.get(root_key) {
                result.extend(group.iter().cloned());
            }
        }
        Some(result)
    }
}

pub(super) fn slice_partial_visible_rows(
    candidate_rows: &[PartialRefreshCandidateRow],
    metadata: &RowsSnapshotPartialRefreshMetadata,
) -> Vec<Rc<Row>> {
    candidate_rows
        .iter()
        .skip(metadata.visible_offset)
        .take(metadata.visible_limit)
        .map(|entry| entry.row.clone())
        .collect()
}

fn compare_partial_refresh_values(
    left: Option<&Value>,
    right: Option<&Value>,
    order: cynos_query::ast::SortOrder,
) -> Ordering {
    let cmp = match (left, right) {
        (Some(left), Some(right)) => left.cmp(right),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    };

    match order {
        cynos_query::ast::SortOrder::Asc => cmp,
        cynos_query::ast::SortOrder::Desc => cmp.reverse(),
    }
}

pub(super) fn compare_partial_refresh_rows(
    left: &PartialRefreshCandidateRow,
    right: &PartialRefreshCandidateRow,
    order_keys: &[RowsSnapshotOrderKey],
) -> Ordering {
    for order_key in order_keys {
        let cmp = compare_partial_refresh_values(
            left.row.get(order_key.output_index),
            right.row.get(order_key.output_index),
            order_key.order,
        );
        if cmp != Ordering::Equal {
            return cmp;
        }
    }

    left.root_row_id
        .cmp(&right.root_row_id)
        .then_with(|| left.row.values().cmp(right.row.values()))
        .then_with(|| left.row.version().cmp(&right.row.version()))
}

pub(super) fn insert_row_ids_by_column_values(
    store: &RowStore,
    column_index: usize,
    lookup: &RowsSnapshotLookupPrimitive,
    values: &HashSet<Value>,
    row_ids: &mut HashSet<u64>,
) -> bool {
    if values.is_empty() {
        return false;
    }

    let mut added = false;
    match lookup {
        RowsSnapshotLookupPrimitive::PrimaryKey => {
            for value in values {
                store.visit_row_ids_by_pk_values(core::slice::from_ref(value), |row_id| {
                    added |= row_ids.insert(row_id);
                    true
                });
            }
        }
        RowsSnapshotLookupPrimitive::SingleColumnIndex { index_name } => {
            for value in values {
                store.visit_index_row_ids_by_value(index_name, value, |row_id| {
                    added |= row_ids.insert(row_id);
                    true
                });
            }
        }
        RowsSnapshotLookupPrimitive::ScanFallback => {
            store.visit_rows(|row| {
                if row
                    .get(column_index)
                    .map(|candidate| values.iter().any(|value| candidate.sql_eq(value)))
                    .unwrap_or(false)
                {
                    added |= row_ids.insert(row.id());
                }
                true
            });
        }
    }

    added
}

pub(super) fn collect_join_values_by_row_ids(
    store: &RowStore,
    row_ids: &[u64],
    column_index: usize,
    join_values: &mut HashSet<Value>,
) {
    join_values.clear();
    store.visit_rows_by_ids(row_ids, |row| {
        if let Some(value) = row.get(column_index) {
            if !value.is_null() {
                join_values.insert(value.clone());
            }
        }
        true
    });
}

pub(super) fn collect_join_values_by_row_ids_and_deltas(
    store: &RowStore,
    row_ids: &[u64],
    deltas: Option<&Vec<Delta<Row>>>,
    column_index: usize,
    join_values: &mut HashSet<Value>,
) {
    collect_join_values_by_row_ids(store, row_ids, column_index, join_values);
    let Some(deltas) = deltas else {
        return;
    };

    for delta in deltas {
        if let Some(value) = delta.data().get(column_index) {
            if !value.is_null() {
                join_values.insert(value.clone());
            }
        }
    }
}

pub(super) fn resolve_root_row_id_for_output_row(
    cache: &TableCache,
    metadata: &RowsSnapshotPartialRefreshMetadata,
    row: &Rc<Row>,
) -> Option<u64> {
    let store = cache.get_table(&metadata.root_table)?;
    let pk_values: Vec<Value> = metadata
        .root_pk_output_indices
        .iter()
        .map(|&output_index| row.get(output_index).cloned())
        .collect::<Option<Vec<_>>>()?;
    if pk_values.iter().any(Value::is_null) {
        return None;
    }

    store
        .get_by_pk_values(&pk_values)
        .first()
        .map(|root_row| root_row.id())
}

pub(super) fn build_partial_refresh_candidate_rows(
    cache: &TableCache,
    metadata: &RowsSnapshotPartialRefreshMetadata,
    rows: &[Rc<Row>],
) -> Option<Vec<PartialRefreshCandidateRow>> {
    let mut candidate_rows = Vec::with_capacity(rows.len());
    for row in rows {
        candidate_rows.push(PartialRefreshCandidateRow {
            row: row.clone(),
            root_row_id: resolve_root_row_id_for_output_row(cache, metadata, row)?,
        });
    }
    Some(candidate_rows)
}
