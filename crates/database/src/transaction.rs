//! Transaction API for atomic database operations.
//!
//! This module provides transaction support with commit and rollback capabilities.

use crate::convert::{js_array_to_rows, js_to_value};
use crate::expr::{ComparisonOp, Expr, ExprInner};
use crate::live_runtime::LiveRegistry;
use crate::profiling::now_ms;
use crate::query_builder::evaluate_predicate;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::schema::{IndexType, Table};
use cynos_core::{reserve_row_ids, Row};
use cynos_incremental::Delta;
use cynos_index::KeyRange;
use cynos_reactive::TableId;
use cynos_storage::{RowStore, TableCache, Transaction, TransactionState};
use hashbrown::{HashMap, HashSet};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

#[derive(Clone, Debug, Default)]
pub(crate) struct CommitProfile {
    pub storage_commit_ms: f64,
    pub registry_flush_ms: f64,
    pub total_commit_ms: f64,
    pub changed_table_count: usize,
    pub changed_row_count: usize,
    pub delta_row_count: usize,
}

/// JavaScript-friendly transaction wrapper.
#[wasm_bindgen]
pub struct JsTransaction {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<LiveRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    last_commit_profile: Rc<RefCell<Option<CommitProfile>>>,
    inner: Option<Transaction>,
    /// Pending changes grouped by table so one commit triggers one live flush per table.
    pending_changes: HashMap<TableId, HashSet<u64>>,
    /// Pending row deltas grouped by table for trace()/delta-backed subscriptions.
    pending_deltas: HashMap<TableId, Vec<Delta<Row>>>,
}

impl JsTransaction {
    pub(crate) fn new(
        cache: Rc<RefCell<TableCache>>,
        query_registry: Rc<RefCell<LiveRegistry>>,
        table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
        last_commit_profile: Rc<RefCell<Option<CommitProfile>>>,
    ) -> Self {
        Self {
            cache,
            query_registry,
            table_id_map,
            last_commit_profile,
            inner: Some(Transaction::begin()),
            pending_changes: HashMap::new(),
            pending_deltas: HashMap::new(),
        }
    }

    fn record_pending_change(
        &mut self,
        table_id: TableId,
        changed_ids: HashSet<u64>,
        deltas: Vec<Delta<Row>>,
    ) {
        if changed_ids.is_empty() {
            return;
        }

        self.pending_changes
            .entry(table_id)
            .or_insert_with(HashSet::new)
            .extend(changed_ids);

        if !deltas.is_empty() {
            self.pending_deltas
                .entry(table_id)
                .or_insert_with(Vec::new)
                .extend(deltas);
        }
    }

    fn collect_candidate_rows(
        store: &RowStore,
        schema: &Table,
        predicate: Option<&Expr>,
    ) -> Result<Vec<Row>, JsValue> {
        let Some(predicate) = predicate else {
            return Ok(store.scan().map(|rc| (*rc).clone()).collect());
        };

        if let Some(rows) = Self::lookup_rows_for_predicate(store, schema, predicate)? {
            return Ok(rows);
        }

        Ok(store
            .scan()
            .filter(|row| evaluate_predicate(predicate, &**row, schema))
            .map(|rc| (*rc).clone())
            .collect())
    }

    fn lookup_rows_for_predicate(
        store: &RowStore,
        schema: &Table,
        predicate: &Expr,
    ) -> Result<Option<Vec<Row>>, JsValue> {
        let mut equalities = HashMap::<usize, cynos_core::Value>::new();
        if !Self::collect_point_equalities(schema, predicate, &mut equalities)? {
            return Ok(None);
        }

        if !store.pk_columns().is_empty()
            && store
                .pk_columns()
                .iter()
                .all(|idx| equalities.contains_key(idx))
        {
            let pk_values: Vec<_> = store
                .pk_columns()
                .iter()
                .filter_map(|idx| equalities.get(idx).cloned())
                .collect();
            let rows = store
                .get_by_pk_values(&pk_values)
                .into_iter()
                .filter(|row| evaluate_predicate(predicate, &**row, schema))
                .map(|row| (*row).clone())
                .collect();
            return Ok(Some(rows));
        }

        if equalities.len() != 1 {
            return Ok(None);
        }

        let Some((&column_idx, value)) = equalities.iter().next() else {
            return Ok(None);
        };
        let Some(index_name) = Self::find_single_column_index(schema, column_idx) else {
            return Ok(None);
        };
        let rows = store
            .index_scan(&index_name, Some(&KeyRange::only(value.clone())))
            .into_iter()
            .filter(|row| evaluate_predicate(predicate, &**row, schema))
            .map(|row| (*row).clone())
            .collect();
        Ok(Some(rows))
    }

    fn collect_point_equalities(
        schema: &Table,
        predicate: &Expr,
        equalities: &mut HashMap<usize, cynos_core::Value>,
    ) -> Result<bool, JsValue> {
        match predicate.inner() {
            ExprInner::And { left, right } => {
                Ok(Self::collect_point_equalities(schema, left, equalities)?
                    && Self::collect_point_equalities(schema, right, equalities)?)
            }
            ExprInner::Comparison { column, op, value } if *op == ComparisonOp::Eq => {
                if value.is_object() {
                    return Ok(false);
                }

                if let Some(table_name) = column.table_name() {
                    if table_name != schema.name() {
                        return Ok(false);
                    }
                }

                let Some(schema_column) = schema.get_column(&column.name()) else {
                    return Ok(false);
                };
                let literal = js_to_value(value, schema_column.data_type())?;
                match equalities.get(&schema_column.index()) {
                    Some(existing) if existing != &literal => Ok(false),
                    Some(_) => Ok(true),
                    None => {
                        equalities.insert(schema_column.index(), literal);
                        Ok(true)
                    }
                }
            }
            _ => Ok(false),
        }
    }

    fn find_single_column_index(schema: &Table, column_idx: usize) -> Option<String> {
        schema
            .indices()
            .iter()
            .find(|index| {
                index.get_index_type() != IndexType::Gin
                    && index.columns().len() == 1
                    && index
                        .columns()
                        .first()
                        .and_then(|col| schema.get_column_index(&col.name))
                        == Some(column_idx)
            })
            .map(|index| index.name().to_string())
    }
}

#[wasm_bindgen]
impl JsTransaction {
    /// Inserts rows into a table within the transaction.
    pub fn insert(&mut self, table: &str, values: &JsValue) -> Result<(), JsValue> {
        let tx = self
            .inner
            .as_mut()
            .ok_or_else(|| JsValue::from_str("Transaction already completed"))?;

        let mut cache = self.cache.borrow_mut();
        let store = cache
            .get_table_mut(table)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table)))?;

        let schema = store.schema().clone();

        // Get the count of rows to insert first
        let arr = js_sys::Array::from(values);
        let row_count = arr.length() as u64;

        // Reserve row IDs for all rows at once to avoid ID conflicts
        let start_row_id = reserve_row_ids(row_count);

        let rows = js_array_to_rows(values, &schema, start_row_id)?;

        // Collect inserted row IDs
        let mut inserted_ids = HashSet::new();
        let mut deltas = Vec::with_capacity(rows.len());

        // Insert through transaction
        for row in rows {
            inserted_ids.insert(row.id());
            deltas.push(Delta::insert(row.clone()));
            tx.insert(&mut *cache, table, row)
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
        }

        drop(cache);

        // Store pending changes
        let table_id = self.table_id_map.borrow().get(table).copied();
        if let Some(table_id) = table_id {
            self.record_pending_change(table_id, inserted_ids, deltas);
        }

        Ok(())
    }

    /// Updates rows in a table within the transaction.
    pub fn update(
        &mut self,
        table: &str,
        set_values: &JsValue,
        predicate: Option<Expr>,
    ) -> Result<usize, JsValue> {
        let tx = self
            .inner
            .as_mut()
            .ok_or_else(|| JsValue::from_str("Transaction already completed"))?;

        let mut cache = self.cache.borrow_mut();
        let store = cache
            .get_table_mut(table)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table)))?;

        let schema = store.schema().clone();

        // Parse set values
        let set_obj = set_values
            .dyn_ref::<js_sys::Object>()
            .ok_or_else(|| JsValue::from_str("set_values must be an object"))?;

        let keys = js_sys::Object::keys(set_obj);
        let mut updates: Vec<(String, JsValue)> = Vec::new();
        for key in keys.iter() {
            if let Some(k) = key.as_string() {
                let val = js_sys::Reflect::get(set_obj, &key).unwrap_or(JsValue::NULL);
                updates.push((k, val));
            }
        }

        // Find rows to update
        let rows_to_update = Self::collect_candidate_rows(store, &schema, predicate.as_ref())?;

        let mut updated_ids = HashSet::new();
        let mut update_count = 0;
        let mut deltas = Vec::with_capacity(rows_to_update.len() * 2);

        for old_row in rows_to_update {
            let mut new_values = old_row.values().to_vec();

            for (col_name, js_val) in &updates {
                if let Some(col) = schema.get_column(col_name) {
                    let idx = col.index();
                    let value = js_to_value(js_val, col.data_type())?;
                    if idx < new_values.len() {
                        new_values[idx] = value;
                    }
                }
            }

            // Create new row with incremented version
            let new_version = old_row.version().wrapping_add(1);
            let new_row = Row::new_with_version(old_row.id(), new_version, new_values);

            updated_ids.insert(old_row.id());
            deltas.push(Delta::delete(old_row.clone()));
            deltas.push(Delta::insert(new_row.clone()));

            tx.update(&mut *cache, table, old_row.id(), new_row)
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

            update_count += 1;
        }

        drop(cache);

        let table_id = self.table_id_map.borrow().get(table).copied();
        if let Some(table_id) = table_id {
            self.record_pending_change(table_id, updated_ids, deltas);
        }

        Ok(update_count)
    }

    /// Deletes rows from a table within the transaction.
    pub fn delete(&mut self, table: &str, predicate: Option<Expr>) -> Result<usize, JsValue> {
        let tx = self
            .inner
            .as_mut()
            .ok_or_else(|| JsValue::from_str("Transaction already completed"))?;

        let mut cache = self.cache.borrow_mut();
        let store = cache
            .get_table_mut(table)
            .ok_or_else(|| JsValue::from_str(&alloc::format!("Table not found: {}", table)))?;

        let schema = store.schema().clone();

        // Find rows to delete
        let rows_to_delete = Self::collect_candidate_rows(store, &schema, predicate.as_ref())?;

        let delete_count = rows_to_delete.len();

        let mut deleted_ids = HashSet::new();
        let mut deltas = Vec::with_capacity(delete_count);
        for row in rows_to_delete {
            deleted_ids.insert(row.id());
            deltas.push(Delta::delete(row.clone()));
            tx.delete(&mut *cache, table, row.id())
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
        }

        drop(cache);

        let table_id = self.table_id_map.borrow().get(table).copied();
        if let Some(table_id) = table_id {
            self.record_pending_change(table_id, deleted_ids, deltas);
        }

        Ok(delete_count)
    }

    /// Commits the transaction.
    pub fn commit(&mut self) -> Result<(), JsValue> {
        let tx = self
            .inner
            .take()
            .ok_or_else(|| JsValue::from_str("Transaction already completed"))?;

        let storage_started_at = now_ms();
        tx.commit()
            .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
        let storage_commit_ms = now_ms() - storage_started_at;

        // Notify query registry of all changes
        let notify_started_at = now_ms();
        let mut changed_table_count = 0usize;
        let mut changed_row_count = 0usize;
        let mut delta_row_count = 0usize;
        {
            let mut registry = self.query_registry.borrow_mut();
            for (table_id, changed_ids) in self.pending_changes.drain() {
                changed_table_count += 1;
                changed_row_count += changed_ids.len();
                let deltas = self.pending_deltas.remove(&table_id).unwrap_or_default();
                delta_row_count += deltas.len();
                if deltas.is_empty() {
                    registry.on_table_change(table_id, &changed_ids);
                } else {
                    registry.on_table_change_delta(table_id, deltas, &changed_ids);
                }
            }

            if registry.has_pending_changes() {
                registry.flush();
            }
        }
        let registry_flush_ms = now_ms() - notify_started_at;

        *self.last_commit_profile.borrow_mut() = Some(CommitProfile {
            storage_commit_ms,
            registry_flush_ms,
            total_commit_ms: storage_commit_ms + registry_flush_ms,
            changed_table_count,
            changed_row_count,
            delta_row_count,
        });

        Ok(())
    }

    /// Rolls back the transaction.
    pub fn rollback(&mut self) -> Result<(), JsValue> {
        let tx = self
            .inner
            .take()
            .ok_or_else(|| JsValue::from_str("Transaction already completed"))?;

        let mut cache = self.cache.borrow_mut();
        tx.rollback(&mut *cache)
            .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

        self.pending_deltas.clear();

        // Notify Live Query of rollback changes (data was restored)
        let mut registry = self.query_registry.borrow_mut();
        for (table_id, changed_ids) in self.pending_changes.drain() {
            registry.on_table_change(table_id, &changed_ids);
        }
        if registry.has_pending_changes() {
            registry.flush();
        }

        Ok(())
    }

    /// Returns whether the transaction is still active.
    #[wasm_bindgen(getter)]
    pub fn active(&self) -> bool {
        self.inner.is_some()
    }

    /// Returns the transaction state.
    #[wasm_bindgen(getter)]
    pub fn state(&self) -> String {
        match &self.inner {
            Some(tx) => match tx.state() {
                TransactionState::Active => "active".to_string(),
                TransactionState::Committed => "committed".to_string(),
                TransactionState::RolledBack => "rolledback".to_string(),
            },
            None => "completed".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::database::Database;
    use crate::table::ColumnOptions;
    use crate::JsDataType;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    fn setup_db() -> Database {
        let db = Database::new("test");
        let builder = db
            .create_table("users")
            .column(
                "id",
                JsDataType::Int64,
                Some(ColumnOptions::new().set_primary_key(true)),
            )
            .column("name", JsDataType::String, None)
            .column("age", JsDataType::Int32, None);
        db.register_table(&builder).unwrap();
        db
    }

    #[wasm_bindgen_test]
    fn test_transaction_insert_commit() {
        let db = setup_db();
        let mut tx = db.transaction();

        let values = js_sys::JSON::parse(r#"[{"id": 1, "name": "Alice", "age": 25}]"#).unwrap();
        tx.insert("users", &values).unwrap();
        tx.commit().unwrap();

        assert_eq!(db.total_row_count(), 1);
    }

    #[wasm_bindgen_test]
    fn test_transaction_insert_rollback() {
        let db = setup_db();
        let mut tx = db.transaction();

        let values = js_sys::JSON::parse(r#"[{"id": 1, "name": "Alice", "age": 25}]"#).unwrap();
        tx.insert("users", &values).unwrap();
        tx.rollback().unwrap();

        assert_eq!(db.total_row_count(), 0);
    }

    #[wasm_bindgen_test]
    fn test_transaction_state() {
        let db = setup_db();
        let mut tx = db.transaction();

        assert!(tx.active());
        assert_eq!(tx.state(), "active");

        tx.commit().unwrap();

        assert!(!tx.active());
    }

    #[wasm_bindgen_test]
    fn test_transaction_multiple_operations() {
        let db = setup_db();
        let mut tx = db.transaction();

        let values1 = js_sys::JSON::parse(r#"[{"id": 1, "name": "Alice", "age": 25}]"#).unwrap();
        tx.insert("users", &values1).unwrap();

        let values2 = js_sys::JSON::parse(r#"[{"id": 2, "name": "Bob", "age": 30}]"#).unwrap();
        tx.insert("users", &values2).unwrap();

        tx.commit().unwrap();

        assert_eq!(db.total_row_count(), 2);
    }
}
