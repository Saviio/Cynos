//! Transaction API for atomic database operations.
//!
//! This module provides transaction support with commit and rollback capabilities.

use crate::convert::{js_array_to_rows, js_to_value};
use crate::expr::Expr;
use crate::live_runtime::LiveRegistry;
use crate::query_builder::evaluate_predicate;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;
use cynos_core::{reserve_row_ids, Row};
use cynos_reactive::TableId;
use cynos_storage::{TableCache, Transaction, TransactionState};
use hashbrown::{HashMap, HashSet};
use wasm_bindgen::prelude::*;

/// JavaScript-friendly transaction wrapper.
#[wasm_bindgen]
pub struct JsTransaction {
    cache: Rc<RefCell<TableCache>>,
    query_registry: Rc<RefCell<LiveRegistry>>,
    table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    inner: Option<Transaction>,
    /// Pending changes grouped by table so one commit triggers one live flush per table.
    pending_changes: HashMap<TableId, HashSet<u64>>,
}

impl JsTransaction {
    pub(crate) fn new(
        cache: Rc<RefCell<TableCache>>,
        query_registry: Rc<RefCell<LiveRegistry>>,
        table_id_map: Rc<RefCell<hashbrown::HashMap<String, TableId>>>,
    ) -> Self {
        Self {
            cache,
            query_registry,
            table_id_map,
            inner: Some(Transaction::begin()),
            pending_changes: HashMap::new(),
        }
    }

    fn record_pending_change(&mut self, table_id: TableId, changed_ids: HashSet<u64>) {
        if changed_ids.is_empty() {
            return;
        }

        self.pending_changes
            .entry(table_id)
            .or_insert_with(HashSet::new)
            .extend(changed_ids);
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

        // Insert through transaction
        for row in rows {
            inserted_ids.insert(row.id());
            tx.insert(&mut *cache, table, row)
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
        }

        drop(cache);

        // Store pending changes
        let table_id = self.table_id_map.borrow().get(table).copied();
        if let Some(table_id) = table_id {
            self.record_pending_change(table_id, inserted_ids);
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
        let rows_to_update: Vec<Row> = store
            .scan()
            .filter(|row| {
                if let Some(ref pred) = predicate {
                    evaluate_predicate(pred, &**row, &schema)
                } else {
                    true
                }
            })
            .map(|rc| (*rc).clone())
            .collect();

        let mut updated_ids = HashSet::new();
        let mut update_count = 0;

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

            tx.update(&mut *cache, table, old_row.id(), new_row)
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

            update_count += 1;
        }

        drop(cache);

        let table_id = self.table_id_map.borrow().get(table).copied();
        if let Some(table_id) = table_id {
            self.record_pending_change(table_id, updated_ids);
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
        let rows_to_delete: Vec<Row> = store
            .scan()
            .filter(|row| {
                if let Some(ref pred) = predicate {
                    evaluate_predicate(pred, &**row, &schema)
                } else {
                    true
                }
            })
            .map(|rc| (*rc).clone())
            .collect();

        let delete_count = rows_to_delete.len();

        let mut deleted_ids = HashSet::new();
        for row in rows_to_delete {
            deleted_ids.insert(row.id());
            tx.delete(&mut *cache, table, row.id())
                .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;
        }

        drop(cache);

        let table_id = self.table_id_map.borrow().get(table).copied();
        if let Some(table_id) = table_id {
            self.record_pending_change(table_id, deleted_ids);
        }

        Ok(delete_count)
    }

    /// Commits the transaction.
    pub fn commit(&mut self) -> Result<(), JsValue> {
        let tx = self
            .inner
            .take()
            .ok_or_else(|| JsValue::from_str("Transaction already completed"))?;

        tx.commit()
            .map_err(|e| JsValue::from_str(&alloc::format!("{:?}", e)))?;

        // Notify query registry of all changes
        for (table_id, changed_ids) in self.pending_changes.drain() {
            self.query_registry
                .borrow_mut()
                .on_table_change(table_id, &changed_ids);
        }

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

        // Notify Live Query of rollback changes (data was restored)
        for (table_id, changed_ids) in self.pending_changes.drain() {
            self.query_registry
                .borrow_mut()
                .on_table_change(table_id, &changed_ids);
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
