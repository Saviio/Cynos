//! Type conversion utilities between JavaScript and Rust types.
//!
//! This module provides functions to convert between JS values and Cynos's
//! internal types (Value, Row, etc.).

use alloc::collections::BTreeMap;
use alloc::rc::{Rc, Weak};
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::schema::Table;
use cynos_core::{DataType, Row, Value};
use hashbrown::HashMap;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

type JsonbJsCache = HashMap<Vec<u8>, JsValue>;
type GraphqlFieldKeyCache = HashMap<Rc<str>, JsValue>;
type GraphqlObjectJsCache =
    HashMap<*const [cynos_gql::ResponseField], GraphqlSharedJsCacheEntry<cynos_gql::ResponseField>>;
type GraphqlListJsCache =
    HashMap<*const [cynos_gql::ResponseValue], GraphqlSharedJsCacheEntry<cynos_gql::ResponseValue>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JsonbJsCachePolicy {
    max_entries: usize,
    target_entries: usize,
    max_bytes: usize,
    target_bytes: usize,
}

impl JsonbJsCachePolicy {
    pub(crate) const DEFAULT: Self = Self {
        max_entries: 8_192,
        target_entries: 6_144,
        max_bytes: 4 * 1024 * 1024,
        target_bytes: 3 * 1024 * 1024,
    };

    pub(crate) fn should_prune(self, entries: usize, bytes: usize) -> bool {
        entries > self.max_entries || bytes > self.max_bytes
    }

    pub(crate) fn target_entries(self) -> usize {
        self.target_entries.min(self.max_entries)
    }

    pub(crate) fn target_bytes(self) -> usize {
        self.target_bytes.min(self.max_bytes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GraphqlJsEncodeCachePolicy {
    shared_max_entries: usize,
    shared_target_entries: usize,
    field_key_max_entries: usize,
    field_key_target_entries: usize,
    jsonb: JsonbJsCachePolicy,
}

impl GraphqlJsEncodeCachePolicy {
    const DEFAULT: Self = Self {
        shared_max_entries: 65_536,
        shared_target_entries: 49_152,
        field_key_max_entries: 4_096,
        field_key_target_entries: 3_072,
        jsonb: JsonbJsCachePolicy::DEFAULT,
    };
}

struct GraphqlSharedJsCacheEntry<T> {
    weak: Weak<[T]>,
    value: JsValue,
}

#[derive(Default)]
pub(crate) struct GraphqlJsEncodeCache {
    jsonb_cache: JsonbJsCache,
    jsonb_cache_bytes: usize,
    field_key_cache: GraphqlFieldKeyCache,
    object_cache: GraphqlObjectJsCache,
    list_cache: GraphqlListJsCache,
}

#[derive(Default)]
pub(crate) struct GraphqlRootListJsCache {
    list_len: Option<usize>,
    array: Option<js_sys::Array>,
}

impl GraphqlJsEncodeCache {
    fn maybe_prune(&mut self) {
        self.maybe_prune_with_policy(GraphqlJsEncodeCachePolicy::DEFAULT);
    }

    fn maybe_prune_with_policy(&mut self, policy: GraphqlJsEncodeCachePolicy) {
        self.maybe_prune_shared_with_limits(
            policy.shared_max_entries,
            policy.shared_target_entries,
        );
        self.maybe_prune_field_keys_with_limits(
            policy.field_key_max_entries,
            policy.field_key_target_entries,
        );
        self.maybe_prune_jsonb_with_policy(policy.jsonb);
    }

    fn maybe_prune_shared_with_limits(&mut self, max_entries: usize, target_entries: usize) {
        if self.object_cache.len() + self.list_cache.len() <= max_entries {
            return;
        }

        self.object_cache
            .retain(|_, entry| entry.weak.upgrade().is_some());
        self.list_cache
            .retain(|_, entry| entry.weak.upgrade().is_some());

        let target_entries = target_entries.min(max_entries);
        while self.object_cache.len() + self.list_cache.len() > target_entries {
            let total_len = self.object_cache.len() + self.list_cache.len();
            let to_remove = total_len.saturating_sub(target_entries);
            if self.object_cache.len() >= self.list_cache.len() {
                self.evict_object_entries(to_remove);
            } else {
                self.evict_list_entries(to_remove);
            }
        }
    }

    fn maybe_prune_field_keys_with_limits(&mut self, max_entries: usize, target_entries: usize) {
        if self.field_key_cache.len() <= max_entries {
            return;
        }

        let target_entries = target_entries.min(max_entries);
        let to_remove = self.field_key_cache.len().saturating_sub(target_entries);
        if to_remove == 0 {
            return;
        }

        let keys: Vec<_> = self
            .field_key_cache
            .keys()
            .take(to_remove)
            .cloned()
            .collect();
        for key in keys {
            self.field_key_cache.remove(key.as_ref());
        }
    }

    fn maybe_prune_jsonb_with_policy(&mut self, policy: JsonbJsCachePolicy) {
        if !policy.should_prune(self.jsonb_cache.len(), self.jsonb_cache_bytes) {
            return;
        }

        let target_entries = policy.target_entries();
        let target_bytes = policy.target_bytes();
        let mut projected_len = self.jsonb_cache.len();
        let mut projected_bytes = self.jsonb_cache_bytes;
        let mut keys_to_remove = Vec::new();

        for key in self.jsonb_cache.keys() {
            if projected_len <= target_entries && projected_bytes <= target_bytes {
                break;
            }
            projected_len = projected_len.saturating_sub(1);
            projected_bytes = projected_bytes.saturating_sub(key.len());
            keys_to_remove.push(key.clone());
        }

        for key in keys_to_remove {
            if self.jsonb_cache.remove(key.as_slice()).is_some() {
                self.jsonb_cache_bytes = self.jsonb_cache_bytes.saturating_sub(key.len());
            }
        }
    }

    fn evict_object_entries(&mut self, count: usize) {
        let keys: Vec<_> = self.object_cache.keys().take(count.max(1)).copied().collect();
        for key in keys {
            self.object_cache.remove(&key);
        }
    }

    fn evict_list_entries(&mut self, count: usize) {
        let keys: Vec<_> = self.list_cache.keys().take(count.max(1)).copied().collect();
        for key in keys {
            self.list_cache.remove(&key);
        }
    }

    fn value_to_js(&mut self, value: &Value) -> JsValue {
        match value {
            Value::Jsonb(jsonb) => {
                if let Some(cached) = self.jsonb_cache.get(jsonb.0.as_slice()) {
                    return cached.clone();
                }

                let parsed = value_to_js(value);
                self.jsonb_cache_bytes = self.jsonb_cache_bytes.saturating_add(jsonb.0.len());
                self.jsonb_cache.insert(jsonb.0.clone(), parsed.clone());
                self.maybe_prune_jsonb_with_policy(JsonbJsCachePolicy::DEFAULT);
                parsed
            }
            _ => value_to_js(value),
        }
    }
}

/// Converts a JavaScript value to an Cynos Value.
///
/// The conversion is based on the expected data type:
/// - Boolean: JS boolean
/// - Int32/Int64: JS number (truncated to integer)
/// - Float64: JS number
/// - String: JS string
/// - DateTime: JS number (Unix timestamp in ms) or Date object
/// - Bytes: JS Uint8Array
/// - Jsonb: Any JS value (serialized to JSON)
pub fn js_to_value(js: &JsValue, expected_type: DataType) -> Result<Value, JsValue> {
    if js.is_null() || js.is_undefined() {
        return Ok(Value::Null);
    }

    match expected_type {
        DataType::Boolean => {
            if let Some(b) = js.as_bool() {
                Ok(Value::Boolean(b))
            } else {
                Err(JsValue::from_str("Expected boolean value"))
            }
        }
        DataType::Int32 => {
            if let Some(n) = js.as_f64() {
                Ok(Value::Int32(n as i32))
            } else {
                Err(JsValue::from_str("Expected number value"))
            }
        }
        DataType::Int64 => {
            if let Some(n) = js.as_f64() {
                Ok(Value::Int64(n as i64))
            } else if js.is_bigint() {
                // Handle BigInt
                let s = js_sys::BigInt::from(js.clone())
                    .to_string(10)
                    .map_err(|_| JsValue::from_str("Failed to convert BigInt"))?;
                let n: i64 = String::from(s)
                    .parse()
                    .map_err(|_| JsValue::from_str("BigInt out of i64 range"))?;
                Ok(Value::Int64(n))
            } else {
                Err(JsValue::from_str("Expected number or BigInt value"))
            }
        }
        DataType::Float64 => {
            if let Some(n) = js.as_f64() {
                Ok(Value::Float64(n))
            } else {
                Err(JsValue::from_str("Expected number value"))
            }
        }
        DataType::String => {
            if let Some(s) = js.as_string() {
                Ok(Value::String(s))
            } else {
                Err(JsValue::from_str("Expected string value"))
            }
        }
        DataType::DateTime => {
            if let Some(n) = js.as_f64() {
                Ok(Value::DateTime(n as i64))
            } else if js.is_object() {
                // Try to get time from Date object
                let date = js_sys::Date::from(js.clone());
                Ok(Value::DateTime(date.get_time() as i64))
            } else {
                Err(JsValue::from_str("Expected number or Date value"))
            }
        }
        DataType::Bytes => {
            if js.is_object() {
                let arr = js_sys::Uint8Array::new(js);
                Ok(Value::Bytes(arr.to_vec()))
            } else {
                Err(JsValue::from_str("Expected Uint8Array value"))
            }
        }
        DataType::Jsonb => {
            // Serialize any JS value to JSON bytes
            let json_str = js_sys::JSON::stringify(js)
                .map_err(|_| JsValue::from_str("Failed to stringify JSON"))?;
            let bytes = String::from(json_str).into_bytes();
            Ok(Value::Jsonb(cynos_core::JsonbValue::new(bytes)))
        }
    }
}

/// Converts an Cynos Value to a JavaScript value.
pub fn value_to_js(value: &Value) -> JsValue {
    match value {
        Value::Null => JsValue::NULL,
        Value::Boolean(b) => JsValue::from_bool(*b),
        Value::Int32(n) => JsValue::from_f64(*n as f64),
        Value::Int64(n) => JsValue::from_f64(*n as f64),
        Value::Float64(n) => JsValue::from_f64(*n),
        Value::String(s) => JsValue::from_str(s),
        Value::DateTime(ts) => {
            // Return as Date object
            js_sys::Date::new(&JsValue::from_f64(*ts as f64)).into()
        }
        Value::Bytes(b) => {
            let arr = js_sys::Uint8Array::new_with_length(b.len() as u32);
            arr.copy_from(b);
            arr.into()
        }
        Value::Jsonb(j) => {
            // Parse JSON bytes back to JS value
            if let Ok(s) = core::str::from_utf8(&j.0) {
                js_sys::JSON::parse(s).unwrap_or(JsValue::NULL)
            } else {
                JsValue::NULL
            }
        }
    }
}

fn value_to_js_with_cache(value: &Value, jsonb_cache: &mut JsonbJsCache) -> JsValue {
    match value {
        Value::Jsonb(j) => {
            if let Some(cached) = jsonb_cache.get(j.0.as_slice()) {
                return cached.clone();
            }

            let parsed = value_to_js(value);
            jsonb_cache.insert(j.0.clone(), parsed.clone());
            parsed
        }
        _ => value_to_js(value),
    }
}

fn row_to_js_with_cache(row: &Row, schema: &Table, jsonb_cache: &mut JsonbJsCache) -> JsValue {
    let obj = js_sys::Object::new();
    let columns = schema.columns();

    for (i, col) in columns.iter().enumerate() {
        if let Some(value) = row.get(i) {
            let js_val = value_to_js_with_cache(value, jsonb_cache);
            js_sys::Reflect::set(&obj, &JsValue::from_str(col.name()), &js_val).ok();
        }
    }

    obj.into()
}

/// Converts an Cynos Row to a JavaScript object.
///
/// The returned object has properties named after the table columns.
pub fn row_to_js(row: &Row, schema: &Table) -> JsValue {
    let mut jsonb_cache = JsonbJsCache::new();
    row_to_js_with_cache(row, schema, &mut jsonb_cache)
}

/// Converts a JavaScript object to an Cynos Row.
///
/// The object properties are matched against the table schema columns.
pub fn js_to_row(js: &JsValue, schema: &Table, row_id: u64) -> Result<Row, JsValue> {
    if !js.is_object() {
        return Err(JsValue::from_str("Expected object value"));
    }

    let columns = schema.columns();
    let mut values = Vec::with_capacity(columns.len());

    for col in columns {
        let prop = js_sys::Reflect::get(js, &JsValue::from_str(col.name()))
            .map_err(|_| JsValue::from_str(&alloc::format!("Missing column: {}", col.name())))?;

        let value = if prop.is_undefined() || prop.is_null() {
            if col.is_nullable() {
                Value::Null
            } else {
                return Err(JsValue::from_str(&alloc::format!(
                    "Column {} is not nullable",
                    col.name()
                )));
            }
        } else {
            js_to_value(&prop, col.data_type())?
        };

        values.push(value);
    }

    Ok(Row::new(row_id, values))
}

/// Converts a JavaScript array of objects to a vector of Rows.
pub fn js_array_to_rows(
    js: &JsValue,
    schema: &Table,
    start_row_id: u64,
) -> Result<Vec<Row>, JsValue> {
    if !js_sys::Array::is_array(js) {
        return Err(JsValue::from_str("Expected array value"));
    }

    let arr = js_sys::Array::from(js);
    let mut rows = Vec::with_capacity(arr.length() as usize);

    for (i, item) in arr.iter().enumerate() {
        let row = js_to_row(&item, schema, start_row_id + i as u64)?;
        rows.push(row);
    }

    Ok(rows)
}

/// Converts a vector of Rows to a JavaScript array of objects.
pub fn rows_to_js_array(rows: &[Rc<Row>], schema: &Table) -> JsValue {
    let arr = js_sys::Array::new_with_length(rows.len() as u32);
    let mut jsonb_cache = JsonbJsCache::new();

    for (i, row) in rows.iter().enumerate() {
        let obj = row_to_js_with_cache(row, schema, &mut jsonb_cache);
        arr.set(i as u32, obj);
    }

    arr.into()
}

/// Converts a vector of projected Rows to a JavaScript array of objects.
///
/// This function is used when only specific columns are selected (projection).
/// The `column_names` parameter specifies the names of the projected columns
/// in the order they appear in the row.
pub fn projected_rows_to_js_array(rows: &[Rc<Row>], column_names: &[String]) -> JsValue {
    let arr = js_sys::Array::new_with_length(rows.len() as u32);
    let mut jsonb_cache = JsonbJsCache::new();

    // Extract just the column part from qualified names and count occurrences
    let mut name_counts: hashbrown::HashMap<&str, usize> = hashbrown::HashMap::new();
    for col_name in column_names {
        let simple_name = if let Some(dot_pos) = col_name.find('.') {
            &col_name[dot_pos + 1..]
        } else {
            col_name.as_str()
        };
        *name_counts.entry(simple_name).or_insert(0) += 1;
    }

    // Build the final column names - use simple names when unique, qualified when duplicate
    let final_names: Vec<&str> = column_names
        .iter()
        .map(|col_name| {
            if let Some(dot_pos) = col_name.find('.') {
                let simple_name = &col_name[dot_pos + 1..];
                if name_counts.get(simple_name).copied().unwrap_or(0) > 1 {
                    // Duplicate - keep qualified name
                    col_name.as_str()
                } else {
                    // Unique - use simple name
                    simple_name
                }
            } else {
                col_name.as_str()
            }
        })
        .collect();

    for (i, row) in rows.iter().enumerate() {
        let obj = js_sys::Object::new();
        for (col_idx, col_name) in final_names.iter().enumerate() {
            if let Some(value) = row.get(col_idx) {
                let js_val = value_to_js_with_cache(value, &mut jsonb_cache);
                js_sys::Reflect::set(&obj, &JsValue::from_str(col_name), &js_val).ok();
            }
        }
        arr.set(i as u32, obj.into());
    }

    arr.into()
}

/// Converts a vector of Rows to a JavaScript array of objects using multiple schemas.
///
/// This function is used for JOIN queries where the result contains columns from multiple tables.
/// The `schemas` parameter specifies the schemas of all joined tables in order.
/// For duplicate column names across tables, we use `table.column` format to distinguish them.
pub fn joined_rows_to_js_array(rows: &[Rc<Row>], schemas: &[&Table]) -> JsValue {
    let arr = js_sys::Array::new_with_length(rows.len() as u32);
    let mut jsonb_cache = JsonbJsCache::new();

    // First pass: count occurrences of each column name
    let mut name_counts: hashbrown::HashMap<&str, usize> = hashbrown::HashMap::new();
    for schema in schemas {
        for col in schema.columns() {
            *name_counts.entry(col.name()).or_insert(0) += 1;
        }
    }

    // Second pass: build column mapping with qualified names for duplicates
    let mut column_names: Vec<String> = Vec::new();
    for schema in schemas {
        let table_name = schema.name();
        for col in schema.columns() {
            let col_name = col.name();
            if name_counts.get(col_name).copied().unwrap_or(0) > 1 {
                // Duplicate column name - use table.column format
                column_names.push(alloc::format!("{}.{}", table_name, col_name));
            } else {
                // Unique column name - use as-is
                column_names.push(col_name.to_string());
            }
        }
    }

    for (i, row) in rows.iter().enumerate() {
        let obj = js_sys::Object::new();
        for (col_idx, col_name) in column_names.iter().enumerate() {
            if let Some(value) = row.get(col_idx) {
                let js_val = value_to_js_with_cache(value, &mut jsonb_cache);
                js_sys::Reflect::set(&obj, &JsValue::from_str(col_name), &js_val).ok();
            }
        }
        arr.set(i as u32, obj.into());
    }

    arr.into()
}

/// Converts a JS variables object into GraphQL variables.
pub fn js_to_gql_variables(js: Option<&JsValue>) -> Result<cynos_gql::VariableValues, JsValue> {
    let Some(js) = js else {
        return Ok(BTreeMap::new());
    };

    if js.is_null() || js.is_undefined() {
        return Ok(BTreeMap::new());
    }

    if !js.is_object() || js_sys::Array::is_array(js) {
        return Err(JsValue::from_str("GraphQL variables must be an object"));
    }

    let keys = js_sys::Reflect::own_keys(js)
        .map_err(|_| JsValue::from_str("Failed to enumerate GraphQL variables"))?;
    let mut variables = BTreeMap::new();
    for key in keys.iter() {
        let Some(name) = key.as_string() else {
            continue;
        };
        let value = js_sys::Reflect::get(js, &key)
            .map_err(|_| JsValue::from_str("Failed to read GraphQL variable"))?;
        variables.insert(name, js_to_gql_input_value(&value)?);
    }

    Ok(variables)
}

/// Converts a GraphQL execution response into the standard `{ data }` object shape.
pub fn gql_response_to_js(response: &cynos_gql::GraphqlResponse) -> Result<JsValue, JsValue> {
    let mut cache = GraphqlJsEncodeCache::default();
    gql_response_to_js_with_cache(response, &mut cache)
}

pub(crate) fn gql_response_to_js_with_cache(
    response: &cynos_gql::GraphqlResponse,
    cache: &mut GraphqlJsEncodeCache,
) -> Result<JsValue, JsValue> {
    let obj = js_sys::Object::new();
    let data = gql_value_to_js_with_cache(&response.data, cache);
    js_sys::Reflect::set(&obj, &JsValue::from_str("data"), &data)?;
    Ok(obj.into())
}

pub(crate) fn gql_response_to_js_with_root_list_patch(
    response: &cynos_gql::GraphqlResponse,
    cache: &mut GraphqlJsEncodeCache,
    root_list_cache: &mut GraphqlRootListJsCache,
    patch: Option<&cynos_gql::GraphqlRootListPatch>,
) -> Result<JsValue, JsValue> {
    let cynos_gql::ResponseValue::Object(data_fields) = &response.data else {
        return gql_response_to_js_with_cache(response, cache);
    };
    if data_fields.len() != 1 {
        return gql_response_to_js_with_cache(response, cache);
    }

    let root_field = &data_fields[0];
    let cynos_gql::ResponseValue::List(root_items) = &root_field.value else {
        return gql_response_to_js_with_cache(response, cache);
    };

    let array = match (root_list_cache.list_len, root_list_cache.array.as_ref(), patch) {
        (
            Some(previous_len),
            Some(previous_array),
            Some(cynos_gql::GraphqlRootListPatch::StablePositions(positions)),
        ) if previous_len == root_items.len() => {
            let next = previous_array.slice(0, previous_len as u32);
            for &position in positions {
                if let Some(value) = root_items.get(position) {
                    next.set(position as u32, gql_value_to_js_with_cache(value, cache));
                }
            }
            next
        }
        (
            Some(previous_len),
            Some(previous_array),
            Some(cynos_gql::GraphqlRootListPatch::Splice {
                removed_positions,
                inserted_positions,
                updated_positions,
            }),
        ) => {
            let next = previous_array.slice(0, previous_len as u32);
            for (start, delete_count) in contiguous_remove_groups(removed_positions) {
                array_splice(&next, start as u32, delete_count as u32, &[])?;
            }
            for (start, end) in contiguous_insert_groups(inserted_positions) {
                let items = (start..end)
                    .filter_map(|position| root_items.get(position))
                    .map(|value| gql_value_to_js_with_cache(value, cache))
                    .collect::<Vec<_>>();
                array_splice(&next, start as u32, 0, &items)?;
            }
            for &position in updated_positions {
                if let Some(value) = root_items.get(position) {
                    next.set(position as u32, gql_value_to_js_with_cache(value, cache));
                }
            }
            next
        }
        _ => encode_graphql_root_list(root_items, cache),
    };

    root_list_cache.list_len = Some(root_items.len());
    root_list_cache.array = Some(array.clone());

    let data = js_sys::Object::new();
    let root_key = cache
        .field_key_cache
        .entry(root_field.name.clone())
        .or_insert_with(|| JsValue::from_str(root_field.name.as_ref()))
        .clone();
    js_sys::Reflect::set(&data, &root_key, &array.clone().into())?;

    let obj = js_sys::Object::new();
    js_sys::Reflect::set(&obj, &JsValue::from_str("data"), &data.into())?;
    Ok(obj.into())
}

fn encode_graphql_root_list(
    root_items: &[cynos_gql::ResponseValue],
    cache: &mut GraphqlJsEncodeCache,
) -> js_sys::Array {
    let array = js_sys::Array::new_with_length(root_items.len() as u32);
    for (index, value) in root_items.iter().enumerate() {
        array.set(index as u32, gql_value_to_js_with_cache(value, cache));
    }
    array
}

fn array_splice(
    array: &js_sys::Array,
    start: u32,
    delete_count: u32,
    items: &[JsValue],
) -> Result<(), JsValue> {
    let splice = js_sys::Reflect::get(array.as_ref(), &JsValue::from_str("splice"))?
        .dyn_into::<js_sys::Function>()?;
    let args = js_sys::Array::new_with_length((2 + items.len()) as u32);
    args.set(0, JsValue::from_f64(start as f64));
    args.set(1, JsValue::from_f64(delete_count as f64));
    for (index, item) in items.iter().enumerate() {
        args.set((index + 2) as u32, item.clone());
    }
    splice.apply(array.as_ref(), &args)?;
    Ok(())
}

fn contiguous_remove_groups(removed_positions: &[usize]) -> Vec<(usize, usize)> {
    if removed_positions.is_empty() {
        return Vec::new();
    }

    let mut positions = removed_positions.to_vec();
    positions.sort_unstable();

    let mut groups = Vec::new();
    let mut start = positions[0];
    let mut len = 1usize;
    for &position in positions.iter().skip(1) {
        if position == start + len {
            len += 1;
        } else {
            groups.push((start, len));
            start = position;
            len = 1;
        }
    }
    groups.push((start, len));
    groups.reverse();
    groups
}

fn contiguous_insert_groups(inserted_positions: &[usize]) -> Vec<(usize, usize)> {
    if inserted_positions.is_empty() {
        return Vec::new();
    }

    let mut groups = Vec::new();
    let mut start = inserted_positions[0];
    let mut end = start + 1;
    for &position in inserted_positions.iter().skip(1) {
        if position == end {
            end += 1;
        } else {
            groups.push((start, end));
            start = position;
            end = position + 1;
        }
    }
    groups.push((start, end));
    groups
}

fn js_to_gql_input_value(js: &JsValue) -> Result<cynos_gql::InputValue, JsValue> {
    if js.is_null() || js.is_undefined() {
        return Ok(cynos_gql::InputValue::Null);
    }

    if let Some(value) = js.as_bool() {
        return Ok(cynos_gql::InputValue::Boolean(value));
    }

    if js.is_bigint() {
        let string = js_sys::BigInt::from(js.clone())
            .to_string(10)
            .map_err(|_| JsValue::from_str("Failed to convert BigInt"))?;
        let value: i64 = String::from(string)
            .parse()
            .map_err(|_| JsValue::from_str("BigInt out of i64 range"))?;
        return Ok(cynos_gql::InputValue::Int(value));
    }

    if let Some(value) = js.as_f64() {
        if value.fract() == 0.0 {
            return Ok(cynos_gql::InputValue::Int(value as i64));
        }
        return Ok(cynos_gql::InputValue::Float(
            cynos_gql::ast::FloatValue::new(value),
        ));
    }

    if let Some(value) = js.as_string() {
        return Ok(cynos_gql::InputValue::String(value));
    }

    if js.is_instance_of::<js_sys::Date>() {
        let date = js_sys::Date::from(js.clone());
        return Ok(cynos_gql::InputValue::Int(date.get_time() as i64));
    }

    if js.is_instance_of::<js_sys::Uint8Array>() {
        let array = js_sys::Uint8Array::new(js);
        let values = array
            .to_vec()
            .into_iter()
            .map(|value| cynos_gql::InputValue::Int(value as i64))
            .collect();
        return Ok(cynos_gql::InputValue::List(values));
    }

    if js_sys::Array::is_array(js) {
        let array = js_sys::Array::from(js);
        let mut values = Vec::with_capacity(array.length() as usize);
        for value in array.iter() {
            values.push(js_to_gql_input_value(&value)?);
        }
        return Ok(cynos_gql::InputValue::List(values));
    }

    if js.is_object() {
        let keys = js_sys::Reflect::own_keys(js)
            .map_err(|_| JsValue::from_str("Failed to enumerate GraphQL input object"))?;
        let mut fields = Vec::with_capacity(keys.length() as usize);
        for key in keys.iter() {
            let Some(name) = key.as_string() else {
                continue;
            };
            let value = js_sys::Reflect::get(js, &key)
                .map_err(|_| JsValue::from_str("Failed to read GraphQL input field"))?;
            fields.push(cynos_gql::ast::ObjectField {
                name,
                value: js_to_gql_input_value(&value)?,
            });
        }
        return Ok(cynos_gql::InputValue::Object(fields));
    }

    Err(JsValue::from_str("Unsupported GraphQL input value"))
}

fn gql_value_to_js_with_cache(
    value: &cynos_gql::ResponseValue,
    cache: &mut GraphqlJsEncodeCache,
) -> JsValue {
    match value {
        cynos_gql::ResponseValue::Null => JsValue::NULL,
        cynos_gql::ResponseValue::Scalar(value) => cache.value_to_js(value),
        cynos_gql::ResponseValue::List(values) => {
            let cache_key = Rc::as_ptr(values);
            if let Some(cached) = cache.list_cache.get(&cache_key) {
                if let Some(shared) = cached.weak.upgrade() {
                    if Rc::ptr_eq(&shared, values) {
                        return cached.value.clone();
                    }
                }
            }
            let array = js_sys::Array::new_with_length(values.len() as u32);
            for (index, value) in values.iter().enumerate() {
                array.set(index as u32, gql_value_to_js_with_cache(value, cache));
            }
            let js_value: JsValue = array.into();
            cache.list_cache.insert(
                cache_key,
                GraphqlSharedJsCacheEntry {
                    weak: Rc::downgrade(values),
                    value: js_value.clone(),
                },
            );
            cache.maybe_prune();
            js_value
        }
        cynos_gql::ResponseValue::Object(fields) => {
            let cache_key = Rc::as_ptr(fields);
            if let Some(cached) = cache.object_cache.get(&cache_key) {
                if let Some(shared) = cached.weak.upgrade() {
                    if Rc::ptr_eq(&shared, fields) {
                        return cached.value.clone();
                    }
                }
            }
            let object = js_sys::Object::new();
            for field in fields.as_ref() {
                let key = cache
                    .field_key_cache
                    .entry(field.name.clone())
                    .or_insert_with(|| JsValue::from_str(field.name.as_ref()))
                    .clone();
                js_sys::Reflect::set(
                    &object,
                    &key,
                    &gql_value_to_js_with_cache(&field.value, cache),
                )
                .ok();
            }
            let js_value: JsValue = object.into();
            cache.object_cache.insert(
                cache_key,
                GraphqlSharedJsCacheEntry {
                    weak: Rc::downgrade(fields),
                    value: js_value.clone(),
                },
            );
            cache.maybe_prune();
            js_value
        }
    }
}

/// Infers the data type from a JavaScript value.
pub fn infer_type(js: &JsValue) -> Option<DataType> {
    if js.is_null() || js.is_undefined() {
        None
    } else if js.as_bool().is_some() {
        Some(DataType::Boolean)
    } else if js.is_bigint() {
        Some(DataType::Int64)
    } else if js.as_f64().is_some() {
        // Check if it's an integer
        let n = js.as_f64().unwrap();
        if n.fract() == 0.0 && n >= i32::MIN as f64 && n <= i32::MAX as f64 {
            Some(DataType::Int32)
        } else {
            Some(DataType::Float64)
        }
    } else if js.as_string().is_some() {
        Some(DataType::String)
    } else if js.is_object() {
        // Could be Date, Uint8Array, or generic object (JSONB)
        if js.is_instance_of::<js_sys::Date>() {
            Some(DataType::DateTime)
        } else if js.is_instance_of::<js_sys::Uint8Array>() {
            Some(DataType::Bytes)
        } else {
            Some(DataType::Jsonb)
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::rc::Rc;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn test_js_to_value_boolean() {
        let js = JsValue::from_bool(true);
        let result = js_to_value(&js, DataType::Boolean).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_graphql_js_encode_cache_prunes_jsonb_incrementally() {
        let mut cache = GraphqlJsEncodeCache::default();
        for seed in 0..5u8 {
            let key = vec![seed; 3];
            cache.jsonb_cache_bytes += key.len();
            cache.jsonb_cache.insert(key, JsValue::NULL);
        }

        cache.maybe_prune_jsonb_with_policy(JsonbJsCachePolicy {
            max_entries: 4,
            target_entries: 3,
            max_bytes: 12,
            target_bytes: 9,
        });

        assert!(cache.jsonb_cache.len() <= 3);
        assert!(cache.jsonb_cache_bytes <= 9);
        assert!(!cache.jsonb_cache.is_empty());
    }

    #[test]
    fn test_graphql_js_encode_cache_prunes_dead_shared_entries_without_full_clear() {
        let mut cache = GraphqlJsEncodeCache::default();
        let live_fields: Rc<[cynos_gql::ResponseField]> = Rc::from(
            vec![cynos_gql::ResponseField {
                name: Rc::<str>::from("live"),
                value: cynos_gql::ResponseValue::Null,
            }]
            .into_boxed_slice(),
        );
        let live_key = Rc::as_ptr(&live_fields);
        cache.object_cache.insert(
            live_key,
            GraphqlSharedJsCacheEntry {
                weak: Rc::downgrade(&live_fields),
                value: JsValue::NULL,
            },
        );

        for _ in 0..4 {
            let fields: Rc<[cynos_gql::ResponseField]> = Rc::from(
                vec![cynos_gql::ResponseField {
                    name: Rc::<str>::from("dead"),
                    value: cynos_gql::ResponseValue::Null,
                }]
                .into_boxed_slice(),
            );
            cache.object_cache.insert(
                Rc::as_ptr(&fields),
                GraphqlSharedJsCacheEntry {
                    weak: Rc::downgrade(&fields),
                    value: JsValue::NULL,
                },
            );
            drop(fields);
        }

        cache.maybe_prune_shared_with_limits(2, 1);

        assert_eq!(cache.object_cache.len() + cache.list_cache.len(), 1);
        assert!(cache.object_cache.contains_key(&live_key));
    }

    #[wasm_bindgen_test]
    fn test_js_to_value_int32() {
        let js = JsValue::from_f64(42.0);
        let result = js_to_value(&js, DataType::Int32).unwrap();
        assert_eq!(result, Value::Int32(42));
    }

    #[wasm_bindgen_test]
    fn test_js_to_value_int64() {
        let js = JsValue::from_f64(1234567890.0);
        let result = js_to_value(&js, DataType::Int64).unwrap();
        assert_eq!(result, Value::Int64(1234567890));
    }

    #[wasm_bindgen_test]
    fn test_js_to_value_float64() {
        let js = JsValue::from_f64(3.14159);
        let result = js_to_value(&js, DataType::Float64).unwrap();
        assert_eq!(result, Value::Float64(3.14159));
    }

    #[wasm_bindgen_test]
    fn test_js_to_value_string() {
        let js = JsValue::from_str("hello");
        let result = js_to_value(&js, DataType::String).unwrap();
        assert_eq!(result, Value::String("hello".to_string()));
    }

    #[wasm_bindgen_test]
    fn test_js_to_value_null() {
        let js = JsValue::NULL;
        let result = js_to_value(&js, DataType::String).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[wasm_bindgen_test]
    fn test_value_to_js_boolean() {
        let value = Value::Boolean(true);
        let js = value_to_js(&value);
        assert_eq!(js.as_bool(), Some(true));
    }

    #[wasm_bindgen_test]
    fn test_value_to_js_int32() {
        let value = Value::Int32(42);
        let js = value_to_js(&value);
        assert_eq!(js.as_f64(), Some(42.0));
    }

    #[wasm_bindgen_test]
    fn test_value_to_js_string() {
        let value = Value::String("hello".to_string());
        let js = value_to_js(&value);
        assert_eq!(js.as_string(), Some("hello".to_string()));
    }

    #[wasm_bindgen_test]
    fn test_value_to_js_null() {
        let value = Value::Null;
        let js = value_to_js(&value);
        assert!(js.is_null());
    }

    #[wasm_bindgen_test]
    fn test_infer_type_boolean() {
        let js = JsValue::from_bool(true);
        assert_eq!(infer_type(&js), Some(DataType::Boolean));
    }

    #[wasm_bindgen_test]
    fn test_infer_type_number() {
        let js = JsValue::from_f64(42.0);
        assert_eq!(infer_type(&js), Some(DataType::Int32));

        let js = JsValue::from_f64(3.14);
        assert_eq!(infer_type(&js), Some(DataType::Float64));
    }

    #[wasm_bindgen_test]
    fn test_infer_type_string() {
        let js = JsValue::from_str("hello");
        assert_eq!(infer_type(&js), Some(DataType::String));
    }

    #[wasm_bindgen_test]
    fn test_infer_type_null() {
        let js = JsValue::NULL;
        assert_eq!(infer_type(&js), None);
    }
}
