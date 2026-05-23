use cynos_database::binary_protocol::{BinaryDataType, BinaryResult, SchemaLayout, HEADER_SIZE};
use cynos_database::table::ColumnOptions;
use cynos_database::{col, Database, JsDataType, JsSortOrder, PreparedSelectQuery, SelectBuilder};
use js_sys::{Array, Date, Object, Reflect, Uint8Array, JSON};
use std::convert::TryInto;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

#[derive(Clone, Copy, Debug)]
enum CellKind {
    Bool,
    I32,
    I64,
    F64,
    String,
    DateTime,
    Bytes,
    Json,
}

#[derive(Clone, Debug, PartialEq)]
enum Cell {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    String(String),
    DateTime(i64),
    Bytes(Vec<u8>),
    Json(String),
}

#[derive(Clone, Copy, Debug)]
struct ColumnSpec {
    name: &'static str,
    kind: CellKind,
    nullable: bool,
}

fn spec(name: &'static str, kind: CellKind, nullable: bool) -> ColumnSpec {
    ColumnSpec {
        name,
        kind,
        nullable,
    }
}

fn js_array(values: impl IntoIterator<Item = JsValue>) -> JsValue {
    let arr = Array::new();
    for value in values {
        arr.push(&value);
    }
    arr.into()
}

fn js_str_array(values: &[&str]) -> JsValue {
    js_array(values.iter().map(|value| JsValue::from_str(value)))
}

fn js_object(entries: &[(&str, JsValue)]) -> JsValue {
    let object = Object::new();
    for (key, value) in entries {
        Reflect::set(&object, &JsValue::from_str(key), value).unwrap();
    }
    object.into()
}

fn js_bytes(bytes: &[u8]) -> JsValue {
    let array = Uint8Array::new_with_length(bytes.len() as u32);
    array.copy_from(bytes);
    array.into()
}

fn js_json(text: &str) -> JsValue {
    JSON::parse(text).unwrap()
}

fn decode_js_rows(value: JsValue, specs: &[ColumnSpec]) -> Vec<Vec<Cell>> {
    let array = Array::from(&value);
    array
        .iter()
        .map(|row| {
            specs
                .iter()
                .map(|spec| {
                    let field = Reflect::get(&row, &JsValue::from_str(spec.name)).unwrap();
                    decode_js_value(&field, spec.kind)
                })
                .collect()
        })
        .collect()
}

fn decode_js_value(value: &JsValue, kind: CellKind) -> Cell {
    if value.is_null() || value.is_undefined() {
        return Cell::Null;
    }

    match kind {
        CellKind::Bool => Cell::Bool(value.as_bool().unwrap()),
        CellKind::I32 => Cell::I32(value.as_f64().unwrap() as i32),
        CellKind::I64 => Cell::I64(value.as_f64().unwrap() as i64),
        CellKind::F64 => Cell::F64(value.as_f64().unwrap()),
        CellKind::String => Cell::String(value.as_string().unwrap()),
        CellKind::DateTime => {
            let ts = value
                .as_f64()
                .map(|number| number as i64)
                .unwrap_or_else(|| Date::from(value.clone()).get_time() as i64);
            Cell::DateTime(ts)
        }
        CellKind::Bytes => Cell::Bytes(Uint8Array::new(value).to_vec()),
        CellKind::Json => Cell::Json(String::from(JSON::stringify(value).unwrap())),
    }
}

fn binary_result_bytes(result: &BinaryResult) -> Vec<u8> {
    unsafe { std::slice::from_raw_parts(result.ptr() as *const u8, result.len()) }.to_vec()
}

fn decode_binary_rows(layout: &SchemaLayout, result: &BinaryResult) -> Vec<Vec<Cell>> {
    let buffer = binary_result_bytes(result);
    let row_count = u32::from_le_bytes(buffer[0..4].try_into().unwrap()) as usize;
    let row_stride = u32::from_le_bytes(buffer[4..8].try_into().unwrap()) as usize;
    let var_offset = u32::from_le_bytes(buffer[8..12].try_into().unwrap()) as usize;

    assert_eq!(row_stride, layout.row_stride());

    let columns = layout.columns();
    let null_mask_size = layout.null_mask_size();

    (0..row_count)
        .map(|row_index| {
            let row_base = HEADER_SIZE + row_index * row_stride;
            columns
                .iter()
                .enumerate()
                .map(|(column_index, column)| {
                    let null_byte = buffer[row_base + column_index / 8];
                    let is_null = ((null_byte >> (column_index % 8)) & 1) == 1;
                    if is_null {
                        return Cell::Null;
                    }

                    let data_start = row_base + null_mask_size + column.offset;
                    match column.data_type {
                        BinaryDataType::Boolean => Cell::Bool(buffer[data_start] != 0),
                        BinaryDataType::Int32 => Cell::I32(i32::from_le_bytes(
                            buffer[data_start..data_start + 4].try_into().unwrap(),
                        )),
                        BinaryDataType::Int64 => Cell::I64(f64::from_le_bytes(
                            buffer[data_start..data_start + 8].try_into().unwrap(),
                        ) as i64),
                        BinaryDataType::Float64 => Cell::F64(f64::from_le_bytes(
                            buffer[data_start..data_start + 8].try_into().unwrap(),
                        )),
                        BinaryDataType::DateTime => Cell::DateTime(f64::from_le_bytes(
                            buffer[data_start..data_start + 8].try_into().unwrap(),
                        )
                            as i64),
                        BinaryDataType::String => {
                            let (offset, len) = read_varlen(&buffer, data_start);
                            let bytes = &buffer[var_offset + offset..var_offset + offset + len];
                            Cell::String(String::from_utf8(bytes.to_vec()).unwrap())
                        }
                        BinaryDataType::Bytes => {
                            let (offset, len) = read_varlen(&buffer, data_start);
                            Cell::Bytes(
                                buffer[var_offset + offset..var_offset + offset + len].to_vec(),
                            )
                        }
                        BinaryDataType::Jsonb => {
                            let (offset, len) = read_varlen(&buffer, data_start);
                            let bytes = &buffer[var_offset + offset..var_offset + offset + len];
                            Cell::Json(String::from_utf8(bytes.to_vec()).unwrap())
                        }
                    }
                })
                .collect()
        })
        .collect()
}

fn read_varlen(buffer: &[u8], offset: usize) -> (usize, usize) {
    let relative_offset =
        u32::from_le_bytes(buffer[offset..offset + 4].try_into().unwrap()) as usize;
    let len = u32::from_le_bytes(buffer[offset + 4..offset + 8].try_into().unwrap()) as usize;
    (relative_offset, len)
}

fn assert_layout(layout: &SchemaLayout, specs: &[ColumnSpec]) {
    assert_eq!(layout.columns().len(), specs.len());
    for (column, spec) in layout.columns().iter().zip(specs.iter()) {
        assert_eq!(column.name, spec.name);
        assert_eq!(column.data_type, expected_binary_type(spec.kind));
        assert_eq!(column.is_nullable, spec.nullable);
    }
}

fn expected_binary_type(kind: CellKind) -> BinaryDataType {
    match kind {
        CellKind::Bool => BinaryDataType::Boolean,
        CellKind::I32 => BinaryDataType::Int32,
        CellKind::I64 => BinaryDataType::Int64,
        CellKind::F64 => BinaryDataType::Float64,
        CellKind::String => BinaryDataType::String,
        CellKind::DateTime => BinaryDataType::DateTime,
        CellKind::Bytes => BinaryDataType::Bytes,
        CellKind::Json => BinaryDataType::Jsonb,
    }
}

fn assert_rows_eq(actual: &[Vec<Cell>], expected: &[Vec<Cell>]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "row count mismatch: actual={actual:?} expected={expected:?}"
    );
    for (row_index, (actual_row, expected_row)) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(
            actual_row.len(),
            expected_row.len(),
            "column count mismatch at row {row_index}"
        );
        for (column_index, (actual_cell, expected_cell)) in
            actual_row.iter().zip(expected_row.iter()).enumerate()
        {
            assert_cell_eq(actual_cell, expected_cell, row_index, column_index);
        }
    }
}

fn assert_cell_eq(actual: &Cell, expected: &Cell, row_index: usize, column_index: usize) {
    match (actual, expected) {
        (Cell::F64(actual_value), Cell::F64(expected_value)) => {
            assert!(
                (actual_value - expected_value).abs() < 1e-9,
                "float mismatch at row {row_index}, column {column_index}: actual={actual_value} expected={expected_value}"
            );
        }
        _ => assert_eq!(
            actual, expected,
            "cell mismatch at row {row_index}, column {column_index}"
        ),
    }
}

async fn assert_select_matches(
    query: &SelectBuilder,
    specs: &[ColumnSpec],
    expected: &[Vec<Cell>],
) {
    let exec_rows = decode_js_rows(query.exec().await.unwrap(), specs);
    assert_rows_eq(&exec_rows, expected);

    let layout = query.get_schema_layout().unwrap();
    assert_layout(&layout, specs);
    let binary_rows = decode_binary_rows(&layout, &query.exec_binary().await.unwrap());
    assert_rows_eq(&binary_rows, expected);

    let prepared = query.prepare().unwrap();
    assert_prepared_matches(&prepared, specs, expected).await;
}

async fn assert_prepared_matches(
    query: &PreparedSelectQuery,
    specs: &[ColumnSpec],
    expected: &[Vec<Cell>],
) {
    let exec_rows = decode_js_rows(query.exec().await.unwrap(), specs);
    assert_rows_eq(&exec_rows, expected);

    let layout = query.get_schema_layout();
    assert_layout(&layout, specs);
    let binary_rows = decode_binary_rows(&layout, &query.exec_binary().await.unwrap());
    assert_rows_eq(&binary_rows, expected);
}

fn assert_error_string(error: JsValue, expected: &str) {
    assert_eq!(error.as_string().as_deref(), Some(expected));
}

fn assert_error_contains(error: JsValue, expected_fragment: &str) {
    let text = error.as_string().unwrap_or_default();
    assert!(
        text.contains(expected_fragment),
        "expected error to contain {expected_fragment:?}, got {text:?}"
    );
}

fn register_rich_users_table(db: &Database) {
    let builder = db
        .create_table("users")
        .column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        )
        .column("name", JsDataType::String, None)
        .column("age", JsDataType::Int32, None)
        .column("active", JsDataType::Boolean, None)
        .column("score", JsDataType::Float64, None)
        .column("joined_at", JsDataType::DateTime, None)
        .column(
            "avatar",
            JsDataType::Bytes,
            Some(ColumnOptions::new().set_nullable(true)),
        )
        .column(
            "city",
            JsDataType::String,
            Some(ColumnOptions::new().set_nullable(true)),
        );
    db.register_table(&builder).unwrap();
}

fn register_filter_users_table(db: &Database) {
    let builder = db
        .create_table("users")
        .column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        )
        .column("name", JsDataType::String, None)
        .column("age", JsDataType::Int32, None)
        .column("active", JsDataType::Boolean, None)
        .column("score", JsDataType::Float64, None)
        .column(
            "city",
            JsDataType::String,
            Some(ColumnOptions::new().set_nullable(true)),
        );
    db.register_table(&builder).unwrap();
}

fn register_employees_table(db: &Database) {
    let builder = db
        .create_table("employees")
        .column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        )
        .column("name", JsDataType::String, None)
        .column(
            "manager_id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_nullable(true)),
        );
    db.register_table(&builder).unwrap();
}

fn register_metrics_table(db: &Database) {
    let builder = db
        .create_table("metrics")
        .column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        )
        .column("category", JsDataType::String, None)
        .column("value", JsDataType::Int64, None);
    db.register_table(&builder).unwrap();
}

fn register_documents_table(db: &Database) {
    let builder = db
        .create_table("documents")
        .column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        )
        .column("title", JsDataType::String, None)
        .column("metadata", JsDataType::Jsonb, None)
        .jsonb_index(
            "metadata",
            &js_str_array(&["$.category", "$.tags", "$.author.name"]),
        );
    db.register_table(&builder).unwrap();
}

fn register_customers_table(db: &Database) {
    let builder = db
        .create_table("customers")
        .column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        )
        .column("name", JsDataType::String, None);
    db.register_table(&builder).unwrap();
}

fn register_orders_table(db: &Database) {
    let builder = db
        .create_table("orders")
        .column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        )
        .column("customer_id", JsDataType::Int64, None)
        .column("amount", JsDataType::Int64, None);
    db.register_table(&builder).unwrap();
}

fn register_payments_table(db: &Database) {
    let builder = db
        .create_table("payments")
        .column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        )
        .column("order_id", JsDataType::Int64, None)
        .column("method", JsDataType::String, None)
        .column("settled", JsDataType::Boolean, None);
    db.register_table(&builder).unwrap();
}

async fn seed_filter_users(db: &Database) {
    db.insert("users")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(1.0)),
                ("name", JsValue::from_str("Alice")),
                ("age", JsValue::from_f64(25.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(85.5)),
                ("city", JsValue::from_str("Beijing")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(2.0)),
                ("name", JsValue::from_str("Bob")),
                ("age", JsValue::from_f64(30.0)),
                ("active", JsValue::from_bool(false)),
                ("score", JsValue::from_f64(90.0)),
                ("city", JsValue::from_str("Shanghai")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(3.0)),
                ("name", JsValue::from_str("Charlie")),
                ("age", JsValue::from_f64(25.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(78.0)),
                ("city", JsValue::from_str("Beijing")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(4.0)),
                ("name", JsValue::from_str("David")),
                ("age", JsValue::from_f64(35.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(92.0)),
                ("city", JsValue::from_str("Shanghai")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(5.0)),
                ("name", JsValue::from_str("Eve")),
                ("age", JsValue::from_f64(28.0)),
                ("active", JsValue::from_bool(false)),
                ("score", JsValue::from_f64(88.5)),
                ("city", JsValue::from_str("Guangzhou")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(6.0)),
                ("name", JsValue::from_str("Frank")),
                ("age", JsValue::from_f64(31.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(82.0)),
                ("city", JsValue::from_str("Shenzhen")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(7.0)),
                ("name", JsValue::from_str("Grace")),
                ("age", JsValue::from_f64(29.0)),
                ("active", JsValue::from_bool(false)),
                ("score", JsValue::from_f64(80.0)),
                ("city", JsValue::NULL),
            ]),
        ]))
        .exec()
        .await
        .unwrap();
}

async fn seed_employees(db: &Database) {
    db.insert("employees")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(1.0)),
                ("name", JsValue::from_str("CEO")),
                ("manager_id", JsValue::NULL),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(2.0)),
                ("name", JsValue::from_str("Manager")),
                ("manager_id", JsValue::from_f64(1.0)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(3.0)),
                ("name", JsValue::from_str("Engineer")),
                ("manager_id", JsValue::from_f64(2.0)),
            ]),
        ]))
        .exec()
        .await
        .unwrap();
}

async fn seed_metrics(db: &Database) {
    db.insert("metrics")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(1.0)),
                ("category", JsValue::from_str("A")),
                ("value", JsValue::from_f64(2.0)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(2.0)),
                ("category", JsValue::from_str("A")),
                ("value", JsValue::from_f64(8.0)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(3.0)),
                ("category", JsValue::from_str("B")),
                ("value", JsValue::from_f64(4.0)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(4.0)),
                ("category", JsValue::from_str("B")),
                ("value", JsValue::from_f64(4.0)),
            ]),
        ]))
        .exec()
        .await
        .unwrap();
}

async fn seed_documents(db: &Database) {
    db.insert("documents")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(1.0)),
                ("title", JsValue::from_str("Rust Book")),
                (
                    "metadata",
                    js_json(
                        r#"{"tags":["rust","wasm"],"author":{"name":"Ada"},"category":"tech"}"#,
                    ),
                ),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(2.0)),
                ("title", JsValue::from_str("JS Guide")),
                (
                    "metadata",
                    js_json(r#"{"tags":["js"],"author":{"name":"Bea"},"category":"ops"}"#),
                ),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(3.0)),
                ("title", JsValue::from_str("Database Notes")),
                (
                    "metadata",
                    js_json(r#"{"tags":["storage","wasm"],"author":null,"category":"tech"}"#),
                ),
            ]),
        ]))
        .exec()
        .await
        .unwrap();
}

async fn seed_customers_and_orders(db: &Database) {
    db.insert("customers")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(1.0)),
                ("name", JsValue::from_str("Alice")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(2.0)),
                ("name", JsValue::from_str("Bob")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(3.0)),
                ("name", JsValue::from_str("Cara")),
            ]),
        ]))
        .exec()
        .await
        .unwrap();

    db.insert("orders")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(11.0)),
                ("customer_id", JsValue::from_f64(1.0)),
                ("amount", JsValue::from_f64(100.0)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(12.0)),
                ("customer_id", JsValue::from_f64(1.0)),
                ("amount", JsValue::from_f64(50.0)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(13.0)),
                ("customer_id", JsValue::from_f64(2.0)),
                ("amount", JsValue::from_f64(80.0)),
            ]),
        ]))
        .exec()
        .await
        .unwrap();
}

async fn seed_customers_orders_and_payments(db: &Database) {
    seed_customers_and_orders(db).await;

    db.insert("payments")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(101.0)),
                ("order_id", JsValue::from_f64(11.0)),
                ("method", JsValue::from_str("card")),
                ("settled", JsValue::from_bool(true)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(102.0)),
                ("order_id", JsValue::from_f64(13.0)),
                ("method", JsValue::from_str("cash")),
                ("settled", JsValue::from_bool(false)),
            ]),
        ]))
        .exec()
        .await
        .unwrap();
}

async fn seed_rich_users(db: &Database) {
    db.insert("users")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(1.0)),
                ("name", JsValue::from_str("Alice")),
                ("age", JsValue::from_f64(30.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(98.5)),
                (
                    "joined_at",
                    Date::new(&JsValue::from_f64(1_704_067_200_000.0)).into(),
                ),
                ("avatar", js_bytes(&[1, 2, 3])),
                ("city", JsValue::from_str("Beijing")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(2.0)),
                ("name", JsValue::from_str("Bob")),
                ("age", JsValue::from_f64(41.0)),
                ("active", JsValue::from_bool(false)),
                ("score", JsValue::from_f64(88.0)),
                (
                    "joined_at",
                    Date::new(&JsValue::from_f64(1_704_153_600_000.0)).into(),
                ),
                ("avatar", JsValue::NULL),
                ("city", JsValue::NULL),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(3.0)),
                ("name", JsValue::from_str("Cara")),
                ("age", JsValue::from_f64(27.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(91.25)),
                (
                    "joined_at",
                    Date::new(&JsValue::from_f64(1_704_240_000_000.0)).into(),
                ),
                ("avatar", js_bytes(&[9, 8])),
                ("city", JsValue::from_str("Shanghai")),
            ]),
        ]))
        .exec()
        .await
        .unwrap();
}

#[wasm_bindgen_test]
fn database_metadata_and_table_introspection_are_correct() {
    let db = Database::new("query_correctness_metadata");
    register_filter_users_table(&db);
    register_metrics_table(&db);

    assert_eq!(db.name(), "query_correctness_metadata");
    assert_eq!(db.table_count(), 2);
    assert!(db.has_table("users"));
    assert!(db.has_table("metrics"));

    let mut table_names: Vec<String> = db
        .table_names()
        .iter()
        .map(|value| value.as_string().unwrap())
        .collect();
    table_names.sort();
    assert_eq!(
        table_names,
        vec!["metrics".to_string(), "users".to_string()]
    );

    let users = db.table("users").unwrap();
    assert_eq!(users.name(), "users");
    assert_eq!(users.column_count(), 6);
    assert_eq!(users.get_column_type("score"), Some(JsDataType::Float64));
    assert!(users.is_column_nullable("city"));
    assert!(!users.is_column_nullable("name"));

    let column_names: Vec<String> = users
        .column_names()
        .iter()
        .map(|value| value.as_string().unwrap())
        .collect();
    assert_eq!(
        column_names,
        vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "active".to_string(),
            "score".to_string(),
            "city".to_string(),
        ]
    );

    let primary_key_columns: Vec<String> = users
        .primary_key_columns()
        .iter()
        .map(|value| value.as_string().unwrap())
        .collect();
    assert_eq!(primary_key_columns, vec!["id".to_string()]);
}

#[wasm_bindgen_test(async)]
async fn database_clear_table_clear_and_drop_preserve_expected_metadata() {
    let db = Database::new("query_correctness_clear_drop");
    register_filter_users_table(&db);
    register_metrics_table(&db);

    seed_filter_users(&db).await;
    seed_metrics(&db).await;
    assert_eq!(db.total_row_count(), 11);

    db.clear_table("metrics").unwrap();
    assert_eq!(db.total_row_count(), 7);
    assert!(db.has_table("metrics"));
    assert!(db.table("metrics").is_some());

    db.clear();
    assert_eq!(db.total_row_count(), 0);
    assert!(db.has_table("users"));
    assert!(db.has_table("metrics"));

    db.drop_table("metrics").unwrap();
    assert!(!db.has_table("metrics"));
    assert!(db.table("metrics").is_none());
    assert_eq!(db.table_count(), 1);
}

#[wasm_bindgen_test(async)]
async fn projection_layout_for_simple_partial_projection_is_correct() {
    let db = Database::new("query_correctness_projection_partial");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let specs = [
        spec("name", CellKind::String, true),
        spec("city", CellKind::String, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::String("Beijing".into())],
        vec![Cell::String("Bob".into()), Cell::String("Shanghai".into())],
    ];

    let query = db
        .select(&js_str_array(&["name", "city"]))
        .from("users")
        .order_by("id", JsSortOrder::Asc)
        .limit(2);

    assert_select_matches(&query, &specs, &expected).await;

    let layout = query.get_schema_layout().unwrap();
    assert_eq!(layout.columns()[0].name, "name");
    assert_eq!(layout.columns()[1].name, "city");
}

#[wasm_bindgen_test(async)]
async fn join_star_layout_qualifies_duplicate_column_names_and_nullability() {
    let db = Database::new("query_correctness_join_star_layout");
    register_employees_table(&db);
    seed_employees(&db).await;

    let specs = [
        spec("employees.id", CellKind::I64, false),
        spec("employees.name", CellKind::String, false),
        spec("employees.manager_id", CellKind::I64, true),
        spec("managers.id", CellKind::I64, true),
        spec("managers.name", CellKind::String, true),
        spec("managers.manager_id", CellKind::I64, true),
    ];
    let expected = vec![
        vec![
            Cell::I64(1),
            Cell::String("CEO".into()),
            Cell::Null,
            Cell::Null,
            Cell::Null,
            Cell::Null,
        ],
        vec![
            Cell::I64(2),
            Cell::String("Manager".into()),
            Cell::I64(1),
            Cell::I64(1),
            Cell::String("CEO".into()),
            Cell::Null,
        ],
        vec![
            Cell::I64(3),
            Cell::String("Engineer".into()),
            Cell::I64(2),
            Cell::I64(2),
            Cell::String("Manager".into()),
            Cell::I64(1),
        ],
    ];

    let query = db
        .select(&JsValue::from_str("*"))
        .from("employees")
        .left_join(
            "employees as managers",
            &col("employees.manager_id").eq(&JsValue::from_str("managers.id")),
        )
        .order_by("employees.id", JsSortOrder::Asc);

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn filter_equality_and_inequality_predicates_are_correct() {
    let db = Database::new("query_correctness_filter_eq_ne");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let specs = [spec("id", CellKind::I64, true)];

    let eq_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("city").eq(&JsValue::from_str("Beijing")))
        .order_by("id", JsSortOrder::Asc);
    let eq_expected = vec![vec![Cell::I64(1)], vec![Cell::I64(3)]];
    assert_select_matches(&eq_query, &specs, &eq_expected).await;

    let ne_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("age").ne(&JsValue::from_f64(25.0)))
        .order_by("id", JsSortOrder::Asc);
    let ne_expected = vec![
        vec![Cell::I64(2)],
        vec![Cell::I64(4)],
        vec![Cell::I64(5)],
        vec![Cell::I64(6)],
        vec![Cell::I64(7)],
    ];
    assert_select_matches(&ne_query, &specs, &ne_expected).await;
}

#[wasm_bindgen_test(async)]
async fn filter_range_predicates_are_correct() {
    let db = Database::new("query_correctness_filter_ranges");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let specs = [spec("id", CellKind::I64, true)];

    let gt_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("score").gt(&JsValue::from_f64(88.0)))
        .order_by("id", JsSortOrder::Asc);
    let gt_expected = vec![vec![Cell::I64(2)], vec![Cell::I64(4)], vec![Cell::I64(5)]];
    assert_select_matches(&gt_query, &specs, &gt_expected).await;

    let gte_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("age").gte(&JsValue::from_f64(30.0)))
        .order_by("id", JsSortOrder::Asc);
    let gte_expected = vec![vec![Cell::I64(2)], vec![Cell::I64(4)], vec![Cell::I64(6)]];
    assert_select_matches(&gte_query, &specs, &gte_expected).await;

    let lt_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("age").lt(&JsValue::from_f64(28.0)))
        .order_by("id", JsSortOrder::Asc);
    let lt_expected = vec![vec![Cell::I64(1)], vec![Cell::I64(3)]];
    assert_select_matches(&lt_query, &specs, &lt_expected).await;

    let lte_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("age").lte(&JsValue::from_f64(28.0)))
        .order_by("id", JsSortOrder::Asc);
    let lte_expected = vec![vec![Cell::I64(1)], vec![Cell::I64(3)], vec![Cell::I64(5)]];
    assert_select_matches(&lte_query, &specs, &lte_expected).await;
}

#[wasm_bindgen_test(async)]
async fn filter_membership_and_pattern_predicates_are_correct() {
    let db = Database::new("query_correctness_filter_patterns");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let specs = [spec("id", CellKind::I64, true)];

    let in_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("city").in_(&js_array([
            JsValue::from_str("Beijing"),
            JsValue::from_str("Shanghai"),
        ])))
        .order_by("id", JsSortOrder::Asc);
    let in_expected = vec![
        vec![Cell::I64(1)],
        vec![Cell::I64(2)],
        vec![Cell::I64(3)],
        vec![Cell::I64(4)],
    ];
    assert_select_matches(&in_query, &specs, &in_expected).await;

    let not_in_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("age").not_in(&js_array([
            JsValue::from_f64(25.0),
            JsValue::from_f64(30.0),
        ])))
        .order_by("id", JsSortOrder::Asc);
    let not_in_expected = vec![
        vec![Cell::I64(4)],
        vec![Cell::I64(5)],
        vec![Cell::I64(6)],
        vec![Cell::I64(7)],
    ];
    assert_select_matches(&not_in_query, &specs, &not_in_expected).await;

    let like_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("name").like("A%"))
        .order_by("id", JsSortOrder::Asc);
    let like_expected = vec![vec![Cell::I64(1)]];
    assert_select_matches(&like_query, &specs, &like_expected).await;

    let regex_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("name").regex_match("^[AE].*"))
        .order_by("id", JsSortOrder::Asc);
    let regex_expected = vec![vec![Cell::I64(1)], vec![Cell::I64(5)]];
    assert_select_matches(&regex_query, &specs, &regex_expected).await;
}

#[wasm_bindgen_test(async)]
async fn filter_null_and_chained_where_with_pagination_are_correct() {
    let db = Database::new("query_correctness_filter_null_chain");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let specs = [spec("id", CellKind::I64, true)];

    let null_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("city").is_null())
        .order_by("id", JsSortOrder::Asc);
    let null_expected = vec![vec![Cell::I64(7)]];
    assert_select_matches(&null_query, &specs, &null_expected).await;

    let chain_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("age").gte(&JsValue::from_f64(25.0)))
        .where_(&col("active").eq(&JsValue::from_bool(true)))
        .where_(&col("city").eq(&JsValue::from_str("Beijing")))
        .order_by("id", JsSortOrder::Asc);
    let chain_expected = vec![vec![Cell::I64(1)], vec![Cell::I64(3)]];
    assert_select_matches(&chain_query, &specs, &chain_expected).await;

    let paged_query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)))
        .order_by("score", JsSortOrder::Desc)
        .limit(2)
        .offset(1);
    let paged_expected = vec![vec![Cell::I64(1)], vec![Cell::I64(6)]];
    assert_select_matches(&paged_query, &specs, &paged_expected).await;
}

#[wasm_bindgen_test(async)]
async fn update_object_form_and_delete_paths_are_correct() {
    let db = Database::new("query_correctness_update_delete");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let updated = db
        .update("users")
        .set(
            &js_object(&[
                ("city", JsValue::from_str("Hangzhou")),
                ("active", JsValue::from_bool(true)),
            ]),
            None,
        )
        .where_(&col("id").eq(&JsValue::from_f64(2.0)))
        .exec()
        .await
        .unwrap();
    assert_eq!(updated.as_f64().unwrap() as usize, 1);

    let row_specs = [
        spec("id", CellKind::I64, true),
        spec("active", CellKind::Bool, true),
        spec("city", CellKind::String, true),
    ];
    let updated_query = db
        .select(&js_str_array(&["id", "active", "city"]))
        .from("users")
        .where_(&col("id").eq(&JsValue::from_f64(2.0)))
        .order_by("id", JsSortOrder::Asc);
    let updated_expected = vec![vec![
        Cell::I64(2),
        Cell::Bool(true),
        Cell::String("Hangzhou".into()),
    ]];
    assert_select_matches(&updated_query, &row_specs, &updated_expected).await;

    let filtered_deleted = db
        .delete("users")
        .where_(&col("id").eq(&JsValue::from_f64(5.0)))
        .exec()
        .await
        .unwrap();
    assert_eq!(filtered_deleted.as_f64().unwrap() as usize, 1);

    let remaining_deleted = db.delete("users").exec().await.unwrap();
    assert_eq!(remaining_deleted.as_f64().unwrap() as usize, 6);
    assert_eq!(db.total_row_count(), 0);
}

#[wasm_bindgen_test(async)]
async fn primary_key_update_candidate_path_preserves_predicate_semantics() {
    let db = Database::new("query_correctness_update_pk_candidate");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let updated = db
        .update("users")
        .set(
            &JsValue::from_str("city"),
            Some(JsValue::from_str("Chengdu")),
        )
        .where_(
            &col("id")
                .eq(&JsValue::from_f64(3.0))
                .and(&col("active").eq(&JsValue::from_bool(true))),
        )
        .exec()
        .await
        .unwrap();
    assert_eq!(updated.as_f64().unwrap() as usize, 1);

    let contradictory = db
        .update("users")
        .set(
            &JsValue::from_str("city"),
            Some(JsValue::from_str("Xiamen")),
        )
        .where_(
            &col("id")
                .eq(&JsValue::from_f64(3.0))
                .and(&col("id").eq(&JsValue::from_f64(4.0))),
        )
        .exec()
        .await
        .unwrap();
    assert_eq!(contradictory.as_f64().unwrap() as usize, 0);

    let or_updated = db
        .update("users")
        .set(
            &JsValue::from_str("city"),
            Some(JsValue::from_str("Nanjing")),
        )
        .where_(
            &col("id")
                .eq(&JsValue::from_f64(3.0))
                .or(&col("id").eq(&JsValue::from_f64(4.0))),
        )
        .exec()
        .await
        .unwrap();
    assert_eq!(or_updated.as_f64().unwrap() as usize, 2);

    let row_specs = [
        spec("id", CellKind::I64, true),
        spec("city", CellKind::String, true),
    ];
    let query = db
        .select(&js_str_array(&["id", "city"]))
        .from("users")
        .where_(&col("id").in_(&js_array([JsValue::from_f64(3.0), JsValue::from_f64(4.0)])))
        .order_by("id", JsSortOrder::Asc);
    let expected = vec![
        vec![Cell::I64(3), Cell::String("Nanjing".into())],
        vec![Cell::I64(4), Cell::String("Nanjing".into())],
    ];
    assert_select_matches(&query, &row_specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn primary_key_update_candidate_path_supports_finite_point_sets() {
    let db = Database::new("query_correctness_update_pk_candidate_sets");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let in_updated = db
        .update("users")
        .set(
            &JsValue::from_str("city"),
            Some(JsValue::from_str("Hangzhou")),
        )
        .where_(&col("id").in_(&js_array([
            JsValue::from_f64(3.0),
            JsValue::from_f64(4.0),
            JsValue::from_f64(4.0),
        ])))
        .exec()
        .await
        .unwrap();
    assert_eq!(in_updated.as_f64().unwrap() as usize, 2);

    let and_rechecked = db
        .update("users")
        .set(
            &JsValue::from_str("city"),
            Some(JsValue::from_str("Suzhou")),
        )
        .where_(
            &col("id")
                .in_(&js_array([JsValue::from_f64(3.0), JsValue::from_f64(4.0)]))
                .and(&col("active").eq(&JsValue::from_bool(true))),
        )
        .exec()
        .await
        .unwrap();
    assert_eq!(and_rechecked.as_f64().unwrap() as usize, 2);

    let intersection = db
        .update("users")
        .set(
            &JsValue::from_str("city"),
            Some(JsValue::from_str("Wuxi")),
        )
        .where_(
            &col("id")
                .in_(&js_array([JsValue::from_f64(3.0), JsValue::from_f64(4.0)]))
                .and(&col("id").eq(&JsValue::from_f64(4.0))),
        )
        .exec()
        .await
        .unwrap();
    assert_eq!(intersection.as_f64().unwrap() as usize, 1);

    let empty_intersection = db
        .update("users")
        .set(
            &JsValue::from_str("city"),
            Some(JsValue::from_str("Ningbo")),
        )
        .where_(
            &col("id")
                .in_(&js_array([JsValue::from_f64(3.0), JsValue::from_f64(4.0)]))
                .and(&col("id").eq(&JsValue::from_f64(5.0))),
        )
        .exec()
        .await
        .unwrap();
    assert_eq!(empty_intersection.as_f64().unwrap() as usize, 0);

    let or_with_non_pk_falls_back_correctly = db
        .update("users")
        .set(
            &JsValue::from_str("city"),
            Some(JsValue::from_str("Fallback")),
        )
        .where_(
            &col("id")
                .eq(&JsValue::from_f64(1.0))
                .or(&col("active").eq(&JsValue::from_bool(false))),
        )
        .exec()
        .await
        .unwrap();
    assert_eq!(or_with_non_pk_falls_back_correctly.as_f64().unwrap() as usize, 4);

    let row_specs = [
        spec("id", CellKind::I64, true),
        spec("city", CellKind::String, true),
    ];
    let query = db
        .select(&js_str_array(&["id", "city"]))
        .from("users")
        .order_by("id", JsSortOrder::Asc);
    let expected = vec![
        vec![Cell::I64(1), Cell::String("Fallback".into())],
        vec![Cell::I64(2), Cell::String("Fallback".into())],
        vec![Cell::I64(3), Cell::String("Suzhou".into())],
        vec![Cell::I64(4), Cell::String("Wuxi".into())],
        vec![Cell::I64(5), Cell::String("Fallback".into())],
        vec![Cell::I64(6), Cell::String("Shenzhen".into())],
        vec![Cell::I64(7), Cell::String("Fallback".into())],
    ];
    assert_select_matches(&query, &row_specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn primary_key_delete_candidate_path_supports_finite_point_sets() {
    let db = Database::new("query_correctness_delete_pk_candidate_sets");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let deleted = db
        .delete("users")
        .where_(
            &col("id")
                .eq(&JsValue::from_f64(3.0))
                .or(&col("id").in_(&js_array([
                    JsValue::from_f64(4.0),
                    JsValue::from_f64(4.0),
                ]))),
        )
        .exec()
        .await
        .unwrap();
    assert_eq!(deleted.as_f64().unwrap() as usize, 2);

    let filtered_deleted = db
        .delete("users")
        .where_(
            &col("id")
                .in_(&js_array([JsValue::from_f64(5.0), JsValue::from_f64(6.0)]))
                .and(&col("active").eq(&JsValue::from_bool(true))),
        )
        .exec()
        .await
        .unwrap();
    assert_eq!(filtered_deleted.as_f64().unwrap() as usize, 1);

    let row_specs = [
        spec("id", CellKind::I64, true),
        spec("name", CellKind::String, true),
    ];
    let query = db
        .select(&js_str_array(&["id", "name"]))
        .from("users")
        .order_by("id", JsSortOrder::Asc);
    let expected = vec![
        vec![Cell::I64(1), Cell::String("Alice".into())],
        vec![Cell::I64(2), Cell::String("Bob".into())],
        vec![Cell::I64(5), Cell::String("Eve".into())],
        vec![Cell::I64(7), Cell::String("Grace".into())],
    ];
    assert_select_matches(&query, &row_specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn multi_column_group_by_is_correct() {
    let db = Database::new("query_correctness_group_by_multi_column");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let specs = [
        spec("city", CellKind::String, true),
        spec("active", CellKind::Bool, true),
        spec("count", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::Null, Cell::Bool(false), Cell::I64(1)],
        vec![
            Cell::String("Beijing".into()),
            Cell::Bool(true),
            Cell::I64(2),
        ],
        vec![
            Cell::String("Guangzhou".into()),
            Cell::Bool(false),
            Cell::I64(1),
        ],
        vec![
            Cell::String("Shanghai".into()),
            Cell::Bool(false),
            Cell::I64(1),
        ],
        vec![
            Cell::String("Shanghai".into()),
            Cell::Bool(true),
            Cell::I64(1),
        ],
        vec![
            Cell::String("Shenzhen".into()),
            Cell::Bool(true),
            Cell::I64(1),
        ],
    ];

    let query = db
        .select(&JsValue::from_str("*"))
        .from("users")
        .group_by(&js_str_array(&["city", "active"]))
        .count()
        .order_by("city", JsSortOrder::Asc)
        .order_by("active", JsSortOrder::Asc);

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test]
fn union_rejects_incompatible_outputs() {
    let db = Database::new("query_correctness_union_incompatible");
    register_filter_users_table(&db);
    register_metrics_table(&db);

    let left = db.select(&js_str_array(&["name"])).from("users");
    let right = db.select(&js_str_array(&["value"])).from("metrics");

    let error = match left.union(&right) {
        Ok(_) => panic!("union should reject incompatible outputs"),
        Err(error) => error,
    };
    assert_eq!(
        error.as_string().as_deref(),
        Some("UNION operands must produce the same number of columns with matching types")
    );
}

#[wasm_bindgen_test(async)]
async fn transaction_commit_and_rollback_are_visible_to_queries() {
    let db = Database::new("query_correctness_transactions");
    register_filter_users_table(&db);

    let mut tx = db.transaction();
    assert!(tx.active());
    assert_eq!(tx.state(), "active");

    tx.insert(
        "users",
        &js_array([js_object(&[
            ("id", JsValue::from_f64(1.0)),
            ("name", JsValue::from_str("Alice")),
            ("age", JsValue::from_f64(25.0)),
            ("active", JsValue::from_bool(true)),
            ("score", JsValue::from_f64(85.5)),
            ("city", JsValue::from_str("Beijing")),
        ])]),
    )
    .unwrap();
    tx.commit().unwrap();
    assert!(!tx.active());

    let specs = [
        spec("id", CellKind::I64, true),
        spec("name", CellKind::String, true),
    ];
    let committed_query = db
        .select(&js_str_array(&["id", "name"]))
        .from("users")
        .order_by("id", JsSortOrder::Asc);
    let committed_expected = vec![vec![Cell::I64(1), Cell::String("Alice".into())]];
    assert_select_matches(&committed_query, &specs, &committed_expected).await;

    let mut rollback_tx = db.transaction();
    let update_count = rollback_tx
        .update(
            "users",
            &js_object(&[("name", JsValue::from_str("Alicia"))]),
            Some(col("id").eq(&JsValue::from_f64(1.0))),
        )
        .unwrap();
    assert_eq!(update_count, 1);
    rollback_tx.rollback().unwrap();

    assert_select_matches(&committed_query, &specs, &committed_expected).await;
}

#[wasm_bindgen_test(async)]
async fn exec_and_exec_binary_roundtrip_scalar_types_and_prepared_queries() {
    let db = Database::new("query_correctness_scalar_roundtrip");
    register_rich_users_table(&db);

    let inserted = db
        .insert("users")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(1.0)),
                ("name", JsValue::from_str("Alice")),
                ("age", JsValue::from_f64(30.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(98.5)),
                (
                    "joined_at",
                    Date::new(&JsValue::from_f64(1_704_067_200_000.0)).into(),
                ),
                ("avatar", js_bytes(&[1, 2, 3])),
                ("city", JsValue::from_str("Beijing")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(2.0)),
                ("name", JsValue::from_str("Bob")),
                ("age", JsValue::from_f64(41.0)),
                ("active", JsValue::from_bool(false)),
                ("score", JsValue::from_f64(88.0)),
                (
                    "joined_at",
                    Date::new(&JsValue::from_f64(1_704_153_600_000.0)).into(),
                ),
                ("avatar", JsValue::NULL),
                ("city", JsValue::NULL),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(3.0)),
                ("name", JsValue::from_str("Cara")),
                ("age", JsValue::from_f64(27.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(91.25)),
                (
                    "joined_at",
                    Date::new(&JsValue::from_f64(1_704_240_000_000.0)).into(),
                ),
                ("avatar", js_bytes(&[9, 8])),
                ("city", JsValue::from_str("Shanghai")),
            ]),
        ]))
        .exec()
        .await
        .unwrap();
    assert_eq!(inserted.as_f64().unwrap() as usize, 3);
    assert_eq!(db.total_row_count(), 3);

    let specs = [
        spec("id", CellKind::I64, false),
        spec("name", CellKind::String, false),
        spec("age", CellKind::I32, false),
        spec("active", CellKind::Bool, false),
        spec("score", CellKind::F64, false),
        spec("joined_at", CellKind::DateTime, false),
        spec("avatar", CellKind::Bytes, true),
        spec("city", CellKind::String, true),
    ];
    let expected = vec![
        vec![
            Cell::I64(1),
            Cell::String("Alice".into()),
            Cell::I32(30),
            Cell::Bool(true),
            Cell::F64(98.5),
            Cell::DateTime(1_704_067_200_000),
            Cell::Bytes(vec![1, 2, 3]),
            Cell::String("Beijing".into()),
        ],
        vec![
            Cell::I64(2),
            Cell::String("Bob".into()),
            Cell::I32(41),
            Cell::Bool(false),
            Cell::F64(88.0),
            Cell::DateTime(1_704_153_600_000),
            Cell::Null,
            Cell::Null,
        ],
        vec![
            Cell::I64(3),
            Cell::String("Cara".into()),
            Cell::I32(27),
            Cell::Bool(true),
            Cell::F64(91.25),
            Cell::DateTime(1_704_240_000_000),
            Cell::Bytes(vec![9, 8]),
            Cell::String("Shanghai".into()),
        ],
    ];

    let query = db
        .select(&JsValue::from_str("*"))
        .from("users")
        .order_by("id", JsSortOrder::Asc);

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn filter_queries_cover_core_predicates_and_and_chaining() {
    let db = Database::new("query_correctness_filters");
    register_filter_users_table(&db);

    db.insert("users")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(1.0)),
                ("name", JsValue::from_str("Alice")),
                ("age", JsValue::from_f64(25.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(85.5)),
                ("city", JsValue::from_str("Beijing")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(2.0)),
                ("name", JsValue::from_str("Bob")),
                ("age", JsValue::from_f64(30.0)),
                ("active", JsValue::from_bool(false)),
                ("score", JsValue::from_f64(90.0)),
                ("city", JsValue::from_str("Shanghai")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(3.0)),
                ("name", JsValue::from_str("Charlie")),
                ("age", JsValue::from_f64(25.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(78.0)),
                ("city", JsValue::from_str("Beijing")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(4.0)),
                ("name", JsValue::from_str("David")),
                ("age", JsValue::from_f64(35.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(92.0)),
                ("city", JsValue::from_str("Shanghai")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(5.0)),
                ("name", JsValue::from_str("Eve")),
                ("age", JsValue::from_f64(28.0)),
                ("active", JsValue::from_bool(false)),
                ("score", JsValue::from_f64(88.5)),
                ("city", JsValue::from_str("Guangzhou")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(6.0)),
                ("name", JsValue::from_str("Frank")),
                ("age", JsValue::from_f64(31.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(82.0)),
                ("city", JsValue::from_str("Shenzhen")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(7.0)),
                ("name", JsValue::from_str("Grace")),
                ("age", JsValue::from_f64(29.0)),
                ("active", JsValue::from_bool(false)),
                ("score", JsValue::from_f64(80.0)),
                ("city", JsValue::NULL),
            ]),
        ]))
        .exec()
        .await
        .unwrap();

    let specs = [spec("id", CellKind::I64, true)];

    let cases = [
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("active").eq(&JsValue::from_bool(true)))
                .order_by("id", JsSortOrder::Asc),
            vec![1, 3, 4, 6],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("age").between(&JsValue::from_f64(26.0), &JsValue::from_f64(31.0)))
                .order_by("id", JsSortOrder::Asc),
            vec![2, 5, 6, 7],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("age").not_between(&JsValue::from_f64(26.0), &JsValue::from_f64(31.0)))
                .order_by("id", JsSortOrder::Asc),
            vec![1, 3, 4],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("city").in_(&js_array([
                    JsValue::from_str("Beijing"),
                    JsValue::from_str("Shanghai"),
                ])))
                .order_by("id", JsSortOrder::Asc),
            vec![1, 2, 3, 4],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("age").not_in(&js_array([
                    JsValue::from_f64(25.0),
                    JsValue::from_f64(30.0),
                ])))
                .order_by("id", JsSortOrder::Asc),
            vec![4, 5, 6, 7],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("name").like("A%"))
                .order_by("id", JsSortOrder::Asc),
            vec![1],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("name").not_like("%i%"))
                .order_by("id", JsSortOrder::Asc),
            vec![2, 5, 6, 7],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("name").regex_match("^[ADF].*"))
                .order_by("id", JsSortOrder::Asc),
            vec![1, 4, 6],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("name").not_regex_match("^[ADF].*"))
                .order_by("id", JsSortOrder::Asc),
            vec![2, 3, 5, 7],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("city").is_null())
                .order_by("id", JsSortOrder::Asc),
            vec![7],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("city").is_not_null())
                .order_by("id", JsSortOrder::Asc),
            vec![1, 2, 3, 4, 5, 6],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("age").gte(&JsValue::from_f64(25.0)))
                .where_(&col("active").eq(&JsValue::from_bool(true)))
                .where_(&col("city").eq(&JsValue::from_str("Beijing")))
                .order_by("id", JsSortOrder::Asc),
            vec![1, 3],
        ),
        (
            db.select(&js_str_array(&["id"]))
                .from("users")
                .where_(&col("active").eq(&JsValue::from_bool(true)))
                .order_by("score", JsSortOrder::Desc)
                .limit(2)
                .offset(1),
            vec![1, 6],
        ),
    ];

    for (query, ids) in cases {
        let expected: Vec<Vec<Cell>> = ids.into_iter().map(|id| vec![Cell::I64(id)]).collect();
        assert_select_matches(&query, &specs, &expected).await;
    }
}

#[wasm_bindgen_test(async)]
async fn self_join_projection_preserves_qualified_columns_across_exec_paths() {
    let db = Database::new("query_correctness_self_join");
    register_employees_table(&db);

    db.insert("employees")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(1.0)),
                ("name", JsValue::from_str("CEO")),
                ("manager_id", JsValue::NULL),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(2.0)),
                ("name", JsValue::from_str("Manager")),
                ("manager_id", JsValue::from_f64(1.0)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(3.0)),
                ("name", JsValue::from_str("Engineer")),
                ("manager_id", JsValue::from_f64(2.0)),
            ]),
        ]))
        .exec()
        .await
        .unwrap();

    let specs = [
        spec("employees.name", CellKind::String, true),
        spec("managers.name", CellKind::String, true),
    ];
    let expected = vec![
        vec![Cell::String("CEO".into()), Cell::Null],
        vec![Cell::String("Manager".into()), Cell::String("CEO".into())],
        vec![
            Cell::String("Engineer".into()),
            Cell::String("Manager".into()),
        ],
    ];

    let query = db
        .select(&js_str_array(&["employees.name", "managers.name"]))
        .from("employees")
        .left_join(
            "employees as managers",
            &col("employees.manager_id").eq(&JsValue::from_str("managers.id")),
        )
        .order_by("employees.id", JsSortOrder::Asc);

    assert_select_matches(&query, &specs, &expected).await;

    let layout = query.get_schema_layout().unwrap();
    assert_eq!(layout.columns()[0].name, "employees.name");
    assert_eq!(layout.columns()[1].name, "managers.name");
    assert!(layout.columns()[1].is_nullable);
}

#[wasm_bindgen_test(async)]
async fn grouped_aggregates_cover_supported_numeric_summaries() {
    let db = Database::new("query_correctness_aggregates");
    register_metrics_table(&db);

    db.insert("metrics")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(1.0)),
                ("category", JsValue::from_str("A")),
                ("value", JsValue::from_f64(2.0)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(2.0)),
                ("category", JsValue::from_str("A")),
                ("value", JsValue::from_f64(8.0)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(3.0)),
                ("category", JsValue::from_str("B")),
                ("value", JsValue::from_f64(4.0)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(4.0)),
                ("category", JsValue::from_str("B")),
                ("value", JsValue::from_f64(4.0)),
            ]),
        ]))
        .exec()
        .await
        .unwrap();

    let specs = [
        spec("category", CellKind::String, true),
        spec("count", CellKind::I64, true),
        spec("sum_value", CellKind::I64, true),
        spec("avg_value", CellKind::F64, true),
        spec("min_value", CellKind::I64, true),
        spec("max_value", CellKind::I64, true),
        spec("stddev_value", CellKind::F64, true),
        spec("geomean_value", CellKind::F64, true),
        spec("distinct_value", CellKind::I64, true),
    ];
    let expected = vec![
        vec![
            Cell::String("A".into()),
            Cell::I64(2),
            Cell::I64(10),
            Cell::F64(5.0),
            Cell::I64(2),
            Cell::I64(8),
            Cell::F64(3.0),
            Cell::F64(4.0),
            Cell::I64(2),
        ],
        vec![
            Cell::String("B".into()),
            Cell::I64(2),
            Cell::I64(8),
            Cell::F64(4.0),
            Cell::I64(4),
            Cell::I64(4),
            Cell::F64(0.0),
            Cell::F64(4.0),
            Cell::I64(1),
        ],
    ];

    let query = db
        .select(&JsValue::from_str("*"))
        .from("metrics")
        .group_by(&js_str_array(&["category"]))
        .count()
        .sum("value")
        .avg("value")
        .min("value")
        .max("value")
        .stddev("value")
        .geomean("value")
        .distinct("value")
        .order_by("category", JsSortOrder::Asc);

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn prepared_queries_and_union_variants_remain_correct_after_mutations() {
    let db = Database::new("query_correctness_prepared_union");
    register_filter_users_table(&db);

    db.insert("users")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(1.0)),
                ("name", JsValue::from_str("Alice")),
                ("age", JsValue::from_f64(25.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(85.5)),
                ("city", JsValue::from_str("Beijing")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(2.0)),
                ("name", JsValue::from_str("Bob")),
                ("age", JsValue::from_f64(30.0)),
                ("active", JsValue::from_bool(false)),
                ("score", JsValue::from_f64(90.0)),
                ("city", JsValue::from_str("Shanghai")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(3.0)),
                ("name", JsValue::from_str("Charlie")),
                ("age", JsValue::from_f64(25.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(78.0)),
                ("city", JsValue::from_str("Beijing")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(4.0)),
                ("name", JsValue::from_str("David")),
                ("age", JsValue::from_f64(35.0)),
                ("active", JsValue::from_bool(true)),
                ("score", JsValue::from_f64(92.0)),
                ("city", JsValue::from_str("Shenzhen")),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(5.0)),
                ("name", JsValue::from_str("Eve")),
                ("age", JsValue::from_f64(28.0)),
                ("active", JsValue::from_bool(false)),
                ("score", JsValue::from_f64(88.5)),
                ("city", JsValue::from_str("Shanghai")),
            ]),
        ]))
        .exec()
        .await
        .unwrap();

    let active_specs = [
        spec("id", CellKind::I64, true),
        spec("name", CellKind::String, true),
    ];
    let active_query = db
        .select(&js_str_array(&["id", "name"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)))
        .order_by("id", JsSortOrder::Asc);

    let prepared = active_query.prepare().unwrap();
    let initial_active = vec![
        vec![Cell::I64(1), Cell::String("Alice".into())],
        vec![Cell::I64(3), Cell::String("Charlie".into())],
        vec![Cell::I64(4), Cell::String("David".into())],
    ];
    assert_select_matches(&active_query, &active_specs, &initial_active).await;
    assert_prepared_matches(&prepared, &active_specs, &initial_active).await;

    let updated = db
        .update("users")
        .set(
            &JsValue::from_str("active"),
            Some(JsValue::from_bool(false)),
        )
        .where_(&col("id").eq(&JsValue::from_f64(3.0)))
        .exec()
        .await
        .unwrap();
    assert_eq!(updated.as_f64().unwrap() as usize, 1);

    let inserted = db
        .insert("users")
        .values(&js_array([js_object(&[
            ("id", JsValue::from_f64(6.0)),
            ("name", JsValue::from_str("Frank")),
            ("age", JsValue::from_f64(31.0)),
            ("active", JsValue::from_bool(true)),
            ("score", JsValue::from_f64(82.0)),
            ("city", JsValue::from_str("Shanghai")),
        ])]))
        .exec()
        .await
        .unwrap();
    assert_eq!(inserted.as_f64().unwrap() as usize, 1);

    let deleted = db
        .delete("users")
        .where_(&col("id").eq(&JsValue::from_f64(1.0)))
        .exec()
        .await
        .unwrap();
    assert_eq!(deleted.as_f64().unwrap() as usize, 1);
    assert_eq!(db.total_row_count(), 5);

    let updated_active = vec![
        vec![Cell::I64(4), Cell::String("David".into())],
        vec![Cell::I64(6), Cell::String("Frank".into())],
    ];
    assert_select_matches(&active_query, &active_specs, &updated_active).await;
    assert_prepared_matches(&prepared, &active_specs, &updated_active).await;

    let union_specs = [spec("name", CellKind::String, true)];
    let left_for_union = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)));
    let right_for_union = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(false)));

    let union = left_for_union
        .union(&right_for_union)
        .unwrap()
        .order_by("name", JsSortOrder::Asc);
    let union_expected = vec![
        vec![Cell::String("Bob".into())],
        vec![Cell::String("Charlie".into())],
        vec![Cell::String("David".into())],
        vec![Cell::String("Eve".into())],
        vec![Cell::String("Frank".into())],
    ];
    assert_select_matches(&union, &union_specs, &union_expected).await;

    let left_for_union_all = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)));
    let right_for_union_all = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(false)));

    let union_all = left_for_union_all
        .union_all(&right_for_union_all)
        .unwrap()
        .order_by("name", JsSortOrder::Asc);
    let union_all_expected = vec![
        vec![Cell::String("Bob".into())],
        vec![Cell::String("Charlie".into())],
        vec![Cell::String("David".into())],
        vec![Cell::String("Eve".into())],
        vec![Cell::String("Frank".into())],
    ];
    assert_select_matches(&union_all, &union_specs, &union_all_expected).await;
}

#[wasm_bindgen_test(async)]
async fn jsonb_predicates_and_database_metadata_methods_are_correct() {
    let db = Database::new("query_correctness_jsonb");
    register_documents_table(&db);

    assert_eq!(db.table_count(), 1);
    assert!(db.has_table("documents"));

    seed_documents(&db).await;
    assert_eq!(db.total_row_count(), 3);

    let id_specs = [spec("id", CellKind::I64, true)];
    let tech_query = db
        .select(&js_str_array(&["id"]))
        .from("documents")
        .where_(
            &col("metadata")
                .get("$.category")
                .eq(&JsValue::from_str("tech")),
        )
        .order_by("id", JsSortOrder::Asc);
    let tech_expected = vec![vec![Cell::I64(1)], vec![Cell::I64(3)]];
    assert_select_matches(&tech_query, &id_specs, &tech_expected).await;

    let contains_specs = [
        spec("id", CellKind::I64, true),
        spec("title", CellKind::String, true),
    ];
    let contains_query = db
        .select(&js_str_array(&["id", "title"]))
        .from("documents")
        .where_(
            &col("metadata")
                .get("$.tags")
                .contains(&JsValue::from_str("wasm")),
        )
        .order_by("id", JsSortOrder::Asc);
    let contains_expected = vec![
        vec![Cell::I64(1), Cell::String("Rust Book".into())],
        vec![Cell::I64(3), Cell::String("Database Notes".into())],
    ];
    assert_select_matches(&contains_query, &contains_specs, &contains_expected).await;

    let exists_query = db
        .select(&js_str_array(&["id"]))
        .from("documents")
        .where_(&col("metadata").get("$.author.name").exists())
        .order_by("id", JsSortOrder::Asc);
    let exists_expected = vec![vec![Cell::I64(1)], vec![Cell::I64(2)]];
    assert_select_matches(&exists_query, &id_specs, &exists_expected).await;

    let metadata_specs = [
        spec("id", CellKind::I64, true),
        spec("metadata", CellKind::Json, true),
    ];
    let metadata_query = db
        .select(&js_str_array(&["id", "metadata"]))
        .from("documents")
        .where_(&col("id").eq(&JsValue::from_f64(1.0)))
        .order_by("id", JsSortOrder::Asc);
    let metadata_expected = vec![vec![
        Cell::I64(1),
        Cell::Json(r#"{"tags":["rust","wasm"],"author":{"name":"Ada"},"category":"tech"}"#.into()),
    ]];
    assert_select_matches(&metadata_query, &metadata_specs, &metadata_expected).await;
}

#[wasm_bindgen_test(async)]
async fn count_aggregate_without_group_by_is_correct() {
    let db = Database::new("query_correctness_aggregate_count");
    register_metrics_table(&db);
    seed_metrics(&db).await;

    let specs = [spec("count", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(4)]];

    let query = db.select(&JsValue::from_str("*")).from("metrics").count();
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn count_column_aggregate_ignores_nulls_is_correct() {
    let db = Database::new("query_correctness_aggregate_count_col");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let specs = [spec("count_city", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(6)]];

    let query = db
        .select(&JsValue::from_str("*"))
        .from("users")
        .count_col("city");
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn avg_min_max_aggregates_without_group_by_are_correct() {
    let db = Database::new("query_correctness_aggregate_avg_min_max");
    register_metrics_table(&db);
    seed_metrics(&db).await;

    let specs = [
        spec("avg_value", CellKind::F64, true),
        spec("min_value", CellKind::I64, true),
        spec("max_value", CellKind::I64, true),
    ];
    let expected = vec![vec![Cell::F64(4.5), Cell::I64(2), Cell::I64(8)]];

    let query = db
        .select(&JsValue::from_str("*"))
        .from("metrics")
        .avg("value")
        .min("value")
        .max("value");
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn stddev_geomean_and_distinct_aggregates_without_group_by_are_correct() {
    let db = Database::new("query_correctness_aggregate_stddev_geomean_distinct");
    register_metrics_table(&db);
    seed_metrics(&db).await;

    let specs = [
        spec("stddev_value", CellKind::F64, true),
        spec("geomean_value", CellKind::F64, true),
        spec("distinct_value", CellKind::I64, true),
    ];
    let expected = vec![vec![
        Cell::F64(2.179449471770337),
        Cell::F64(4.0),
        Cell::I64(3),
    ]];

    let query = db
        .select(&JsValue::from_str("*"))
        .from("metrics")
        .stddev("value")
        .geomean("value")
        .distinct("value");
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn single_column_group_by_count_is_correct() {
    let db = Database::new("query_correctness_group_by_single");
    register_metrics_table(&db);
    seed_metrics(&db).await;

    let specs = [
        spec("category", CellKind::String, true),
        spec("count", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("A".into()), Cell::I64(2)],
        vec![Cell::String("B".into()), Cell::I64(2)],
    ];

    let query = db
        .select(&JsValue::from_str("*"))
        .from("metrics")
        .group_by(&js_str_array(&["category"]))
        .count()
        .order_by("category", JsSortOrder::Asc);
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn inner_join_projection_across_tables_is_correct() {
    let db = Database::new("query_correctness_inner_join");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(100)],
        vec![Cell::String("Alice".into()), Cell::I64(50)],
        vec![Cell::String("Bob".into()), Cell::I64(80)],
    ];

    let query = db
        .select(&js_str_array(&["customers.name", "orders.amount"]))
        .from("customers")
        .inner_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .order_by("orders.id", JsSortOrder::Asc);
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn left_join_preserves_unmatched_left_rows() {
    let db = Database::new("query_correctness_left_join");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(100)],
        vec![Cell::String("Alice".into()), Cell::I64(50)],
        vec![Cell::String("Bob".into()), Cell::I64(80)],
        vec![Cell::String("Cara".into()), Cell::Null],
    ];

    let query = db
        .select(&js_str_array(&["customers.name", "orders.amount"]))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .order_by("customers.id", JsSortOrder::Asc)
        .order_by("orders.id", JsSortOrder::Asc);
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn union_distinct_query_is_correct() {
    let db = Database::new("query_correctness_union_distinct");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let left = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)));
    let right = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("city").eq(&JsValue::from_str("Shanghai")));

    let specs = [spec("name", CellKind::String, true)];
    let expected = vec![
        vec![Cell::String("Alice".into())],
        vec![Cell::String("Bob".into())],
        vec![Cell::String("Charlie".into())],
        vec![Cell::String("David".into())],
        vec![Cell::String("Frank".into())],
    ];

    let query = left
        .union(&right)
        .unwrap()
        .order_by("name", JsSortOrder::Asc);
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn union_all_query_preserves_duplicates() {
    let db = Database::new("query_correctness_union_all");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let left = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)));
    let right = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("city").eq(&JsValue::from_str("Shanghai")));

    let specs = [spec("name", CellKind::String, true)];
    let expected = vec![
        vec![Cell::String("Alice".into())],
        vec![Cell::String("Bob".into())],
        vec![Cell::String("Charlie".into())],
        vec![Cell::String("David".into())],
        vec![Cell::String("David".into())],
        vec![Cell::String("Frank".into())],
    ];

    let query = left
        .union_all(&right)
        .unwrap()
        .order_by("name", JsSortOrder::Asc);
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn jsonb_eq_query_is_correct() {
    let db = Database::new("query_correctness_jsonb_eq");
    register_documents_table(&db);
    seed_documents(&db).await;

    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(1)], vec![Cell::I64(3)]];

    let query = db
        .select(&js_str_array(&["id"]))
        .from("documents")
        .where_(
            &col("metadata")
                .get("$.category")
                .eq(&JsValue::from_str("tech")),
        )
        .order_by("id", JsSortOrder::Asc);
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn jsonb_contains_query_is_correct() {
    let db = Database::new("query_correctness_jsonb_contains");
    register_documents_table(&db);
    seed_documents(&db).await;

    let specs = [
        spec("id", CellKind::I64, true),
        spec("title", CellKind::String, true),
    ];
    let expected = vec![
        vec![Cell::I64(1), Cell::String("Rust Book".into())],
        vec![Cell::I64(3), Cell::String("Database Notes".into())],
    ];

    let query = db
        .select(&js_str_array(&["id", "title"]))
        .from("documents")
        .where_(
            &col("metadata")
                .get("$.tags")
                .contains(&JsValue::from_str("wasm")),
        )
        .order_by("id", JsSortOrder::Asc);
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn jsonb_exists_query_is_correct() {
    let db = Database::new("query_correctness_jsonb_exists");
    register_documents_table(&db);
    seed_documents(&db).await;

    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(1)], vec![Cell::I64(2)]];

    let query = db
        .select(&js_str_array(&["id"]))
        .from("documents")
        .where_(&col("metadata").get("$.author.name").exists())
        .order_by("id", JsSortOrder::Asc);
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn update_two_argument_set_form_is_correct() {
    let db = Database::new("query_correctness_update_two_arg");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let updated = db
        .update("users")
        .set(
            &JsValue::from_str("city"),
            Some(JsValue::from_str("Suzhou")),
        )
        .where_(&col("id").eq(&JsValue::from_f64(6.0)))
        .exec()
        .await
        .unwrap();
    assert_eq!(updated.as_f64().unwrap() as usize, 1);

    let specs = [
        spec("id", CellKind::I64, true),
        spec("city", CellKind::String, true),
    ];
    let expected = vec![vec![Cell::I64(6), Cell::String("Suzhou".into())]];

    let query = db
        .select(&js_str_array(&["id", "city"]))
        .from("users")
        .where_(&col("id").eq(&JsValue::from_f64(6.0)))
        .order_by("id", JsSortOrder::Asc);
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn delete_filtered_query_is_correct() {
    let db = Database::new("query_correctness_delete_filtered");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let deleted = db
        .delete("users")
        .where_(&col("active").eq(&JsValue::from_bool(false)))
        .exec()
        .await
        .unwrap();
    assert_eq!(deleted.as_f64().unwrap() as usize, 3);

    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![
        vec![Cell::I64(1)],
        vec![Cell::I64(3)],
        vec![Cell::I64(4)],
        vec![Cell::I64(6)],
    ];
    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .order_by("id", JsSortOrder::Asc);
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn delete_all_query_is_correct() {
    let db = Database::new("query_correctness_delete_all");
    register_metrics_table(&db);
    seed_metrics(&db).await;

    let deleted = db.delete("metrics").exec().await.unwrap();
    assert_eq!(deleted.as_f64().unwrap() as usize, 4);
    assert_eq!(db.total_row_count(), 0);

    let specs = [spec("id", CellKind::I64, true)];
    let expected: Vec<Vec<Cell>> = Vec::new();
    let query = db
        .select(&js_str_array(&["id"]))
        .from("metrics")
        .order_by("id", JsSortOrder::Asc);
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn select_without_from_returns_error() {
    let db = Database::new("query_correctness_missing_from");
    let query = db.select(&js_str_array(&["id"]));

    let error = match query.exec().await {
        Ok(_) => panic!("query without FROM should fail"),
        Err(error) => error,
    };
    assert_eq!(
        error.as_string().as_deref(),
        Some("FROM table not specified")
    );
}

#[wasm_bindgen_test(async)]
async fn selecting_missing_table_returns_error() {
    let db = Database::new("query_correctness_missing_table");
    let query = db.select(&js_str_array(&["id"])).from("missing_users");

    let error = match query.exec().await {
        Ok(_) => panic!("query against missing table should fail"),
        Err(error) => error,
    };
    assert_eq!(
        error.as_string().as_deref(),
        Some("Table not found: missing_users")
    );
}

#[wasm_bindgen_test(async)]
async fn insert_missing_non_nullable_column_returns_error() {
    let db = Database::new("query_correctness_insert_missing_column");
    register_filter_users_table(&db);

    let error = match db
        .insert("users")
        .values(&js_array([js_object(&[
            ("id", JsValue::from_f64(1.0)),
            ("age", JsValue::from_f64(25.0)),
            ("active", JsValue::from_bool(true)),
            ("score", JsValue::from_f64(85.5)),
            ("city", JsValue::from_str("Beijing")),
        ])]))
        .exec()
        .await
    {
        Ok(_) => panic!("insert missing required column should fail"),
        Err(error) => error,
    };

    assert_eq!(
        error.as_string().as_deref(),
        Some("Column name is not nullable")
    );
}

#[wasm_bindgen_test(async)]
async fn insert_wrong_type_returns_error() {
    let db = Database::new("query_correctness_insert_wrong_type");
    register_filter_users_table(&db);

    let error = match db
        .insert("users")
        .values(&js_array([js_object(&[
            ("id", JsValue::from_f64(1.0)),
            ("name", JsValue::from_str("Alice")),
            ("age", JsValue::from_str("not-a-number")),
            ("active", JsValue::from_bool(true)),
            ("score", JsValue::from_f64(85.5)),
            ("city", JsValue::from_str("Beijing")),
        ])]))
        .exec()
        .await
    {
        Ok(_) => panic!("insert with wrong type should fail"),
        Err(error) => error,
    };

    assert_eq!(error.as_string().as_deref(), Some("Expected number value"));
}

#[wasm_bindgen_test]
fn explain_returns_logical_optimized_and_physical_plan_strings() {
    let db = Database::new("query_correctness_explain");
    register_customers_table(&db);
    register_orders_table(&db);

    let query = db
        .select(&js_str_array(&["customers.name", "orders.amount"]))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .order_by("customers.id", JsSortOrder::Asc)
        .limit(2);

    let explain = query.explain().unwrap();
    let logical = Reflect::get(&explain, &JsValue::from_str("logical"))
        .unwrap()
        .as_string()
        .unwrap();
    let optimized = Reflect::get(&explain, &JsValue::from_str("optimized"))
        .unwrap()
        .as_string()
        .unwrap();
    let physical = Reflect::get(&explain, &JsValue::from_str("physical"))
        .unwrap()
        .as_string()
        .unwrap();

    assert!(logical.contains("Join"));
    assert!(logical.contains("Sort"));
    assert!(optimized.contains("Project"));
    assert!(physical.contains("Join") || physical.contains("HashJoin"));
}

#[wasm_bindgen_test(async)]
async fn select_with_undefined_columns_behaves_like_star() {
    let db = Database::new("query_correctness_select_undefined_star");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let columns = JsValue::UNDEFINED;
    let query = db
        .select(&columns)
        .from("users")
        .order_by("id", JsSortOrder::Asc)
        .limit(2);

    let specs = [
        spec("id", CellKind::I64, false),
        spec("name", CellKind::String, false),
        spec("age", CellKind::I32, false),
        spec("active", CellKind::Bool, false),
        spec("score", CellKind::F64, false),
        spec("city", CellKind::String, true),
    ];
    let expected = vec![
        vec![
            Cell::I64(1),
            Cell::String("Alice".into()),
            Cell::I32(25),
            Cell::Bool(true),
            Cell::F64(85.5),
            Cell::String("Beijing".into()),
        ],
        vec![
            Cell::I64(2),
            Cell::String("Bob".into()),
            Cell::I32(30),
            Cell::Bool(false),
            Cell::F64(90.0),
            Cell::String("Shanghai".into()),
        ],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn select_with_empty_array_behaves_like_star() {
    let db = Database::new("query_correctness_select_empty_array_star");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let columns: JsValue = Array::new().into();
    let query = db
        .select(&columns)
        .from("users")
        .order_by("id", JsSortOrder::Desc)
        .limit(1);

    let specs = [
        spec("id", CellKind::I64, false),
        spec("name", CellKind::String, false),
        spec("age", CellKind::I32, false),
        spec("active", CellKind::Bool, false),
        spec("score", CellKind::F64, false),
        spec("city", CellKind::String, true),
    ];
    let expected = vec![vec![
        Cell::I64(7),
        Cell::String("Grace".into()),
        Cell::I32(29),
        Cell::Bool(false),
        Cell::F64(80.0),
        Cell::Null,
    ]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn select_with_nested_array_projection_is_parsed_correctly() {
    let db = Database::new("query_correctness_select_nested_projection");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let query = db
        .select(&js_array([js_str_array(&["name", "age"])]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)))
        .order_by("id", JsSortOrder::Asc)
        .limit(2);

    let specs = [
        spec("name", CellKind::String, true),
        spec("age", CellKind::I32, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I32(25)],
        vec![Cell::String("Charlie".into()), Cell::I32(25)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn order_by_multiple_columns_with_mixed_directions_is_correct() {
    let db = Database::new("query_correctness_order_by_multiple_columns");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .order_by("age", JsSortOrder::Asc)
        .order_by("score", JsSortOrder::Desc)
        .order_by("id", JsSortOrder::Asc);

    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![
        vec![Cell::I64(1)],
        vec![Cell::I64(3)],
        vec![Cell::I64(5)],
        vec![Cell::I64(7)],
        vec![Cell::I64(2)],
        vec![Cell::I64(6)],
        vec![Cell::I64(4)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn offset_without_limit_uses_default_large_limit_and_is_correct() {
    let db = Database::new("query_correctness_offset_without_limit");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .order_by("id", JsSortOrder::Asc)
        .offset(5);

    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(6)], vec![Cell::I64(7)]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn empty_result_set_roundtrips_exec_and_exec_binary() {
    let db = Database::new("query_correctness_empty_result_set");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let query = db
        .select(&js_str_array(&["id", "name"]))
        .from("users")
        .where_(&col("city").eq(&JsValue::from_str("Nanjing")))
        .order_by("id", JsSortOrder::Asc);

    let specs = [
        spec("id", CellKind::I64, true),
        spec("name", CellKind::String, true),
    ];
    let expected: Vec<Vec<Cell>> = Vec::new();

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn filter_or_predicate_is_correct() {
    let db = Database::new("query_correctness_filter_or");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let predicate = col("city")
        .is_null()
        .or(&col("score").gt(&JsValue::from_f64(91.0)));
    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&predicate)
        .order_by("id", JsSortOrder::Asc);

    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(4)], vec![Cell::I64(7)]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn filter_not_predicate_is_correct() {
    let db = Database::new("query_correctness_filter_not");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)).not())
        .order_by("id", JsSortOrder::Asc);

    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(2)], vec![Cell::I64(5)], vec![Cell::I64(7)]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn filter_is_not_null_predicate_is_correct() {
    let db = Database::new("query_correctness_filter_is_not_null");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("city").is_not_null())
        .order_by("id", JsSortOrder::Asc);

    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![
        vec![Cell::I64(1)],
        vec![Cell::I64(2)],
        vec![Cell::I64(3)],
        vec![Cell::I64(4)],
        vec![Cell::I64(5)],
        vec![Cell::I64(6)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn filter_not_like_predicate_is_correct() {
    let db = Database::new("query_correctness_filter_not_like");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("name").not_like("%i%"))
        .order_by("id", JsSortOrder::Asc);

    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![
        vec![Cell::I64(2)],
        vec![Cell::I64(5)],
        vec![Cell::I64(6)],
        vec![Cell::I64(7)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn filter_not_match_predicate_is_correct() {
    let db = Database::new("query_correctness_filter_not_match");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("name").not_regex_match("^[A-D].*"))
        .order_by("id", JsSortOrder::Asc);

    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(5)], vec![Cell::I64(6)], vec![Cell::I64(7)]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn count_aggregate_with_where_is_correct() {
    let db = Database::new("query_correctness_count_with_where");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)))
        .count();

    let specs = [spec("count", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(4)]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn sum_aggregate_without_group_by_is_correct() {
    let db = Database::new("query_correctness_sum_aggregate");
    register_metrics_table(&db);
    seed_metrics(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("metrics")
        .sum("value");

    let specs = [spec("sum_value", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(18)]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn float_sum_aggregate_without_group_by_is_correct() {
    let db = Database::new("query_correctness_float_sum_aggregate");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("users")
        .sum("score");

    let specs = [spec("sum_score", CellKind::F64, true)];
    let expected = vec![vec![Cell::F64(596.0)]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn prepared_aggregate_query_reflects_insert_update_and_delete() {
    let db = Database::new("query_correctness_prepared_aggregate_mutations");
    register_metrics_table(&db);
    seed_metrics(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("metrics")
        .group_by(&js_str_array(&["category"]))
        .sum("value")
        .order_by("category", JsSortOrder::Asc);
    let specs = [
        spec("category", CellKind::String, true),
        spec("sum_value", CellKind::I64, true),
    ];
    let initial = vec![
        vec![Cell::String("A".into()), Cell::I64(10)],
        vec![Cell::String("B".into()), Cell::I64(8)],
    ];

    let prepared = query.prepare().unwrap();
    assert_select_matches(&query, &specs, &initial).await;
    assert_prepared_matches(&prepared, &specs, &initial).await;

    let updated = db
        .update("metrics")
        .set(&JsValue::from_str("value"), Some(JsValue::from_f64(6.0)))
        .where_(&col("id").eq(&JsValue::from_f64(2.0)))
        .exec()
        .await
        .unwrap();
    assert_eq!(updated.as_f64().unwrap() as usize, 1);

    let inserted = db
        .insert("metrics")
        .values(&js_array([js_object(&[
            ("id", JsValue::from_f64(5.0)),
            ("category", JsValue::from_str("C")),
            ("value", JsValue::from_f64(9.0)),
        ])]))
        .exec()
        .await
        .unwrap();
    assert_eq!(inserted.as_f64().unwrap() as usize, 1);

    let deleted = db
        .delete("metrics")
        .where_(&col("id").eq(&JsValue::from_f64(4.0)))
        .exec()
        .await
        .unwrap();
    assert_eq!(deleted.as_f64().unwrap() as usize, 1);

    let expected = vec![
        vec![Cell::String("A".into()), Cell::I64(8)],
        vec![Cell::String("B".into()), Cell::I64(4)],
        vec![Cell::String("C".into()), Cell::I64(9)],
    ];
    assert_select_matches(&query, &specs, &expected).await;
    assert_prepared_matches(&prepared, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn join_condition_with_column_object_is_correct() {
    let db = Database::new("query_correctness_join_column_object");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let right_col: JsValue = col("orders.customer_id").into();
    let query = db
        .select(&js_str_array(&["customers.name", "orders.amount"]))
        .from("customers")
        .inner_join("orders", &col("customers.id").eq(&right_col))
        .order_by("orders.id", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(100)],
        vec![Cell::String("Alice".into()), Cell::I64(50)],
        vec![Cell::String("Bob".into()), Cell::I64(80)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn join_condition_with_non_ambiguous_unqualified_columns_is_correct() {
    let db = Database::new("query_correctness_join_unqualified_columns");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let query = db
        .select(&js_str_array(&["customers.name", "orders.amount"]))
        .from("orders")
        .inner_join(
            "customers",
            &col("customer_id").eq(&JsValue::from_str("id")),
        )
        .order_by("orders.id", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(100)],
        vec![Cell::String("Alice".into()), Cell::I64(50)],
        vec![Cell::String("Bob".into()), Cell::I64(80)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn join_with_alias_projection_and_filter_is_correct() {
    let db = Database::new("query_correctness_join_alias_projection");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let query = db
        .select(&js_str_array(&["customers.name", "purchases.amount"]))
        .from("customers")
        .inner_join(
            "orders AS purchases",
            &col("customers.id").eq(&JsValue::from_str("purchases.customer_id")),
        )
        .where_(&col("purchases.amount").gt(&JsValue::from_f64(60.0)))
        .order_by("purchases.amount", JsSortOrder::Desc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(100)],
        vec![Cell::String("Bob".into()), Cell::I64(80)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn join_then_group_by_count_and_sum_is_correct() {
    let db = Database::new("query_correctness_join_group_by");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .inner_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .group_by(&js_str_array(&["customers.name"]))
        .count()
        .sum("orders.amount")
        .order_by("name", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count", CellKind::I64, true),
        spec("sum_amount", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(2), Cell::I64(150)],
        vec![Cell::String("Bob".into()), Cell::I64(1), Cell::I64(80)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn join_then_limit_and_offset_are_correct() {
    let db = Database::new("query_correctness_join_limit_offset");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let query = db
        .select(&js_str_array(&["customers.name", "orders.amount"]))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .order_by("customers.id", JsSortOrder::Asc)
        .order_by("orders.id", JsSortOrder::Asc)
        .offset(1)
        .limit(2);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(50)],
        vec![Cell::String("Bob".into()), Cell::I64(80)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn union_then_where_filters_frozen_output_correctly() {
    let db = Database::new("query_correctness_union_then_where");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let left = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)));
    let right = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("city").eq(&JsValue::from_str("Shanghai")));

    let query = left
        .union(&right)
        .unwrap()
        .where_(&col("name").like("D%"))
        .order_by("name", JsSortOrder::Asc);

    let specs = [spec("name", CellKind::String, true)];
    let expected = vec![vec![Cell::String("David".into())]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn union_then_limit_and_offset_are_correct() {
    let db = Database::new("query_correctness_union_limit_offset");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let left = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)));
    let right = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("city").eq(&JsValue::from_str("Shanghai")));

    let query = left
        .union_all(&right)
        .unwrap()
        .order_by("name", JsSortOrder::Asc)
        .offset(2)
        .limit(2);

    let specs = [spec("name", CellKind::String, true)];
    let expected = vec![
        vec![Cell::String("Charlie".into())],
        vec![Cell::String("David".into())],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn prepared_union_query_reflects_later_mutations() {
    let db = Database::new("query_correctness_prepared_union_mutations");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let left = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)));
    let right = db
        .select(&js_str_array(&["name"]))
        .from("users")
        .where_(&col("city").eq(&JsValue::from_str("Shanghai")));
    let query = left
        .union(&right)
        .unwrap()
        .order_by("name", JsSortOrder::Asc);

    let specs = [spec("name", CellKind::String, true)];
    let initial = vec![
        vec![Cell::String("Alice".into())],
        vec![Cell::String("Bob".into())],
        vec![Cell::String("Charlie".into())],
        vec![Cell::String("David".into())],
        vec![Cell::String("Frank".into())],
    ];

    let prepared = query.prepare().unwrap();
    assert_select_matches(&query, &specs, &initial).await;
    assert_prepared_matches(&prepared, &specs, &initial).await;

    let inserted = db
        .insert("users")
        .values(&js_array([js_object(&[
            ("id", JsValue::from_f64(8.0)),
            ("name", JsValue::from_str("Hank")),
            ("age", JsValue::from_f64(26.0)),
            ("active", JsValue::from_bool(false)),
            ("score", JsValue::from_f64(79.0)),
            ("city", JsValue::from_str("Shanghai")),
        ])]))
        .exec()
        .await
        .unwrap();
    assert_eq!(inserted.as_f64().unwrap() as usize, 1);

    let expected = vec![
        vec![Cell::String("Alice".into())],
        vec![Cell::String("Bob".into())],
        vec![Cell::String("Charlie".into())],
        vec![Cell::String("David".into())],
        vec![Cell::String("Frank".into())],
        vec![Cell::String("Hank".into())],
    ];
    assert_select_matches(&query, &specs, &expected).await;
    assert_prepared_matches(&prepared, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn update_without_where_updates_all_rows() {
    let db = Database::new("query_correctness_update_all_rows");
    register_metrics_table(&db);
    seed_metrics(&db).await;

    let updated = db
        .update("metrics")
        .set(&JsValue::from_str("category"), Some(JsValue::from_str("Z")))
        .exec()
        .await
        .unwrap();
    assert_eq!(updated.as_f64().unwrap() as usize, 4);

    let query = db
        .select(&js_str_array(&["id", "category"]))
        .from("metrics")
        .order_by("id", JsSortOrder::Asc);
    let specs = [
        spec("id", CellKind::I64, true),
        spec("category", CellKind::String, true),
    ];
    let expected = vec![
        vec![Cell::I64(1), Cell::String("Z".into())],
        vec![Cell::I64(2), Cell::String("Z".into())],
        vec![Cell::I64(3), Cell::String("Z".into())],
        vec![Cell::I64(4), Cell::String("Z".into())],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn delete_where_matching_no_rows_returns_zero() {
    let db = Database::new("query_correctness_delete_zero_rows");
    register_metrics_table(&db);
    seed_metrics(&db).await;

    let deleted = db
        .delete("metrics")
        .where_(&col("value").gt(&JsValue::from_f64(100.0)))
        .exec()
        .await
        .unwrap();
    assert_eq!(deleted.as_f64().unwrap() as usize, 0);

    let query = db
        .select(&js_str_array(&["id"]))
        .from("metrics")
        .order_by("id", JsSortOrder::Asc);
    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![
        vec![Cell::I64(1)],
        vec![Cell::I64(2)],
        vec![Cell::I64(3)],
        vec![Cell::I64(4)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn transaction_multiple_mutations_commit_are_visible_to_queries() {
    let db = Database::new("query_correctness_transaction_multi_commit");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let mut tx = db.transaction();
    let updated = tx
        .update(
            "users",
            &js_object(&[("active", JsValue::from_bool(true))]),
            Some(col("id").eq(&JsValue::from_f64(2.0))),
        )
        .unwrap();
    assert_eq!(updated, 1);

    let deleted = tx
        .delete("users", Some(col("id").eq(&JsValue::from_f64(1.0))))
        .unwrap();
    assert_eq!(deleted, 1);

    tx.insert(
        "users",
        &js_array([js_object(&[
            ("id", JsValue::from_f64(8.0)),
            ("name", JsValue::from_str("Hank")),
            ("age", JsValue::from_f64(26.0)),
            ("active", JsValue::from_bool(true)),
            ("score", JsValue::from_f64(79.0)),
            ("city", JsValue::from_str("Nanjing")),
        ])]),
    )
    .unwrap();
    tx.commit().unwrap();

    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)))
        .order_by("id", JsSortOrder::Asc);
    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![
        vec![Cell::I64(2)],
        vec![Cell::I64(3)],
        vec![Cell::I64(4)],
        vec![Cell::I64(6)],
        vec![Cell::I64(8)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn transaction_multiple_mutations_rollback_restores_query_results() {
    let db = Database::new("query_correctness_transaction_multi_rollback");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let mut tx = db.transaction();
    let updated = tx
        .update(
            "users",
            &js_object(&[("active", JsValue::from_bool(true))]),
            Some(col("id").eq(&JsValue::from_f64(2.0))),
        )
        .unwrap();
    assert_eq!(updated, 1);

    let deleted = tx
        .delete("users", Some(col("id").eq(&JsValue::from_f64(1.0))))
        .unwrap();
    assert_eq!(deleted, 1);

    tx.insert(
        "users",
        &js_array([js_object(&[
            ("id", JsValue::from_f64(8.0)),
            ("name", JsValue::from_str("Hank")),
            ("age", JsValue::from_f64(26.0)),
            ("active", JsValue::from_bool(true)),
            ("score", JsValue::from_f64(79.0)),
            ("city", JsValue::from_str("Nanjing")),
        ])]),
    )
    .unwrap();
    tx.rollback().unwrap();

    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)))
        .order_by("id", JsSortOrder::Asc);
    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![
        vec![Cell::I64(1)],
        vec![Cell::I64(3)],
        vec![Cell::I64(4)],
        vec![Cell::I64(6)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn exec_binary_without_from_returns_error() {
    let db = Database::new("query_correctness_exec_binary_without_from");
    let query = db.select(&js_str_array(&["id"]));

    let error = match query.exec_binary().await {
        Ok(_) => panic!("exec_binary without FROM should fail"),
        Err(error) => error,
    };
    assert_error_string(error, "FROM table not specified");
}

#[wasm_bindgen_test]
fn prepare_without_from_returns_error() {
    let db = Database::new("query_correctness_prepare_without_from");
    let query = db.select(&js_str_array(&["id"]));

    let error = match query.prepare() {
        Ok(_) => panic!("prepare without FROM should fail"),
        Err(error) => error,
    };
    assert_error_string(error, "FROM table not specified");
}

#[wasm_bindgen_test]
fn get_schema_layout_without_from_returns_error() {
    let db = Database::new("query_correctness_schema_without_from");
    let query = db.select(&js_str_array(&["id"]));

    let error = match query.get_schema_layout() {
        Ok(_) => panic!("get_schema_layout without FROM should fail"),
        Err(error) => error,
    };
    assert_error_string(error, "FROM table not specified");
}

#[wasm_bindgen_test(async)]
async fn exec_binary_missing_table_returns_error() {
    let db = Database::new("query_correctness_exec_binary_missing_table");
    let query = db.select(&js_str_array(&["id"])).from("missing_users");

    let error = match query.exec_binary().await {
        Ok(_) => panic!("exec_binary against missing table should fail"),
        Err(error) => error,
    };
    assert_error_string(error, "Table not found: missing_users");
}

#[wasm_bindgen_test]
fn prepare_missing_table_returns_error() {
    let db = Database::new("query_correctness_prepare_missing_table");
    let query = db.select(&js_str_array(&["id"])).from("missing_users");

    let error = match query.prepare() {
        Ok(_) => panic!("prepare against missing table should fail"),
        Err(error) => error,
    };
    assert_error_string(error, "Table not found: missing_users");
}

#[wasm_bindgen_test]
fn get_schema_layout_missing_table_returns_error() {
    let db = Database::new("query_correctness_schema_missing_table");
    let query = db.select(&js_str_array(&["id"])).from("missing_users");

    let error = match query.get_schema_layout() {
        Ok(_) => panic!("get_schema_layout against missing table should fail"),
        Err(error) => error,
    };
    assert_error_string(error, "Table not found: missing_users");
}

#[wasm_bindgen_test(async)]
async fn insert_without_values_returns_error() {
    let db = Database::new("query_correctness_insert_without_values");
    register_filter_users_table(&db);

    let error = match db.insert("users").exec().await {
        Ok(_) => panic!("insert without values should fail"),
        Err(error) => error,
    };
    assert_error_string(error, "No values specified");
}

#[wasm_bindgen_test(async)]
async fn update_missing_table_returns_error() {
    let db = Database::new("query_correctness_update_missing_table");

    let error = match db
        .update("missing_users")
        .set(&JsValue::from_str("name"), Some(JsValue::from_str("Alice")))
        .exec()
        .await
    {
        Ok(_) => panic!("update against missing table should fail"),
        Err(error) => error,
    };
    assert_error_string(error, "Table not found: missing_users");
}

#[wasm_bindgen_test(async)]
async fn delete_missing_table_returns_error() {
    let db = Database::new("query_correctness_delete_missing_table");

    let error = match db.delete("missing_users").exec().await {
        Ok(_) => panic!("delete against missing table should fail"),
        Err(error) => error,
    };
    assert_error_string(error, "Table not found: missing_users");
}

#[wasm_bindgen_test]
fn explain_missing_table_returns_error() {
    let db = Database::new("query_correctness_explain_missing_table");
    let query = db.select(&js_str_array(&["id"])).from("missing_users");

    let error = match query.explain() {
        Ok(_) => panic!("explain against missing table should fail"),
        Err(error) => error,
    };
    assert_error_string(error, "Table not found: missing_users");
}

#[wasm_bindgen_test(async)]
async fn datetime_range_predicate_is_correct() {
    let db = Database::new("query_correctness_datetime_range");
    register_rich_users_table(&db);
    seed_rich_users(&db).await;

    let low: JsValue = Date::new(&JsValue::from_f64(1_704_153_600_000.0)).into();
    let high: JsValue = Date::new(&JsValue::from_f64(1_704_240_000_000.0)).into();
    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("joined_at").between(&low, &high))
        .order_by("id", JsSortOrder::Asc);

    let specs = [spec("id", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(2)], vec![Cell::I64(3)]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn three_way_left_join_projection_preserves_nulls_and_ordering() {
    let db = Database::new("query_correctness_three_way_left_join_projection");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&js_str_array(&[
            "customers.name",
            "orders.amount",
            "payments.method",
            "payments.settled",
        ]))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .order_by("customers.id", JsSortOrder::Asc)
        .order_by("orders.id", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
        spec("method", CellKind::String, true),
        spec("settled", CellKind::Bool, true),
    ];
    let expected = vec![
        vec![
            Cell::String("Alice".into()),
            Cell::I64(100),
            Cell::String("card".into()),
            Cell::Bool(true),
        ],
        vec![
            Cell::String("Alice".into()),
            Cell::I64(50),
            Cell::Null,
            Cell::Null,
        ],
        vec![
            Cell::String("Bob".into()),
            Cell::I64(80),
            Cell::String("cash".into()),
            Cell::Bool(false),
        ],
        vec![
            Cell::String("Cara".into()),
            Cell::Null,
            Cell::Null,
            Cell::Null,
        ],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn three_way_join_with_aliases_filter_and_pagination_is_correct() {
    let db = Database::new("query_correctness_three_way_join_alias_filter_page");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&js_str_array(&[
            "customers.name",
            "purchases.amount",
            "receipts.method",
        ]))
        .from("customers")
        .inner_join(
            "orders as purchases",
            &col("customers.id").eq(&JsValue::from_str("purchases.customer_id")),
        )
        .left_join(
            "payments as receipts",
            &col("purchases.id").eq(&JsValue::from_str("receipts.order_id")),
        )
        .where_(&col("purchases.amount").gte(&JsValue::from_f64(50.0)))
        .where_(&col("receipts.method").is_not_null())
        .order_by("purchases.amount", JsSortOrder::Desc)
        .limit(2);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
        spec("method", CellKind::String, true),
    ];
    let expected = vec![
        vec![
            Cell::String("Alice".into()),
            Cell::I64(100),
            Cell::String("card".into()),
        ],
        vec![
            Cell::String("Bob".into()),
            Cell::I64(80),
            Cell::String("cash".into()),
        ],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn left_join_group_by_count_col_preserves_zero_match_groups() {
    let db = Database::new("query_correctness_left_join_group_count");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("orders.id")
        .order_by("name", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count_id", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(2)],
        vec![Cell::String("Bob".into()), Cell::I64(1)],
        vec![Cell::String("Cara".into()), Cell::I64(0)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn left_join_group_by_order_by_aggregate_and_pagination_is_correct() {
    let db = Database::new("query_correctness_left_join_group_order_page");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("orders.id")
        .order_by("count_id", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc)
        .offset(1)
        .limit(2);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count_id", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Bob".into()), Cell::I64(1)],
        vec![Cell::String("Cara".into()), Cell::I64(0)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn join_filter_group_by_sum_and_order_by_aggregate_desc_is_correct() {
    let db = Database::new("query_correctness_join_filter_group_sum");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .inner_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .where_(&col("orders.amount").gte(&JsValue::from_f64(60.0)))
        .group_by(&js_str_array(&["customers.name"]))
        .count()
        .sum("orders.amount")
        .order_by("sum_amount", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count", CellKind::I64, true),
        spec("sum_amount", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(1), Cell::I64(100)],
        vec![Cell::String("Bob".into()), Cell::I64(1), Cell::I64(80)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn left_join_group_by_count_paid_orders_is_correct() {
    let db = Database::new("query_correctness_left_join_group_paid_count");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("payments.id")
        .order_by("name", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count_id", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(1)],
        vec![Cell::String("Bob".into()), Cell::I64(1)],
        vec![Cell::String("Cara".into()), Cell::I64(0)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn prepared_left_join_group_by_query_reflects_join_side_mutations() {
    let db = Database::new("query_correctness_prepared_left_join_group_mutations");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("orders.id")
        .order_by("count_id", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count_id", CellKind::I64, true),
    ];
    let initial = vec![
        vec![Cell::String("Alice".into()), Cell::I64(2)],
        vec![Cell::String("Bob".into()), Cell::I64(1)],
        vec![Cell::String("Cara".into()), Cell::I64(0)],
    ];
    let prepared = query.prepare().unwrap();

    assert_select_matches(&query, &specs, &initial).await;
    assert_prepared_matches(&prepared, &specs, &initial).await;

    let inserted = db
        .insert("orders")
        .values(&js_array([
            js_object(&[
                ("id", JsValue::from_f64(14.0)),
                ("customer_id", JsValue::from_f64(3.0)),
                ("amount", JsValue::from_f64(60.0)),
            ]),
            js_object(&[
                ("id", JsValue::from_f64(15.0)),
                ("customer_id", JsValue::from_f64(3.0)),
                ("amount", JsValue::from_f64(70.0)),
            ]),
        ]))
        .exec()
        .await
        .unwrap();
    assert_eq!(inserted.as_f64().unwrap() as usize, 2);

    let deleted = db
        .delete("orders")
        .where_(&col("id").eq(&JsValue::from_f64(11.0)))
        .exec()
        .await
        .unwrap();
    assert_eq!(deleted.as_f64().unwrap() as usize, 1);

    let expected = vec![
        vec![Cell::String("Cara".into()), Cell::I64(2)],
        vec![Cell::String("Alice".into()), Cell::I64(1)],
        vec![Cell::String("Bob".into()), Cell::I64(1)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
    assert_prepared_matches(&prepared, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn prepared_three_way_left_join_query_reflects_third_table_mutations() {
    let db = Database::new("query_correctness_prepared_three_way_join_mutations");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&js_str_array(&[
            "customers.name",
            "orders.amount",
            "payments.method",
            "payments.settled",
        ]))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .order_by("customers.id", JsSortOrder::Asc)
        .order_by("orders.id", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
        spec("method", CellKind::String, true),
        spec("settled", CellKind::Bool, true),
    ];
    let initial = vec![
        vec![
            Cell::String("Alice".into()),
            Cell::I64(100),
            Cell::String("card".into()),
            Cell::Bool(true),
        ],
        vec![
            Cell::String("Alice".into()),
            Cell::I64(50),
            Cell::Null,
            Cell::Null,
        ],
        vec![
            Cell::String("Bob".into()),
            Cell::I64(80),
            Cell::String("cash".into()),
            Cell::Bool(false),
        ],
        vec![
            Cell::String("Cara".into()),
            Cell::Null,
            Cell::Null,
            Cell::Null,
        ],
    ];
    let prepared = query.prepare().unwrap();

    assert_select_matches(&query, &specs, &initial).await;
    assert_prepared_matches(&prepared, &specs, &initial).await;

    let inserted = db
        .insert("payments")
        .values(&js_array([js_object(&[
            ("id", JsValue::from_f64(103.0)),
            ("order_id", JsValue::from_f64(12.0)),
            ("method", JsValue::from_str("wire")),
            ("settled", JsValue::from_bool(true)),
        ])]))
        .exec()
        .await
        .unwrap();
    assert_eq!(inserted.as_f64().unwrap() as usize, 1);

    let updated = db
        .update("payments")
        .set(
            &JsValue::from_str("method"),
            Some(JsValue::from_str("bank")),
        )
        .where_(&col("id").eq(&JsValue::from_f64(102.0)))
        .exec()
        .await
        .unwrap();
    assert_eq!(updated.as_f64().unwrap() as usize, 1);

    let expected = vec![
        vec![
            Cell::String("Alice".into()),
            Cell::I64(100),
            Cell::String("card".into()),
            Cell::Bool(true),
        ],
        vec![
            Cell::String("Alice".into()),
            Cell::I64(50),
            Cell::String("wire".into()),
            Cell::Bool(true),
        ],
        vec![
            Cell::String("Bob".into()),
            Cell::I64(80),
            Cell::String("bank".into()),
            Cell::Bool(false),
        ],
        vec![
            Cell::String("Cara".into()),
            Cell::Null,
            Cell::Null,
            Cell::Null,
        ],
    ];

    assert_select_matches(&query, &specs, &expected).await;
    assert_prepared_matches(&prepared, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn three_way_left_join_filtering_for_missing_payment_is_correct() {
    let db = Database::new("query_correctness_three_way_missing_payment_filter");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&js_str_array(&["customers.name", "orders.id"]))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .where_(&col("payments.id").is_null())
        .order_by("customers.id", JsSortOrder::Asc)
        .order_by("orders.id", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("id", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(12)],
        vec![Cell::String("Cara".into()), Cell::Null],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn multi_join_aggregate_after_filter_on_third_table_is_correct() {
    let db = Database::new("query_correctness_multi_join_aggregate_third_filter");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .inner_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .inner_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .where_(&col("payments.settled").eq(&JsValue::from_bool(true)))
        .group_by(&js_str_array(&["customers.name"]))
        .count()
        .sum("orders.amount")
        .order_by("sum_amount", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count", CellKind::I64, true),
        spec("sum_amount", CellKind::I64, true),
    ];
    let expected = vec![vec![
        Cell::String("Alice".into()),
        Cell::I64(1),
        Cell::I64(100),
    ]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn three_way_left_join_group_by_multi_aggregates_and_pagination_is_correct() {
    let db = Database::new("query_correctness_three_way_left_join_group_multi");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("orders.amount")
        .count_col("payments.method")
        .order_by("count_amount", JsSortOrder::Desc)
        .order_by("count_method", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc)
        .offset(1)
        .limit(2);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count_amount", CellKind::I64, true),
        spec("count_method", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Bob".into()), Cell::I64(1), Cell::I64(1)],
        vec![Cell::String("Cara".into()), Cell::I64(0), Cell::I64(0)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn three_way_left_join_or_filter_then_projection_and_order_is_correct() {
    let db = Database::new("query_correctness_three_way_left_join_or_filter");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&js_str_array(&[
            "customers.name",
            "orders.amount",
            "payments.method",
            "payments.settled",
        ]))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .where_(
            &col("payments.settled")
                .eq(&JsValue::from_bool(true))
                .or(&col("orders.amount").eq(&JsValue::from_f64(50.0))),
        )
        .order_by("customers.name", JsSortOrder::Asc)
        .order_by("orders.amount", JsSortOrder::Desc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
        spec("method", CellKind::String, true),
        spec("settled", CellKind::Bool, true),
    ];
    let expected = vec![
        vec![
            Cell::String("Alice".into()),
            Cell::I64(100),
            Cell::String("card".into()),
            Cell::Bool(true),
        ],
        vec![
            Cell::String("Alice".into()),
            Cell::I64(50),
            Cell::Null,
            Cell::Null,
        ],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn union_all_of_join_queries_then_post_filter_order_and_limit_is_correct() {
    let db = Database::new("query_correctness_union_all_join_post_filter");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let left = db
        .select(&js_str_array(&["customers.name", "orders.amount"]))
        .from("customers")
        .inner_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .where_(&col("orders.amount").gte(&JsValue::from_f64(80.0)));
    let right = db
        .select(&js_str_array(&["customers.name", "orders.amount"]))
        .from("customers")
        .inner_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .where_(&col("customers.name").eq(&JsValue::from_str("Alice")));

    let query = left
        .union_all(&right)
        .unwrap()
        .where_(&col("amount").gte(&JsValue::from_f64(80.0)))
        .order_by("amount", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc)
        .offset(1)
        .limit(2);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(100)],
        vec![Cell::String("Bob".into()), Cell::I64(80)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn union_distinct_of_grouped_queries_then_order_by_aggregate_is_correct() {
    let db = Database::new("query_correctness_union_grouped_aggregates");
    register_metrics_table(&db);
    seed_metrics(&db).await;

    let left = db
        .select(&JsValue::from_str("*"))
        .from("metrics")
        .where_(&col("value").gte(&JsValue::from_f64(4.0)))
        .group_by(&js_str_array(&["category"]))
        .count()
        .sum("value");
    let right = db
        .select(&JsValue::from_str("*"))
        .from("metrics")
        .where_(&col("value").lte(&JsValue::from_f64(4.0)))
        .group_by(&js_str_array(&["category"]))
        .count()
        .sum("value");

    let query = left
        .union(&right)
        .unwrap()
        .order_by("sum_value", JsSortOrder::Desc)
        .order_by("category", JsSortOrder::Asc)
        .offset(1)
        .limit(2);

    let specs = [
        spec("category", CellKind::String, true),
        spec("count", CellKind::I64, true),
        spec("sum_value", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("B".into()), Cell::I64(2), Cell::I64(8)],
        vec![Cell::String("A".into()), Cell::I64(1), Cell::I64(2)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn prepared_union_all_of_join_queries_reflects_join_side_mutations() {
    let db = Database::new("query_correctness_prepared_union_join_mutations");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let left = db
        .select(&js_str_array(&["customers.name", "orders.amount"]))
        .from("customers")
        .inner_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .where_(&col("orders.amount").gte(&JsValue::from_f64(80.0)));
    let right = db
        .select(&js_str_array(&["customers.name", "orders.amount"]))
        .from("customers")
        .inner_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .where_(&col("customers.name").eq(&JsValue::from_str("Alice")));

    let query = left
        .union_all(&right)
        .unwrap()
        .where_(&col("amount").gte(&JsValue::from_f64(80.0)))
        .order_by("amount", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc)
        .offset(1)
        .limit(2);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
    ];
    let initial = vec![
        vec![Cell::String("Alice".into()), Cell::I64(100)],
        vec![Cell::String("Bob".into()), Cell::I64(80)],
    ];
    let prepared = query.prepare().unwrap();

    assert_select_matches(&query, &specs, &initial).await;
    assert_prepared_matches(&prepared, &specs, &initial).await;

    let inserted = db
        .insert("orders")
        .values(&js_array([js_object(&[
            ("id", JsValue::from_f64(14.0)),
            ("customer_id", JsValue::from_f64(2.0)),
            ("amount", JsValue::from_f64(90.0)),
        ])]))
        .exec()
        .await
        .unwrap();
    assert_eq!(inserted.as_f64().unwrap() as usize, 1);

    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(100)],
        vec![Cell::String("Bob".into()), Cell::I64(90)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
    assert_prepared_matches(&prepared, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn three_way_left_join_group_by_after_or_filter_is_correct() {
    let db = Database::new("query_correctness_three_way_left_join_group_or_filter");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .where_(
            &col("payments.settled")
                .eq(&JsValue::from_bool(true))
                .or(&col("orders.amount").eq(&JsValue::from_f64(50.0)))
                .or(&col("payments.method").eq(&JsValue::from_str("cash"))),
        )
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("orders.amount")
        .sum("orders.amount")
        .order_by("sum_amount", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count_amount", CellKind::I64, true),
        spec("sum_amount", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(2), Cell::I64(150)],
        vec![Cell::String("Bob".into()), Cell::I64(1), Cell::I64(80)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn left_join_missing_payment_rows_grouped_with_null_filter_and_order_is_correct() {
    let db = Database::new("query_correctness_left_join_missing_payment_grouped");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .where_(&col("payments.id").is_null())
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("orders.id")
        .order_by("count_id", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count_id", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(1)],
        vec![Cell::String("Cara".into()), Cell::I64(0)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn three_way_left_join_alias_or_null_and_not_filter_with_pagination_is_correct() {
    let db = Database::new("query_correctness_three_way_alias_or_null_not_page");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&js_str_array(&[
            "customers.name",
            "purchases.amount",
            "receipts.method",
        ]))
        .from("customers")
        .left_join(
            "orders as purchases",
            &col("customers.id").eq(&JsValue::from_str("purchases.customer_id")),
        )
        .left_join(
            "payments as receipts",
            &col("purchases.id").eq(&JsValue::from_str("receipts.order_id")),
        )
        .where_(
            &col("receipts.id")
                .is_null()
                .or(&col("receipts.method").eq(&JsValue::from_str("cash"))),
        )
        .where_(&col("customers.name").like("A%").not())
        .order_by("customers.id", JsSortOrder::Asc)
        .order_by("purchases.id", JsSortOrder::Asc)
        .offset(1)
        .limit(1);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
        spec("method", CellKind::String, true),
    ];
    let expected = vec![vec![Cell::String("Cara".into()), Cell::Null, Cell::Null]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn three_way_left_join_grouped_after_or_null_and_not_filters_is_correct() {
    let db = Database::new("query_correctness_three_way_group_or_null_not");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders as purchases",
            &col("customers.id").eq(&JsValue::from_str("purchases.customer_id")),
        )
        .left_join(
            "payments as receipts",
            &col("purchases.id").eq(&JsValue::from_str("receipts.order_id")),
        )
        .where_(
            &col("receipts.id")
                .is_null()
                .or(&col("receipts.method").eq(&JsValue::from_str("cash"))),
        )
        .where_(&col("customers.name").like("A%").not())
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("purchases.amount")
        .count_col("receipts.method")
        .order_by("count_amount", JsSortOrder::Desc)
        .order_by("count_method", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc)
        .offset(1)
        .limit(1);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count_amount", CellKind::I64, true),
        spec("count_method", CellKind::I64, true),
    ];
    let expected = vec![vec![
        Cell::String("Cara".into()),
        Cell::I64(0),
        Cell::I64(0),
    ]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn complex_left_join_filters_then_group_sum_and_pagination_is_correct() {
    let db = Database::new("query_correctness_complex_left_join_group_sum_page");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .where_(&col("orders.amount").gte(&JsValue::from_f64(50.0)))
        .where_(
            &col("payments.id")
                .is_null()
                .or(&col("payments.settled").eq(&JsValue::from_bool(false))),
        )
        .where_(&col("customers.name").like("C%").not())
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("orders.amount")
        .sum("orders.amount")
        .order_by("sum_amount", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc)
        .offset(1)
        .limit(1);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count_amount", CellKind::I64, true),
        spec("sum_amount", CellKind::I64, true),
    ];
    let expected = vec![vec![
        Cell::String("Alice".into()),
        Cell::I64(1),
        Cell::I64(50),
    ]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn projection_query_with_or_not_between_null_and_not_like_is_correct() {
    let db = Database::new("query_correctness_projection_or_not_between_null_not_like");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&js_str_array(&[
            "customers.name",
            "purchases.amount",
            "receipts.method",
        ]))
        .from("customers")
        .left_join(
            "orders as purchases",
            &col("customers.id").eq(&JsValue::from_str("purchases.customer_id")),
        )
        .left_join(
            "payments as receipts",
            &col("purchases.id").eq(&JsValue::from_str("receipts.order_id")),
        )
        .where_(&col("purchases.amount").is_not_null())
        .where_(
            &col("receipts.id").is_null().or(&col("purchases.amount")
                .not_between(&JsValue::from_f64(60.0), &JsValue::from_f64(90.0))),
        )
        .where_(&col("customers.name").like("B%").not())
        .order_by("purchases.amount", JsSortOrder::Desc)
        .offset(1)
        .limit(1);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
        spec("method", CellKind::String, true),
    ];
    let expected = vec![vec![
        Cell::String("Alice".into()),
        Cell::I64(50),
        Cell::Null,
    ]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn union_all_of_grouped_join_queries_with_post_filter_order_and_limit_is_correct() {
    let db = Database::new("query_correctness_union_all_grouped_join_matrix");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let left = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .where_(
            &col("payments.id")
                .is_null()
                .or(&col("payments.method").eq(&JsValue::from_str("cash"))),
        )
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("orders.amount");
    let right = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .where_(&col("customers.name").like("A%"))
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("orders.amount");

    let query = left
        .union_all(&right)
        .unwrap()
        .where_(&col("count_amount").gte(&JsValue::from_f64(1.0)))
        .order_by("count_amount", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc)
        .offset(1)
        .limit(2);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count_amount", CellKind::I64, true),
    ];
    let expected = vec![
        vec![Cell::String("Alice".into()), Cell::I64(1)],
        vec![Cell::String("Bob".into()), Cell::I64(1)],
    ];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn grouped_join_with_or_not_and_null_filters_orders_by_multiple_aggregates_is_correct() {
    let db = Database::new("query_correctness_grouped_join_or_not_null_multi_agg_order");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&JsValue::from_str("*"))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .where_(&col("orders.amount").is_not_null())
        .where_(
            &col("payments.id")
                .is_null()
                .or(&col("customers.name").like("A%").not()),
        )
        .group_by(&js_str_array(&["customers.name"]))
        .count_col("orders.amount")
        .count_col("payments.method")
        .order_by("count_method", JsSortOrder::Desc)
        .order_by("count_amount", JsSortOrder::Desc)
        .order_by("name", JsSortOrder::Asc)
        .offset(1)
        .limit(1);

    let specs = [
        spec("name", CellKind::String, true),
        spec("count_amount", CellKind::I64, true),
        spec("count_method", CellKind::I64, true),
    ];
    let expected = vec![vec![
        Cell::String("Alice".into()),
        Cell::I64(1),
        Cell::I64(0),
    ]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn projection_query_with_or_null_false_and_not_like_pagination_is_correct() {
    let db = Database::new("query_correctness_projection_or_null_false_not_like_page");
    register_customers_table(&db);
    register_orders_table(&db);
    register_payments_table(&db);
    seed_customers_orders_and_payments(&db).await;

    let query = db
        .select(&js_str_array(&[
            "customers.name",
            "orders.amount",
            "payments.method",
        ]))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .left_join(
            "payments",
            &col("orders.id").eq(&JsValue::from_str("payments.order_id")),
        )
        .where_(&col("orders.amount").gte(&JsValue::from_f64(50.0)))
        .where_(
            &col("payments.id")
                .is_null()
                .or(&col("payments.settled").eq(&JsValue::from_bool(false))),
        )
        .where_(&col("customers.name").like("C%").not())
        .order_by("orders.amount", JsSortOrder::Desc)
        .order_by("customers.name", JsSortOrder::Asc)
        .offset(1)
        .limit(1);

    let specs = [
        spec("name", CellKind::String, true),
        spec("amount", CellKind::I64, true),
        spec("method", CellKind::String, true),
    ];
    let expected = vec![vec![
        Cell::String("Alice".into()),
        Cell::I64(50),
        Cell::Null,
    ]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn insert_empty_array_returns_zero_and_keeps_table_empty() {
    let db = Database::new("query_correctness_insert_empty_array");
    register_filter_users_table(&db);

    let inserted = db
        .insert("users")
        .values(&js_array(Vec::<JsValue>::new()))
        .exec()
        .await
        .unwrap();
    assert_eq!(inserted.as_f64().unwrap() as usize, 0);
    assert_eq!(db.total_row_count(), 0);

    let query = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .order_by("id", JsSortOrder::Asc);
    let specs = [spec("id", CellKind::I64, true)];
    let expected: Vec<Vec<Cell>> = Vec::new();
    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn insert_null_into_non_nullable_column_returns_error() {
    let db = Database::new("query_correctness_insert_null_non_nullable");
    register_filter_users_table(&db);

    let error = match db
        .insert("users")
        .values(&js_array([js_object(&[
            ("id", JsValue::from_f64(1.0)),
            ("name", JsValue::NULL),
            ("age", JsValue::from_f64(25.0)),
            ("active", JsValue::from_bool(true)),
            ("score", JsValue::from_f64(85.5)),
            ("city", JsValue::from_str("Beijing")),
        ])]))
        .exec()
        .await
    {
        Ok(_) => panic!("insert with null in non-nullable column should fail"),
        Err(error) => error,
    };

    assert_error_string(error, "Column name is not nullable");
}

#[wasm_bindgen_test(async)]
async fn insert_duplicate_primary_key_returns_error() {
    let db = Database::new("query_correctness_insert_duplicate_pk");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let error = match db
        .insert("users")
        .values(&js_array([js_object(&[
            ("id", JsValue::from_f64(1.0)),
            ("name", JsValue::from_str("Another Alice")),
            ("age", JsValue::from_f64(20.0)),
            ("active", JsValue::from_bool(false)),
            ("score", JsValue::from_f64(70.0)),
            ("city", JsValue::from_str("Xi'an")),
        ])]))
        .exec()
        .await
    {
        Ok(_) => panic!("duplicate primary key insert should fail"),
        Err(error) => error,
    };

    assert_error_contains(error, "primary_key");
}

#[wasm_bindgen_test(async)]
async fn update_primary_key_to_existing_value_returns_error() {
    let db = Database::new("query_correctness_update_duplicate_pk");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let error = match db
        .update("users")
        .set(&JsValue::from_str("id"), Some(JsValue::from_f64(1.0)))
        .where_(&col("id").eq(&JsValue::from_f64(2.0)))
        .exec()
        .await
    {
        Ok(_) => panic!("updating primary key to an existing value should fail"),
        Err(error) => error,
    };

    assert_error_contains(error, "primary_key");
}

#[wasm_bindgen_test]
fn register_duplicate_table_returns_error() {
    let db = Database::new("query_correctness_duplicate_table");
    let builder = db
        .create_table("users")
        .column(
            "id",
            JsDataType::Int64,
            Some(ColumnOptions::new().set_primary_key(true)),
        )
        .column("name", JsDataType::String, None);

    db.register_table(&builder).unwrap();

    let error = match db.register_table(&builder) {
        Ok(_) => panic!("registering duplicate table should fail"),
        Err(error) => error,
    };

    assert_error_contains(error, "Table already exists: users");
}

#[wasm_bindgen_test]
fn clear_missing_table_returns_error() {
    let db = Database::new("query_correctness_clear_missing_table");

    let error = match db.clear_table("missing_users") {
        Ok(_) => panic!("clearing missing table should fail"),
        Err(error) => error,
    };

    assert_error_contains(error, "missing_users");
}

#[wasm_bindgen_test]
fn drop_missing_table_returns_error() {
    let db = Database::new("query_correctness_drop_missing_table");

    let error = match db.drop_table("missing_users") {
        Ok(_) => panic!("dropping missing table should fail"),
        Err(error) => error,
    };

    assert_error_contains(error, "missing_users");
}

#[wasm_bindgen_test]
fn union_left_without_from_returns_error() {
    let db = Database::new("query_correctness_union_left_without_from");
    register_filter_users_table(&db);

    let left = db.select(&js_str_array(&["id"]));
    let right = db.select(&js_str_array(&["id"])).from("users");

    let error = match left.union(&right) {
        Ok(_) => panic!("union with missing left FROM should fail"),
        Err(error) => error,
    };

    assert_error_string(error, "Left side of UNION is missing FROM");
}

#[wasm_bindgen_test]
fn union_right_without_from_returns_error() {
    let db = Database::new("query_correctness_union_right_without_from");
    register_filter_users_table(&db);

    let left = db.select(&js_str_array(&["id"])).from("users");
    let right = db.select(&js_str_array(&["id"]));

    let error = match left.union(&right) {
        Ok(_) => panic!("union with missing right FROM should fail"),
        Err(error) => error,
    };

    assert_error_string(error, "Right side of UNION is missing FROM");
}

#[wasm_bindgen_test(async)]
async fn prepared_query_exec_after_drop_table_returns_error() {
    let db = Database::new("query_correctness_prepared_after_drop_table");
    register_filter_users_table(&db);
    seed_filter_users(&db).await;

    let prepared = db
        .select(&js_str_array(&["id"]))
        .from("users")
        .where_(&col("active").eq(&JsValue::from_bool(true)))
        .prepare()
        .unwrap();

    db.drop_table("users").unwrap();

    let error = match prepared.exec().await {
        Ok(_) => panic!("prepared query against dropped table should fail"),
        Err(error) => error,
    };
    assert_error_contains(error, "users");

    let binary_error = match prepared.exec_binary().await {
        Ok(_) => panic!("prepared binary query against dropped table should fail"),
        Err(error) => error,
    };
    assert_error_contains(binary_error, "users");
}

#[wasm_bindgen_test(async)]
async fn prepared_join_query_exec_after_join_table_drop_returns_error() {
    let db = Database::new("query_correctness_prepared_join_after_drop");
    register_customers_table(&db);
    register_orders_table(&db);
    seed_customers_and_orders(&db).await;

    let prepared = db
        .select(&js_str_array(&["customers.name", "orders.amount"]))
        .from("customers")
        .left_join(
            "orders",
            &col("customers.id").eq(&JsValue::from_str("orders.customer_id")),
        )
        .prepare()
        .unwrap();

    db.drop_table("orders").unwrap();

    let error = match prepared.exec().await {
        Ok(_) => panic!("prepared join query after dropping join table should fail"),
        Err(error) => error,
    };
    assert_error_contains(error, "orders");
}

#[wasm_bindgen_test]
fn transaction_insert_after_commit_returns_error() {
    let db = Database::new("query_correctness_tx_insert_after_commit");
    register_filter_users_table(&db);

    let mut tx = db.transaction();
    tx.commit().unwrap();

    let error = match tx.insert(
        "users",
        &js_array([js_object(&[
            ("id", JsValue::from_f64(1.0)),
            ("name", JsValue::from_str("Alice")),
            ("age", JsValue::from_f64(25.0)),
            ("active", JsValue::from_bool(true)),
            ("score", JsValue::from_f64(85.5)),
            ("city", JsValue::from_str("Beijing")),
        ])]),
    ) {
        Ok(_) => panic!("insert after commit should fail"),
        Err(error) => error,
    };

    assert_error_string(error, "Transaction already completed");
}

#[wasm_bindgen_test]
fn transaction_update_after_rollback_returns_error() {
    let db = Database::new("query_correctness_tx_update_after_rollback");
    register_filter_users_table(&db);

    let mut tx = db.transaction();
    tx.rollback().unwrap();

    let error = match tx.update(
        "users",
        &js_object(&[("active", JsValue::from_bool(true))]),
        Some(col("id").eq(&JsValue::from_f64(1.0))),
    ) {
        Ok(_) => panic!("update after rollback should fail"),
        Err(error) => error,
    };

    assert_error_string(error, "Transaction already completed");
}

#[wasm_bindgen_test]
fn transaction_delete_after_commit_returns_error() {
    let db = Database::new("query_correctness_tx_delete_after_commit");
    register_filter_users_table(&db);

    let mut tx = db.transaction();
    tx.commit().unwrap();

    let error = match tx.delete("users", Some(col("id").eq(&JsValue::from_f64(1.0)))) {
        Ok(_) => panic!("delete after commit should fail"),
        Err(error) => error,
    };

    assert_error_string(error, "Transaction already completed");
}

#[wasm_bindgen_test]
fn transaction_second_completion_returns_error() {
    let db = Database::new("query_correctness_tx_second_completion");
    register_filter_users_table(&db);

    let mut tx = db.transaction();
    tx.commit().unwrap();

    let commit_error = match tx.commit() {
        Ok(_) => panic!("second commit should fail"),
        Err(error) => error,
    };
    assert_error_string(commit_error, "Transaction already completed");

    let rollback_error = match tx.rollback() {
        Ok(_) => panic!("rollback after commit should fail"),
        Err(error) => error,
    };
    assert_error_string(rollback_error, "Transaction already completed");
}

#[wasm_bindgen_test(async)]
async fn count_aggregate_on_empty_table_returns_zero() {
    let db = Database::new("query_correctness_count_empty_table");
    register_metrics_table(&db);

    let query = db.select(&JsValue::from_str("*")).from("metrics").count();
    let specs = [spec("count", CellKind::I64, true)];
    let expected = vec![vec![Cell::I64(0)]];

    assert_select_matches(&query, &specs, &expected).await;
}

#[wasm_bindgen_test(async)]
async fn grouped_aggregate_on_empty_table_returns_no_rows() {
    let db = Database::new("query_correctness_grouped_empty_table");
    register_metrics_table(&db);

    let query = db
        .select(&JsValue::from_str("*"))
        .from("metrics")
        .group_by(&js_str_array(&["category"]))
        .count()
        .sum("value")
        .order_by("category", JsSortOrder::Asc);
    let specs = [
        spec("category", CellKind::String, true),
        spec("count", CellKind::I64, true),
        spec("sum_value", CellKind::I64, true),
    ];
    let expected: Vec<Vec<Cell>> = Vec::new();

    assert_select_matches(&query, &specs, &expected).await;
}
