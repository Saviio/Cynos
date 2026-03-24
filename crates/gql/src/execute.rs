use alloc::format;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::cmp::Ordering;

use cynos_core::pattern_match::like;
use cynos_core::{reserve_row_ids, Row, Value};
use cynos_jsonb::{JsonPath, JsonbBinary};
#[cfg(test)]
use cynos_storage::RowStore;
use cynos_storage::TableCache;

use crate::batch_render::render_root_field_batched_stateless;
use crate::bind::{
    BoundCollectionQuery, BoundColumnAssignment, BoundFilter, BoundInsertRow, BoundOperation,
    BoundRootField, BoundRootFieldKind, ColumnPredicate, JsonPredicate, PredicateOp,
};
#[cfg(test)]
use crate::bind::{BoundField, BoundSelectionSet};
use crate::catalog::GraphqlCatalog;
#[cfg(test)]
use crate::catalog::RelationMeta;
use crate::error::{GqlError, GqlErrorKind, GqlResult};
use crate::plan::{build_root_field_plan, build_table_query_plan, execute_logical_plan};
use crate::response::{GraphqlResponse, ResponseField, ResponseValue};

#[derive(Clone, Debug)]
pub struct OperationOutcome {
    pub response: GraphqlResponse,
    pub changes: Vec<TableChange>,
}

#[derive(Clone, Debug)]
pub struct TableChange {
    pub table_name: String,
    pub row_changes: Vec<RowChange>,
}

#[derive(Clone, Debug)]
pub enum RowChange {
    Insert(Row),
    Update { old: Row, new: Row },
    Delete(Row),
}

pub fn execute_bound_operation(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    operation: &BoundOperation,
) -> GqlResult<GraphqlResponse> {
    if operation.kind == crate::ast::OperationType::Mutation {
        return Err(GqlError::new(
            GqlErrorKind::Unsupported,
            "mutation execution requires mutable access; use execute_bound_operation_mut",
        ));
    }

    let mut fields = Vec::with_capacity(operation.fields.len());
    for field in &operation.fields {
        fields.push(execute_root_field_readonly(
            cache, catalog, operation, field,
        )?);
    }
    Ok(GraphqlResponse::new(ResponseValue::object(fields)))
}

pub fn execute_bound_operation_mut(
    cache: &mut TableCache,
    catalog: &GraphqlCatalog,
    operation: &BoundOperation,
) -> GqlResult<OperationOutcome> {
    let mut fields = Vec::with_capacity(operation.fields.len());
    let mut changes = Vec::new();

    for field in &operation.fields {
        let (response_field, mut field_changes) =
            execute_root_field_mut(cache, catalog, operation, field)?;
        fields.push(response_field);
        changes.append(&mut field_changes);
    }

    Ok(OperationOutcome {
        response: GraphqlResponse::new(ResponseValue::object(fields)),
        changes,
    })
}

pub fn render_root_field_rows(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
    rows: &[Rc<Row>],
) -> GqlResult<ResponseField> {
    let plan = crate::compile_batch_plan(catalog, field)?;
    render_root_field_batched_stateless(cache, catalog, field, &plan, rows)
}

#[cfg(test)]
pub(crate) fn render_root_field_rows_recursive(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
    rows: &[Rc<Row>],
) -> GqlResult<ResponseField> {
    let value = match &field.kind {
        BoundRootFieldKind::Collection {
            table_name,
            selection,
            ..
        }
        | BoundRootFieldKind::Insert {
            table_name,
            selection,
            ..
        }
        | BoundRootFieldKind::Update {
            table_name,
            selection,
            ..
        }
        | BoundRootFieldKind::Delete {
            table_name,
            selection,
            ..
        } => render_row_list(cache, catalog, table_name, rows, selection)?,
        BoundRootFieldKind::ByPk {
            table_name,
            selection,
            ..
        } => match rows.first() {
            Some(row) => execute_row_selection(cache, catalog, table_name, row, selection)?,
            None => ResponseValue::Null,
        },
        BoundRootFieldKind::Typename => {
            return Err(GqlError::new(
                GqlErrorKind::Unsupported,
                "typename root fields do not accept row rendering",
            ))
        }
    };

    Ok(ResponseField::new(field.response_key.clone(), value))
}

fn execute_root_field_readonly(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    operation: &BoundOperation,
    field: &BoundRootField,
) -> GqlResult<ResponseField> {
    match &field.kind {
        BoundRootFieldKind::Typename => Ok(ResponseField::new(
            field.response_key.clone(),
            ResponseValue::Scalar(Value::String(operation.kind.root_typename().into())),
        )),
        BoundRootFieldKind::Collection { .. } | BoundRootFieldKind::ByPk { .. } => {
            execute_query_root_field(cache, catalog, field)
        }
        BoundRootFieldKind::Insert { .. }
        | BoundRootFieldKind::Update { .. }
        | BoundRootFieldKind::Delete { .. } => Err(GqlError::new(
            GqlErrorKind::Unsupported,
            "mutation root fields require mutable execution",
        )),
    }
}

fn execute_root_field_mut(
    cache: &mut TableCache,
    catalog: &GraphqlCatalog,
    operation: &BoundOperation,
    field: &BoundRootField,
) -> GqlResult<(ResponseField, Vec<TableChange>)> {
    match &field.kind {
        BoundRootFieldKind::Typename => Ok((
            ResponseField::new(
                field.response_key.clone(),
                ResponseValue::Scalar(Value::String(operation.kind.root_typename().into())),
            ),
            Vec::new(),
        )),
        BoundRootFieldKind::Collection { .. } | BoundRootFieldKind::ByPk { .. } => {
            Ok((execute_query_root_field(cache, catalog, field)?, Vec::new()))
        }
        BoundRootFieldKind::Insert {
            table_name,
            rows,
            selection: _,
        } => execute_insert_field(cache, catalog, field, table_name, rows),
        BoundRootFieldKind::Update {
            table_name,
            query,
            assignments,
            selection: _,
        } => execute_update_field(cache, catalog, field, table_name, query, assignments),
        BoundRootFieldKind::Delete {
            table_name,
            query,
            selection: _,
        } => execute_delete_field(cache, catalog, field, table_name, query),
    }
}

fn execute_query_root_field(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
) -> GqlResult<ResponseField> {
    match build_root_field_plan(catalog, field) {
        Ok(plan) => {
            let rows = execute_logical_plan(cache, &plan.table_name, plan.logical_plan)?;
            render_root_field_rows(cache, catalog, field, &rows)
        }
        Err(error) if error.kind() == GqlErrorKind::Unsupported => {
            execute_query_root_field_fallback(cache, catalog, field)
        }
        Err(error) => Err(error),
    }
}

fn execute_query_root_field_fallback(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
) -> GqlResult<ResponseField> {
    match &field.kind {
        BoundRootFieldKind::Collection {
            table_name, query, ..
        } => {
            let rows = select_collection_rows_fallback(cache, table_name, query)?;
            render_root_field_rows(cache, catalog, field, &rows)
        }
        BoundRootFieldKind::ByPk {
            table_name,
            pk_values,
            ..
        } => {
            let store = cache.get_table(table_name).ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Execution,
                    format!("table `{}` was not found", table_name),
                )
            })?;
            let rows: Vec<Rc<Row>> = store.get_by_pk_values(pk_values);
            render_root_field_rows(cache, catalog, field, &rows)
        }
        _ => Err(GqlError::new(
            GqlErrorKind::Unsupported,
            "fallback query execution only supports query root fields",
        )),
    }
}

fn execute_insert_field(
    cache: &mut TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
    table_name: &str,
    rows: &[BoundInsertRow],
) -> GqlResult<(ResponseField, Vec<TableChange>)> {
    let start_row_id = reserve_row_ids(rows.len() as u64);
    let mut inserted_rows = Vec::with_capacity(rows.len());
    let mut row_changes = Vec::with_capacity(rows.len());

    {
        let store = cache.get_table_mut(table_name).ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Execution,
                format!("table `{}` was not found", table_name),
            )
        })?;

        for (index, row) in rows.iter().enumerate() {
            let row_id = start_row_id + index as u64;
            let inserted = Row::new(row_id, row.values.clone());
            store
                .insert(inserted.clone())
                .map_err(|error| GqlError::new(GqlErrorKind::Execution, format!("{:?}", error)))?;
            inserted_rows.push(
                store
                    .get(row_id)
                    .unwrap_or_else(|| Rc::new(inserted.clone())),
            );
            row_changes.push(RowChange::Insert(inserted));
        }
    }

    let response = render_root_field_rows(cache, catalog, field, &inserted_rows)?;
    Ok((
        response,
        vec![TableChange {
            table_name: table_name.to_string(),
            row_changes,
        }],
    ))
}

fn execute_update_field(
    cache: &mut TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
    table_name: &str,
    query: &BoundCollectionQuery,
    assignments: &[BoundColumnAssignment],
) -> GqlResult<(ResponseField, Vec<TableChange>)> {
    let target_rows = select_collection_rows(cache, catalog, table_name, query)?;
    if target_rows.is_empty() {
        let response = render_root_field_rows(cache, catalog, field, &target_rows)?;
        return Ok((response, Vec::new()));
    }

    let mut updated_rows = Vec::with_capacity(target_rows.len());
    let mut row_changes = Vec::with_capacity(target_rows.len());

    {
        let store = cache.get_table_mut(table_name).ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Execution,
                format!("table `{}` was not found", table_name),
            )
        })?;

        for target_row in &target_rows {
            let mut new_values = target_row.values().to_vec();
            for assignment in assignments {
                if assignment.column_index < new_values.len() {
                    new_values[assignment.column_index] = assignment.value.clone();
                }
            }

            let updated = Row::new_with_version(
                target_row.id(),
                target_row.version().wrapping_add(1),
                new_values,
            );
            store
                .update(target_row.id(), updated.clone())
                .map_err(|error| GqlError::new(GqlErrorKind::Execution, format!("{:?}", error)))?;

            updated_rows.push(
                store
                    .get(target_row.id())
                    .unwrap_or_else(|| Rc::new(updated.clone())),
            );
            row_changes.push(RowChange::Update {
                old: (**target_row).clone(),
                new: updated,
            });
        }
    }

    let response = render_root_field_rows(cache, catalog, field, &updated_rows)?;
    Ok((
        response,
        vec![TableChange {
            table_name: table_name.to_string(),
            row_changes,
        }],
    ))
}

fn execute_delete_field(
    cache: &mut TableCache,
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
    table_name: &str,
    query: &BoundCollectionQuery,
) -> GqlResult<(ResponseField, Vec<TableChange>)> {
    let target_rows = select_collection_rows(cache, catalog, table_name, query)?;
    if target_rows.is_empty() {
        let response = render_root_field_rows(cache, catalog, field, &target_rows)?;
        return Ok((response, Vec::new()));
    }

    let row_ids: Vec<u64> = target_rows.iter().map(|row| row.id()).collect();
    {
        let store = cache.get_table_mut(table_name).ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Execution,
                format!("table `{}` was not found", table_name),
            )
        })?;
        store.delete_batch(&row_ids);
    }

    let response = render_root_field_rows(cache, catalog, field, &target_rows)?;
    let row_changes = target_rows
        .iter()
        .map(|row| RowChange::Delete((**row).clone()))
        .collect();
    Ok((
        response,
        vec![TableChange {
            table_name: table_name.to_string(),
            row_changes,
        }],
    ))
}

fn select_collection_rows(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    table_name: &str,
    query: &BoundCollectionQuery,
) -> GqlResult<Vec<Rc<Row>>> {
    let table = catalog.table(table_name).ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Binding,
            format!("table `{}` is not available", table_name),
        )
    })?;

    match build_table_query_plan(table_name, table, query) {
        Ok(plan) => execute_logical_plan(cache, table_name, plan),
        Err(error) if error.kind() == GqlErrorKind::Unsupported => {
            select_collection_rows_fallback(cache, table_name, query)
        }
        Err(error) => Err(error),
    }
}

fn select_collection_rows_fallback(
    cache: &TableCache,
    table_name: &str,
    query: &BoundCollectionQuery,
) -> GqlResult<Vec<Rc<Row>>> {
    let store = cache.get_table(table_name).ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Execution,
            format!("table `{}` was not found", table_name),
        )
    })?;
    let rows: Vec<Rc<Row>> = store.scan().collect();
    Ok(apply_collection_query(rows, query))
}

#[cfg(test)]
fn render_row_list(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    table_name: &str,
    rows: &[Rc<Row>],
    selection: &BoundSelectionSet,
) -> GqlResult<ResponseValue> {
    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        values.push(execute_row_selection(
            cache, catalog, table_name, row, selection,
        )?);
    }
    Ok(ResponseValue::list(values))
}

#[cfg(test)]
fn execute_row_selection(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    table_name: &str,
    row: &Rc<Row>,
    selection: &BoundSelectionSet,
) -> GqlResult<ResponseValue> {
    catalog.table(table_name).ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Execution,
            format!("table metadata for `{}` was not found", table_name),
        )
    })?;

    let mut fields = Vec::with_capacity(selection.fields.len());
    for field in &selection.fields {
        let value = match field {
            BoundField::Typename { value, .. } => {
                ResponseValue::Scalar(Value::String(value.clone()))
            }
            BoundField::Column { column_index, .. } => row
                .get(*column_index)
                .cloned()
                .map(ResponseValue::Scalar)
                .unwrap_or(ResponseValue::Null),
            BoundField::ForwardRelation {
                relation,
                selection,
                ..
            } => execute_forward_relation(cache, catalog, row, relation, selection)?,
            BoundField::ReverseRelation {
                relation,
                query,
                selection,
                ..
            } => execute_reverse_relation(cache, catalog, row, relation, query, selection)?,
        };
        let response_key = match field {
            BoundField::Typename { response_key, .. }
            | BoundField::Column { response_key, .. }
            | BoundField::ForwardRelation { response_key, .. }
            | BoundField::ReverseRelation { response_key, .. } => response_key,
        };
        fields.push(ResponseField::new(response_key.clone(), value));
    }

    Ok(ResponseValue::object(fields))
}

#[cfg(test)]
fn execute_forward_relation(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    row: &Rc<Row>,
    relation: &RelationMeta,
    selection: &BoundSelectionSet,
) -> GqlResult<ResponseValue> {
    let source_value = row
        .get(relation.child_column_index)
        .cloned()
        .unwrap_or(Value::Null);
    if source_value.is_null() {
        return Ok(ResponseValue::Null);
    }

    let store = cache.get_table(&relation.parent_table).ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Execution,
            format!("table `{}` was not found", relation.parent_table),
        )
    })?;

    let target_row = fetch_rows_by_column(store, &relation.parent_column, &source_value)
        .into_iter()
        .next();

    match target_row {
        Some(row) => execute_row_selection(cache, catalog, &relation.parent_table, &row, selection),
        None => Ok(ResponseValue::Null),
    }
}

#[cfg(test)]
fn execute_reverse_relation(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    row: &Rc<Row>,
    relation: &RelationMeta,
    query: &BoundCollectionQuery,
    selection: &BoundSelectionSet,
) -> GqlResult<ResponseValue> {
    let source_value = row
        .get(relation.parent_column_index)
        .cloned()
        .unwrap_or(Value::Null);
    if source_value.is_null() {
        return Ok(ResponseValue::list(Vec::new()));
    }

    let store = cache.get_table(&relation.child_table).ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Execution,
            format!("table `{}` was not found", relation.child_table),
        )
    })?;

    let mut rows = fetch_rows_by_index_or_scan(
        store,
        &relation.fk_name,
        &relation.child_column,
        &source_value,
    );
    rows = apply_collection_query(rows, query);

    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        values.push(execute_row_selection(
            cache,
            catalog,
            &relation.child_table,
            &row,
            selection,
        )?);
    }
    Ok(ResponseValue::list(values))
}

#[cfg(test)]
fn fetch_rows_by_column(store: &RowStore, column_name: &str, value: &Value) -> Vec<Rc<Row>> {
    if let Some(index_name) = find_single_column_index_name(store, column_name) {
        return fetch_rows_by_index_or_scan(store, index_name, column_name, value);
    }

    let Some(column_index) = store.schema().get_column_index(column_name) else {
        return Vec::new();
    };

    store
        .scan()
        .filter(|row| {
            row.get(column_index)
                .map(|candidate| candidate.sql_eq(value))
                .unwrap_or(false)
        })
        .collect()
}

#[cfg(test)]
fn fetch_rows_by_index_or_scan(
    store: &RowStore,
    index_name: &str,
    column_name: &str,
    value: &Value,
) -> Vec<Rc<Row>> {
    let rows = store.index_scan(
        index_name,
        Some(&cynos_index::KeyRange::only(value.clone())),
    );
    if !rows.is_empty() || store.schema().get_column_index(column_name).is_none() {
        return rows;
    }

    let Some(column_index) = store.schema().get_column_index(column_name) else {
        return Vec::new();
    };
    store
        .scan()
        .filter(|row| {
            row.get(column_index)
                .map(|candidate| candidate.sql_eq(value))
                .unwrap_or(false)
        })
        .collect()
}

#[cfg(test)]
fn find_single_column_index_name<'a>(store: &'a RowStore, column_name: &str) -> Option<&'a str> {
    store
        .schema()
        .indices()
        .iter()
        .find(|index| index.columns().len() == 1 && index.columns()[0].name == column_name)
        .map(|index| index.name())
}

pub(crate) fn apply_collection_query(
    mut rows: Vec<Rc<Row>>,
    query: &BoundCollectionQuery,
) -> Vec<Rc<Row>> {
    if let Some(filter) = &query.filter {
        rows.retain(|row| matches_filter(row, filter));
    }

    if !query.order_by.is_empty() {
        rows.sort_by(|left, right| compare_rows(left.as_ref(), right.as_ref(), &query.order_by));
    }

    let start = core::cmp::min(query.offset, rows.len());
    let end = match query.limit {
        Some(limit) => start.saturating_add(limit).min(rows.len()),
        None => rows.len(),
    };
    rows[start..end].to_vec()
}

fn compare_rows(left: &Row, right: &Row, order_by: &[crate::bind::OrderSpec]) -> Ordering {
    for spec in order_by {
        let left_value = left.get(spec.column_index).unwrap_or(&Value::Null);
        let right_value = right.get(spec.column_index).unwrap_or(&Value::Null);
        let ordering = left_value.cmp(right_value);
        if ordering != Ordering::Equal {
            return if spec.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    left.id().cmp(&right.id())
}

fn matches_filter(row: &Row, filter: &BoundFilter) -> bool {
    match filter {
        BoundFilter::And(filters) => filters.iter().all(|filter| matches_filter(row, filter)),
        BoundFilter::Or(filters) => filters.iter().any(|filter| matches_filter(row, filter)),
        BoundFilter::Column(predicate) => matches_column_predicate(row, predicate),
    }
}

fn matches_column_predicate(row: &Row, predicate: &ColumnPredicate) -> bool {
    let value = row.get(predicate.column_index).unwrap_or(&Value::Null);
    predicate.ops.iter().all(|op| match op {
        PredicateOp::IsNull(expected) => value.is_null() == *expected,
        PredicateOp::Eq(expected) => value.sql_eq(expected),
        PredicateOp::Ne(expected) => !value.sql_eq(expected),
        PredicateOp::In(expected) => expected.iter().any(|candidate| value.sql_eq(candidate)),
        PredicateOp::NotIn(expected) => expected.iter().all(|candidate| !value.sql_eq(candidate)),
        PredicateOp::Gt(expected) => value > expected,
        PredicateOp::Gte(expected) => value >= expected,
        PredicateOp::Lt(expected) => value < expected,
        PredicateOp::Lte(expected) => value <= expected,
        PredicateOp::Between(lower, upper) => value >= lower && value <= upper,
        PredicateOp::Like(pattern) => match value {
            Value::String(value) => like(value, pattern),
            Value::Bytes(value) => core::str::from_utf8(value)
                .map(|value| like(value, pattern))
                .unwrap_or(false),
            _ => false,
        },
        PredicateOp::Json(predicate) => matches_json_predicate(value, predicate),
    })
}

fn matches_json_predicate(value: &Value, predicate: &JsonPredicate) -> bool {
    let Value::Jsonb(binary_value) = value else {
        return false;
    };

    let json = JsonbBinary::from_bytes(binary_value.0.clone()).decode();
    let target = match predicate.path.as_deref() {
        Some(path) => {
            let Ok(path) = JsonPath::parse(path) else {
                return false;
            };
            json.query_first(&path)
        }
        None => Some(&json),
    };

    if let Some(expected) = predicate.exists {
        let exists = target.is_some();
        if exists != expected {
            return false;
        }
    }

    if let Some(expected) = &predicate.eq {
        if target != Some(expected) {
            return false;
        }
    }

    if let Some(expected) = &predicate.contains {
        match target {
            Some(actual) if actual.contains(expected) => {}
            _ => return false,
        }
    }

    true
}
