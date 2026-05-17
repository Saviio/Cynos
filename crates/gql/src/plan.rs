use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use cynos_core::schema::IndexType;
use cynos_core::{Row, Value};
use cynos_index::KeyRange;
use cynos_jsonb::{JsonbBinary, JsonbValue};
use cynos_query::ast::{Expr as AstExpr, SortOrder};
use cynos_query::context::{ExecutionContext, IndexInfo, QueryIndexType, TableStats};
use cynos_query::executor::{DataSource, ExecutionError, ExecutionResult, PhysicalPlanRunner};
use cynos_query::planner::{LogicalPlan, QueryPlanner};
use cynos_storage::TableCache;

use crate::bind::{
    BoundCollectionQuery, BoundFilter, BoundRootField, BoundRootFieldKind, JsonPredicate,
    PredicateOp,
};
use crate::catalog::{GraphqlCatalog, RelationMeta, TableMeta};
use crate::error::{GqlError, GqlErrorKind, GqlResult};

#[derive(Clone, Debug)]
pub struct RootFieldPlan {
    pub table_name: String,
    pub logical_plan: LogicalPlan,
}

struct RelationLoweringState<'a> {
    root_table_name: &'a str,
    plan: LogicalPlan,
    joins_by_path: BTreeMap<Vec<String>, String>,
    paths_by_table: BTreeMap<String, Vec<String>>,
}

impl<'a> RelationLoweringState<'a> {
    fn new(
        root_table_name: &'a str,
        root_table: &'a TableMeta,
        _catalog: &'a GraphqlCatalog,
    ) -> Self {
        let root_path = Vec::new();
        let mut paths_by_table = BTreeMap::new();
        paths_by_table.insert(root_table.table_name.clone(), root_path.clone());
        Self {
            root_table_name,
            plan: LogicalPlan::Scan {
                table: root_table_name.to_string(),
            },
            joins_by_path: BTreeMap::new(),
            paths_by_table,
        }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }

    fn has_joins(&self) -> bool {
        !self.joins_by_path.is_empty()
    }

    fn ensure_join(
        &mut self,
        current_table_name: &str,
        current_table: &TableMeta,
        relation: &RelationMeta,
        target_table: &TableMeta,
        path: Vec<String>,
    ) -> GqlResult<String> {
        if let Some(existing) = self.joins_by_path.get(&path) {
            return Ok(existing.clone());
        }

        if target_table.table_name == current_table.table_name
            || target_table.table_name == self.root_table_name
        {
            return Err(GqlError::new(
                GqlErrorKind::Unsupported,
                format!(
                    "relation filter path `{}` requires a self-join or alias, which is not supported yet",
                    render_relation_path(&path)
                ),
            ));
        }

        if let Some(existing_path) = self.paths_by_table.get(&target_table.table_name) {
            if existing_path != &path {
                return Err(GqlError::new(
                    GqlErrorKind::Unsupported,
                    format!(
                        "relation filter path `{}` reuses table `{}` through a second alias-less path, which is not supported yet",
                        render_relation_path(&path),
                        target_table.table_name
                    ),
                ));
            }
            return Ok(target_table.table_name.clone());
        }

        let condition = build_relation_join_condition(
            current_table_name,
            current_table,
            relation,
            &target_table.table_name,
            target_table,
        )?;
        let left = core::mem::replace(&mut self.plan, LogicalPlan::Empty);
        self.plan = LogicalPlan::left_join(
            left,
            LogicalPlan::Scan {
                table: target_table.table_name.clone(),
            },
            condition,
        );
        self.joins_by_path
            .insert(path.clone(), target_table.table_name.clone());
        self.paths_by_table
            .insert(target_table.table_name.clone(), path);
        Ok(target_table.table_name.clone())
    }
}

pub fn build_root_field_plan(
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
) -> GqlResult<RootFieldPlan> {
    match &field.kind {
        BoundRootFieldKind::Collection {
            table_name, query, ..
        } => {
            let table = catalog.table(table_name).ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Binding,
                    format!("table `{}` is not available", table_name),
                )
            })?;
            Ok(RootFieldPlan {
                table_name: table_name.clone(),
                logical_plan: build_table_query_plan(catalog, table_name, table, query)?,
            })
        }
        BoundRootFieldKind::ByPk {
            table_name,
            pk_values,
            ..
        } => {
            let table = catalog.table(table_name).ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Binding,
                    format!("table `{}` is not available", table_name),
                )
            })?;
            Ok(RootFieldPlan {
                table_name: table_name.clone(),
                logical_plan: build_by_pk_plan(table_name, table, pk_values)?,
            })
        }
        _ => Err(GqlError::new(
            GqlErrorKind::Unsupported,
            "planner-backed execution is only available for query/subscription root fields",
        )),
    }
}

pub(crate) fn build_table_query_plan(
    catalog: &GraphqlCatalog,
    table_name: &str,
    table: &TableMeta,
    query: &BoundCollectionQuery,
) -> GqlResult<LogicalPlan> {
    let mut lowering = RelationLoweringState::new(table_name, table, catalog);
    let filter_expr = if let Some(filter) = &query.filter {
        Some(build_filter_expr(
            catalog,
            table_name,
            table,
            filter,
            &mut lowering,
            &[],
        )?)
    } else {
        None
    };
    let has_joins = lowering.has_joins();
    let mut plan = lowering.into_plan();

    if let Some(predicate) = filter_expr {
        plan = LogicalPlan::Filter {
            input: Box::new(plan),
            predicate,
        };
        if has_joins {
            plan = build_root_projection(table_name, table, plan);
        }
    }

    if !query.order_by.is_empty() {
        let mut order_by = Vec::with_capacity(query.order_by.len());
        for spec in &query.order_by {
            let column = table.column_by_index(spec.column_index).ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Binding,
                    format!(
                        "column index {} was not found on `{}`",
                        spec.column_index, table_name
                    ),
                )
            })?;
            order_by.push((
                AstExpr::column(table_name, &column.name, column.index),
                if spec.descending {
                    SortOrder::Desc
                } else {
                    SortOrder::Asc
                },
            ));
        }
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            order_by,
        };
    }

    if query.limit.is_some() || query.offset > 0 {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            limit: query.limit.unwrap_or(usize::MAX),
            offset: query.offset,
        };
    }

    Ok(plan)
}

pub(crate) fn execute_logical_plan(
    cache: &TableCache,
    table_name: &str,
    logical_plan: LogicalPlan,
) -> GqlResult<Vec<Rc<Row>>> {
    let ctx = build_execution_context_for_plan(cache, table_name, &logical_plan);
    let planner = QueryPlanner::new(ctx);
    let physical_plan = planner.plan(logical_plan);

    let data_source = TableCacheDataSource::new(cache);
    let runner = PhysicalPlanRunner::new(&data_source);
    let artifact = runner.compile_execution_artifact_with_data_source(&physical_plan);
    runner
        .execute_with_artifact_row_vec(&physical_plan, &artifact)
        .map_err(|error| {
            GqlError::new(
                GqlErrorKind::Execution,
                format!("planner execution failed: {:?}", error),
            )
        })
}

fn build_by_pk_plan(
    table_name: &str,
    table: &TableMeta,
    pk_values: &[Value],
) -> GqlResult<LogicalPlan> {
    let pk = table.primary_key().ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Binding,
            format!("table `{}` does not define a primary key", table_name),
        )
    })?;

    if pk.columns.len() != pk_values.len() {
        return Err(GqlError::new(
            GqlErrorKind::Binding,
            format!(
                "primary-key arity mismatch for `{}`: expected {}, got {}",
                table_name,
                pk.columns.len(),
                pk_values.len()
            ),
        ));
    }

    let mut predicates = Vec::with_capacity(pk.columns.len());
    for (column, value) in pk.columns.iter().zip(pk_values.iter()) {
        predicates.push(AstExpr::eq(
            AstExpr::column(table_name, &column.name, column.index),
            AstExpr::literal(value.clone()),
        ));
    }

    Ok(LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            table: table_name.to_string(),
        }),
        predicate: and_all(predicates),
    })
}

fn build_filter_expr(
    catalog: &GraphqlCatalog,
    table_name: &str,
    table: &TableMeta,
    filter: &BoundFilter,
    lowering: &mut RelationLoweringState<'_>,
    current_path: &[String],
) -> GqlResult<AstExpr> {
    match filter {
        BoundFilter::And(filters) => {
            let mut expressions = Vec::with_capacity(filters.len());
            for filter in filters {
                expressions.push(build_filter_expr(
                    catalog,
                    table_name,
                    table,
                    filter,
                    lowering,
                    current_path,
                )?);
            }
            Ok(and_all(expressions))
        }
        BoundFilter::Or(filters) => {
            let mut expressions = Vec::with_capacity(filters.len());
            for filter in filters {
                expressions.push(build_filter_expr(
                    catalog,
                    table_name,
                    table,
                    filter,
                    lowering,
                    current_path,
                )?);
            }
            Ok(or_all(expressions))
        }
        BoundFilter::Column(predicate) => {
            let column = table
                .column_by_index(predicate.column_index)
                .ok_or_else(|| {
                    GqlError::new(
                        GqlErrorKind::Binding,
                        format!(
                            "column index {} was not found on `{}`",
                            predicate.column_index, table_name
                        ),
                    )
                })?;
            let column_expr = AstExpr::column(table_name, &column.name, column.index);
            let mut expressions = Vec::with_capacity(predicate.ops.len());
            for op in &predicate.ops {
                expressions.push(build_predicate_expr(column_expr.clone(), op)?);
            }
            Ok(and_all(expressions))
        }
        BoundFilter::Relation(predicate) => {
            let target_table = catalog.table(&predicate.target_table).ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Binding,
                    format!("table `{}` is not available", predicate.target_table),
                )
            })?;
            let mut path = current_path.to_vec();
            path.push(predicate.relation.name.clone());
            let joined_table_name = lowering.ensure_join(
                table_name,
                table,
                &predicate.relation,
                target_table,
                path.clone(),
            )?;
            let nested = build_filter_expr(
                catalog,
                &joined_table_name,
                target_table,
                predicate.filter.as_ref(),
                lowering,
                &path,
            )?;
            Ok(AstExpr::and(
                relation_exists_expr(&joined_table_name, target_table)?,
                nested,
            ))
        }
    }
}

fn build_root_projection(table_name: &str, table: &TableMeta, input: LogicalPlan) -> LogicalPlan {
    let columns = table
        .columns()
        .iter()
        .map(|column| AstExpr::column(table_name, &column.name, column.index))
        .collect();
    LogicalPlan::Project {
        input: Box::new(input),
        columns,
    }
}

fn relation_exists_expr(table_name: &str, table: &TableMeta) -> GqlResult<AstExpr> {
    let column = table
        .primary_key()
        .and_then(|pk| pk.columns.first())
        .or_else(|| table.columns().iter().find(|column| !column.nullable))
        .ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Unsupported,
                format!(
                    "relation filter target `{}` must expose a primary key or a non-null column",
                    table.table_name
                ),
            )
        })?;
    Ok(AstExpr::is_not_null(AstExpr::column(
        table_name,
        &column.name,
        column.index,
    )))
}

fn build_relation_join_condition(
    current_table_name: &str,
    current_table: &TableMeta,
    relation: &RelationMeta,
    target_table_name: &str,
    target_table: &TableMeta,
) -> GqlResult<AstExpr> {
    if current_table.table_name == relation.child_table
        && target_table.table_name == relation.parent_table
    {
        let left = current_table
            .column_by_index(relation.child_column_index)
            .ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Binding,
                    format!(
                        "column index {} was not found on `{}`",
                        relation.child_column_index, current_table.table_name
                    ),
                )
            })?;
        let right = target_table
            .column_by_index(relation.parent_column_index)
            .ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Binding,
                    format!(
                        "column index {} was not found on `{}`",
                        relation.parent_column_index, target_table.table_name
                    ),
                )
            })?;
        return Ok(AstExpr::eq(
            AstExpr::column(current_table_name, &left.name, left.index),
            AstExpr::column(target_table_name, &right.name, right.index),
        ));
    }

    if current_table.table_name == relation.parent_table
        && target_table.table_name == relation.child_table
    {
        let left = current_table
            .column_by_index(relation.parent_column_index)
            .ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Binding,
                    format!(
                        "column index {} was not found on `{}`",
                        relation.parent_column_index, current_table.table_name
                    ),
                )
            })?;
        let right = target_table
            .column_by_index(relation.child_column_index)
            .ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Binding,
                    format!(
                        "column index {} was not found on `{}`",
                        relation.child_column_index, target_table.table_name
                    ),
                )
            })?;
        return Ok(AstExpr::eq(
            AstExpr::column(current_table_name, &left.name, left.index),
            AstExpr::column(target_table_name, &right.name, right.index),
        ));
    }

    Err(GqlError::new(
        GqlErrorKind::Binding,
        format!(
            "relation `{}` does not connect `{}` to `{}`",
            relation.name, current_table.table_name, target_table.table_name
        ),
    ))
}

fn render_relation_path(path: &[String]) -> String {
    if path.is_empty() {
        return "<root>".to_string();
    }
    path.join(".")
}

fn build_predicate_expr(column_expr: AstExpr, op: &PredicateOp) -> GqlResult<AstExpr> {
    match op {
        PredicateOp::IsNull(true) => Ok(AstExpr::is_null(column_expr)),
        PredicateOp::IsNull(false) => Ok(AstExpr::is_not_null(column_expr)),
        PredicateOp::Eq(value) => Ok(AstExpr::eq(column_expr, AstExpr::literal(value.clone()))),
        PredicateOp::Ne(value) => Ok(AstExpr::ne(column_expr, AstExpr::literal(value.clone()))),
        PredicateOp::In(values) => Ok(AstExpr::in_list(column_expr, values.clone())),
        PredicateOp::NotIn(values) => Ok(AstExpr::not_in_list(column_expr, values.clone())),
        PredicateOp::Gt(value) => Ok(AstExpr::gt(column_expr, AstExpr::literal(value.clone()))),
        PredicateOp::Gte(value) => Ok(AstExpr::gte(column_expr, AstExpr::literal(value.clone()))),
        PredicateOp::Lt(value) => Ok(AstExpr::lt(column_expr, AstExpr::literal(value.clone()))),
        PredicateOp::Lte(value) => Ok(AstExpr::lte(column_expr, AstExpr::literal(value.clone()))),
        PredicateOp::Between(lower, upper) => Ok(AstExpr::between(
            column_expr,
            AstExpr::literal(lower.clone()),
            AstExpr::literal(upper.clone()),
        )),
        PredicateOp::Like(pattern) => Ok(AstExpr::like(column_expr, pattern)),
        PredicateOp::Json(predicate) => build_json_predicate_expr(column_expr, predicate),
    }
}

fn build_json_predicate_expr(
    column_expr: AstExpr,
    predicate: &JsonPredicate,
) -> GqlResult<AstExpr> {
    let mut expressions = Vec::new();

    if let Some(exists) = predicate.exists {
        let expression = match predicate.path.as_deref() {
            Some(path) => {
                let exists_expr = AstExpr::jsonb_exists(column_expr.clone(), path);
                if exists {
                    exists_expr
                } else {
                    AstExpr::not(exists_expr)
                }
            }
            None => {
                if exists {
                    AstExpr::is_not_null(column_expr.clone())
                } else {
                    AstExpr::is_null(column_expr.clone())
                }
            }
        };
        expressions.push(expression);
    }

    if let Some(expected) = &predicate.eq {
        let expression = match predicate.path.as_deref() {
            Some(path) => AstExpr::jsonb_path_eq(
                column_expr.clone(),
                path,
                jsonb_value_to_scalar_value(expected).ok_or_else(|| {
                    GqlError::new(
                        GqlErrorKind::Unsupported,
                        "planner-backed JSON path equality only supports scalar values",
                    )
                })?,
            ),
            None => AstExpr::eq(
                column_expr.clone(),
                AstExpr::literal(jsonb_value_to_literal(expected)),
            ),
        };
        expressions.push(expression);
    }

    if let Some(expected) = &predicate.contains {
        let path = predicate.path.as_deref().unwrap_or("$");
        expressions.push(AstExpr::jsonb_contains(
            column_expr,
            path,
            jsonb_value_to_scalar_value(expected).ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Unsupported,
                    "planner-backed JSON contains only supports scalar values",
                )
            })?,
        ));
    }

    if expressions.is_empty() {
        return Err(GqlError::new(
            GqlErrorKind::Unsupported,
            "empty JSON predicate cannot be compiled",
        ));
    }

    Ok(and_all(expressions))
}

fn jsonb_value_to_scalar_value(value: &JsonbValue) -> Option<Value> {
    match value {
        JsonbValue::Null => Some(Value::Null),
        JsonbValue::Bool(value) => Some(Value::Boolean(*value)),
        JsonbValue::Number(value) => {
            if *value == (*value as i64 as f64) {
                Some(Value::Int64(*value as i64))
            } else {
                Some(Value::Float64(*value))
            }
        }
        JsonbValue::String(value) => Some(Value::String(value.clone())),
        JsonbValue::Array(_) | JsonbValue::Object(_) => None,
    }
}

fn jsonb_value_to_literal(value: &JsonbValue) -> Value {
    match value {
        JsonbValue::Null => Value::Null,
        JsonbValue::Bool(value) => Value::Boolean(*value),
        JsonbValue::Number(value) => {
            if *value == (*value as i64 as f64) {
                Value::Int64(*value as i64)
            } else {
                Value::Float64(*value)
            }
        }
        JsonbValue::String(value) => Value::String(value.clone()),
        JsonbValue::Array(_) | JsonbValue::Object(_) => Value::Jsonb(cynos_core::JsonbValue::new(
            JsonbBinary::encode(value).into_bytes(),
        )),
    }
}

fn and_all(mut expressions: Vec<AstExpr>) -> AstExpr {
    let mut expression = expressions
        .pop()
        .unwrap_or_else(|| AstExpr::literal(Value::Boolean(true)));
    while let Some(next) = expressions.pop() {
        expression = AstExpr::and(next, expression);
    }
    expression
}

fn or_all(mut expressions: Vec<AstExpr>) -> AstExpr {
    let mut expression = expressions
        .pop()
        .unwrap_or_else(|| AstExpr::literal(Value::Boolean(false)));
    while let Some(next) = expressions.pop() {
        expression = AstExpr::or(next, expression);
    }
    expression
}

struct TableCacheDataSource<'a> {
    cache: &'a TableCache,
}

impl<'a> TableCacheDataSource<'a> {
    fn new(cache: &'a TableCache) -> Self {
        Self { cache }
    }
}

impl<'a> DataSource for TableCacheDataSource<'a> {
    fn get_table_rows(&self, table: &str) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        Ok(store.scan().collect())
    }

    fn get_index_range_with_limit(
        &self,
        table: &str,
        index: &str,
        range_start: Option<&Value>,
        range_end: Option<&Value>,
        include_start: bool,
        include_end: bool,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;

        let range = match (range_start, range_end) {
            (Some(start), Some(end)) => Some(KeyRange::bound(
                start.clone(),
                end.clone(),
                !include_start,
                !include_end,
            )),
            (Some(start), None) => Some(KeyRange::lower_bound(start.clone(), !include_start)),
            (None, Some(end)) => Some(KeyRange::upper_bound(end.clone(), !include_end)),
            (None, None) => None,
        };

        Ok(store.index_scan_with_options(index, range.as_ref(), limit, offset, reverse))
    }

    fn get_index_range_composite_with_limit(
        &self,
        table: &str,
        index: &str,
        range: Option<&KeyRange<Vec<Value>>>,
        limit: Option<usize>,
        offset: usize,
        reverse: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        Ok(store.index_scan_composite_with_options(index, range, limit, offset, reverse))
    }

    fn get_index_point(
        &self,
        table: &str,
        index: &str,
        key: &Value,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        let range = KeyRange::only(key.clone());
        Ok(store.index_scan(index, Some(&range)))
    }

    fn get_column_count(&self, table: &str) -> ExecutionResult<usize> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        Ok(store.schema().columns().len())
    }

    fn get_table_row_count(&self, table: &str) -> ExecutionResult<usize> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        Ok(store.len())
    }

    fn get_gin_index_rows(
        &self,
        table: &str,
        index: &str,
        key: &str,
        value: &str,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        Ok(store.gin_index_get_by_key_value(index, key, value))
    }

    fn get_gin_index_rows_by_key(
        &self,
        table: &str,
        index: &str,
        key: &str,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        Ok(store.gin_index_get_by_key(index, key))
    }

    fn get_gin_index_rows_multi(
        &self,
        table: &str,
        index: &str,
        pairs: &[(&str, &str)],
        match_all: bool,
    ) -> ExecutionResult<Vec<Rc<Row>>> {
        let store = self
            .cache
            .get_table(table)
            .ok_or_else(|| ExecutionError::TableNotFound(table.into()))?;
        Ok(if match_all {
            store.gin_index_get_by_key_values_all(index, pairs)
        } else {
            store.gin_index_get_by_key_values_any(index, pairs)
        })
    }
}

fn build_execution_context_for_plan(
    cache: &TableCache,
    table_name: &str,
    plan: &LogicalPlan,
) -> ExecutionContext {
    let mut ctx = ExecutionContext::new();
    let mut tables = plan.collect_tables();
    if !tables.iter().any(|table| table == table_name) {
        tables.push(table_name.into());
    }

    for table in tables {
        register_table_context(cache, &mut ctx, &table);
    }

    ctx
}

fn register_table_context(cache: &TableCache, ctx: &mut ExecutionContext, table_name: &str) {
    if let Some(store) = cache.get_table(table_name) {
        let schema = store.schema();

        let mut indexes = Vec::new();
        for idx in schema.indices() {
            let index_type = match idx.get_index_type() {
                IndexType::Hash => QueryIndexType::Hash,
                IndexType::BTree => QueryIndexType::BTree,
                IndexType::Gin => QueryIndexType::Gin,
            };
            indexes.push(
                IndexInfo::new(
                    idx.name(),
                    idx.columns().iter().map(|c| c.name.clone()).collect(),
                    idx.is_unique(),
                )
                .with_type(index_type),
            );
        }

        ctx.register_table(
            table_name,
            TableStats {
                row_count: store.len(),
                is_sorted: false,
                indexes,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::PreparedQuery;
    use alloc::collections::BTreeMap;
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, Row};
    use cynos_query::planner::PhysicalPlan;

    fn build_cache() -> TableCache {
        let mut cache = TableCache::new();
        let users = TableBuilder::new("users")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_users_name", &["name"], false)
            .unwrap()
            .build()
            .unwrap();

        cache.create_table(users).unwrap();
        cache
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                1,
                alloc::vec![Value::Int64(1), Value::String("Alice".into())],
            ))
            .unwrap();
        cache
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                2,
                alloc::vec![Value::Int64(2), Value::String("Bob".into())],
            ))
            .unwrap();
        cache
            .get_table_mut("users")
            .unwrap()
            .insert(Row::new(
                3,
                alloc::vec![Value::Int64(3), Value::String("Cara".into())],
            ))
            .unwrap();
        cache
    }

    fn bind_single_root_field(query: &str) -> (TableCache, GraphqlCatalog, BoundRootField) {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let prepared = PreparedQuery::parse(query).unwrap();
        let bound = prepared.bind(&catalog, None).unwrap();
        let field = bound.fields.into_iter().next().unwrap();
        (cache, catalog, field)
    }

    fn build_relation_filter_cache() -> TableCache {
        let mut cache = build_cache();
        let profiles = TableBuilder::new("profiles")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("user_id", DataType::Int64)
            .unwrap()
            .add_column("bio", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_index("idx_profiles_user_id", &["user_id"], true)
            .unwrap()
            .add_foreign_key_with_graphql_names(
                "fk_profiles_user",
                "user_id",
                "users",
                "id",
                Some("user"),
                Some("profile"),
            )
            .unwrap()
            .build()
            .unwrap();

        cache.create_table(profiles).unwrap();
        cache
            .get_table_mut("profiles")
            .unwrap()
            .insert(Row::new(
                10,
                alloc::vec![
                    Value::Int64(10),
                    Value::Int64(1),
                    Value::String("Alpha".into()),
                ],
            ))
            .unwrap();
        cache
            .get_table_mut("profiles")
            .unwrap()
            .insert(Row::new(
                11,
                alloc::vec![
                    Value::Int64(11),
                    Value::Int64(2),
                    Value::String("Builder".into()),
                ],
            ))
            .unwrap();
        cache
    }

    fn logical_plan_contains_join(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Join { .. } => true,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => logical_plan_contains_join(input),
            LogicalPlan::CrossProduct { left, right } | LogicalPlan::Union { left, right, .. } => {
                logical_plan_contains_join(left) || logical_plan_contains_join(right)
            }
            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. }
            | LogicalPlan::Empty => false,
        }
    }

    #[test]
    fn root_where_eq_lowers_to_index_get() {
        let (cache, catalog, field) =
            bind_single_root_field("{ users(where: { id: { eq: 2 } }) { id name } }");

        let plan = build_root_field_plan(&catalog, &field).unwrap();
        let logical = plan.logical_plan.clone();
        let ctx = build_execution_context_for_plan(&cache, &plan.table_name, &logical);
        let physical = QueryPlanner::new(ctx).plan(logical);

        assert!(matches!(physical, PhysicalPlan::IndexGet { .. }));
    }

    #[test]
    fn root_order_by_limit_lowers_to_reverse_index_scan() {
        let (cache, catalog, field) = bind_single_root_field(
            "{ users(orderBy: [{ field: ID, direction: DESC }], limit: 1) { id } }",
        );

        let plan = build_root_field_plan(&catalog, &field).unwrap();
        let logical = plan.logical_plan.clone();
        let ctx = build_execution_context_for_plan(&cache, &plan.table_name, &logical);
        let physical = QueryPlanner::new(ctx).plan(logical);

        match physical {
            PhysicalPlan::IndexScan { limit, reverse, .. } => {
                assert_eq!(limit, Some(1));
                assert!(reverse);
            }
            other => panic!("expected IndexScan, got {other:?}"),
        }
    }

    #[test]
    fn root_by_pk_lowers_to_index_get_and_executes() {
        let (cache, catalog, field) =
            bind_single_root_field("{ usersByPk(pk: { id: 2 }) { id name } }");

        let plan = build_root_field_plan(&catalog, &field).unwrap();
        let logical = plan.logical_plan.clone();
        let ctx = build_execution_context_for_plan(&cache, &plan.table_name, &logical);
        let physical = QueryPlanner::new(ctx).plan(logical);
        assert!(matches!(physical, PhysicalPlan::IndexGet { .. }));

        let rows = execute_logical_plan(&cache, &plan.table_name, plan.logical_plan).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Int64(2)));
    }

    #[test]
    fn unique_reverse_relation_filter_lowers_to_join_and_executes() {
        let cache = build_relation_filter_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let prepared = PreparedQuery::parse(
            "{ users(where: { profile: { bio: { eq: \"Builder\" } } }) { id name } }",
        )
        .unwrap();
        let bound = prepared.bind(&catalog, None).unwrap();
        let field = bound.fields.first().unwrap();

        let plan = build_root_field_plan(&catalog, field).unwrap();
        assert!(logical_plan_contains_join(&plan.logical_plan));

        let rows = execute_logical_plan(&cache, &plan.table_name, plan.logical_plan).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Int64(2)));
        assert_eq!(rows[0].get(1), Some(&Value::String("Bob".into())));
    }

    #[test]
    fn directive_pruning_keeps_remaining_root_field_on_planner_path() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let prepared = PreparedQuery::parse_with_operation(
            "query Lookup($skipUsers: Boolean!, $id: Long!) { users @skip(if: $skipUsers) { id } usersByPk(pk: { id: $id }) @include(if: true) { id name } }",
            Some("Lookup"),
        )
        .unwrap();

        let mut variables = BTreeMap::new();
        variables.insert("skipUsers".into(), crate::ast::InputValue::Boolean(true));
        variables.insert("id".into(), crate::ast::InputValue::Int(2));

        let bound = prepared.bind(&catalog, Some(&variables)).unwrap();
        assert_eq!(bound.fields.len(), 1);

        let field = &bound.fields[0];
        let plan = build_root_field_plan(&catalog, field).unwrap();
        let logical = plan.logical_plan.clone();
        let ctx = build_execution_context_for_plan(&cache, &plan.table_name, &logical);
        let physical = QueryPlanner::new(ctx).plan(logical);

        assert!(matches!(physical, PhysicalPlan::IndexGet { .. }));
    }
}
