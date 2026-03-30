use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::str::FromStr;

use cynos_core::{DataType, Value};
use cynos_jsonb::{JsonbBinary, JsonbObject, JsonbValue};
use hashbrown::HashSet;

use crate::ast::{
    Directive, Document, Field, InputValue, ObjectField, OperationDefinition, OperationType,
    SelectionSet,
};
use crate::catalog::{
    ColumnMeta, GraphqlCatalog, RelationMeta, RootFieldKind, TableFieldMeta, TableMeta,
};
use crate::error::{GqlError, GqlErrorKind, GqlResult};

#[derive(Clone, Debug)]
pub struct BoundOperation {
    pub kind: OperationType,
    pub fields: Vec<BoundRootField>,
}

#[derive(Clone, Debug)]
pub struct BoundRootField {
    pub response_key: String,
    pub kind: BoundRootFieldKind,
}

#[derive(Clone, Debug)]
pub enum BoundRootFieldKind {
    Typename,
    Collection {
        table_name: String,
        query: BoundCollectionQuery,
        selection: BoundSelectionSet,
    },
    ByPk {
        table_name: String,
        pk_values: Vec<Value>,
        selection: BoundSelectionSet,
    },
    Insert {
        table_name: String,
        rows: Vec<BoundInsertRow>,
        selection: BoundSelectionSet,
    },
    Update {
        table_name: String,
        query: BoundCollectionQuery,
        assignments: Vec<BoundColumnAssignment>,
        selection: BoundSelectionSet,
    },
    Delete {
        table_name: String,
        query: BoundCollectionQuery,
        selection: BoundSelectionSet,
    },
}

#[derive(Clone, Debug)]
pub struct BoundInsertRow {
    pub values: Vec<Value>,
}

#[derive(Clone, Debug)]
pub struct BoundColumnAssignment {
    pub column_index: usize,
    pub value: Value,
}

#[derive(Clone, Debug, Default)]
pub struct BoundCollectionQuery {
    pub filter: Option<BoundFilter>,
    pub order_by: Vec<OrderSpec>,
    pub limit: Option<usize>,
    pub offset: usize,
}

#[derive(Clone, Debug)]
pub struct OrderSpec {
    pub column_index: usize,
    pub descending: bool,
}

#[derive(Clone, Debug)]
pub struct BoundSelectionSet {
    pub fields: Vec<BoundField>,
}

#[derive(Clone, Debug)]
pub enum BoundField {
    Typename {
        response_key: String,
        value: String,
    },
    Column {
        response_key: String,
        column_index: usize,
    },
    ForwardRelation {
        response_key: String,
        relation: RelationMeta,
        selection: BoundSelectionSet,
    },
    ReverseRelation {
        response_key: String,
        relation: RelationMeta,
        query: BoundCollectionQuery,
        selection: BoundSelectionSet,
    },
}

#[derive(Clone, Debug)]
pub enum BoundFilter {
    And(Vec<BoundFilter>),
    Or(Vec<BoundFilter>),
    Column(ColumnPredicate),
    Relation(RelationPredicate),
}

#[derive(Clone, Debug)]
pub struct ColumnPredicate {
    pub column_index: usize,
    pub data_type: DataType,
    pub ops: Vec<PredicateOp>,
}

#[derive(Clone, Debug)]
pub struct RelationPredicate {
    pub relation: RelationMeta,
    pub target_table: String,
    pub filter: Box<BoundFilter>,
}

#[derive(Clone, Debug)]
pub enum PredicateOp {
    IsNull(bool),
    Eq(Value),
    Ne(Value),
    In(Vec<Value>),
    NotIn(Vec<Value>),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
    Between(Value, Value),
    Like(String),
    Json(JsonPredicate),
}

#[derive(Clone, Debug, Default)]
pub struct JsonPredicate {
    pub path: Option<String>,
    pub eq: Option<JsonbValue>,
    pub contains: Option<JsonbValue>,
    pub exists: Option<bool>,
}

pub type VariableValues = BTreeMap<String, InputValue>;

pub fn collect_dependency_tables(field: &BoundRootField) -> Vec<String> {
    let mut tables = hashbrown::HashSet::new();
    collect_root_field_dependency_tables(field, &mut tables);
    let mut tables: Vec<_> = tables.into_iter().collect();
    tables.sort();
    tables
}

pub fn is_delta_capable_root_field(field: &BoundRootField) -> bool {
    match &field.kind {
        BoundRootFieldKind::Collection {
            query, selection, ..
        } => {
            query.order_by.is_empty()
                && query.limit.is_none()
                && query.offset == 0
                && query
                    .filter
                    .as_ref()
                    .map_or(true, is_delta_capable_filter)
                && is_delta_capable_selection(selection)
        }
        BoundRootFieldKind::ByPk { selection, .. } => is_delta_capable_selection(selection),
        BoundRootFieldKind::Typename
        | BoundRootFieldKind::Insert { .. }
        | BoundRootFieldKind::Update { .. }
        | BoundRootFieldKind::Delete { .. } => false,
    }
}

fn is_delta_capable_selection(selection: &BoundSelectionSet) -> bool {
    selection.fields.iter().all(is_delta_capable_field)
}

fn is_delta_capable_field(field: &BoundField) -> bool {
    match field {
        BoundField::Typename { .. } | BoundField::Column { .. } => true,
        BoundField::ForwardRelation { selection, .. } => is_delta_capable_selection(selection),
        BoundField::ReverseRelation {
            relation,
            query, selection, ..
        } => reverse_relation_query_is_delta_capable(relation, query)
            && is_delta_capable_selection(selection),
    }
}

fn is_delta_capable_filter(filter: &BoundFilter) -> bool {
    match filter {
        BoundFilter::And(filters) | BoundFilter::Or(filters) => {
            filters.iter().all(is_delta_capable_filter)
        }
        BoundFilter::Column(_) => true,
        BoundFilter::Relation(predicate) => is_delta_capable_filter(predicate.filter.as_ref()),
    }
}

fn reverse_relation_query_is_delta_capable(
    relation: &RelationMeta,
    query: &BoundCollectionQuery,
) -> bool {
    query.order_by.is_empty()
        && query.offset == 0
        && query
            .filter
            .as_ref()
            .map_or(true, is_delta_capable_filter)
        && match query.limit {
            None => true,
            Some(1) => relation.child_column_unique,
            Some(_) => false,
        }
}

fn collect_root_field_dependency_tables(
    field: &BoundRootField,
    tables: &mut hashbrown::HashSet<String>,
) {
    match &field.kind {
        BoundRootFieldKind::Typename => {}
        BoundRootFieldKind::Collection {
            table_name,
            selection,
            ..
        }
        | BoundRootFieldKind::ByPk {
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
        } => {
            tables.insert(table_name.clone());
            if let BoundRootFieldKind::Collection { query, .. }
            | BoundRootFieldKind::Update { query, .. }
            | BoundRootFieldKind::Delete { query, .. } = &field.kind
            {
                if let Some(filter) = &query.filter {
                    collect_filter_dependency_tables(filter, tables);
                }
            }
            collect_selection_dependency_tables(selection, tables);
        }
    }
}

fn collect_selection_dependency_tables(
    selection: &BoundSelectionSet,
    tables: &mut hashbrown::HashSet<String>,
) {
    for field in &selection.fields {
        match field {
            BoundField::Typename { .. } | BoundField::Column { .. } => {}
            BoundField::ForwardRelation {
                relation,
                selection,
                ..
            } => {
                tables.insert(relation.parent_table.clone());
                collect_selection_dependency_tables(selection, tables);
            }
            BoundField::ReverseRelation {
                query,
                relation,
                selection,
                ..
            } => {
                tables.insert(relation.child_table.clone());
                if let Some(filter) = &query.filter {
                    collect_filter_dependency_tables(filter, tables);
                }
                collect_selection_dependency_tables(selection, tables);
            }
        }
    }
}

fn collect_filter_dependency_tables(
    filter: &BoundFilter,
    tables: &mut hashbrown::HashSet<String>,
) {
    match filter {
        BoundFilter::And(filters) | BoundFilter::Or(filters) => {
            for filter in filters {
                collect_filter_dependency_tables(filter, tables);
            }
        }
        BoundFilter::Column(_) => {}
        BoundFilter::Relation(predicate) => {
            tables.insert(predicate.target_table.clone());
            collect_filter_dependency_tables(predicate.filter.as_ref(), tables);
        }
    }
}

pub fn bind_document(
    document: &Document,
    catalog: &GraphqlCatalog,
    variables: Option<&VariableValues>,
    operation_name: Option<&str>,
) -> GqlResult<BoundOperation> {
    let operation = select_operation(document, operation_name)?;
    let resolved_variables = resolve_variables(operation, variables);
    bind_operation(operation, catalog, &resolved_variables)
}

fn select_operation<'a>(
    document: &'a Document,
    operation_name: Option<&str>,
) -> GqlResult<&'a OperationDefinition> {
    if let Some(name) = operation_name {
        return document
            .operations
            .iter()
            .find(|operation| operation.name.as_deref() == Some(name))
            .ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Validation,
                    format!("operation `{}` was not found", name),
                )
            });
    }

    if document.operations.len() == 1 {
        return document
            .operations
            .first()
            .ok_or_else(|| GqlError::new(GqlErrorKind::Validation, "query document is empty"));
    }

    Err(GqlError::new(
        GqlErrorKind::Validation,
        "multiple operations found; provide an operation name",
    ))
}

fn resolve_variables(
    operation: &OperationDefinition,
    provided: Option<&VariableValues>,
) -> VariableValues {
    let mut variables = provided.cloned().unwrap_or_default();
    for definition in &operation.variable_definitions {
        if !variables.contains_key(&definition.name) {
            if let Some(default_value) = &definition.default_value {
                variables.insert(definition.name.clone(), default_value.clone());
            }
        }
    }
    variables
}

fn bind_operation(
    operation: &OperationDefinition,
    catalog: &GraphqlCatalog,
    variables: &VariableValues,
) -> GqlResult<BoundOperation> {
    let mut fields = Vec::with_capacity(operation.selection_set.fields.len());
    for field in &operation.selection_set.fields {
        if !should_include_field(field, variables)? {
            continue;
        }

        if field.name == "__typename" {
            validate_leaf_field(field)?;
            fields.push(BoundRootField {
                response_key: field.response_key().to_string(),
                kind: BoundRootFieldKind::Typename,
            });
            continue;
        }

        let root_field = catalog
            .root_field(operation.kind, &field.name)
            .ok_or_else(|| {
                GqlError::new(
                    GqlErrorKind::Validation,
                    format!("unknown root field `{}`", field.name),
                )
            })?;
        let table = catalog.table(&root_field.table_name).ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Binding,
                format!("table `{}` is not available", root_field.table_name),
            )
        })?;
        let response_key = field.response_key().to_string();

        let kind = match root_field.kind {
            RootFieldKind::List => BoundRootFieldKind::Collection {
                table_name: root_field.table_name.clone(),
                query: bind_collection_arguments(field, table, catalog, variables)?,
                selection: bind_required_selection_set(field, table, catalog, variables)?,
            },
            RootFieldKind::ByPk => BoundRootFieldKind::ByPk {
                table_name: root_field.table_name.clone(),
                pk_values: bind_pk_arguments(field, table, variables)?,
                selection: bind_required_selection_set(field, table, catalog, variables)?,
            },
            RootFieldKind::Insert => BoundRootFieldKind::Insert {
                table_name: root_field.table_name.clone(),
                rows: bind_insert_rows(field, table, variables)?,
                selection: bind_required_selection_set(field, table, catalog, variables)?,
            },
            RootFieldKind::Update => {
                let arguments = materialize_argument_map(field, variables)?;
                BoundRootFieldKind::Update {
                    table_name: root_field.table_name.clone(),
                    query: bind_collection_arguments_from_map(
                        field,
                        table,
                        catalog,
                        &arguments,
                        &["set"],
                    )?,
                    assignments: bind_assignments_from_map(field, table, &arguments)?,
                    selection: bind_required_selection_set(field, table, catalog, variables)?,
                }
            }
            RootFieldKind::Delete => BoundRootFieldKind::Delete {
                table_name: root_field.table_name.clone(),
                query: bind_collection_arguments(field, table, catalog, variables)?,
                selection: bind_required_selection_set(field, table, catalog, variables)?,
            },
        };

        fields.push(BoundRootField { response_key, kind });
    }

    Ok(BoundOperation {
        kind: operation.kind,
        fields,
    })
}

fn bind_required_selection_set(
    field: &Field,
    table: &TableMeta,
    catalog: &GraphqlCatalog,
    variables: &VariableValues,
) -> GqlResult<BoundSelectionSet> {
    let selection_set = field.selection_set.as_ref().ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Validation,
            format!("field `{}` requires a selection set", field.name),
        )
    })?;
    bind_selection_set(selection_set, table, catalog, variables)
}

fn bind_selection_set(
    selection_set: &SelectionSet,
    table: &TableMeta,
    catalog: &GraphqlCatalog,
    variables: &VariableValues,
) -> GqlResult<BoundSelectionSet> {
    let mut fields = Vec::with_capacity(selection_set.fields.len());
    for field in &selection_set.fields {
        if !should_include_field(field, variables)? {
            continue;
        }

        if field.name == "__typename" {
            validate_leaf_field(field)?;
            fields.push(BoundField::Typename {
                response_key: field.response_key().to_string(),
                value: table.graphql_name.clone(),
            });
            continue;
        }

        let table_field = table.field(&field.name).ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Validation,
                format!(
                    "field `{}` does not exist on GraphQL type `{}`",
                    field.name, table.graphql_name
                ),
            )
        })?;

        match table_field {
            TableFieldMeta::Column(column) => {
                validate_leaf_field(field)?;
                fields.push(BoundField::Column {
                    response_key: field.response_key().to_string(),
                    column_index: column.index,
                });
            }
            TableFieldMeta::ForwardRelation(relation) => {
                if !field.arguments.is_empty() {
                    return Err(GqlError::new(
                        GqlErrorKind::Validation,
                        format!("relation field `{}` does not accept arguments", field.name),
                    ));
                }
                let target_table = catalog.table(&relation.parent_table).ok_or_else(|| {
                    GqlError::new(
                        GqlErrorKind::Binding,
                        format!("table `{}` is not available", relation.parent_table),
                    )
                })?;
                let nested = bind_required_selection_set(field, target_table, catalog, variables)?;
                fields.push(BoundField::ForwardRelation {
                    response_key: field.response_key().to_string(),
                    relation: relation.clone(),
                    selection: nested,
                });
            }
            TableFieldMeta::ReverseRelation(relation) => {
                let target_table = catalog.table(&relation.child_table).ok_or_else(|| {
                    GqlError::new(
                        GqlErrorKind::Binding,
                        format!("table `{}` is not available", relation.child_table),
                    )
                })?;
                let nested = bind_required_selection_set(field, target_table, catalog, variables)?;
                let query = bind_collection_arguments(field, target_table, catalog, variables)?;
                fields.push(BoundField::ReverseRelation {
                    response_key: field.response_key().to_string(),
                    relation: relation.clone(),
                    query,
                    selection: nested,
                });
            }
        }
    }
    Ok(BoundSelectionSet { fields })
}

fn bind_collection_arguments(
    field: &Field,
    table: &TableMeta,
    catalog: &GraphqlCatalog,
    variables: &VariableValues,
) -> GqlResult<BoundCollectionQuery> {
    let arguments = materialize_argument_map(field, variables)?;
    bind_collection_arguments_from_map(field, table, catalog, &arguments, &[])
}

fn bind_collection_arguments_from_map(
    field: &Field,
    table: &TableMeta,
    catalog: &GraphqlCatalog,
    arguments: &BTreeMap<String, InputValue>,
    extra_allowed: &[&str],
) -> GqlResult<BoundCollectionQuery> {
    let mut allowed = alloc::vec!["where", "orderBy", "limit", "offset"];
    allowed.extend_from_slice(extra_allowed);
    validate_allowed_arguments(field, arguments, &allowed)?;

    let filter = arguments
        .get("where")
        .map(|value| bind_where(value, table, catalog))
        .transpose()?
        .flatten();
    let order_by = arguments
        .get("orderBy")
        .map(|value| bind_order_by(value, table))
        .transpose()?
        .unwrap_or_default();
    let limit = arguments
        .get("limit")
        .map(coerce_optional_limit)
        .transpose()?
        .flatten();
    let offset = arguments
        .get("offset")
        .map(coerce_offset)
        .transpose()?
        .unwrap_or(0);

    Ok(BoundCollectionQuery {
        filter,
        order_by,
        limit,
        offset,
    })
}

fn bind_pk_arguments(
    field: &Field,
    table: &TableMeta,
    variables: &VariableValues,
) -> GqlResult<Vec<Value>> {
    let arguments = materialize_argument_map(field, variables)?;
    validate_allowed_arguments(field, &arguments, &["pk"])?;

    let pk_input = arguments.get("pk").ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Validation,
            format!("field `{}` requires a `pk` argument", field.name),
        )
    })?;

    let pk = table.primary_key().ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Binding,
            format!("table `{}` does not define a primary key", table.table_name),
        )
    })?;

    let fields = expect_object(pk_input, "pk")?;
    let mut values = Vec::with_capacity(pk.columns.len());
    for column in &pk.columns {
        let value = find_object_field(fields, &column.name).ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Validation,
                format!("missing primary-key field `{}`", column.name),
            )
        })?;
        values.push(coerce_column_value(value, column)?);
    }
    Ok(values)
}

fn bind_insert_rows(
    field: &Field,
    table: &TableMeta,
    variables: &VariableValues,
) -> GqlResult<Vec<BoundInsertRow>> {
    let arguments = materialize_argument_map(field, variables)?;
    validate_allowed_arguments(field, &arguments, &["input"])?;

    let input = arguments.get("input").ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Validation,
            format!("field `{}` requires an `input` argument", field.name),
        )
    })?;

    let entries = match input {
        InputValue::List(values) => values.as_slice(),
        other => core::slice::from_ref(other),
    };

    if entries.is_empty() {
        return Err(GqlError::new(
            GqlErrorKind::Validation,
            "`input` must contain at least one row",
        ));
    }

    let mut rows = Vec::with_capacity(entries.len());
    for entry in entries {
        rows.push(bind_insert_row(entry, table)?);
    }
    Ok(rows)
}

fn bind_insert_row(value: &InputValue, table: &TableMeta) -> GqlResult<BoundInsertRow> {
    let fields = expect_object(value, "input")?;
    let mut values = Vec::with_capacity(table.columns().len());

    for column in table.columns() {
        let column_value = match find_object_field(fields, &column.name) {
            Some(value) => coerce_column_value(value, column)?,
            None if column.nullable => Value::Null,
            None => {
                return Err(GqlError::new(
                    GqlErrorKind::Validation,
                    format!("missing required insert field `{}`", column.name),
                ))
            }
        };
        values.push(column_value);
    }

    validate_no_unknown_input_fields(fields, table, "input")?;

    Ok(BoundInsertRow { values })
}

fn bind_assignments_from_map(
    field: &Field,
    table: &TableMeta,
    arguments: &BTreeMap<String, InputValue>,
) -> GqlResult<Vec<BoundColumnAssignment>> {
    validate_allowed_arguments(
        field,
        arguments,
        &["set", "where", "orderBy", "limit", "offset"],
    )?;

    let set_value = arguments.get("set").ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Validation,
            format!("field `{}` requires a `set` argument", field.name),
        )
    })?;

    let fields = expect_object(set_value, "set")?;
    if fields.is_empty() {
        return Err(GqlError::new(
            GqlErrorKind::Validation,
            "`set` cannot be empty",
        ));
    }

    let mut assignments = Vec::with_capacity(fields.len());
    let mut seen = HashSet::new();
    for field in fields {
        if !seen.insert(field.name.as_str()) {
            return Err(GqlError::new(
                GqlErrorKind::Validation,
                format!("duplicate set field `{}`", field.name),
            ));
        }

        let column = table.column(&field.name).ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Validation,
                format!(
                    "field `{}` is not an updatable column on `{}`",
                    field.name, table.graphql_name
                ),
            )
        })?;

        assignments.push(BoundColumnAssignment {
            column_index: column.index,
            value: coerce_column_value(&field.value, column)?,
        });
    }

    Ok(assignments)
}

fn materialize_argument_map(
    field: &Field,
    variables: &VariableValues,
) -> GqlResult<BTreeMap<String, InputValue>> {
    materialize_argument_map_with_context(&field.arguments, "field", &field.name, variables)
}

fn materialize_argument_map_with_context(
    source_arguments: &[crate::ast::Argument],
    owner_kind: &str,
    owner_name: &str,
    variables: &VariableValues,
) -> GqlResult<BTreeMap<String, InputValue>> {
    let mut arguments = BTreeMap::new();
    for argument in source_arguments.iter() {
        if arguments.contains_key(&argument.name) {
            return Err(GqlError::new(
                GqlErrorKind::Validation,
                format!(
                    "duplicate argument `{}` on {} `{}`",
                    argument.name, owner_kind, owner_name
                ),
            ));
        }
        arguments.insert(
            argument.name.clone(),
            materialize_input_value(&argument.value, variables)?,
        );
    }
    Ok(arguments)
}

fn should_include_field(field: &Field, variables: &VariableValues) -> GqlResult<bool> {
    let mut include = true;
    let mut seen = HashSet::new();

    for directive in &field.directives {
        if !seen.insert(directive.name.as_str()) {
            return Err(GqlError::new(
                GqlErrorKind::Validation,
                format!(
                    "directive `@{}` can only appear once on field `{}`",
                    directive.name, field.name
                ),
            ));
        }

        match directive.name.as_str() {
            "include" => {
                include &= directive_if_argument(directive, field, variables)?;
            }
            "skip" => {
                if directive_if_argument(directive, field, variables)? {
                    return Ok(false);
                }
            }
            other => {
                return Err(GqlError::new(
                    GqlErrorKind::Unsupported,
                    format!("directive `@{}` is not supported", other),
                ))
            }
        }
    }

    Ok(include)
}

fn directive_if_argument(
    directive: &Directive,
    field: &Field,
    variables: &VariableValues,
) -> GqlResult<bool> {
    let arguments = materialize_argument_map_with_context(
        &directive.arguments,
        "directive",
        &directive.name,
        variables,
    )?;
    validate_allowed_directive_arguments(field, directive, &arguments, &["if"])?;
    let if_value = arguments.get("if").ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Validation,
            format!(
                "directive `@{}` on field `{}` requires an `if` argument",
                directive.name, field.name
            ),
        )
    })?;
    coerce_boolean(if_value)
}

fn materialize_input_value(
    value: &InputValue,
    variables: &VariableValues,
) -> GqlResult<InputValue> {
    match value {
        InputValue::Variable(name) => variables.get(name).cloned().ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Validation,
                format!("variable `${}` was not provided", name),
            )
        }),
        InputValue::List(values) => {
            let mut materialized = Vec::with_capacity(values.len());
            for value in values {
                materialized.push(materialize_input_value(value, variables)?);
            }
            Ok(InputValue::List(materialized))
        }
        InputValue::Object(fields) => {
            let mut materialized = Vec::with_capacity(fields.len());
            for field in fields {
                materialized.push(ObjectField {
                    name: field.name.clone(),
                    value: materialize_input_value(&field.value, variables)?,
                });
            }
            Ok(InputValue::Object(materialized))
        }
        other => Ok(other.clone()),
    }
}

fn bind_where(
    value: &InputValue,
    table: &TableMeta,
    catalog: &GraphqlCatalog,
) -> GqlResult<Option<BoundFilter>> {
    let fields = expect_object(value, "where")?;
    let mut predicates = Vec::new();

    for field in fields {
        match field.name.as_str() {
            "AND" => predicates.push(bind_logical_filter(&field.value, table, catalog, true)?),
            "OR" => predicates.push(bind_logical_filter(&field.value, table, catalog, false)?),
            field_name => {
                let table_field = table.field(field_name).ok_or_else(|| {
                    GqlError::new(
                        GqlErrorKind::Validation,
                        format!(
                            "unknown filter field `{}` on `{}`",
                            field_name, table.graphql_name
                        ),
                    )
                })?;
                match table_field {
                    TableFieldMeta::Column(column) => {
                        predicates.push(BoundFilter::Column(bind_column_predicate(
                            column,
                            &field.value,
                        )?));
                    }
                    TableFieldMeta::ForwardRelation(relation) => {
                        let target_table = catalog.table(&relation.parent_table).ok_or_else(|| {
                            GqlError::new(
                                GqlErrorKind::Binding,
                                format!("table `{}` is not available", relation.parent_table),
                            )
                        })?;
                        predicates.push(bind_single_relation_predicate(
                            relation,
                            target_table,
                            &field.value,
                            catalog,
                            field_name,
                        )?);
                    }
                    TableFieldMeta::ReverseRelation(relation) => {
                        if !relation.child_column_unique {
                            return Err(GqlError::new(
                                GqlErrorKind::Unsupported,
                                format!(
                                    "multi-row reverse relation filter `{}` on `{}` is not supported yet",
                                    field_name, table.graphql_name
                                ),
                            ));
                        }
                        let target_table = catalog.table(&relation.child_table).ok_or_else(|| {
                            GqlError::new(
                                GqlErrorKind::Binding,
                                format!("table `{}` is not available", relation.child_table),
                            )
                        })?;
                        predicates.push(bind_single_relation_predicate(
                            relation,
                            target_table,
                            &field.value,
                            catalog,
                            field_name,
                        )?);
                    }
                }
            }
        }
    }

    match predicates.len() {
        0 => Ok(None),
        1 => Ok(predicates.pop()),
        _ => Ok(Some(BoundFilter::And(predicates))),
    }
}

fn bind_single_relation_predicate(
    relation: &RelationMeta,
    target_table: &TableMeta,
    value: &InputValue,
    catalog: &GraphqlCatalog,
    field_name: &str,
) -> GqlResult<BoundFilter> {
    let filter = bind_where(value, target_table, catalog)?.unwrap_or(BoundFilter::And(Vec::new()));
    let _ = field_name;
    Ok(BoundFilter::Relation(RelationPredicate {
        relation: relation.clone(),
        target_table: target_table.table_name.clone(),
        filter: Box::new(filter),
    }))
}

fn bind_logical_filter(
    value: &InputValue,
    table: &TableMeta,
    catalog: &GraphqlCatalog,
    and: bool,
) -> GqlResult<BoundFilter> {
    let values = expect_list(value, if and { "AND" } else { "OR" })?;
    let mut predicates = Vec::with_capacity(values.len());
    for value in values {
        let predicate = bind_where(value, table, catalog)?.ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Validation,
                if and {
                    "AND entries cannot be empty"
                } else {
                    "OR entries cannot be empty"
                },
            )
        })?;
        predicates.push(predicate);
    }
    if and {
        Ok(BoundFilter::And(predicates))
    } else {
        Ok(BoundFilter::Or(predicates))
    }
}

fn bind_column_predicate(column: &ColumnMeta, value: &InputValue) -> GqlResult<ColumnPredicate> {
    let mut predicate = ColumnPredicate {
        column_index: column.index,
        data_type: column.data_type,
        ops: Vec::new(),
    };

    match value {
        InputValue::Object(fields) => bind_column_predicate_object(&mut predicate, fields)?,
        other => predicate
            .ops
            .push(PredicateOp::Eq(coerce_value(other, column.data_type)?)),
    }

    if predicate.ops.is_empty() {
        return Err(GqlError::new(
            GqlErrorKind::Validation,
            format!("filter for column `{}` is empty", column.name),
        ));
    }

    Ok(predicate)
}

fn bind_column_predicate_object(
    predicate: &mut ColumnPredicate,
    fields: &[ObjectField],
) -> GqlResult<()> {
    let mut json_predicate = JsonPredicate::default();
    let mut saw_json_specific = false;

    for field in fields {
        match field.name.as_str() {
            "isNull" => predicate
                .ops
                .push(PredicateOp::IsNull(coerce_boolean(&field.value)?)),
            "eq" if predicate.data_type == DataType::Jsonb => {
                saw_json_specific = true;
                json_predicate.eq = Some(input_to_jsonb_value(&field.value)?);
            }
            "contains" => {
                if predicate.data_type != DataType::Jsonb {
                    return Err(GqlError::new(
                        GqlErrorKind::Validation,
                        "`contains` is only supported on JSON columns",
                    ));
                }
                saw_json_specific = true;
                json_predicate.contains = Some(input_to_jsonb_value(&field.value)?);
            }
            "exists" => {
                if predicate.data_type != DataType::Jsonb {
                    return Err(GqlError::new(
                        GqlErrorKind::Validation,
                        "`exists` is only supported on JSON columns",
                    ));
                }
                saw_json_specific = true;
                json_predicate.exists = Some(coerce_boolean(&field.value)?);
            }
            "path" => {
                if predicate.data_type != DataType::Jsonb {
                    return Err(GqlError::new(
                        GqlErrorKind::Validation,
                        "`path` is only supported on JSON columns",
                    ));
                }
                saw_json_specific = true;
                json_predicate.path = Some(coerce_string(&field.value)?);
            }
            "eq" => predicate.ops.push(PredicateOp::Eq(coerce_value(
                &field.value,
                predicate.data_type,
            )?)),
            "ne" => predicate.ops.push(PredicateOp::Ne(coerce_value(
                &field.value,
                predicate.data_type,
            )?)),
            "in" => predicate.ops.push(PredicateOp::In(coerce_value_list(
                &field.value,
                predicate.data_type,
            )?)),
            "notIn" => predicate.ops.push(PredicateOp::NotIn(coerce_value_list(
                &field.value,
                predicate.data_type,
            )?)),
            "gt" => predicate.ops.push(PredicateOp::Gt(coerce_value(
                &field.value,
                predicate.data_type,
            )?)),
            "gte" => predicate.ops.push(PredicateOp::Gte(coerce_value(
                &field.value,
                predicate.data_type,
            )?)),
            "lt" => predicate.ops.push(PredicateOp::Lt(coerce_value(
                &field.value,
                predicate.data_type,
            )?)),
            "lte" => predicate.ops.push(PredicateOp::Lte(coerce_value(
                &field.value,
                predicate.data_type,
            )?)),
            "between" => {
                let values = coerce_value_list(&field.value, predicate.data_type)?;
                if values.len() != 2 {
                    return Err(GqlError::new(
                        GqlErrorKind::Validation,
                        "`between` requires exactly two values",
                    ));
                }
                predicate
                    .ops
                    .push(PredicateOp::Between(values[0].clone(), values[1].clone()));
            }
            "like" => {
                if predicate.data_type != DataType::String && predicate.data_type != DataType::Bytes
                {
                    return Err(GqlError::new(
                        GqlErrorKind::Validation,
                        "`like` is only supported on String and Bytes columns",
                    ));
                }
                predicate
                    .ops
                    .push(PredicateOp::Like(coerce_string(&field.value)?));
            }
            other => {
                return Err(GqlError::new(
                    GqlErrorKind::Validation,
                    format!("unsupported filter operator `{}`", other),
                ))
            }
        }
    }

    if saw_json_specific {
        predicate.ops.push(PredicateOp::Json(json_predicate));
    }

    Ok(())
}

fn bind_order_by(value: &InputValue, table: &TableMeta) -> GqlResult<Vec<OrderSpec>> {
    let entries = match value {
        InputValue::List(values) => values.as_slice(),
        other => core::slice::from_ref(other),
    };
    let mut specs = Vec::with_capacity(entries.len());
    for entry in entries {
        let fields = expect_object(entry, "orderBy")?;
        let field_name = find_object_field(fields, "field").ok_or_else(|| {
            GqlError::new(GqlErrorKind::Validation, "orderBy entry requires a `field`")
        })?;
        let direction = find_object_field(fields, "direction").ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Validation,
                "orderBy entry requires a `direction`",
            )
        })?;
        let column = resolve_order_column(table, field_name)?;
        let descending = match coerce_string_or_enum(direction)?.as_str() {
            "ASC" => false,
            "DESC" => true,
            other => {
                return Err(GqlError::new(
                    GqlErrorKind::Validation,
                    format!("unsupported sort direction `{}`", other),
                ))
            }
        };
        specs.push(OrderSpec {
            column_index: column.index,
            descending,
        });
    }
    Ok(specs)
}

fn resolve_order_column<'a>(table: &'a TableMeta, value: &InputValue) -> GqlResult<&'a ColumnMeta> {
    let raw = coerce_string_or_enum(value)?;
    if let Some(column) = table.column(&raw) {
        return Ok(column);
    }
    table
        .columns()
        .iter()
        .find(|column| to_order_enum_value(&column.name) == raw)
        .ok_or_else(|| {
            GqlError::new(
                GqlErrorKind::Validation,
                format!("unknown order field `{}` on `{}`", raw, table.graphql_name),
            )
        })
}

fn validate_leaf_field(field: &Field) -> GqlResult<()> {
    if !field.arguments.is_empty() {
        return Err(GqlError::new(
            GqlErrorKind::Validation,
            format!("field `{}` does not accept arguments", field.name),
        ));
    }
    if field.selection_set.is_some() {
        return Err(GqlError::new(
            GqlErrorKind::Validation,
            format!("field `{}` cannot have a selection set", field.name),
        ));
    }
    Ok(())
}

fn validate_allowed_arguments(
    field: &Field,
    arguments: &BTreeMap<String, InputValue>,
    allowed: &[&str],
) -> GqlResult<()> {
    for name in arguments.keys() {
        if !allowed
            .iter()
            .any(|allowed_name| *allowed_name == name.as_str())
        {
            return Err(GqlError::new(
                GqlErrorKind::Validation,
                format!("field `{}` does not accept argument `{}`", field.name, name),
            ));
        }
    }
    Ok(())
}

fn validate_allowed_directive_arguments(
    field: &Field,
    directive: &Directive,
    arguments: &BTreeMap<String, InputValue>,
    allowed: &[&str],
) -> GqlResult<()> {
    for name in arguments.keys() {
        if !allowed
            .iter()
            .any(|allowed_name| *allowed_name == name.as_str())
        {
            return Err(GqlError::new(
                GqlErrorKind::Validation,
                format!(
                    "directive `@{}` on field `{}` does not accept argument `{}`",
                    directive.name, field.name, name
                ),
            ));
        }
    }
    Ok(())
}

fn validate_no_unknown_input_fields(
    fields: &[ObjectField],
    table: &TableMeta,
    input_name: &str,
) -> GqlResult<()> {
    let mut seen = HashSet::new();
    for field in fields {
        if !seen.insert(field.name.as_str()) {
            return Err(GqlError::new(
                GqlErrorKind::Validation,
                format!("duplicate field `{}` in `{}`", field.name, input_name),
            ));
        }
        if table.column(&field.name).is_none() {
            return Err(GqlError::new(
                GqlErrorKind::Validation,
                format!(
                    "field `{}` is not a column on `{}`",
                    field.name, table.graphql_name
                ),
            ));
        }
    }
    Ok(())
}

fn coerce_optional_limit(value: &InputValue) -> GqlResult<Option<usize>> {
    if matches!(value, InputValue::Null) {
        return Ok(None);
    }
    Ok(Some(coerce_non_negative_usize(value, "limit")?))
}

fn coerce_offset(value: &InputValue) -> GqlResult<usize> {
    coerce_non_negative_usize(value, "offset")
}

fn coerce_non_negative_usize(value: &InputValue, name: &str) -> GqlResult<usize> {
    match value {
        InputValue::Int(value) if *value >= 0 => Ok(*value as usize),
        _ => Err(GqlError::new(
            GqlErrorKind::Validation,
            format!("`{}` must be a non-negative integer", name),
        )),
    }
}

fn coerce_boolean(value: &InputValue) -> GqlResult<bool> {
    match value {
        InputValue::Boolean(value) => Ok(*value),
        _ => Err(GqlError::new(
            GqlErrorKind::Validation,
            "expected a boolean value",
        )),
    }
}

fn coerce_string(value: &InputValue) -> GqlResult<String> {
    match value {
        InputValue::String(value) | InputValue::Enum(value) => Ok(value.clone()),
        _ => Err(GqlError::new(
            GqlErrorKind::Validation,
            "expected a string value",
        )),
    }
}

fn coerce_string_or_enum(value: &InputValue) -> GqlResult<String> {
    coerce_string(value)
}

fn coerce_value_list(value: &InputValue, data_type: DataType) -> GqlResult<Vec<Value>> {
    let values = expect_list(value, "list")?;
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(coerce_value(value, data_type)?);
    }
    Ok(out)
}

fn coerce_column_value(value: &InputValue, column: &ColumnMeta) -> GqlResult<Value> {
    let value = coerce_value(value, column.data_type)?;
    if value.is_null() && !column.nullable {
        return Err(GqlError::new(
            GqlErrorKind::Validation,
            format!("column `{}` does not allow null values", column.name),
        ));
    }
    Ok(value)
}

fn coerce_value(value: &InputValue, data_type: DataType) -> GqlResult<Value> {
    match data_type {
        DataType::Boolean => match value {
            InputValue::Null => Ok(Value::Null),
            InputValue::Boolean(value) => Ok(Value::Boolean(*value)),
            _ => type_error(data_type),
        },
        DataType::Int32 => match value {
            InputValue::Null => Ok(Value::Null),
            InputValue::Int(value) => Ok(Value::Int32(*value as i32)),
            InputValue::Float(value) => Ok(Value::Int32(value.as_f64() as i32)),
            _ => type_error(data_type),
        },
        DataType::Int64 => match value {
            InputValue::Null => Ok(Value::Null),
            InputValue::Int(value) => Ok(Value::Int64(*value)),
            InputValue::Float(value) => Ok(Value::Int64(value.as_f64() as i64)),
            _ => type_error(data_type),
        },
        DataType::Float64 => match value {
            InputValue::Null => Ok(Value::Null),
            InputValue::Int(value) => Ok(Value::Float64(*value as f64)),
            InputValue::Float(value) => Ok(Value::Float64(value.as_f64())),
            _ => type_error(data_type),
        },
        DataType::String => match value {
            InputValue::Null => Ok(Value::Null),
            InputValue::String(value) | InputValue::Enum(value) => Ok(Value::String(value.clone())),
            _ => type_error(data_type),
        },
        DataType::DateTime => match value {
            InputValue::Null => Ok(Value::Null),
            InputValue::Int(value) => Ok(Value::DateTime(*value)),
            InputValue::Float(value) => Ok(Value::DateTime(value.as_f64() as i64)),
            InputValue::String(value) => {
                Ok(Value::DateTime(i64::from_str(value).map_err(|_| {
                    GqlError::new(GqlErrorKind::Validation, "invalid DateTime value")
                })?))
            }
            _ => type_error(data_type),
        },
        DataType::Bytes => match value {
            InputValue::Null => Ok(Value::Null),
            InputValue::String(value) => Ok(Value::Bytes(value.as_bytes().to_vec())),
            InputValue::List(values) => {
                let mut bytes = Vec::with_capacity(values.len());
                for value in values {
                    match value {
                        InputValue::Int(value) if *value >= 0 && *value <= 255 => {
                            bytes.push(*value as u8)
                        }
                        _ => {
                            return Err(GqlError::new(
                                GqlErrorKind::Validation,
                                "Bytes values must be strings or integer arrays",
                            ))
                        }
                    }
                }
                Ok(Value::Bytes(bytes))
            }
            _ => type_error(data_type),
        },
        DataType::Jsonb => {
            if matches!(value, InputValue::Null) {
                return Ok(Value::Null);
            }
            let json = input_to_jsonb_value(value)?;
            Ok(Value::Jsonb(cynos_core::JsonbValue::new(
                JsonbBinary::encode(&json).into_bytes(),
            )))
        }
    }
}

fn input_to_jsonb_value(value: &InputValue) -> GqlResult<JsonbValue> {
    match value {
        InputValue::Null => Ok(JsonbValue::Null),
        InputValue::Boolean(value) => Ok(JsonbValue::Bool(*value)),
        InputValue::Int(value) => Ok(JsonbValue::Number(*value as f64)),
        InputValue::Float(value) => Ok(JsonbValue::Number(value.as_f64())),
        InputValue::String(value) | InputValue::Enum(value) => {
            Ok(JsonbValue::String(value.clone()))
        }
        InputValue::List(values) => {
            let mut items = Vec::with_capacity(values.len());
            for value in values {
                items.push(input_to_jsonb_value(value)?);
            }
            Ok(JsonbValue::Array(items))
        }
        InputValue::Object(fields) => {
            let mut object = JsonbObject::new();
            for field in fields {
                object.insert(field.name.clone(), input_to_jsonb_value(&field.value)?);
            }
            Ok(JsonbValue::Object(object))
        }
        InputValue::Variable(_) => Err(GqlError::new(
            GqlErrorKind::Validation,
            "unresolved variable in JSON value",
        )),
    }
}

fn expect_object<'a>(value: &'a InputValue, name: &str) -> GqlResult<&'a [ObjectField]> {
    value.as_object().ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Validation,
            format!("`{}` must be an input object", name),
        )
    })
}

fn expect_list<'a>(value: &'a InputValue, name: &str) -> GqlResult<&'a [InputValue]> {
    value.as_list().ok_or_else(|| {
        GqlError::new(
            GqlErrorKind::Validation,
            format!("`{}` must be a list", name),
        )
    })
}

fn find_object_field<'a>(fields: &'a [ObjectField], name: &str) -> Option<&'a InputValue> {
    fields
        .iter()
        .find(|field| field.name == name)
        .map(|field| &field.value)
}

fn to_order_enum_value(name: &str) -> String {
    let mut out = String::new();
    let mut previous_was_underscore = false;
    for ch in name.chars() {
        if ch == '_' {
            if !previous_was_underscore {
                out.push('_');
                previous_was_underscore = true;
            }
            continue;
        }
        previous_was_underscore = false;
        out.extend(ch.to_uppercase());
    }
    out
}

fn type_error<T>(data_type: DataType) -> GqlResult<T> {
    Err(GqlError::new(
        GqlErrorKind::Validation,
        format!("value is not compatible with {:?}", data_type),
    ))
}
