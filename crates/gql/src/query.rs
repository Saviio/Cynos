use alloc::borrow::ToOwned;
use alloc::string::String;

use crate::ast::Document;
use crate::bind::{bind_document, BoundOperation, VariableValues};
use crate::catalog::GraphqlCatalog;
use crate::error::GqlResult;
use crate::execute::{execute_bound_operation, execute_bound_operation_mut, OperationOutcome};
use crate::parser::parse_document;
use crate::response::GraphqlResponse;
use cynos_storage::TableCache;

#[derive(Clone, Debug)]
pub struct PreparedQuery {
    document: Document,
    operation_name: Option<String>,
}

impl PreparedQuery {
    pub fn parse(query: &str) -> GqlResult<Self> {
        Self::parse_with_operation(query, None)
    }

    pub fn parse_with_operation(query: &str, operation_name: Option<&str>) -> GqlResult<Self> {
        Ok(Self {
            document: parse_document(query)?,
            operation_name: operation_name.map(ToOwned::to_owned),
        })
    }

    pub fn execute(
        &self,
        cache: &TableCache,
        catalog: &GraphqlCatalog,
        variables: Option<&VariableValues>,
    ) -> GqlResult<GraphqlResponse> {
        let bound = self.bind(catalog, variables)?;
        execute_bound_operation(cache, catalog, &bound)
    }

    pub fn execute_mut(
        &self,
        cache: &mut TableCache,
        catalog: &GraphqlCatalog,
        variables: Option<&VariableValues>,
    ) -> GqlResult<OperationOutcome> {
        let bound = self.bind(catalog, variables)?;
        execute_bound_operation_mut(cache, catalog, &bound)
    }

    pub fn bind(
        &self,
        catalog: &GraphqlCatalog,
        variables: Option<&VariableValues>,
    ) -> GqlResult<BoundOperation> {
        bind_document(
            &self.document,
            catalog,
            variables,
            self.operation_name.as_deref(),
        )
    }
}

pub fn execute_query(
    cache: &TableCache,
    catalog: &GraphqlCatalog,
    query: &str,
    variables: Option<&VariableValues>,
    operation_name: Option<&str>,
) -> GqlResult<GraphqlResponse> {
    PreparedQuery::parse_with_operation(query, operation_name)?.execute(cache, catalog, variables)
}

pub fn execute_operation(
    cache: &mut TableCache,
    catalog: &GraphqlCatalog,
    query: &str,
    variables: Option<&VariableValues>,
    operation_name: Option<&str>,
) -> GqlResult<OperationOutcome> {
    PreparedQuery::parse_with_operation(query, operation_name)?
        .execute_mut(cache, catalog, variables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::collections::BTreeMap;
    use cynos_core::schema::TableBuilder;
    use cynos_core::{DataType, Row, Value};
    use cynos_storage::TableCache;

    use crate::ast::InputValue;
    use crate::response::{ResponseField, ResponseValue};

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
            .build()
            .unwrap();
        let orders = TableBuilder::new("orders")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("user_id", DataType::Int64)
            .unwrap()
            .add_column("total", DataType::Float64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .add_foreign_key_with_graphql_names(
                "fk_orders_user",
                "user_id",
                "users",
                "id",
                Some("buyer"),
                Some("orders"),
            )
            .unwrap()
            .build()
            .unwrap();

        cache.create_table(users).unwrap();
        cache.create_table(orders).unwrap();

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
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(
                10,
                alloc::vec![Value::Int64(10), Value::Int64(1), Value::Float64(50.0)],
            ))
            .unwrap();
        cache
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(
                11,
                alloc::vec![Value::Int64(11), Value::Int64(1), Value::Float64(120.0)],
            ))
            .unwrap();
        cache
            .get_table_mut("orders")
            .unwrap()
            .insert(Row::new(
                12,
                alloc::vec![Value::Int64(12), Value::Int64(2), Value::Float64(80.0)],
            ))
            .unwrap();

        cache
    }

    fn object_fields(value: &ResponseValue) -> &[ResponseField] {
        match value {
            ResponseValue::Object(fields) => fields.as_ref(),
            other => panic!("expected object, got {other:?}"),
        }
    }

    fn list_items(value: &ResponseValue) -> &[ResponseValue] {
        match value {
            ResponseValue::List(items) => items.as_ref(),
            other => panic!("expected list, got {other:?}"),
        }
    }

    fn field<'a>(fields: &'a [ResponseField], name: &str) -> &'a ResponseValue {
        fields
            .iter()
            .find(|field| field.name.as_ref() == name)
            .map(|field| &field.value)
            .unwrap_or_else(|| panic!("missing field `{name}`"))
    }

    fn has_field(fields: &[ResponseField], name: &str) -> bool {
        fields.iter().any(|field| field.name.as_ref() == name)
    }

    fn int64(value: &ResponseValue) -> i64 {
        match value {
            ResponseValue::Scalar(Value::Int64(value)) => *value,
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    fn float64(value: &ResponseValue) -> f64 {
        match value {
            ResponseValue::Scalar(Value::Float64(value)) => *value,
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    fn string(value: &ResponseValue) -> &str {
        match value {
            ResponseValue::Scalar(Value::String(value)) => value,
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn execute_nested_query_with_filters_and_relations() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let response = execute_query(
            &cache,
            &catalog,
            "{ orders(where: { total: { gte: 80 } }, orderBy: [{ field: TOTAL, direction: DESC }]) { id total buyer { id name } } }",
            None,
            None,
        )
        .unwrap();

        let root = object_fields(&response.data);
        let orders = list_items(field(root, "orders"));
        assert_eq!(orders.len(), 2);

        let first = object_fields(&orders[0]);
        assert_eq!(int64(field(first, "id")), 11);
        assert_eq!(float64(field(first, "total")), 120.0);
        let buyer = object_fields(field(first, "buyer"));
        assert_eq!(int64(field(buyer, "id")), 1);
        assert_eq!(string(field(buyer, "name")), "Alice");

        let second = object_fields(&orders[1]);
        assert_eq!(int64(field(second, "id")), 12);
        assert_eq!(
            string(field(object_fields(field(second, "buyer")), "name")),
            "Bob"
        );
    }

    #[test]
    fn prepared_query_supports_variables_and_typenames() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query = PreparedQuery::parse_with_operation(
            "query UserOrders($userId: Long!, $min: Float = 0) { __typename usersByPk(pk: { id: $userId }) { __typename name orders(where: { total: { gte: $min } }, orderBy: [{ field: TOTAL, direction: DESC }], limit: 1) { id total } } }",
            Some("UserOrders"),
        )
        .unwrap();

        let mut variables = BTreeMap::new();
        variables.insert("userId".into(), InputValue::Int(1));
        variables.insert("min".into(), InputValue::Int(60));

        let response = query.execute(&cache, &catalog, Some(&variables)).unwrap();
        let root = object_fields(&response.data);
        assert_eq!(string(field(root, "__typename")), "Query");

        let user = object_fields(field(root, "usersByPk"));
        assert_eq!(string(field(user, "__typename")), "Users");
        assert_eq!(string(field(user, "name")), "Alice");

        let orders = list_items(field(user, "orders"));
        assert_eq!(orders.len(), 1);
        let top_order = object_fields(&orders[0]);
        assert_eq!(int64(field(top_order, "id")), 11);
        assert_eq!(float64(field(top_order, "total")), 120.0);
    }

    #[test]
    fn execute_operation_supports_mutation_lifecycle() {
        let mut cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);

        let inserted = execute_operation(
            &mut cache,
            &catalog,
            "mutation { insertUsers(input: [{ id: 3, name: \"Cara\" }]) { id name } }",
            None,
            None,
        )
        .unwrap();
        let inserted_root = object_fields(&inserted.response.data);
        let inserted_users = list_items(field(inserted_root, "insertUsers"));
        assert_eq!(inserted_users.len(), 1);
        assert_eq!(
            string(field(object_fields(&inserted_users[0]), "name")),
            "Cara"
        );
        assert_eq!(inserted.changes.len(), 1);

        let updated = execute_operation(
            &mut cache,
            &catalog,
            "mutation { updateUsers(where: { id: { eq: 3 } }, set: { name: \"Caroline\" }) { id name } }",
            None,
            None,
        )
        .unwrap();
        let updated_root = object_fields(&updated.response.data);
        let updated_users = list_items(field(updated_root, "updateUsers"));
        assert_eq!(updated_users.len(), 1);
        assert_eq!(
            string(field(object_fields(&updated_users[0]), "name")),
            "Caroline"
        );

        let deleted = execute_operation(
            &mut cache,
            &catalog,
            "mutation { deleteUsers(where: { id: { eq: 3 } }) { id name } }",
            None,
            None,
        )
        .unwrap();
        let deleted_root = object_fields(&deleted.response.data);
        let deleted_users = list_items(field(deleted_root, "deleteUsers"));
        assert_eq!(deleted_users.len(), 1);
        assert_eq!(
            string(field(object_fields(&deleted_users[0]), "name")),
            "Caroline"
        );

        let after = execute_query(
            &cache,
            &catalog,
            "{ users(orderBy: [{ field: ID, direction: ASC }]) { id } }",
            None,
            None,
        )
        .unwrap();
        let after_root = object_fields(&after.data);
        let users = list_items(field(after_root, "users"));
        assert_eq!(users.len(), 2);
        assert_eq!(int64(field(object_fields(&users[0]), "id")), 1);
        assert_eq!(int64(field(object_fields(&users[1]), "id")), 2);
    }

    #[test]
    fn directives_prune_root_and_nested_fields_after_variable_resolution() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let query = PreparedQuery::parse_with_operation(
            "query Feed($showUsers: Boolean!, $showName: Boolean!, $showOrders: Boolean!) { users @include(if: $showUsers) { id name @include(if: $showName) orders @skip(if: $showOrders) { id total } } orders @skip(if: $showUsers) { id } }",
            Some("Feed"),
        )
        .unwrap();

        let mut variables = BTreeMap::new();
        variables.insert("showUsers".into(), InputValue::Boolean(true));
        variables.insert("showName".into(), InputValue::Boolean(false));
        variables.insert("showOrders".into(), InputValue::Boolean(true));

        let response = query.execute(&cache, &catalog, Some(&variables)).unwrap();
        let root = object_fields(&response.data);
        assert_eq!(root.len(), 1);
        assert!(has_field(root, "users"));
        assert!(!has_field(root, "orders"));

        let users = list_items(field(root, "users"));
        assert_eq!(users.len(), 2);
        let first_user = object_fields(&users[0]);
        assert!(has_field(first_user, "id"));
        assert!(!has_field(first_user, "name"));
        assert!(!has_field(first_user, "orders"));
    }

    #[test]
    fn directives_can_prune_all_root_fields_for_queries() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let response = execute_query(
            &cache,
            &catalog,
            "{ users @skip(if: true) { id } }",
            None,
            None,
        )
        .unwrap();

        let root = object_fields(&response.data);
        assert!(root.is_empty());
    }
}
