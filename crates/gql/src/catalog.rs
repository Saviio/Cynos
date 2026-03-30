use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use cynos_core::schema::{ForeignKey, Table};
use cynos_core::DataType;
use cynos_storage::TableCache;
use hashbrown::HashSet;

use crate::ast::OperationType;

#[derive(Clone, Debug, Default)]
pub struct GraphqlCatalog {
    tables: Vec<TableMeta>,
    table_lookup: BTreeMap<String, usize>,
    query_fields: Vec<RootFieldMeta>,
    query_field_lookup: BTreeMap<String, usize>,
    mutation_fields: Vec<RootFieldMeta>,
    mutation_field_lookup: BTreeMap<String, usize>,
    subscription_fields: Vec<RootFieldMeta>,
    subscription_field_lookup: BTreeMap<String, usize>,
}

impl GraphqlCatalog {
    pub fn from_table_cache(cache: &TableCache) -> Self {
        let table_names = cache.table_names();
        let mut tables = Vec::with_capacity(table_names.len());
        let mut source_tables = Vec::with_capacity(table_names.len());
        let mut type_names = BTreeMap::new();

        for table_name in table_names {
            if let Some(store) = cache.get_table(table_name) {
                let table = store.schema().clone();
                type_names.insert(table.name().to_string(), table_type_name(table.name()));
                source_tables.push(table);
            }
        }

        let reverse_relations = build_reverse_relations(&source_tables);
        let table_map: BTreeMap<String, &Table> = source_tables
            .iter()
            .map(|table| (table.name().to_string(), table))
            .collect();

        for table in &source_tables {
            let graphql_name = type_names
                .get(table.name())
                .cloned()
                .unwrap_or_else(|| table_type_name(table.name()));
            tables.push(build_table_meta(
                table,
                &graphql_name,
                &table_map,
                &reverse_relations,
            ));
        }

        let mut table_lookup = BTreeMap::new();
        for (index, table) in tables.iter().enumerate() {
            table_lookup.insert(table.table_name.clone(), index);
        }

        let mut query_fields = Vec::new();
        let mut mutation_fields = Vec::new();
        let mut subscription_fields = Vec::new();
        for table in &tables {
            query_fields.push(RootFieldMeta {
                name: table.table_name.clone(),
                table_name: table.table_name.clone(),
                kind: RootFieldKind::List,
            });
            subscription_fields.push(RootFieldMeta {
                name: table.table_name.clone(),
                table_name: table.table_name.clone(),
                kind: RootFieldKind::List,
            });

            if table.primary_key.is_some() {
                let by_pk_name = format!("{}ByPk", table.table_name);
                query_fields.push(RootFieldMeta {
                    name: by_pk_name.clone(),
                    table_name: table.table_name.clone(),
                    kind: RootFieldKind::ByPk,
                });
                subscription_fields.push(RootFieldMeta {
                    name: by_pk_name,
                    table_name: table.table_name.clone(),
                    kind: RootFieldKind::ByPk,
                });
            }

            mutation_fields.push(RootFieldMeta {
                name: format!("insert{}", table.graphql_name),
                table_name: table.table_name.clone(),
                kind: RootFieldKind::Insert,
            });
            mutation_fields.push(RootFieldMeta {
                name: format!("update{}", table.graphql_name),
                table_name: table.table_name.clone(),
                kind: RootFieldKind::Update,
            });
            mutation_fields.push(RootFieldMeta {
                name: format!("delete{}", table.graphql_name),
                table_name: table.table_name.clone(),
                kind: RootFieldKind::Delete,
            });
        }

        Self {
            table_lookup,
            query_field_lookup: build_lookup(&query_fields),
            mutation_field_lookup: build_lookup(&mutation_fields),
            subscription_field_lookup: build_lookup(&subscription_fields),
            tables,
            query_fields,
            mutation_fields,
            subscription_fields,
        }
    }

    pub fn tables(&self) -> &[TableMeta] {
        &self.tables
    }

    pub fn table(&self, table_name: &str) -> Option<&TableMeta> {
        self.table_lookup
            .get(table_name)
            .and_then(|index| self.tables.get(*index))
    }

    pub fn query_fields(&self) -> &[RootFieldMeta] {
        &self.query_fields
    }

    pub fn mutation_fields(&self) -> &[RootFieldMeta] {
        &self.mutation_fields
    }

    pub fn subscription_fields(&self) -> &[RootFieldMeta] {
        &self.subscription_fields
    }

    pub fn query_field(&self, field_name: &str) -> Option<&RootFieldMeta> {
        lookup_root_field(&self.query_fields, &self.query_field_lookup, field_name)
    }

    pub fn mutation_field(&self, field_name: &str) -> Option<&RootFieldMeta> {
        lookup_root_field(
            &self.mutation_fields,
            &self.mutation_field_lookup,
            field_name,
        )
    }

    pub fn subscription_field(&self, field_name: &str) -> Option<&RootFieldMeta> {
        lookup_root_field(
            &self.subscription_fields,
            &self.subscription_field_lookup,
            field_name,
        )
    }

    pub fn root_field(
        &self,
        operation_type: OperationType,
        field_name: &str,
    ) -> Option<&RootFieldMeta> {
        match operation_type {
            OperationType::Query => self.query_field(field_name),
            OperationType::Mutation => self.mutation_field(field_name),
            OperationType::Subscription => self.subscription_field(field_name),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TableMeta {
    pub table_name: String,
    pub graphql_name: String,
    columns: Vec<ColumnMeta>,
    column_lookup: BTreeMap<String, usize>,
    fields: Vec<TableFieldMeta>,
    field_lookup: BTreeMap<String, usize>,
    primary_key: Option<PrimaryKeyMeta>,
}

impl TableMeta {
    pub fn columns(&self) -> &[ColumnMeta] {
        &self.columns
    }

    pub fn column(&self, name: &str) -> Option<&ColumnMeta> {
        self.column_lookup
            .get(name)
            .and_then(|index| self.columns.get(*index))
    }

    pub fn column_by_index(&self, index: usize) -> Option<&ColumnMeta> {
        self.columns.get(index)
    }

    pub fn fields(&self) -> &[TableFieldMeta] {
        &self.fields
    }

    pub fn field(&self, name: &str) -> Option<&TableFieldMeta> {
        self.field_lookup
            .get(name)
            .and_then(|index| self.fields.get(*index))
    }

    pub fn primary_key(&self) -> Option<&PrimaryKeyMeta> {
        self.primary_key.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnMeta {
    pub name: String,
    pub index: usize,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableFieldMeta {
    Column(ColumnMeta),
    ForwardRelation(RelationMeta),
    ReverseRelation(RelationMeta),
}

impl TableFieldMeta {
    pub fn name(&self) -> &str {
        match self {
            Self::Column(column) => &column.name,
            Self::ForwardRelation(relation) | Self::ReverseRelation(relation) => &relation.name,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelationMeta {
    pub name: String,
    pub fk_name: String,
    pub child_table: String,
    pub child_column: String,
    pub child_column_index: usize,
    pub child_column_unique: bool,
    pub parent_table: String,
    pub parent_column: String,
    pub parent_column_index: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrimaryKeyMeta {
    pub columns: Vec<ColumnMeta>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootFieldMeta {
    pub name: String,
    pub table_name: String,
    pub kind: RootFieldKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootFieldKind {
    List,
    ByPk,
    Insert,
    Update,
    Delete,
}

fn build_lookup(fields: &[RootFieldMeta]) -> BTreeMap<String, usize> {
    let mut lookup = BTreeMap::new();
    for (index, field) in fields.iter().enumerate() {
        lookup.insert(field.name.clone(), index);
    }
    lookup
}

fn lookup_root_field<'a>(
    fields: &'a [RootFieldMeta],
    lookup: &BTreeMap<String, usize>,
    field_name: &str,
) -> Option<&'a RootFieldMeta> {
    lookup.get(field_name).and_then(|index| fields.get(*index))
}

fn build_table_meta(
    table: &Table,
    graphql_name: &str,
    table_map: &BTreeMap<String, &Table>,
    reverse_relations: &BTreeMap<String, Vec<ForeignKey>>,
) -> TableMeta {
    let mut columns = Vec::with_capacity(table.columns().len());
    let mut column_lookup = BTreeMap::new();
    let mut fields = Vec::new();
    let mut field_lookup = BTreeMap::new();
    let mut used_names = HashSet::new();

    for column in table.columns() {
        let column_meta = ColumnMeta {
            name: column.name().to_string(),
            index: column.index(),
            data_type: column.data_type(),
            nullable: column.is_nullable(),
        };
        used_names.insert(column_meta.name.clone());
        column_lookup.insert(column_meta.name.clone(), columns.len());
        field_lookup.insert(column_meta.name.clone(), fields.len());
        columns.push(column_meta.clone());
        fields.push(TableFieldMeta::Column(column_meta));
    }

    for fk in table.constraints().get_foreign_keys() {
        let Some(parent_table) = table_map.get(&fk.parent_table) else {
            continue;
        };
        let Some(child_column_index) = table.get_column_index(&fk.child_column) else {
            continue;
        };
        let Some(parent_column_index) = parent_table.get_column_index(&fk.parent_column) else {
            continue;
        };
        let field_name = disambiguate_field_name(
            fk.graphql_forward_field().unwrap_or(&fk.parent_table),
            &mut used_names,
        );
        let relation = RelationMeta {
            name: field_name.clone(),
            fk_name: fk.name.clone(),
            child_table: fk.child_table.clone(),
            child_column: fk.child_column.clone(),
            child_column_index,
            child_column_unique: is_single_column_unique_key(table, &fk.child_column),
            parent_table: fk.parent_table.clone(),
            parent_column: fk.parent_column.clone(),
            parent_column_index,
        };
        field_lookup.insert(field_name, fields.len());
        fields.push(TableFieldMeta::ForwardRelation(relation));
    }

    if let Some(reverse) = reverse_relations.get(table.name()) {
        for fk in reverse {
            let Some(child_table) = table_map.get(&fk.child_table) else {
                continue;
            };
            let Some(child_column_index) = child_table.get_column_index(&fk.child_column) else {
                continue;
            };
            let Some(parent_column_index) = table.get_column_index(&fk.parent_column) else {
                continue;
            };
            let field_name = disambiguate_field_name(
                fk.graphql_reverse_field().unwrap_or(&fk.child_table),
                &mut used_names,
            );
            let relation = RelationMeta {
                name: field_name.clone(),
                fk_name: fk.name.clone(),
                child_table: fk.child_table.clone(),
                child_column: fk.child_column.clone(),
                child_column_index,
                child_column_unique: is_single_column_unique_key(child_table, &fk.child_column),
                parent_table: fk.parent_table.clone(),
                parent_column: fk.parent_column.clone(),
                parent_column_index,
            };
            field_lookup.insert(field_name, fields.len());
            fields.push(TableFieldMeta::ReverseRelation(relation));
        }
    }

    let primary_key = table.primary_key().map(|pk| PrimaryKeyMeta {
        columns: pk
            .columns()
            .iter()
            .filter_map(|indexed_column| table.get_column(&indexed_column.name))
            .map(|column| ColumnMeta {
                name: column.name().to_string(),
                index: column.index(),
                data_type: column.data_type(),
                nullable: column.is_nullable(),
            })
            .collect(),
    });

    TableMeta {
        table_name: table.name().to_string(),
        graphql_name: graphql_name.to_string(),
        columns,
        column_lookup,
        fields,
        field_lookup,
        primary_key,
    }
}

fn is_single_column_unique_key(table: &Table, column_name: &str) -> bool {
    table.indices().iter().any(|index| {
        index.is_unique()
            && index.columns().len() == 1
            && index.columns()[0].name == column_name
    }) || table.primary_key().is_some_and(|primary_key| {
        primary_key.columns().len() == 1 && primary_key.columns()[0].name == column_name
    })
}

fn build_reverse_relations(tables: &[Table]) -> BTreeMap<String, Vec<ForeignKey>> {
    let mut map: BTreeMap<String, Vec<ForeignKey>> = BTreeMap::new();
    for table in tables {
        for fk in table.constraints().get_foreign_keys() {
            map.entry(fk.parent_table.clone())
                .or_default()
                .push(fk.clone());
        }
    }
    map
}

fn disambiguate_field_name(base_name: &str, used_names: &mut HashSet<String>) -> String {
    if !used_names.contains(base_name) {
        let name = base_name.to_string();
        used_names.insert(name.clone());
        return name;
    }

    let mut attempt = format!("{}_rel", base_name);
    let mut suffix = 2usize;
    while used_names.contains(&attempt) {
        attempt = format!("{}_rel{}", base_name, suffix);
        suffix += 1;
    }
    used_names.insert(attempt.clone());
    attempt
}

pub fn table_type_name(table_name: &str) -> String {
    let mut out = String::new();
    let mut uppercase_next = true;
    for ch in table_name.chars() {
        if ch == '_' {
            uppercase_next = true;
            continue;
        }
        if uppercase_next {
            out.extend(ch.to_uppercase());
            uppercase_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::schema::TableBuilder;
    use cynos_storage::TableCache;

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
    }

    #[test]
    fn catalog_exposes_query_mutation_and_subscription_fields() {
        let cache = build_cache();
        let catalog = GraphqlCatalog::from_table_cache(&cache);
        let users = catalog.table("users").unwrap();
        let orders = catalog.table("orders").unwrap();

        assert!(catalog.query_field("users").is_some());
        assert!(catalog.query_field("usersByPk").is_some());
        assert!(catalog.mutation_field("insertUsers").is_some());
        assert!(catalog.mutation_field("updateUsers").is_some());
        assert!(catalog.mutation_field("deleteUsers").is_some());
        assert!(catalog.subscription_field("users").is_some());
        assert!(orders.field("buyer").is_some());
        assert!(users.field("orders").is_some());
    }
}
