use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use hashbrown::{HashMap, HashSet};

use crate::bind::{
    BoundCollectionQuery, BoundField, BoundFilter, BoundRootField, BoundRootFieldKind,
    BoundSelectionSet,
};
use crate::catalog::{GraphqlCatalog, RelationMeta, TableMeta};
use crate::error::{GqlError, GqlErrorKind, GqlResult};

pub type NodeId = usize;
pub type EdgeId = usize;

#[derive(Clone, Debug)]
pub struct GraphqlBatchPlan {
    root_node: NodeId,
    nodes: Vec<RenderNodePlan>,
    edges: Vec<RelationEdgePlan>,
    table_node_lookup: HashMap<String, Vec<NodeId>>,
    table_edge_lookup: HashMap<String, Vec<EdgeId>>,
    incoming_edges: Vec<Vec<EdgeId>>,
    has_relations: bool,
}

impl GraphqlBatchPlan {
    pub fn root_node(&self) -> NodeId {
        self.root_node
    }

    pub fn nodes(&self) -> &[RenderNodePlan] {
        &self.nodes
    }

    pub fn edges(&self) -> &[RelationEdgePlan] {
        &self.edges
    }

    pub fn nodes_for_table(&self, table_name: &str) -> &[NodeId] {
        self.table_node_lookup
            .get(table_name)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn edges_for_table(&self, table_name: &str) -> &[EdgeId] {
        self.table_edge_lookup
            .get(table_name)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn incoming_edges(&self, node_id: NodeId) -> &[EdgeId] {
        self.incoming_edges
            .get(node_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn node(&self, node_id: NodeId) -> &RenderNodePlan {
        &self.nodes[node_id]
    }

    pub fn edge(&self, edge_id: EdgeId) -> &RelationEdgePlan {
        &self.edges[edge_id]
    }

    pub fn has_relations(&self) -> bool {
        self.has_relations
    }
}

#[derive(Clone, Debug)]
pub struct RenderNodePlan {
    pub id: NodeId,
    pub table_name: String,
    pub fields: Vec<RenderFieldPlan>,
    pub dependency_tables: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct RenderFieldPlan {
    pub response_key: String,
    pub kind: RenderFieldKind,
}

#[derive(Clone, Debug)]
pub enum RenderFieldKind {
    Typename { value: String },
    Column { column_index: usize },
    ForwardRelation { edge_id: EdgeId },
    ReverseRelation { edge_id: EdgeId },
}

#[derive(Clone, Debug)]
pub struct RelationEdgePlan {
    pub id: EdgeId,
    pub parent_node: NodeId,
    pub kind: RelationEdgeKind,
    pub relation: RelationMeta,
    pub child_node: NodeId,
    pub query: Option<BoundCollectionQuery>,
    pub strategy: RelationFetchStrategy,
    pub direct_table: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelationEdgeKind {
    Forward,
    Reverse,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelationFetchStrategy {
    PlannerBatch,
    IndexedProbeBatch,
    ScanAndBucket,
}

pub fn compile_batch_plan(
    catalog: &GraphqlCatalog,
    field: &BoundRootField,
) -> GqlResult<GraphqlBatchPlan> {
    let (table_name, selection) = match &field.kind {
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
        } => (table_name.as_str(), selection),
        BoundRootFieldKind::Typename => {
            return Err(GqlError::new(
                GqlErrorKind::Unsupported,
                "typename root fields do not use batch render plans",
            ));
        }
    };

    let mut builder = BatchPlanBuilder {
        catalog,
        nodes: Vec::new(),
        edges: Vec::new(),
        has_relations: false,
    };
    let root_node = builder.compile_node(table_name, selection)?;
    let mut table_node_lookup: HashMap<String, Vec<NodeId>> = HashMap::new();
    for node in &builder.nodes {
        table_node_lookup
            .entry(node.table_name.clone())
            .or_insert_with(Vec::new)
            .push(node.id);
    }
    let mut table_edge_lookup: HashMap<String, Vec<EdgeId>> = HashMap::new();
    let mut incoming_edges = vec![Vec::new(); builder.nodes.len()];
    for edge in &builder.edges {
        table_edge_lookup
            .entry(edge.direct_table.clone())
            .or_insert_with(Vec::new)
            .push(edge.id);
        incoming_edges[edge.child_node].push(edge.id);
    }
    Ok(GraphqlBatchPlan {
        root_node,
        nodes: builder.nodes,
        edges: builder.edges,
        table_node_lookup,
        table_edge_lookup,
        incoming_edges,
        has_relations: builder.has_relations,
    })
}

struct BatchPlanBuilder<'a> {
    catalog: &'a GraphqlCatalog,
    nodes: Vec<RenderNodePlan>,
    edges: Vec<RelationEdgePlan>,
    has_relations: bool,
}

impl<'a> BatchPlanBuilder<'a> {
    fn compile_node(
        &mut self,
        table_name: &str,
        selection: &BoundSelectionSet,
    ) -> GqlResult<NodeId> {
        let node_id = self.nodes.len();
        self.nodes.push(RenderNodePlan {
            id: node_id,
            table_name: table_name.into(),
            fields: Vec::new(),
            dependency_tables: Vec::new(),
        });

        let mut fields = Vec::with_capacity(selection.fields.len());
        let mut dependencies = HashSet::new();
        dependencies.insert(table_name.into());

        for field in &selection.fields {
            match field {
                BoundField::Typename {
                    response_key,
                    value,
                } => fields.push(RenderFieldPlan {
                    response_key: response_key.clone(),
                    kind: RenderFieldKind::Typename {
                        value: value.clone(),
                    },
                }),
                BoundField::Column {
                    response_key,
                    column_index,
                } => fields.push(RenderFieldPlan {
                    response_key: response_key.clone(),
                    kind: RenderFieldKind::Column {
                        column_index: *column_index,
                    },
                }),
                BoundField::ForwardRelation {
                    response_key,
                    relation,
                    selection,
                } => {
                    self.has_relations = true;
                    let child_node = self.compile_node(&relation.parent_table, selection)?;
                    dependencies.extend(self.nodes[child_node].dependency_tables.iter().cloned());
                    let edge_id = self.edges.len();
                    self.edges.push(RelationEdgePlan {
                        id: edge_id,
                        parent_node: node_id,
                        kind: RelationEdgeKind::Forward,
                        relation: relation.clone(),
                        child_node,
                        query: None,
                        strategy: choose_forward_strategy(self.catalog, relation),
                        direct_table: relation.parent_table.clone(),
                    });
                    fields.push(RenderFieldPlan {
                        response_key: response_key.clone(),
                        kind: RenderFieldKind::ForwardRelation { edge_id },
                    });
                }
                BoundField::ReverseRelation {
                    response_key,
                    relation,
                    query,
                    selection,
                } => {
                    self.has_relations = true;
                    let child_node = self.compile_node(&relation.child_table, selection)?;
                    dependencies.extend(self.nodes[child_node].dependency_tables.iter().cloned());
                    let edge_id = self.edges.len();
                    self.edges.push(RelationEdgePlan {
                        id: edge_id,
                        parent_node: node_id,
                        kind: RelationEdgeKind::Reverse,
                        relation: relation.clone(),
                        child_node,
                        query: Some(query.clone()),
                        strategy: choose_reverse_strategy(query),
                        direct_table: relation.child_table.clone(),
                    });
                    fields.push(RenderFieldPlan {
                        response_key: response_key.clone(),
                        kind: RenderFieldKind::ReverseRelation { edge_id },
                    });
                }
            }
        }

        let mut dependency_tables: Vec<_> = dependencies.into_iter().collect();
        dependency_tables.sort();
        self.nodes[node_id] = RenderNodePlan {
            id: node_id,
            table_name: table_name.into(),
            fields,
            dependency_tables,
        };
        Ok(node_id)
    }
}

fn choose_forward_strategy(
    catalog: &GraphqlCatalog,
    relation: &RelationMeta,
) -> RelationFetchStrategy {
    let Some(parent_table) = catalog.table(&relation.parent_table) else {
        return RelationFetchStrategy::PlannerBatch;
    };
    if is_single_column_primary_key(parent_table, &relation.parent_column) {
        RelationFetchStrategy::IndexedProbeBatch
    } else {
        RelationFetchStrategy::PlannerBatch
    }
}

fn choose_reverse_strategy(query: &BoundCollectionQuery) -> RelationFetchStrategy {
    if query.order_by.is_empty() && query.filter.as_ref().is_none_or(|filter| !uses_relation_filter(filter)) {
        RelationFetchStrategy::IndexedProbeBatch
    } else {
        RelationFetchStrategy::PlannerBatch
    }
}

fn uses_relation_filter(filter: &BoundFilter) -> bool {
    match filter {
        BoundFilter::And(filters) | BoundFilter::Or(filters) => {
            filters.iter().any(uses_relation_filter)
        }
        BoundFilter::Column(_) => false,
        BoundFilter::Relation(_) => true,
    }
}

fn is_single_column_primary_key(table: &TableMeta, column_name: &str) -> bool {
    table
        .primary_key()
        .is_some_and(|pk| pk.columns.len() == 1 && pk.columns[0].name == column_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bind::{ColumnPredicate, PredicateOp};
    use cynos_core::{DataType, Value};

    #[test]
    fn reverse_strategy_uses_indexed_probe_for_windowed_non_relational_queries() {
        let query = BoundCollectionQuery {
            filter: Some(BoundFilter::Column(ColumnPredicate {
                column_index: 1,
                data_type: DataType::Int64,
                ops: vec![PredicateOp::Eq(Value::Int64(1))],
            })),
            order_by: Vec::new(),
            limit: Some(5),
            offset: 2,
        };

        assert_eq!(
            choose_reverse_strategy(&query),
            RelationFetchStrategy::IndexedProbeBatch
        );
    }

    #[test]
    fn reverse_strategy_keeps_planner_batch_for_ordered_queries() {
        let query = BoundCollectionQuery {
            filter: None,
            order_by: vec![crate::bind::OrderSpec {
                column_index: 0,
                descending: true,
            }],
            limit: Some(1),
            offset: 0,
        };

        assert_eq!(
            choose_reverse_strategy(&query),
            RelationFetchStrategy::PlannerBatch
        );
    }
}
