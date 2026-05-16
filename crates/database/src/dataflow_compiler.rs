//! PhysicalPlan → DataflowNode compiler.
//!
//! Compiles a query optimizer's PhysicalPlan into a DataflowNode graph
//! for incremental view maintenance. The optimizer's decisions (join order,
//! predicate pushdown, projection pushdown) are preserved in the dataflow.
//!
//! The PhysicalPlan is used for:
//!   1. Bootstrap: execute once to get initial result set
//!   2. Compile: produce DataflowNode graph for incremental maintenance
//!
//! Non-incrementalizable operators (Sort, Limit, TopN) cause the compiler
//! to return None, signaling fallback to re-query strategy.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::{schema::Table, Row, Value};
use cynos_incremental::{
    AggregateType, DataflowNode, JoinKeySpec, JoinType as IvmJoinType, TableId, TraceTupleArena,
    TraceTupleHandle,
};
use cynos_index::KeyRange;
use cynos_jsonb::path::{decode_json_string_literal, trim_json_bytes, SimpleJsonPath};
use cynos_jsonb::{JsonPath, JsonbObject, JsonbValue};
use cynos_query::ast::JoinType as QueryJoinType;
use cynos_query::ast::{AggregateFunc, BinaryOp, Expr, UnaryOp};
use cynos_query::planner::{IndexBounds, PhysicalPlan};
use hashbrown::HashMap;

/// Result of compiling a PhysicalPlan to a DataflowNode.
pub struct CompileResult {
    /// The dataflow node graph for incremental maintenance
    pub dataflow: DataflowNode,
    /// Mapping from table name → table ID used by this dataflow graph
    pub table_ids: HashMap<String, TableId>,
}

#[derive(Clone)]
struct CompileLayout {
    tables: Vec<String>,
    table_column_counts: Vec<usize>,
}

impl CompileLayout {
    fn table(table: &str, column_count: usize) -> Self {
        Self {
            tables: alloc::vec![table.into()],
            table_column_counts: alloc::vec![column_count],
        }
    }

    fn projected(&self, output_column_count: usize) -> Self {
        Self {
            tables: self.tables.clone(),
            table_column_counts: alloc::vec![output_column_count],
        }
    }

    fn join(left: &Self, right: &Self, output_tables: &[String]) -> Self {
        let mut table_column_counts = Vec::with_capacity(output_tables.len());
        for table in output_tables {
            if let Some(width) = left.table_width(table) {
                table_column_counts.push(width);
                continue;
            }
            if let Some(width) = right.table_width(table) {
                table_column_counts.push(width);
            }
        }

        Self {
            tables: output_tables.to_vec(),
            table_column_counts,
        }
    }

    fn combined(left: &Self, right: &Self) -> Self {
        let mut tables = left.tables.clone();
        tables.extend(right.tables.iter().cloned());
        Self::join(left, right, &tables)
    }

    fn contains_table(&self, table: &str) -> bool {
        self.tables.iter().any(|candidate| candidate == table)
    }

    fn resolve_column_index(&self, table_name: &str, table_relative_index: usize) -> usize {
        if table_name.is_empty() {
            return table_relative_index;
        }

        let mut offset = 0usize;
        for (index, table) in self.tables.iter().enumerate() {
            if table == table_name {
                return offset + table_relative_index;
            }
            offset += self.table_column_counts.get(index).copied().unwrap_or(0);
        }

        table_relative_index
    }

    fn table_width(&self, table_name: &str) -> Option<usize> {
        self.tables
            .iter()
            .position(|table| table == table_name)
            .and_then(|index| self.table_column_counts.get(index).copied())
    }

    fn projection_indices_for(&self, output_tables: &[String]) -> Vec<usize> {
        let mut indices = Vec::new();
        for table in output_tables {
            let start = self.resolve_column_index(table, 0);
            let width = self.table_width(table).unwrap_or(0);
            indices.extend(start..start.saturating_add(width));
        }
        indices
    }

    fn total_width(&self) -> usize {
        self.table_column_counts.iter().sum()
    }
}

struct CompiledNode {
    dataflow: DataflowNode,
    layout: CompileLayout,
}

#[derive(Clone)]
struct IndexedColumnRef {
    name: String,
    index: usize,
}

struct TableIdResolver<'a> {
    existing: &'a HashMap<String, TableId>,
    used: HashMap<String, TableId>,
    next_id: TableId,
}

impl<'a> TableIdResolver<'a> {
    fn new(existing: &'a HashMap<String, TableId>) -> Self {
        let next_id = existing
            .values()
            .copied()
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        Self {
            existing,
            used: HashMap::new(),
            next_id,
        }
    }

    fn resolve(&mut self, table: &str) -> TableId {
        if let Some(existing) = self.used.get(table) {
            return *existing;
        }

        let table_id = self.existing.get(table).copied().unwrap_or_else(|| {
            let allocated = self.next_id;
            self.next_id = self.next_id.saturating_add(1);
            allocated
        });
        self.used.insert(table.into(), table_id);
        table_id
    }

    fn into_used(self) -> HashMap<String, TableId> {
        self.used
    }
}

/// Compiles a PhysicalPlan into a DataflowNode for IVM.
///
/// Returns None if the plan contains non-incrementalizable operators
/// (Sort, Limit, TopN), signaling that re-query should be used instead.
pub fn compile_to_dataflow(
    plan: &PhysicalPlan,
    table_id_map: &HashMap<String, TableId>,
    table_schemas: &HashMap<String, Table>,
) -> Option<CompileResult> {
    if !plan.is_incrementalizable() {
        return None;
    }

    let mut table_ids = TableIdResolver::new(table_id_map);
    let compiled = compile_node(plan, &mut table_ids, table_schemas)?;
    Some(CompileResult {
        dataflow: compiled.dataflow,
        table_ids: table_ids.into_used(),
    })
}

fn compile_source_node(
    table: &str,
    table_ids: &mut TableIdResolver<'_>,
    table_schemas: &HashMap<String, Table>,
) -> Option<CompiledNode> {
    let table_id = table_ids.resolve(table);
    let column_count = table_schemas.get(table)?.columns().len();
    Some(CompiledNode {
        dataflow: DataflowNode::source(table_id),
        layout: CompileLayout::table(table, column_count),
    })
}

fn compile_filtered_source(
    table: &str,
    predicate: Option<Expr>,
    table_ids: &mut TableIdResolver<'_>,
    table_schemas: &HashMap<String, Table>,
) -> Option<CompiledNode> {
    let source = compile_source_node(table, table_ids, table_schemas)?;
    if let Some(predicate) = predicate {
        let bound_predicate = bind_expr_to_layout(&predicate, &source.layout);
        let pred_fn = compile_predicate(&bound_predicate);
        let trace_pred_fn = compile_trace_predicate(&bound_predicate);
        return Some(CompiledNode {
            dataflow: DataflowNode::Filter {
                input: Box::new(source.dataflow),
                predicate: pred_fn,
                trace_predicate: Some(trace_pred_fn),
            },
            layout: source.layout,
        });
    }

    Some(source)
}

fn lookup_index_columns(
    table_schemas: &HashMap<String, Table>,
    table: &str,
    index_name: &str,
) -> Option<Vec<IndexedColumnRef>> {
    let schema = table_schemas.get(table)?;
    let index = schema
        .get_index(index_name)
        .or_else(|| {
            schema
                .indices()
                .iter()
                .find(|candidate| candidate.normalized_name() == index_name)
        })
        .or_else(|| {
            schema.primary_key().filter(|candidate| {
                candidate.name() == index_name || candidate.normalized_name() == index_name
            })
        })?;

    index
        .columns()
        .iter()
        .map(|column| {
            Some(IndexedColumnRef {
                name: column.name.clone(),
                index: schema.get_column_index(&column.name)?,
            })
        })
        .collect()
}

fn column_expr(table: &str, column: &IndexedColumnRef) -> Expr {
    Expr::column(table, column.name.clone(), column.index)
}

fn combine_with_and(mut predicates: Vec<Expr>) -> Option<Expr> {
    let first = predicates.pop()?;
    Some(
        predicates
            .into_iter()
            .fold(first, |combined, predicate| Expr::and(combined, predicate)),
    )
}

fn build_scalar_range_predicate(
    table: &str,
    column: &IndexedColumnRef,
    range: &KeyRange<Value>,
) -> Option<Expr> {
    let expr = column_expr(table, column);
    match range {
        KeyRange::All => None,
        KeyRange::Only(value) => Some(Expr::eq(expr, Expr::Literal(value.clone()))),
        KeyRange::LowerBound { value, exclusive } => Some(if *exclusive {
            Expr::gt(expr, Expr::Literal(value.clone()))
        } else {
            Expr::ge(expr, Expr::Literal(value.clone()))
        }),
        KeyRange::UpperBound { value, exclusive } => Some(if *exclusive {
            Expr::lt(expr, Expr::Literal(value.clone()))
        } else {
            Expr::le(expr, Expr::Literal(value.clone()))
        }),
        KeyRange::Bound {
            lower,
            upper,
            lower_exclusive,
            upper_exclusive,
        } => combine_with_and(alloc::vec![
            if *lower_exclusive {
                Expr::gt(column_expr(table, column), Expr::Literal(lower.clone()))
            } else {
                Expr::ge(column_expr(table, column), Expr::Literal(lower.clone()))
            },
            if *upper_exclusive {
                Expr::lt(column_expr(table, column), Expr::Literal(upper.clone()))
            } else {
                Expr::le(column_expr(table, column), Expr::Literal(upper.clone()))
            },
        ]),
    }
}

fn build_composite_only_predicate(
    table: &str,
    indexed_columns: &[IndexedColumnRef],
    values: &[Value],
) -> Result<Expr, ()> {
    if indexed_columns.len() != values.len() || indexed_columns.is_empty() {
        return Err(());
    }

    combine_with_and(
        indexed_columns
            .iter()
            .zip(values.iter())
            .map(|(column, value)| {
                Expr::eq(column_expr(table, column), Expr::Literal(value.clone()))
            })
            .collect(),
    )
    .ok_or(())
}

fn build_index_scan_predicate(
    table: &str,
    indexed_columns: &[IndexedColumnRef],
    bounds: &IndexBounds,
) -> Result<Option<Expr>, ()> {
    match bounds {
        IndexBounds::Unbounded => Ok(None),
        IndexBounds::Scalar(range) => {
            if indexed_columns.len() != 1 {
                return Err(());
            }
            Ok(build_scalar_range_predicate(
                table,
                &indexed_columns[0],
                range,
            ))
        }
        IndexBounds::Composite(range) => match range {
            KeyRange::All => Ok(None),
            KeyRange::Only(values) => {
                build_composite_only_predicate(table, indexed_columns, values).map(Some)
            }
            KeyRange::Bound {
                lower,
                upper,
                lower_exclusive,
                upper_exclusive,
            } if lower == upper && !lower_exclusive && !upper_exclusive => {
                build_composite_only_predicate(table, indexed_columns, lower).map(Some)
            }
            _ => Err(()),
        },
    }
}

fn compile_node(
    plan: &PhysicalPlan,
    table_ids: &mut TableIdResolver<'_>,
    table_schemas: &HashMap<String, Table>,
) -> Option<CompiledNode> {
    match plan {
        PhysicalPlan::TableScan { table } => compile_source_node(table, table_ids, table_schemas),

        PhysicalPlan::IndexScan {
            table,
            index,
            bounds,
            limit,
            offset,
            reverse,
        } => {
            if *reverse || limit.is_some() || offset.unwrap_or(0) > 0 {
                return None;
            }
            let indexed_columns = lookup_index_columns(table_schemas, table, index)?;
            let predicate = build_index_scan_predicate(table, &indexed_columns, bounds).ok()?;
            compile_filtered_source(table, predicate, table_ids, table_schemas)
        }

        PhysicalPlan::IndexGet {
            table,
            index,
            key,
            limit,
        } => {
            if limit.is_some() {
                return None;
            }
            let indexed_columns = lookup_index_columns(table_schemas, table, index)?;
            if indexed_columns.len() != 1 {
                return None;
            }
            let predicate = Some(Expr::eq(
                column_expr(table, &indexed_columns[0]),
                Expr::Literal(key.clone()),
            ));
            compile_filtered_source(table, predicate, table_ids, table_schemas)
        }

        PhysicalPlan::IndexInGet { table, index, keys } => {
            let indexed_columns = lookup_index_columns(table_schemas, table, index)?;
            if indexed_columns.len() != 1 {
                return None;
            }
            let predicate = Some(Expr::In {
                expr: Box::new(column_expr(table, &indexed_columns[0])),
                list: keys.iter().cloned().map(Expr::Literal).collect(),
            });
            compile_filtered_source(table, predicate, table_ids, table_schemas)
        }

        PhysicalPlan::GinIndexScan { table, recheck, .. }
        | PhysicalPlan::GinIndexScanMulti { table, recheck, .. } => {
            compile_filtered_source(table, Some(recheck.clone()?), table_ids, table_schemas)
        }

        PhysicalPlan::Filter { input, predicate } => {
            let input_node = compile_node(input, table_ids, table_schemas)?;
            let bound_predicate = bind_expr_to_layout(predicate, &input_node.layout);
            let pred_fn = compile_predicate(&bound_predicate);
            let trace_pred_fn = compile_trace_predicate(&bound_predicate);
            Some(CompiledNode {
                dataflow: DataflowNode::Filter {
                    input: Box::new(input_node.dataflow),
                    predicate: pred_fn,
                    trace_predicate: Some(trace_pred_fn),
                },
                layout: input_node.layout,
            })
        }

        PhysicalPlan::Project { input, columns } => {
            let input_node = compile_node(input, table_ids, table_schemas)?;
            let bound_columns: Vec<Expr> = columns
                .iter()
                .map(|expr| bind_expr_to_layout(expr, &input_node.layout))
                .collect();
            // Extract column indices from projection expressions
            let col_indices: Vec<usize> = bound_columns
                .iter()
                .filter_map(|expr| extract_column_index(expr))
                .collect();

            if col_indices.len() == columns.len() {
                // Pure column projection — use Project node
                Some(CompiledNode {
                    dataflow: DataflowNode::project(input_node.dataflow, col_indices),
                    layout: input_node.layout.projected(columns.len()),
                })
            } else {
                // Has computed expressions — use Map node
                let exprs = bound_columns;
                let trace_mapper = compile_trace_mapper(&exprs);
                let row_exprs = exprs.clone();
                Some(CompiledNode {
                    dataflow: DataflowNode::Map {
                        input: Box::new(input_node.dataflow),
                        mapper: Box::new(move |row: &Row| {
                            let values: Vec<Value> =
                                row_exprs.iter().map(|expr| eval_expr(expr, row)).collect();
                            Row::new_with_version(row.id(), row.version(), values)
                        }),
                        trace_mapper: Some(trace_mapper),
                    },
                    layout: input_node.layout.projected(columns.len()),
                })
            }
        }

        // All join types compile to Join node with appropriate JoinType
        PhysicalPlan::HashJoin {
            left,
            right,
            condition,
            join_type,
            output_tables,
        }
        | PhysicalPlan::SortMergeJoin {
            left,
            right,
            condition,
            join_type,
            output_tables,
        }
        | PhysicalPlan::NestedLoopJoin {
            left,
            right,
            condition,
            join_type,
            output_tables,
        } => {
            let left_node = compile_node(left, table_ids, table_schemas)?;
            let right_node = compile_node(right, table_ids, table_schemas)?;
            let ivm_join_type = convert_join_type(join_type);
            let (left_key, right_key) =
                extract_join_keys(condition, &left_node.layout, &right_node.layout);
            let raw_layout = CompileLayout::combined(&left_node.layout, &right_node.layout);
            let join_node = DataflowNode::Join {
                left: Box::new(left_node.dataflow),
                right: Box::new(right_node.dataflow),
                left_key,
                right_key,
                join_type: ivm_join_type,
                left_width: left_node.layout.total_width(),
                right_width: right_node.layout.total_width(),
            };
            Some(reorder_join_output(join_node, raw_layout, output_tables))
        }

        PhysicalPlan::IndexNestedLoopJoin {
            outer,
            inner_table,
            condition,
            join_type,
            outer_is_left,
            output_tables,
            ..
        } => {
            let outer_node = compile_node(outer, table_ids, table_schemas)?;
            let inner_table_id = table_ids.resolve(inner_table);
            let inner_column_count = table_schemas.get(inner_table)?.columns().len();
            let inner_layout = CompileLayout::table(inner_table, inner_column_count);
            let inner_node = CompiledNode {
                dataflow: DataflowNode::source(inner_table_id),
                layout: inner_layout,
            };
            let ivm_join_type = convert_join_type(join_type);
            let (left_node, right_node) = if *outer_is_left {
                (outer_node, inner_node)
            } else {
                (inner_node, outer_node)
            };
            let (left_key, right_key) =
                extract_join_keys(condition, &left_node.layout, &right_node.layout);
            let raw_layout = CompileLayout::combined(&left_node.layout, &right_node.layout);

            let join_node = DataflowNode::Join {
                left: Box::new(left_node.dataflow),
                right: Box::new(right_node.dataflow),
                left_key,
                right_key,
                join_type: ivm_join_type,
                left_width: left_node.layout.total_width(),
                right_width: right_node.layout.total_width(),
            };

            Some(reorder_join_output(join_node, raw_layout, output_tables))
        }

        PhysicalPlan::CrossProduct { left, right } => {
            let left_node = compile_node(left, table_ids, table_schemas)?;
            let right_node = compile_node(right, table_ids, table_schemas)?;
            let raw_layout = CompileLayout::combined(&left_node.layout, &right_node.layout);
            // Cross product = join with constant key (everything matches)
            Some(CompiledNode {
                dataflow: DataflowNode::Join {
                    left: Box::new(left_node.dataflow),
                    right: Box::new(right_node.dataflow),
                    left_key: JoinKeySpec::Constant(alloc::vec![Value::Int64(0)]),
                    right_key: JoinKeySpec::Constant(alloc::vec![Value::Int64(0)]),
                    join_type: IvmJoinType::Inner,
                    left_width: left_node.layout.total_width(),
                    right_width: right_node.layout.total_width(),
                },
                layout: raw_layout,
            })
        }

        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
        } => {
            let input_node = compile_node(input, table_ids, table_schemas)?;
            let bound_group_by: Vec<Expr> = group_by
                .iter()
                .map(|expr| bind_expr_to_layout(expr, &input_node.layout))
                .collect();
            let bound_aggregates: Vec<(AggregateFunc, Expr)> = aggregates
                .iter()
                .map(|(func, expr)| (*func, bind_expr_to_layout(expr, &input_node.layout)))
                .collect();

            let group_by_indices: Vec<usize> = bound_group_by
                .iter()
                .filter_map(|expr| extract_column_index(expr))
                .collect();

            let functions: Vec<(usize, AggregateType)> = bound_aggregates
                .iter()
                .filter_map(|(func, expr)| {
                    let col_idx = match expr {
                        Expr::Aggregate {
                            expr: Some(inner), ..
                        } => extract_column_index(inner),
                        Expr::Column(col_ref) => Some(col_ref.index),
                        _ => Some(0), // COUNT(*) uses column 0
                    };
                    col_idx.map(|idx| (idx, convert_aggregate_func(func)))
                })
                .collect();

            Some(CompiledNode {
                dataflow: DataflowNode::Aggregate {
                    input: Box::new(input_node.dataflow),
                    group_by: group_by_indices,
                    functions,
                },
                layout: input_node
                    .layout
                    .projected(group_by.len().saturating_add(aggregates.len())),
            })
        }

        PhysicalPlan::NoOp { input } => compile_node(input, table_ids, table_schemas),
        PhysicalPlan::Empty => Some(CompiledNode {
            dataflow: DataflowNode::source(u32::MAX),
            layout: CompileLayout {
                tables: Vec::new(),
                table_column_counts: Vec::new(),
            },
        }),

        // Non-incrementalizable — should have been caught by is_incrementalizable()
        PhysicalPlan::Sort { .. }
        | PhysicalPlan::Limit { .. }
        | PhysicalPlan::TopN { .. }
        | PhysicalPlan::Union { .. } => None,
    }
}

// ---------------------------------------------------------------------------
// Expression compilation: Expr → closures
// ---------------------------------------------------------------------------

/// Compiles an Expr predicate into a closure for DataflowNode::Filter.
fn compile_predicate(expr: &Expr) -> Box<dyn Fn(&Row) -> bool + Send + Sync> {
    let expr = expr.clone();
    Box::new(move |row: &Row| {
        let mut value_at = |index: usize| row.get(index).cloned();
        matches!(eval_expr_with(&expr, &mut value_at), Value::Boolean(true))
    })
}

fn compile_trace_predicate(
    expr: &Expr,
) -> Box<dyn Fn(&TraceTupleArena, &TraceTupleHandle) -> bool + Send + Sync> {
    let predicate = CompiledTracePredicate::compile(expr);
    Box::new(move |arena: &TraceTupleArena, handle: &TraceTupleHandle| {
        predicate.eval(arena, handle)
    })
}

fn compile_trace_mapper(
    exprs: &[Expr],
) -> Box<dyn Fn(&TraceTupleArena, &TraceTupleHandle) -> Row + Send + Sync> {
    let exprs = exprs.to_vec();
    Box::new(move |arena: &TraceTupleArena, handle: &TraceTupleHandle| {
        let values = exprs
            .iter()
            .map(|expr| {
                let mut value_at = |index: usize| arena.value_at(handle, index);
                eval_expr_with(expr, &mut value_at)
            })
            .collect();
        Row::new_with_version(arena.row_id(handle), arena.version(handle), values)
    })
}

#[derive(Clone)]
enum CompiledTracePredicate {
    JsonPathEq {
        column_index: usize,
        path: SimpleJsonPath,
        expected: Value,
    },
    JsonPathExists {
        column_index: usize,
        path: SimpleJsonPath,
    },
    And(Box<CompiledTracePredicate>, Box<CompiledTracePredicate>),
    Or(Box<CompiledTracePredicate>, Box<CompiledTracePredicate>),
    Not(Box<CompiledTracePredicate>),
    Generic(Expr),
}

impl CompiledTracePredicate {
    fn compile(expr: &Expr) -> Self {
        match expr {
            Expr::Function { name, args } => Self::compile_json_function(name, args)
                .unwrap_or_else(|| Self::Generic(expr.clone())),
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => Self::And(
                Box::new(Self::compile(left)),
                Box::new(Self::compile(right)),
            ),
            Expr::BinaryOp {
                left,
                op: BinaryOp::Or,
                right,
            } => Self::Or(
                Box::new(Self::compile(left)),
                Box::new(Self::compile(right)),
            ),
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: inner,
            } => {
                let compiled = Self::compile(inner);
                if compiled.is_boolean_output() {
                    Self::Not(Box::new(compiled))
                } else {
                    Self::Generic(expr.clone())
                }
            }
            _ => Self::Generic(expr.clone()),
        }
    }

    fn compile_json_function(name: &str, args: &[Expr]) -> Option<Self> {
        let name = name.to_ascii_uppercase();
        match name.as_str() {
            "JSONB_PATH_EQ" if args.len() >= 3 => {
                let Expr::Column(column) = &args[0] else {
                    return None;
                };
                let Expr::Literal(Value::String(path)) = &args[1] else {
                    return None;
                };
                let Expr::Literal(expected) = &args[2] else {
                    return None;
                };
                Some(Self::JsonPathEq {
                    column_index: column.index,
                    path: SimpleJsonPath::parse(path)?,
                    expected: expected.clone(),
                })
            }
            "JSONB_EXISTS" if args.len() >= 2 => {
                let Expr::Column(column) = &args[0] else {
                    return None;
                };
                let Expr::Literal(Value::String(path)) = &args[1] else {
                    return None;
                };
                Some(Self::JsonPathExists {
                    column_index: column.index,
                    path: SimpleJsonPath::parse(path)?,
                })
            }
            _ => None,
        }
    }

    fn is_boolean_output(&self) -> bool {
        match self {
            Self::JsonPathEq { .. }
            | Self::JsonPathExists { .. }
            | Self::And(_, _)
            | Self::Or(_, _)
            | Self::Not(_) => true,
            Self::Generic(_) => false,
        }
    }

    fn eval(&self, arena: &TraceTupleArena, handle: &TraceTupleHandle) -> bool {
        match self {
            Self::JsonPathEq {
                column_index,
                path,
                expected,
            } => {
                let Some(Value::Jsonb(jsonb)) = arena.value_at(handle, *column_index) else {
                    return false;
                };
                path.extract(&jsonb.0)
                    .map(|actual| trace_json_raw_eq_value(actual, expected))
                    .unwrap_or(false)
            }
            Self::JsonPathExists { column_index, path } => {
                let Some(Value::Jsonb(jsonb)) = arena.value_at(handle, *column_index) else {
                    return false;
                };
                path.extract(&jsonb.0).is_some()
            }
            Self::And(left, right) => left.eval(arena, handle) && right.eval(arena, handle),
            Self::Or(left, right) => left.eval(arena, handle) || right.eval(arena, handle),
            Self::Not(inner) => !inner.eval(arena, handle),
            Self::Generic(expr) => {
                let mut value_at = |index: usize| arena.value_at(handle, index);
                matches!(eval_expr_with(expr, &mut value_at), Value::Boolean(true))
            }
        }
    }
}

fn trace_json_raw_eq_value(raw: &[u8], expected: &Value) -> bool {
    let raw = trim_json_bytes(raw);
    match expected {
        Value::Null => raw == b"null",
        Value::Boolean(value) => {
            let expected = if *value {
                b"true".as_slice()
            } else {
                b"false".as_slice()
            };
            raw == expected
        }
        Value::Int32(value) => trace_json_raw_number_eq(raw, *value as f64),
        Value::Int64(value) => trace_json_raw_number_eq(raw, *value as f64),
        Value::Float64(value) => trace_json_raw_number_eq(raw, *value),
        Value::String(value) => decode_json_string_literal(raw)
            .map(|actual| actual == *value)
            .unwrap_or(false),
        _ => false,
    }
}

fn trace_json_raw_number_eq(raw: &[u8], expected: f64) -> bool {
    core::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .map(|actual| (actual - expected).abs() < f64::EPSILON)
        .unwrap_or(false)
}

/// Evaluates an expression against a row.
fn eval_expr(expr: &Expr, row: &Row) -> Value {
    let mut value_at = |index: usize| row.get(index).cloned();
    eval_expr_with(expr, &mut value_at)
}

fn eval_expr_with<F>(expr: &Expr, value_at: &mut F) -> Value
where
    F: FnMut(usize) -> Option<Value>,
{
    match expr {
        Expr::Column(col_ref) => value_at(col_ref.index).unwrap_or(Value::Null),
        Expr::Literal(val) => val.clone(),
        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr_with(left, value_at);
            let rval = eval_expr_with(right, value_at);
            eval_binary_op(&lval, op, &rval)
        }
        Expr::UnaryOp { op, expr: inner } => {
            let val = eval_expr_with(inner, value_at);
            eval_unary_op(op, &val)
        }
        Expr::Function { name, args } => {
            let arg_values: Vec<Value> = args
                .iter()
                .map(|arg| eval_expr_with(arg, value_at))
                .collect();
            eval_function(name, &arg_values)
        }
        Expr::In { expr, list } => {
            let val = eval_expr_with(expr, value_at);
            let found = list
                .iter()
                .any(|item| eval_expr_with(item, value_at) == val);
            Value::Boolean(found)
        }
        Expr::NotIn { expr, list } => {
            let val = eval_expr_with(expr, value_at);
            let found = list
                .iter()
                .any(|item| eval_expr_with(item, value_at) == val);
            Value::Boolean(!found)
        }
        Expr::Between { expr, low, high } => {
            let val = eval_expr_with(expr, value_at);
            let lo = eval_expr_with(low, value_at);
            let hi = eval_expr_with(high, value_at);
            Value::Boolean(val >= lo && val <= hi)
        }
        Expr::NotBetween { expr, low, high } => {
            let val = eval_expr_with(expr, value_at);
            let lo = eval_expr_with(low, value_at);
            let hi = eval_expr_with(high, value_at);
            Value::Boolean(val < lo || val > hi)
        }
        Expr::Like { expr, pattern } => {
            let val = eval_expr_with(expr, value_at);
            if let Value::String(s) = val {
                Value::Boolean(cynos_core::pattern_match::like(&s, pattern))
            } else {
                Value::Boolean(false)
            }
        }
        Expr::NotLike { expr, pattern } => {
            let val = eval_expr_with(expr, value_at);
            if let Value::String(s) = val {
                Value::Boolean(!cynos_core::pattern_match::like(&s, pattern))
            } else {
                Value::Boolean(true)
            }
        }
        Expr::Match { expr, pattern } => {
            let val = eval_expr_with(expr, value_at);
            if let Value::String(s) = val {
                Value::Boolean(cynos_core::pattern_match::regex(&s, pattern))
            } else {
                Value::Boolean(false)
            }
        }
        Expr::NotMatch { expr, pattern } => {
            let val = eval_expr_with(expr, value_at);
            if let Value::String(s) = val {
                Value::Boolean(!cynos_core::pattern_match::regex(&s, pattern))
            } else {
                Value::Boolean(true)
            }
        }
        // Aggregate expressions are not expected in row-level evaluation.
        _ => Value::Null,
    }
}

fn eval_function(name: &str, args: &[Value]) -> Value {
    if name.eq_ignore_ascii_case("ABS") {
        return match args.first() {
            Some(Value::Int32(value)) => Value::Int32(value.abs()),
            Some(Value::Int64(value)) => Value::Int64(value.abs()),
            Some(Value::Float64(value)) => Value::Float64(value.abs()),
            _ => Value::Null,
        };
    }

    if name.eq_ignore_ascii_case("UPPER") {
        return match args.first() {
            Some(Value::String(value)) => Value::String(value.to_uppercase().into()),
            _ => Value::Null,
        };
    }

    if name.eq_ignore_ascii_case("LOWER") {
        return match args.first() {
            Some(Value::String(value)) => Value::String(value.to_lowercase().into()),
            _ => Value::Null,
        };
    }

    if name.eq_ignore_ascii_case("LENGTH") {
        return match args.first() {
            Some(Value::String(value)) => Value::Int64(value.len() as i64),
            _ => Value::Null,
        };
    }

    if name.eq_ignore_ascii_case("COALESCE") {
        for value in args {
            if !value.is_null() {
                return value.clone();
            }
        }
        return Value::Null;
    }

    if name.eq_ignore_ascii_case("JSONB_PATH_EQ") {
        return match (args.first(), args.get(1), args.get(2)) {
            (Some(Value::Jsonb(jsonb)), Some(Value::String(path)), Some(expected)) => {
                jsonb_path_eq(jsonb, path, expected)
            }
            _ => Value::Boolean(false),
        };
    }

    if name.eq_ignore_ascii_case("JSONB_CONTAINS") {
        return match (args.first(), args.get(1), args.get(2)) {
            (Some(Value::Jsonb(jsonb)), Some(Value::String(path)), Some(expected)) => {
                jsonb_contains(jsonb, path, expected)
            }
            _ => Value::Boolean(false),
        };
    }

    if name.eq_ignore_ascii_case("JSONB_EXISTS") {
        return match (args.first(), args.get(1)) {
            (Some(Value::Jsonb(jsonb)), Some(Value::String(path))) => {
                jsonb_path_exists(jsonb, path)
            }
            _ => Value::Boolean(false),
        };
    }

    Value::Null
}

fn jsonb_path_eq(jsonb: &cynos_core::JsonbValue, path: &str, expected: &Value) -> Value {
    let json_value = match parse_json_bytes(&jsonb.0) {
        Some(value) => value,
        None => return Value::Boolean(false),
    };

    let json_path = match JsonPath::parse(path) {
        Ok(path) => path,
        Err(_) => return Value::Boolean(false),
    };

    match json_value.query_first(&json_path) {
        Some(actual) => Value::Boolean(compare_jsonb_with_value(actual, expected)),
        None => Value::Boolean(false),
    }
}

fn jsonb_path_exists(jsonb: &cynos_core::JsonbValue, path: &str) -> Value {
    let json_value = match parse_json_bytes(&jsonb.0) {
        Some(value) => value,
        None => return Value::Boolean(false),
    };

    let json_path = match JsonPath::parse(path) {
        Ok(path) => path,
        Err(_) => return Value::Boolean(false),
    };

    Value::Boolean(json_value.query_first(&json_path).is_some())
}

fn jsonb_contains(jsonb: &cynos_core::JsonbValue, path: &str, expected: &Value) -> Value {
    let json_value = match parse_json_bytes(&jsonb.0) {
        Some(value) => value,
        None => return Value::Boolean(false),
    };

    let json_path = match JsonPath::parse(path) {
        Ok(path) => path,
        Err(_) => return Value::Boolean(false),
    };

    let Some(actual) = json_value.query_first(&json_path) else {
        return Value::Boolean(false);
    };

    let contains = match expected {
        Value::String(expected_string) => {
            jsonb_value_to_string(actual).contains(expected_string.as_str())
        }
        _ => compare_jsonb_with_value(actual, expected),
    };

    Value::Boolean(contains)
}

fn parse_json_bytes(bytes: &[u8]) -> Option<JsonbValue> {
    let json_str = core::str::from_utf8(bytes).ok()?;
    parse_json_text(json_str)
}

fn parse_json_text(s: &str) -> Option<JsonbValue> {
    let s = s.trim();
    if s == "null" {
        return Some(JsonbValue::Null);
    }
    if s == "true" {
        return Some(JsonbValue::Bool(true));
    }
    if s == "false" {
        return Some(JsonbValue::Bool(false));
    }
    if let Ok(number) = s.parse::<f64>() {
        return Some(JsonbValue::Number(number));
    }
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        return Some(JsonbValue::String(unescape_json(inner)));
    }
    if s.starts_with('{') {
        return parse_json_object(s);
    }
    if s.starts_with('[') {
        return parse_json_array(s);
    }
    None
}

fn unescape_json(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
                Some('r') => result.push('\r'),
                Some('"') => result.push('"'),
                Some('\\') => result.push('\\'),
                Some('/') => result.push('/'),
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn parse_json_object(s: &str) -> Option<JsonbValue> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return None;
    }

    let inner = s[1..s.len() - 1].trim();
    if inner.is_empty() {
        return Some(JsonbValue::Object(JsonbObject::new()));
    }

    let mut obj = JsonbObject::new();
    for pair in split_json_top_level(inner, ',') {
        let pair = pair.trim();
        let colon_pos = find_json_colon(pair)?;
        let key_str = pair[..colon_pos].trim();
        let val_str = pair[colon_pos + 1..].trim();
        if key_str.starts_with('"') && key_str.ends_with('"') && key_str.len() >= 2 {
            let key = unescape_json(&key_str[1..key_str.len() - 1]);
            let value = parse_json_text(val_str)?;
            obj.insert(key, value);
        } else {
            return None;
        }
    }

    Some(JsonbValue::Object(obj))
}

fn parse_json_array(s: &str) -> Option<JsonbValue> {
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        return None;
    }

    let inner = s[1..s.len() - 1].trim();
    if inner.is_empty() {
        return Some(JsonbValue::Array(Vec::new()));
    }

    let mut arr = Vec::new();
    for elem in split_json_top_level(inner, ',') {
        arr.push(parse_json_text(elem.trim())?);
    }
    Some(JsonbValue::Array(arr))
}

fn split_json_top_level(s: &str, sep: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    let mut start = 0usize;

    for (index, c) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if c == '\\' && in_string {
            escape = true;
            continue;
        }
        if c == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        if c == '{' || c == '[' {
            depth += 1;
        } else if c == '}' || c == ']' {
            depth -= 1;
        } else if c == sep && depth == 0 {
            parts.push(&s[start..index]);
            start = index + c.len_utf8();
        }
    }

    if start <= s.len() {
        parts.push(&s[start..]);
    }
    parts
}

fn find_json_colon(s: &str) -> Option<usize> {
    let mut in_string = false;
    let mut escape = false;
    for (index, c) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if c == '\\' && in_string {
            escape = true;
            continue;
        }
        if c == '"' {
            in_string = !in_string;
            continue;
        }
        if !in_string && c == ':' {
            return Some(index);
        }
    }
    None
}

fn compare_jsonb_with_value(jsonb: &JsonbValue, value: &Value) -> bool {
    match (jsonb, value) {
        (JsonbValue::Null, Value::Null) => true,
        (JsonbValue::Bool(left), Value::Boolean(right)) => left == right,
        (JsonbValue::Number(left), Value::Int32(right)) => {
            (*left - *right as f64).abs() < f64::EPSILON
        }
        (JsonbValue::Number(left), Value::Int64(right)) => {
            (*left - *right as f64).abs() < f64::EPSILON
        }
        (JsonbValue::Number(left), Value::Float64(right)) => (*left - *right).abs() < f64::EPSILON,
        (JsonbValue::String(left), Value::String(right)) => left == right,
        _ => false,
    }
}

fn jsonb_value_to_string(value: &JsonbValue) -> String {
    match value {
        JsonbValue::Null => String::from("null"),
        JsonbValue::Bool(boolean) => {
            if *boolean {
                String::from("true")
            } else {
                String::from("false")
            }
        }
        JsonbValue::Number(number) => alloc::format!("{}", number),
        JsonbValue::String(string) => string.clone(),
        _ => alloc::format!("{:?}", value),
    }
}

fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Value {
    match op {
        BinaryOp::Eq => Value::Boolean(left == right),
        BinaryOp::Ne => Value::Boolean(left != right),
        BinaryOp::Lt => Value::Boolean(left < right),
        BinaryOp::Le => Value::Boolean(left <= right),
        BinaryOp::Gt => Value::Boolean(left > right),
        BinaryOp::Ge => Value::Boolean(left >= right),
        BinaryOp::And => {
            let lb = matches!(left, Value::Boolean(true));
            let rb = matches!(right, Value::Boolean(true));
            Value::Boolean(lb && rb)
        }
        BinaryOp::Or => {
            let lb = matches!(left, Value::Boolean(true));
            let rb = matches!(right, Value::Boolean(true));
            Value::Boolean(lb || rb)
        }
        BinaryOp::Add => numeric_op(left, right, |a, b| a + b),
        BinaryOp::Sub => numeric_op(left, right, |a, b| a - b),
        BinaryOp::Mul => numeric_op(left, right, |a, b| a * b),
        BinaryOp::Div => numeric_op(left, right, |a, b| if b != 0.0 { a / b } else { 0.0 }),
        BinaryOp::Mod => numeric_op(left, right, |a, b| if b != 0.0 { a % b } else { 0.0 }),
        _ => Value::Null,
    }
}

fn eval_unary_op(op: &UnaryOp, val: &Value) -> Value {
    match op {
        UnaryOp::Not => match val {
            Value::Boolean(b) => Value::Boolean(!b),
            _ => Value::Null,
        },
        UnaryOp::Neg => match val {
            Value::Int32(v) => Value::Int32(-v),
            Value::Int64(v) => Value::Int64(-v),
            Value::Float64(v) => Value::Float64(-v),
            _ => Value::Null,
        },
        UnaryOp::IsNull => Value::Boolean(matches!(val, Value::Null)),
        UnaryOp::IsNotNull => Value::Boolean(!matches!(val, Value::Null)),
    }
}

fn numeric_op(left: &Value, right: &Value, op: fn(f64, f64) -> f64) -> Value {
    let l = as_f64(left);
    let r = as_f64(right);
    match (l, r) {
        (Some(a), Some(b)) => Value::Float64(op(a, b)),
        _ => Value::Null,
    }
}

fn as_f64(val: &Value) -> Option<f64> {
    match val {
        Value::Int32(v) => Some(*v as f64),
        Value::Int64(v) => Some(*v as f64),
        Value::Float64(v) => Some(*v),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Join key extraction
// ---------------------------------------------------------------------------

fn reorder_join_output(
    join_node: DataflowNode,
    raw_layout: CompileLayout,
    output_tables: &[String],
) -> CompiledNode {
    let desired_layout = CompileLayout::join(
        &raw_layout,
        &CompileLayout {
            tables: Vec::new(),
            table_column_counts: Vec::new(),
        },
        output_tables,
    );
    if raw_layout.tables == desired_layout.tables
        && raw_layout.table_column_counts == desired_layout.table_column_counts
    {
        return CompiledNode {
            dataflow: join_node,
            layout: raw_layout,
        };
    }

    let projection = raw_layout.projection_indices_for(output_tables);
    CompiledNode {
        dataflow: DataflowNode::project(join_node, projection),
        layout: desired_layout,
    }
}

/// Extracts left and right key extractor functions from a join condition.
/// Handles equi-join conditions like `left.col = right.col`.
fn extract_join_keys(
    condition: &Expr,
    left_layout: &CompileLayout,
    right_layout: &CompileLayout,
) -> (JoinKeySpec, JoinKeySpec) {
    let mut left_indices = Vec::new();
    let mut right_indices = Vec::new();
    collect_equi_join_keys(
        condition,
        left_layout,
        right_layout,
        &mut left_indices,
        &mut right_indices,
    );

    if left_indices.is_empty() || right_indices.is_empty() {
        return (
            JoinKeySpec::Dynamic(Box::new(|row: &Row| row.values().to_vec())),
            JoinKeySpec::Dynamic(Box::new(|row: &Row| row.values().to_vec())),
        );
    }

    (
        JoinKeySpec::Columns(left_indices),
        JoinKeySpec::Columns(right_indices),
    )
}

fn collect_equi_join_keys(
    expr: &Expr,
    left_layout: &CompileLayout,
    right_layout: &CompileLayout,
    left_indices: &mut Vec<usize>,
    right_indices: &mut Vec<usize>,
) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            if let (Some(left_col), Some(right_col)) =
                (extract_column_ref(left), extract_column_ref(right))
            {
                if left_layout.contains_table(&left_col.table)
                    && right_layout.contains_table(&right_col.table)
                {
                    left_indices
                        .push(left_layout.resolve_column_index(&left_col.table, left_col.index));
                    right_indices
                        .push(right_layout.resolve_column_index(&right_col.table, right_col.index));
                } else if left_layout.contains_table(&right_col.table)
                    && right_layout.contains_table(&left_col.table)
                {
                    left_indices
                        .push(left_layout.resolve_column_index(&right_col.table, right_col.index));
                    right_indices
                        .push(right_layout.resolve_column_index(&left_col.table, left_col.index));
                }
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            collect_equi_join_keys(left, left_layout, right_layout, left_indices, right_indices);
            collect_equi_join_keys(
                right,
                left_layout,
                right_layout,
                left_indices,
                right_indices,
            );
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bind_expr_to_layout(expr: &Expr, layout: &CompileLayout) -> Expr {
    match expr {
        Expr::Column(col_ref) => Expr::column(
            col_ref.table.clone(),
            col_ref.column.clone(),
            layout.resolve_column_index(&col_ref.table, col_ref.index),
        ),
        Expr::Literal(value) => Expr::Literal(value.clone()),
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(bind_expr_to_layout(left, layout)),
            op: *op,
            right: Box::new(bind_expr_to_layout(right, layout)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(bind_expr_to_layout(expr, layout)),
        },
        Expr::Function { name, args } => Expr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| bind_expr_to_layout(arg, layout))
                .collect(),
        },
        Expr::Aggregate {
            func,
            expr,
            distinct,
        } => Expr::Aggregate {
            func: *func,
            expr: expr
                .as_ref()
                .map(|inner| Box::new(bind_expr_to_layout(inner, layout))),
            distinct: *distinct,
        },
        Expr::Between { expr, low, high } => Expr::Between {
            expr: Box::new(bind_expr_to_layout(expr, layout)),
            low: Box::new(bind_expr_to_layout(low, layout)),
            high: Box::new(bind_expr_to_layout(high, layout)),
        },
        Expr::NotBetween { expr, low, high } => Expr::NotBetween {
            expr: Box::new(bind_expr_to_layout(expr, layout)),
            low: Box::new(bind_expr_to_layout(low, layout)),
            high: Box::new(bind_expr_to_layout(high, layout)),
        },
        Expr::In { expr, list } => Expr::In {
            expr: Box::new(bind_expr_to_layout(expr, layout)),
            list: list
                .iter()
                .map(|item| bind_expr_to_layout(item, layout))
                .collect(),
        },
        Expr::NotIn { expr, list } => Expr::NotIn {
            expr: Box::new(bind_expr_to_layout(expr, layout)),
            list: list
                .iter()
                .map(|item| bind_expr_to_layout(item, layout))
                .collect(),
        },
        Expr::Like { expr, pattern } => Expr::Like {
            expr: Box::new(bind_expr_to_layout(expr, layout)),
            pattern: pattern.clone(),
        },
        Expr::NotLike { expr, pattern } => Expr::NotLike {
            expr: Box::new(bind_expr_to_layout(expr, layout)),
            pattern: pattern.clone(),
        },
        Expr::Match { expr, pattern } => Expr::Match {
            expr: Box::new(bind_expr_to_layout(expr, layout)),
            pattern: pattern.clone(),
        },
        Expr::NotMatch { expr, pattern } => Expr::NotMatch {
            expr: Box::new(bind_expr_to_layout(expr, layout)),
            pattern: pattern.clone(),
        },
    }
}

fn extract_column_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Column(col_ref) => Some(col_ref.index),
        _ => None,
    }
}

fn extract_column_ref(expr: &Expr) -> Option<&cynos_query::ast::ColumnRef> {
    match expr {
        Expr::Column(col_ref) => Some(col_ref),
        _ => None,
    }
}

fn convert_join_type(jt: &QueryJoinType) -> IvmJoinType {
    match jt {
        QueryJoinType::Inner | QueryJoinType::Cross => IvmJoinType::Inner,
        QueryJoinType::LeftOuter => IvmJoinType::LeftOuter,
        QueryJoinType::RightOuter => IvmJoinType::RightOuter,
        QueryJoinType::FullOuter => IvmJoinType::FullOuter,
    }
}

fn convert_aggregate_func(func: &AggregateFunc) -> AggregateType {
    match func {
        AggregateFunc::Count => AggregateType::Count,
        AggregateFunc::Sum => AggregateType::Sum,
        AggregateFunc::Avg => AggregateType::Avg,
        AggregateFunc::Min => AggregateType::Min,
        AggregateFunc::Max => AggregateType::Max,
        // Unsupported aggregates fall back to Count
        _ => AggregateType::Count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cynos_core::{schema::TableBuilder, DataType};
    use cynos_query::ast::Expr;

    fn table_schemas(entries: &[(&str, &[&str])]) -> HashMap<String, Table> {
        entries
            .iter()
            .map(|(table, columns)| {
                let mut builder = TableBuilder::new(*table).unwrap();
                for column in *columns {
                    builder = builder.add_column(*column, DataType::String).unwrap();
                }
                ((*table).into(), builder.build().unwrap())
            })
            .collect()
    }

    fn jsonb_value(json: &str) -> Value {
        Value::Jsonb(cynos_core::JsonbValue::new(json.as_bytes().to_vec()))
    }

    #[test]
    fn test_compile_table_scan() {
        let plan = PhysicalPlan::table_scan("users");
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);
        let table_schemas = table_schemas(&[("users", &["id", "name"])]);

        let result = compile_to_dataflow(&plan, &table_ids, &table_schemas).unwrap();
        assert!(matches!(
            result.dataflow,
            DataflowNode::Source { table_id: 1 }
        ));
    }

    #[test]
    fn test_compile_filter() {
        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::gt(Expr::column("users", "age", 1), Expr::literal(18i64)),
        );
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);
        let table_schemas = table_schemas(&[("users", &["id", "age"])]);

        let result = compile_to_dataflow(&plan, &table_ids, &table_schemas).unwrap();
        match result.dataflow {
            DataflowNode::Filter {
                trace_predicate, ..
            } => assert!(trace_predicate.is_some()),
            _ => panic!("Expected Filter node"),
        }
    }

    #[test]
    fn test_compile_index_get_lowers_to_filtered_source() {
        use cynos_incremental::{Delta, MaterializedView};

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
        let pk_name = users.primary_key().unwrap().name().to_string();
        let mut table_schemas = HashMap::new();
        table_schemas.insert("users".into(), users);

        let plan = PhysicalPlan::index_get("users", pk_name, Value::Int64(1));
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);

        let result = compile_to_dataflow(&plan, &table_ids, &table_schemas).unwrap();
        assert!(matches!(result.dataflow, DataflowNode::Filter { .. }));

        let mut view = MaterializedView::new(result.dataflow);
        view.on_table_change(
            1,
            vec![
                Delta::insert(Row::new(
                    1,
                    vec![Value::Int64(1), Value::String("Alice".into())],
                )),
                Delta::insert(Row::new(
                    2,
                    vec![Value::Int64(2), Value::String("Bob".into())],
                )),
            ],
        );

        let rows = view.result();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Int64(1)));
    }

    #[test]
    fn test_compile_reverse_index_scan_is_not_incrementalizable() {
        let users = TableBuilder::new("users")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();
        let pk_name = users.primary_key().unwrap().name().to_string();
        let mut table_schemas = HashMap::new();
        table_schemas.insert("users".into(), users);

        let plan = PhysicalPlan::IndexScan {
            table: "users".into(),
            index: pk_name,
            bounds: IndexBounds::Unbounded,
            limit: None,
            offset: None,
            reverse: true,
        };
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);

        assert!(compile_to_dataflow(&plan, &table_ids, &table_schemas).is_none());
    }

    #[test]
    fn test_compile_non_incrementalizable() {
        let plan = PhysicalPlan::sort(
            PhysicalPlan::table_scan("users"),
            alloc::vec![(
                Expr::column("users", "id", 0),
                cynos_query::ast::SortOrder::Asc
            )],
        );
        let table_ids = HashMap::new();
        let table_schemas = table_schemas(&[("users", &["id"])]);
        assert!(compile_to_dataflow(&plan, &table_ids, &table_schemas).is_none());
    }

    #[test]
    fn test_compile_hash_join() {
        use cynos_query::ast::JoinType;
        let plan = PhysicalPlan::hash_join(
            PhysicalPlan::table_scan("employees"),
            PhysicalPlan::table_scan("departments"),
            Expr::eq(
                Expr::column("employees", "dept_id", 2),
                Expr::column("departments", "id", 0),
            ),
            JoinType::LeftOuter,
        );
        let mut table_ids = HashMap::new();
        table_ids.insert("employees".into(), 1u32);
        table_ids.insert("departments".into(), 2u32);
        let table_schemas = table_schemas(&[
            ("employees", &["id", "name", "dept_id"]),
            ("departments", &["id", "name"]),
        ]);

        let result = compile_to_dataflow(&plan, &table_ids, &table_schemas).unwrap();
        match &result.dataflow {
            DataflowNode::Join { join_type, .. } => {
                assert_eq!(*join_type, IvmJoinType::LeftOuter);
            }
            _ => panic!("Expected Join node"),
        }
    }

    #[test]
    fn test_compile_computed_project_includes_trace_mapper() {
        let plan = PhysicalPlan::project(
            PhysicalPlan::table_scan("users"),
            alloc::vec![
                Expr::column("users", "id", 0),
                Expr::Function {
                    name: "UPPER".into(),
                    args: alloc::vec![Expr::column("users", "name", 1)],
                },
            ],
        );
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);
        let table_schemas = table_schemas(&[("users", &["id", "name"])]);

        let result = compile_to_dataflow(&plan, &table_ids, &table_schemas).unwrap();
        match result.dataflow {
            DataflowNode::Map { trace_mapper, .. } => assert!(trace_mapper.is_some()),
            _ => panic!("Expected Map node"),
        }
    }

    #[test]
    fn test_compile_reordered_join_wraps_join_with_projection() {
        use cynos_query::ast::JoinType;

        let plan = PhysicalPlan::hash_join_with_output_tables(
            PhysicalPlan::table_scan("departments"),
            PhysicalPlan::table_scan("employees"),
            Expr::eq(
                Expr::column("employees", "dept_id", 1),
                Expr::column("departments", "id", 0),
            ),
            JoinType::Inner,
            alloc::vec!["employees".into(), "departments".into()],
        );
        let mut table_ids = HashMap::new();
        table_ids.insert("employees".into(), 1u32);
        table_ids.insert("departments".into(), 2u32);
        let table_schemas = table_schemas(&[
            ("employees", &["id", "dept_id"]),
            ("departments", &["id", "name"]),
        ]);

        let result = compile_to_dataflow(&plan, &table_ids, &table_schemas).unwrap();
        match &result.dataflow {
            DataflowNode::Project { input, columns } => {
                assert_eq!(columns, &[2, 3, 0, 1]);
                assert!(matches!(input.as_ref(), DataflowNode::Join { .. }));
            }
            other => panic!(
                "Expected Project over Join, got {:?}",
                core::mem::discriminant(other)
            ),
        }
    }

    #[test]
    fn test_compile_aggregate() {
        let plan = PhysicalPlan::hash_aggregate(
            PhysicalPlan::table_scan("orders"),
            alloc::vec![Expr::column("orders", "customer_id", 0)],
            alloc::vec![
                (AggregateFunc::Count, Expr::column("orders", "id", 1)),
                (AggregateFunc::Sum, Expr::column("orders", "amount", 2)),
            ],
        );
        let mut table_ids = HashMap::new();
        table_ids.insert("orders".into(), 1u32);
        let table_schemas = table_schemas(&[("orders", &["customer_id", "id", "amount"])]);

        let result = compile_to_dataflow(&plan, &table_ids, &table_schemas).unwrap();
        match &result.dataflow {
            DataflowNode::Aggregate {
                group_by,
                functions,
                ..
            } => {
                assert_eq!(group_by, &[0]);
                assert_eq!(functions.len(), 2);
                assert_eq!(functions[0].1, AggregateType::Count);
                assert_eq!(functions[1].1, AggregateType::Sum);
            }
            _ => panic!("Expected Aggregate node"),
        }
    }

    #[test]
    fn test_eval_in_expr() {
        let row = Row::new(1, vec![Value::Int64(3), Value::String("Alice".into())]);
        let expr = Expr::In {
            expr: Box::new(Expr::column("t", "id", 0)),
            list: vec![
                Expr::literal(Value::Int64(1)),
                Expr::literal(Value::Int64(3)),
                Expr::literal(Value::Int64(5)),
            ],
        };
        assert_eq!(eval_expr(&expr, &row), Value::Boolean(true));

        let expr_miss = Expr::In {
            expr: Box::new(Expr::column("t", "id", 0)),
            list: vec![
                Expr::literal(Value::Int64(2)),
                Expr::literal(Value::Int64(4)),
            ],
        };
        assert_eq!(eval_expr(&expr_miss, &row), Value::Boolean(false));
    }

    #[test]
    fn test_eval_not_in_expr() {
        let row = Row::new(1, vec![Value::Int64(3)]);
        let expr = Expr::NotIn {
            expr: Box::new(Expr::column("t", "id", 0)),
            list: vec![
                Expr::literal(Value::Int64(1)),
                Expr::literal(Value::Int64(3)),
            ],
        };
        assert_eq!(eval_expr(&expr, &row), Value::Boolean(false));
    }

    #[test]
    fn test_eval_between_expr() {
        let row = Row::new(1, vec![Value::Int64(15)]);
        let expr = Expr::Between {
            expr: Box::new(Expr::column("t", "v", 0)),
            low: Box::new(Expr::literal(Value::Int64(10))),
            high: Box::new(Expr::literal(Value::Int64(20))),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Boolean(true));

        let row_out = Row::new(2, vec![Value::Int64(25)]);
        assert_eq!(eval_expr(&expr, &row_out), Value::Boolean(false));
    }

    #[test]
    fn test_eval_like_expr() {
        let row = Row::new(1, vec![Value::String("Alice".into())]);
        let expr = Expr::Like {
            expr: Box::new(Expr::column("t", "name", 0)),
            pattern: "Al%".into(),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Boolean(true));

        let expr2 = Expr::Like {
            expr: Box::new(Expr::column("t", "name", 0)),
            pattern: "Bo%".into(),
        };
        assert_eq!(eval_expr(&expr2, &row), Value::Boolean(false));

        // underscore wildcard
        let expr3 = Expr::Like {
            expr: Box::new(Expr::column("t", "name", 0)),
            pattern: "A_ice".into(),
        };
        assert_eq!(eval_expr(&expr3, &row), Value::Boolean(true));
    }

    #[test]
    fn test_eval_match_expr() {
        let row = Row::new(1, vec![Value::String("abc123".into())]);
        let expr = Expr::Match {
            expr: Box::new(Expr::column("t", "v", 0)),
            pattern: "\\d+".into(),
        };
        assert_eq!(eval_expr(&expr, &row), Value::Boolean(true));

        let expr2 = Expr::Match {
            expr: Box::new(Expr::column("t", "v", 0)),
            pattern: "^[A-Z]".into(),
        };
        assert_eq!(eval_expr(&expr2, &row), Value::Boolean(false));
    }

    #[test]
    fn test_in_filter_via_compile() {
        // End-to-end: compile a Filter with IN predicate, then run through MaterializedView
        use cynos_incremental::{Delta, MaterializedView};

        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("users"),
            Expr::In {
                expr: Box::new(Expr::column("users", "id", 0)),
                list: vec![
                    Expr::literal(Value::Int64(1)),
                    Expr::literal(Value::Int64(3)),
                ],
            },
        );
        let mut table_ids = HashMap::new();
        table_ids.insert("users".into(), 1u32);
        let table_schemas = table_schemas(&[("users", &["id", "name"])]);

        let result = compile_to_dataflow(&plan, &table_ids, &table_schemas).unwrap();
        let mut view = MaterializedView::new(result.dataflow);

        // Insert rows: id=1 (match), id=2 (no match), id=3 (match)
        view.on_table_change(
            1,
            vec![
                Delta::insert(Row::new(
                    1,
                    vec![Value::Int64(1), Value::String("Alice".into())],
                )),
                Delta::insert(Row::new(
                    2,
                    vec![Value::Int64(2), Value::String("Bob".into())],
                )),
                Delta::insert(Row::new(
                    3,
                    vec![Value::Int64(3), Value::String("Carol".into())],
                )),
            ],
        );

        assert_eq!(view.len(), 2); // only id=1 and id=3
        let rows = view.result();
        let ids: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.get(0).and_then(|v| v.as_i64()))
            .collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
        assert!(!ids.contains(&2));
    }

    #[test]
    fn test_filter_over_reordered_join_uses_logical_output_layout() {
        use cynos_incremental::{Delta, MaterializedView};
        use cynos_query::ast::JoinType;

        let join = PhysicalPlan::hash_join_with_output_tables(
            PhysicalPlan::table_scan("departments"),
            PhysicalPlan::table_scan("employees"),
            Expr::eq(
                Expr::column("employees", "dept_id", 1),
                Expr::column("departments", "id", 0),
            ),
            JoinType::Inner,
            alloc::vec!["employees".into(), "departments".into()],
        );
        let plan = PhysicalPlan::filter(
            join,
            Expr::eq(Expr::column("employees", "id", 0), Expr::literal(1i64)),
        );

        let mut table_ids = HashMap::new();
        table_ids.insert("employees".into(), 1u32);
        table_ids.insert("departments".into(), 2u32);
        let table_schemas = table_schemas(&[
            ("employees", &["id", "dept_id"]),
            ("departments", &["id", "name"]),
        ]);

        let result = compile_to_dataflow(&plan, &table_ids, &table_schemas).unwrap();
        let mut view = MaterializedView::new(result.dataflow);

        view.on_table_change(
            1,
            vec![
                Delta::insert(Row::new(1, vec![Value::Int64(1), Value::Int64(10)])),
                Delta::insert(Row::new(2, vec![Value::Int64(2), Value::Int64(20)])),
            ],
        );
        view.on_table_change(
            2,
            vec![
                Delta::insert(Row::new(
                    10,
                    vec![Value::Int64(10), Value::String("Engineering".into())],
                )),
                Delta::insert(Row::new(
                    20,
                    vec![Value::Int64(20), Value::String("Sales".into())],
                )),
            ],
        );

        let rows = view.result();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Int64(1)));
        assert_eq!(rows[0].get(2), Some(&Value::Int64(10)));
        assert_eq!(rows[0].get(3), Some(&Value::String("Engineering".into())));
    }

    #[test]
    fn test_filter_with_json_function_propagates_updates() {
        use cynos_incremental::{Delta, MaterializedView};

        let projects = TableBuilder::new("projects")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("healthScore", DataType::Int32)
            .unwrap()
            .add_column("metadata", DataType::Jsonb)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut table_schemas = HashMap::new();
        table_schemas.insert("projects".into(), projects);

        let plan = PhysicalPlan::filter(
            PhysicalPlan::table_scan("projects"),
            Expr::and(
                Expr::ge(
                    Expr::column("projects", "healthScore", 1),
                    Expr::Literal(Value::Int32(45)),
                ),
                Expr::jsonb_path_eq(
                    Expr::column("projects", "metadata", 2),
                    "$.risk.bucket",
                    Value::String("high".into()),
                ),
            ),
        );

        let mut table_ids = HashMap::new();
        table_ids.insert("projects".into(), 1u32);

        let result = compile_to_dataflow(&plan, &table_ids, &table_schemas).unwrap();
        let mut view = MaterializedView::new(result.dataflow);

        let initial_row = Row::new(
            1,
            vec![
                Value::Int64(1),
                Value::Int32(61),
                jsonb_value(r#"{"risk":{"bucket":"high"}}"#),
            ],
        );
        let updated_row = Row::new(
            1,
            vec![
                Value::Int64(1),
                Value::Int32(12),
                jsonb_value(r#"{"risk":{"bucket":"high"}}"#),
            ],
        );

        let initial_deltas =
            view.on_table_change(1, alloc::vec![Delta::insert(initial_row.clone())]);
        assert_eq!(initial_deltas.len(), 1);
        assert!(initial_deltas[0].is_insert());
        assert_eq!(view.len(), 1);

        let update_deltas = view.on_table_change(
            1,
            alloc::vec![Delta::delete(initial_row), Delta::insert(updated_row)],
        );
        assert_eq!(update_deltas.len(), 1);
        assert!(update_deltas[0].is_delete());
        assert_eq!(view.len(), 0);
    }

    #[test]
    fn test_compiled_trace_json_predicate_matches_generic_eval() {
        let expr = Expr::and(
            Expr::ge(
                Expr::column("projects", "healthScore", 1),
                Expr::literal(45i32),
            ),
            Expr::jsonb_path_eq(
                Expr::column("projects", "metadata", 2),
                "$.risk.bucket",
                Value::String("high".into()),
            ),
        );
        let compiled = CompiledTracePredicate::compile(&expr);
        let arena = TraceTupleArena::default();

        let matching = arena.owned(Row::new(
            1,
            vec![
                Value::Int64(1),
                Value::Int32(61),
                jsonb_value(r#"{"risk":{"bucket":"high"}}"#),
            ],
        ));
        let low_score = arena.owned(Row::new(
            2,
            vec![
                Value::Int64(2),
                Value::Int32(12),
                jsonb_value(r#"{"risk":{"bucket":"high"}}"#),
            ],
        ));
        let different_bucket = arena.owned(Row::new(
            3,
            vec![
                Value::Int64(3),
                Value::Int32(61),
                jsonb_value(r#"{"risk":{"bucket":"low"}}"#),
            ],
        ));

        assert!(compiled.eval(&arena, &matching));
        assert!(!compiled.eval(&arena, &low_score));
        assert!(!compiled.eval(&arena, &different_bucket));
    }

    #[test]
    fn test_compiled_trace_json_exists_supports_array_index_path() {
        let expr = Expr::Function {
            name: "JSONB_EXISTS".into(),
            args: alloc::vec![
                Expr::column("projects", "metadata", 0),
                Expr::literal("$.risk.history[1]"),
            ],
        };
        let compiled = CompiledTracePredicate::compile(&expr);
        let arena = TraceTupleArena::default();

        let matching = arena.owned(Row::new(
            1,
            vec![jsonb_value(r#"{"risk":{"history":["low","high"]}}"#)],
        ));
        let missing = arena.owned(Row::new(
            2,
            vec![jsonb_value(r#"{"risk":{"history":["low"]}}"#)],
        ));

        assert!(compiled.eval(&arena, &matching));
        assert!(!compiled.eval(&arena, &missing));
    }

    #[test]
    fn test_compiled_trace_json_predicate_matches_generic_escaped_value() {
        let expr = Expr::jsonb_path_eq(
            Expr::column("projects", "metadata", 0),
            "$.risk.label",
            Value::String("hi\nthere".into()),
        );
        let compiled = CompiledTracePredicate::compile(&expr);
        let arena = TraceTupleArena::default();

        let matching = arena.owned(Row::new(
            1,
            vec![jsonb_value(r#"{"risk":{"label":"hi\nthere"}}"#)],
        ));
        let different = arena.owned(Row::new(
            2,
            vec![jsonb_value(r#"{"risk":{"label":"hi there"}}"#)],
        ));

        assert!(compiled.eval(&arena, &matching));
        assert!(!compiled.eval(&arena, &different));
    }

    #[test]
    fn test_filter_with_json_function_over_multi_join_propagates_updates() {
        use cynos_incremental::{Delta, MaterializedView};
        use cynos_query::ast::JoinType;

        let projects = TableBuilder::new("projects")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("organizationId", DataType::Int64)
            .unwrap()
            .add_column("healthScore", DataType::Int32)
            .unwrap()
            .add_column("metadata", DataType::Jsonb)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();
        let organizations = TableBuilder::new("organizations")
            .unwrap()
            .add_column("id", DataType::Int64)
            .unwrap()
            .add_column("name", DataType::String)
            .unwrap()
            .add_primary_key(&["id"], false)
            .unwrap()
            .build()
            .unwrap();
        let counters = TableBuilder::new("projectCounters")
            .unwrap()
            .add_column("projectId", DataType::Int64)
            .unwrap()
            .add_column("openIssueCount", DataType::Int32)
            .unwrap()
            .add_primary_key(&["projectId"], false)
            .unwrap()
            .build()
            .unwrap();
        let snapshots = TableBuilder::new("projectSnapshots")
            .unwrap()
            .add_column("projectId", DataType::Int64)
            .unwrap()
            .add_column("velocity", DataType::Int32)
            .unwrap()
            .add_primary_key(&["projectId"], false)
            .unwrap()
            .build()
            .unwrap();

        let mut table_schemas = HashMap::new();
        table_schemas.insert("projects".into(), projects);
        table_schemas.insert("organizations".into(), organizations);
        table_schemas.insert("projectCounters".into(), counters);
        table_schemas.insert("projectSnapshots".into(), snapshots);

        let project_org_join = PhysicalPlan::hash_join_with_output_tables(
            PhysicalPlan::table_scan("projects"),
            PhysicalPlan::table_scan("organizations"),
            Expr::eq(
                Expr::column("projects", "organizationId", 1),
                Expr::column("organizations", "id", 0),
            ),
            JoinType::LeftOuter,
            alloc::vec!["projects".into(), "organizations".into()],
        );
        let with_counters = PhysicalPlan::hash_join_with_output_tables(
            project_org_join,
            PhysicalPlan::table_scan("projectCounters"),
            Expr::eq(
                Expr::column("projects", "id", 0),
                Expr::column("projectCounters", "projectId", 0),
            ),
            JoinType::LeftOuter,
            alloc::vec![
                "projects".into(),
                "organizations".into(),
                "projectCounters".into(),
            ],
        );
        let with_snapshots = PhysicalPlan::hash_join_with_output_tables(
            with_counters,
            PhysicalPlan::table_scan("projectSnapshots"),
            Expr::eq(
                Expr::column("projects", "id", 0),
                Expr::column("projectSnapshots", "projectId", 0),
            ),
            JoinType::LeftOuter,
            alloc::vec![
                "projects".into(),
                "organizations".into(),
                "projectCounters".into(),
                "projectSnapshots".into(),
            ],
        );
        let plan = PhysicalPlan::filter(
            with_snapshots,
            Expr::and(
                Expr::and(
                    Expr::and(
                        Expr::ge(
                            Expr::column("projects", "healthScore", 2),
                            Expr::Literal(Value::Int32(45)),
                        ),
                        Expr::jsonb_path_eq(
                            Expr::column("projects", "metadata", 3),
                            "$.risk.bucket",
                            Value::String("high".into()),
                        ),
                    ),
                    Expr::ge(
                        Expr::column("projectCounters", "openIssueCount", 1),
                        Expr::Literal(Value::Int32(4)),
                    ),
                ),
                Expr::ge(
                    Expr::column("projectSnapshots", "velocity", 1),
                    Expr::Literal(Value::Int32(20)),
                ),
            ),
        );

        let mut table_ids = HashMap::new();
        table_ids.insert("projects".into(), 1u32);
        table_ids.insert("organizations".into(), 2u32);
        table_ids.insert("projectCounters".into(), 3u32);
        table_ids.insert("projectSnapshots".into(), 4u32);

        let result = compile_to_dataflow(&plan, &table_ids, &table_schemas).unwrap();
        let mut view = MaterializedView::new(result.dataflow);

        view.on_table_change(
            2,
            alloc::vec![Delta::insert(Row::new(
                10,
                vec![Value::Int64(10), Value::String("Org".into())],
            ))],
        );
        view.on_table_change(
            3,
            alloc::vec![Delta::insert(Row::new(
                1,
                vec![Value::Int64(1), Value::Int32(7)],
            ))],
        );
        view.on_table_change(
            4,
            alloc::vec![Delta::insert(Row::new(
                1,
                vec![Value::Int64(1), Value::Int32(35)],
            ))],
        );

        let initial_row = Row::new(
            1,
            vec![
                Value::Int64(1),
                Value::Int64(10),
                Value::Int32(61),
                jsonb_value(r#"{"risk":{"bucket":"high"}}"#),
            ],
        );
        let updated_row = Row::new(
            1,
            vec![
                Value::Int64(1),
                Value::Int64(10),
                Value::Int32(12),
                jsonb_value(r#"{"risk":{"bucket":"high"}}"#),
            ],
        );

        let initial_deltas =
            view.on_table_change(1, alloc::vec![Delta::insert(initial_row.clone())]);
        assert_eq!(initial_deltas.len(), 1);
        assert!(initial_deltas[0].is_insert());
        assert_eq!(view.len(), 1);

        let update_deltas = view.on_table_change(
            1,
            alloc::vec![Delta::delete(initial_row), Delta::insert(updated_row)],
        );
        assert_eq!(update_deltas.len(), 1);
        assert!(update_deltas[0].is_delete());
        assert_eq!(view.len(), 0);
    }
}
