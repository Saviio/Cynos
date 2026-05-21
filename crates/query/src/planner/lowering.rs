use crate::ast::Expr;
use crate::planner::{JoinAlgorithm, LogicalPlan, PhysicalPlan};
use alloc::boxed::Box;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use cynos_core::Value;

pub(crate) fn logical_to_physical(plan: LogicalPlan) -> PhysicalPlan {
    match plan {
        LogicalPlan::Scan { table } => PhysicalPlan::table_scan(table),

        LogicalPlan::IndexScan {
            table,
            index,
            bounds,
        } => PhysicalPlan::IndexScan {
            table,
            index,
            bounds,
            limit: None,
            offset: None,
            reverse: false,
        },

        LogicalPlan::IndexGet { table, index, key } => PhysicalPlan::index_get(table, index, key),

        LogicalPlan::IndexInGet { table, index, keys } => {
            PhysicalPlan::index_in_get(table, index, keys)
        }

        LogicalPlan::GinIndexScan {
            table,
            index,
            column: _,
            column_index: _,
            path,
            value,
            query_type,
            recheck,
        } => {
            let key = gin_path_to_key(&path);
            let value_str = value.map(gin_lookup_value_to_string);
            PhysicalPlan::gin_index_scan(table, index, key, value_str, query_type, recheck)
        }

        LogicalPlan::GinIndexScanMulti {
            table,
            index,
            column: _,
            pairs,
            match_all,
            recheck,
        } => {
            let string_pairs: Vec<(String, String)> = pairs
                .into_iter()
                .map(|(path, value)| (gin_path_to_key(&path), gin_lookup_value_to_string(value)))
                .collect();
            PhysicalPlan::gin_index_scan_multi(table, index, string_pairs, match_all, recheck)
        }

        LogicalPlan::Filter { input, predicate } => {
            let input_physical = logical_to_physical(*input);
            PhysicalPlan::filter(input_physical, predicate)
        }

        LogicalPlan::Project { input, columns } => {
            let input_physical = logical_to_physical(*input);
            PhysicalPlan::project(input_physical, columns)
        }

        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
            output_tables,
        } => {
            let left_physical = logical_to_physical(*left);
            let right_physical = logical_to_physical(*right);

            match choose_join_algorithm(&condition) {
                JoinAlgorithm::Hash => PhysicalPlan::hash_join_with_output_tables(
                    left_physical,
                    right_physical,
                    condition,
                    join_type,
                    output_tables,
                ),
                JoinAlgorithm::SortMerge => PhysicalPlan::sort_merge_join_with_output_tables(
                    left_physical,
                    right_physical,
                    condition,
                    join_type,
                    output_tables,
                ),
                JoinAlgorithm::NestedLoop | JoinAlgorithm::IndexNestedLoop => {
                    PhysicalPlan::nested_loop_join_with_output_tables(
                        left_physical,
                        right_physical,
                        condition,
                        join_type,
                        output_tables,
                    )
                }
            }
        }

        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let input_physical = logical_to_physical(*input);
            PhysicalPlan::hash_aggregate(input_physical, group_by, aggregates)
        }

        LogicalPlan::Sort { input, order_by } => {
            let input_physical = logical_to_physical(*input);
            PhysicalPlan::sort(input_physical, order_by)
        }

        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            let input_physical = logical_to_physical(*input);
            PhysicalPlan::limit(input_physical, limit, offset)
        }

        LogicalPlan::CrossProduct { left, right } => {
            let left_physical = logical_to_physical(*left);
            let right_physical = logical_to_physical(*right);
            PhysicalPlan::CrossProduct {
                left: Box::new(left_physical),
                right: Box::new(right_physical),
            }
        }

        LogicalPlan::Union { left, right, all } => {
            let left_physical = logical_to_physical(*left);
            let right_physical = logical_to_physical(*right);
            PhysicalPlan::union(left_physical, right_physical, all)
        }

        LogicalPlan::Empty => PhysicalPlan::Empty,
    }
}

fn choose_join_algorithm(condition: &Expr) -> JoinAlgorithm {
    if condition.is_equi_join() {
        return JoinAlgorithm::Hash;
    }

    if condition.is_range_join() {
        return JoinAlgorithm::NestedLoop;
    }

    JoinAlgorithm::NestedLoop
}

fn gin_path_to_key(path: &str) -> String {
    path.trim_start_matches("$.").to_string()
}

fn gin_lookup_value_to_string(value: Value) -> String {
    match value {
        Value::String(value) => value,
        Value::Int32(value) => format!("{}", value),
        Value::Int64(value) => format!("{}", value),
        Value::Float64(value) => format!("{}", value),
        Value::Boolean(value) => format!("{}", value),
        _ => format!("{:?}", value),
    }
}
