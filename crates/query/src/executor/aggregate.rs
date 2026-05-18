//! Aggregate executor.

use crate::ast::AggregateFunc;
use crate::executor::{Relation, RelationEntry, SharedTables};
use alloc::collections::{BTreeMap, BTreeSet};
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::{aggregate_group_row_id, Row, Value};
use libm::{exp, log, sqrt};

/// Aggregate executor - computes aggregate functions.
pub struct AggregateExecutor {
    /// Group by column indices.
    group_by: Vec<usize>,
    /// Aggregates to compute: (function, column_index).
    aggregates: Vec<(AggregateFunc, Option<usize>)>,
}

struct GroupState {
    group_values: Vec<Value>,
    version_sum: u64,
    aggregate_states: Vec<AggregateState>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum GroupKeyValue {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(u64),
    String(String),
    DateTime(i64),
    Bytes(Vec<u8>),
    Jsonb(Vec<u8>),
}

enum SumOutputMode {
    Integer,
    Float,
}

enum AggregateState {
    CountAll {
        count: i64,
    },
    CountNonNull {
        column_index: usize,
        count: i64,
    },
    Sum {
        column_index: usize,
        sum: f64,
        output_mode: SumOutputMode,
    },
    Avg {
        column_index: usize,
        sum: f64,
        count: u64,
    },
    Min {
        column_index: usize,
        value: Option<Value>,
    },
    Max {
        column_index: usize,
        value: Option<Value>,
    },
    Distinct {
        column_index: usize,
        seen: BTreeSet<Value>,
    },
    StdDev {
        column_index: usize,
        count: u64,
        mean: f64,
        m2: f64,
    },
    GeoMean {
        column_index: usize,
        count: u64,
        log_sum: f64,
    },
}

impl AggregateExecutor {
    /// Creates a new aggregate executor.
    pub fn new(group_by: Vec<usize>, aggregates: Vec<(AggregateFunc, Option<usize>)>) -> Self {
        Self {
            group_by,
            aggregates,
        }
    }

    /// Creates an aggregate executor with no grouping.
    pub fn no_group(aggregates: Vec<(AggregateFunc, Option<usize>)>) -> Self {
        Self::new(Vec::new(), aggregates)
    }

    /// Executes the aggregation on the input relation.
    pub fn execute(&self, input: Relation) -> Relation {
        let tables = input.tables().to_vec();
        let shared_tables: SharedTables = tables.clone().into();
        let result_column_count = self.group_by.len() + self.aggregates.len();

        if self.group_by.is_empty() {
            let mut version_sum = 0u64;
            let mut states = self.init_states();
            for entry in input.iter() {
                version_sum = version_sum.wrapping_add(entry.row.version());
                self.update_states(&mut states, entry);
            }
            let values = self.finalize_states(states);
            let entry = RelationEntry::new_combined(
                Rc::new(Row::new_with_version(
                    aggregate_group_row_id(&[]),
                    version_sum,
                    values,
                )),
                shared_tables,
            );
            return Relation {
                entries: alloc::vec![entry],
                tables,
                table_column_counts: alloc::vec![result_column_count],
            };
        }

        let mut groups: BTreeMap<Vec<GroupKeyValue>, GroupState> = BTreeMap::new();

        for entry in input.iter() {
            let group_values = self.extract_group_values(entry);
            let group_key = GroupKeyValue::from_values(&group_values);
            let group = groups.entry(group_key).or_insert_with(|| GroupState {
                group_values,
                version_sum: 0,
                aggregate_states: self.init_states(),
            });
            group.version_sum = group.version_sum.wrapping_add(entry.row.version());
            self.update_states(&mut group.aggregate_states, entry);
        }

        let entries: Vec<RelationEntry> = groups
            .into_iter()
            .map(|(_, group_state)| {
                let row_id = aggregate_group_row_id(&group_state.group_values);
                let mut values = group_state.group_values;
                values.extend(self.finalize_states(group_state.aggregate_states));

                RelationEntry::new_combined(
                    Rc::new(Row::new_with_version(
                        row_id,
                        group_state.version_sum,
                        values,
                    )),
                    shared_tables.clone(),
                )
            })
            .collect();

        Relation {
            entries,
            tables,
            table_column_counts: alloc::vec![result_column_count],
        }
    }

    fn extract_group_values(&self, entry: &RelationEntry) -> Vec<Value> {
        self.group_by
            .iter()
            .map(|&idx| entry.get_field(idx).cloned().unwrap_or(Value::Null))
            .collect()
    }

    fn init_states(&self) -> Vec<AggregateState> {
        self.aggregates
            .iter()
            .map(|(func, column_index)| AggregateState::new(*func, *column_index))
            .collect()
    }

    fn update_states(&self, states: &mut [AggregateState], entry: &RelationEntry) {
        for state in states {
            state.update(entry);
        }
    }

    fn finalize_states(&self, states: Vec<AggregateState>) -> Vec<Value> {
        states.into_iter().map(AggregateState::finalize).collect()
    }
}

impl AggregateState {
    fn new(func: AggregateFunc, column_index: Option<usize>) -> Self {
        match func {
            AggregateFunc::Count => match column_index {
                Some(column_index) => Self::CountNonNull {
                    column_index,
                    count: 0,
                },
                None => Self::CountAll { count: 0 },
            },
            AggregateFunc::Sum => Self::Sum {
                column_index: column_index.unwrap_or(0),
                sum: 0.0,
                output_mode: SumOutputMode::Integer,
            },
            AggregateFunc::Avg => Self::Avg {
                column_index: column_index.unwrap_or(0),
                sum: 0.0,
                count: 0,
            },
            AggregateFunc::Min => Self::Min {
                column_index: column_index.unwrap_or(0),
                value: None,
            },
            AggregateFunc::Max => Self::Max {
                column_index: column_index.unwrap_or(0),
                value: None,
            },
            AggregateFunc::Distinct => Self::Distinct {
                column_index: column_index.unwrap_or(0),
                seen: BTreeSet::new(),
            },
            AggregateFunc::StdDev => Self::StdDev {
                column_index: column_index.unwrap_or(0),
                count: 0,
                mean: 0.0,
                m2: 0.0,
            },
            AggregateFunc::GeoMean => Self::GeoMean {
                column_index: column_index.unwrap_or(0),
                count: 0,
                log_sum: 0.0,
            },
        }
    }

    fn update(&mut self, entry: &RelationEntry) {
        match self {
            Self::CountAll { count } => {
                *count += 1;
            }
            Self::CountNonNull {
                column_index,
                count,
            } => {
                if entry
                    .get_field(*column_index)
                    .map(|value| !value.is_null())
                    .unwrap_or(false)
                {
                    *count += 1;
                }
            }
            Self::Sum {
                column_index,
                sum,
                output_mode,
            } => {
                let Some(value) = entry.get_field(*column_index) else {
                    return;
                };
                if value.is_null() {
                    return;
                }
                match value {
                    Value::Int32(value) => *sum += *value as f64,
                    Value::Int64(value) => *sum += *value as f64,
                    Value::Float64(value) => {
                        *sum += *value;
                        *output_mode = SumOutputMode::Float;
                    }
                    _ => {
                        *output_mode = SumOutputMode::Float;
                    }
                }
            }
            Self::Avg {
                column_index,
                sum,
                count,
            } => {
                if let Some(value) = entry.get_field(*column_index).and_then(Self::numeric_value) {
                    *sum += value;
                    *count += 1;
                }
            }
            Self::Min {
                column_index,
                value,
            } => {
                let Some(candidate) = entry.get_field(*column_index) else {
                    return;
                };
                if candidate.is_null() {
                    return;
                }
                match value {
                    Some(current) if candidate >= current => {}
                    slot => *slot = Some(candidate.clone()),
                }
            }
            Self::Max {
                column_index,
                value,
            } => {
                let Some(candidate) = entry.get_field(*column_index) else {
                    return;
                };
                if candidate.is_null() {
                    return;
                }
                match value {
                    Some(current) if candidate <= current => {}
                    slot => *slot = Some(candidate.clone()),
                }
            }
            Self::Distinct { column_index, seen } => {
                if let Some(value) = entry.get_field(*column_index) {
                    seen.insert(value.clone());
                }
            }
            Self::StdDev {
                column_index,
                count,
                mean,
                m2,
            } => {
                let Some(value) = entry.get_field(*column_index).and_then(Self::numeric_value)
                else {
                    return;
                };
                *count += 1;
                let delta = value - *mean;
                *mean += delta / *count as f64;
                let delta2 = value - *mean;
                *m2 += delta * delta2;
            }
            Self::GeoMean {
                column_index,
                count,
                log_sum,
            } => {
                let Some(value) = entry.get_field(*column_index).and_then(Self::numeric_value)
                else {
                    return;
                };
                if value > 0.0 {
                    *count += 1;
                    *log_sum += log(value);
                }
            }
        }
    }

    fn finalize(self) -> Value {
        match self {
            Self::CountAll { count } | Self::CountNonNull { count, .. } => Value::Int64(count),
            Self::Sum {
                sum,
                output_mode: SumOutputMode::Integer,
                ..
            } => Value::Int64(sum as i64),
            Self::Sum {
                sum,
                output_mode: SumOutputMode::Float,
                ..
            } => Value::Float64(sum),
            Self::Avg { sum, count, .. } => {
                if count == 0 {
                    Value::Null
                } else {
                    Value::Float64(sum / count as f64)
                }
            }
            Self::Min { value, .. } | Self::Max { value, .. } => value.unwrap_or(Value::Null),
            Self::Distinct { seen, .. } => Value::Int64(seen.len() as i64),
            Self::StdDev { count, m2, .. } => {
                if count == 0 {
                    Value::Null
                } else {
                    Value::Float64(sqrt(m2 / count as f64))
                }
            }
            Self::GeoMean { count, log_sum, .. } => {
                if count == 0 {
                    Value::Null
                } else {
                    Value::Float64(exp(log_sum / count as f64))
                }
            }
        }
    }

    #[inline]
    fn numeric_value(value: &Value) -> Option<f64> {
        match value {
            Value::Int32(value) => Some(*value as f64),
            Value::Int64(value) => Some(*value as f64),
            Value::Float64(value) => Some(*value),
            _ => None,
        }
    }
}

impl GroupKeyValue {
    fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Boolean(value) => Self::Boolean(*value),
            Value::Int32(value) => Self::Int32(*value),
            Value::Int64(value) => Self::Int64(*value),
            Value::Float64(value) => Self::Float64(value.to_bits()),
            Value::String(value) => Self::String(value.clone()),
            Value::DateTime(value) => Self::DateTime(*value),
            Value::Bytes(value) => Self::Bytes(value.clone()),
            Value::Jsonb(value) => Self::Jsonb(value.0.clone()),
        }
    }

    fn from_values(values: &[Value]) -> Vec<Self> {
        values.iter().map(Self::from_value).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn test_count_star() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Int64(2)]),
            Row::new(2, vec![Value::Int64(3)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Count, None)]);
        let result = executor.execute(input);

        assert_eq!(result.len(), 1);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(3)));
    }

    #[test]
    fn test_count_column() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Int64(3)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Count, Some(0))]);
        let result = executor.execute(input);

        assert_eq!(result.len(), 1);
        // Should count only non-null values
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(2)));
    }

    #[test]
    fn test_sum() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(10)]),
            Row::new(1, vec![Value::Int64(20)]),
            Row::new(2, vec![Value::Int64(30)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Sum, Some(0))]);
        let result = executor.execute(input);

        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(60)));
    }

    #[test]
    fn test_avg() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(10)]),
            Row::new(1, vec![Value::Int64(20)]),
            Row::new(2, vec![Value::Int64(30)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Avg, Some(0))]);
        let result = executor.execute(input);

        assert_eq!(result.entries[0].get_field(0), Some(&Value::Float64(20.0)));
    }

    #[test]
    fn test_min_max() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(30)]),
            Row::new(1, vec![Value::Int64(10)]),
            Row::new(2, vec![Value::Int64(20)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![
            (AggregateFunc::Min, Some(0)),
            (AggregateFunc::Max, Some(0)),
        ]);
        let result = executor.execute(input);

        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(10)));
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_group_by() {
        let rows = vec![
            Row::new(0, vec![Value::String("A".into()), Value::Int64(10)]),
            Row::new(1, vec![Value::String("A".into()), Value::Int64(20)]),
            Row::new(2, vec![Value::String("B".into()), Value::Int64(30)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::new(
            vec![0], // group by first column
            vec![(AggregateFunc::Sum, Some(1))],
        );
        let result = executor.execute(input);

        assert_eq!(result.len(), 2);
        // Verify actual group values - BTreeMap orders by key, so "A" comes before "B"
        // Group A: 10 + 20 = 30, Group B: 30
        let mut sums: Vec<i64> = result
            .entries
            .iter()
            .filter_map(|e| match e.get_field(1) {
                Some(Value::Int64(v)) => Some(*v),
                _ => None,
            })
            .collect();
        sums.sort();
        assert_eq!(sums, vec![30, 30]); // Both groups sum to 30
    }

    #[test]
    fn test_empty_relation() {
        let input = Relation::from_rows_owned(Vec::new(), vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![
            (AggregateFunc::Count, None),
            (AggregateFunc::Sum, Some(0)),
            (AggregateFunc::Avg, Some(0)),
        ]);
        let result = executor.execute(input);

        assert_eq!(result.len(), 1);
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(0))); // COUNT
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Int64(0))); // SUM
        assert_eq!(result.entries[0].get_field(2), Some(&Value::Null)); // AVG
    }

    #[test]
    fn test_stddev() {
        let rows = vec![
            Row::new(0, vec![Value::Float64(2.0)]),
            Row::new(1, vec![Value::Float64(4.0)]),
            Row::new(2, vec![Value::Float64(4.0)]),
            Row::new(3, vec![Value::Float64(4.0)]),
            Row::new(4, vec![Value::Float64(5.0)]),
            Row::new(5, vec![Value::Float64(5.0)]),
            Row::new(6, vec![Value::Float64(7.0)]),
            Row::new(7, vec![Value::Float64(9.0)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::StdDev, Some(0))]);
        let result = executor.execute(input);

        // Standard deviation should be 2.0
        if let Some(Value::Float64(stddev)) = result.entries[0].get_field(0) {
            assert!((stddev - 2.0).abs() < 0.001);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_distinct() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Int64(2)]),
            Row::new(2, vec![Value::Int64(1)]), // duplicate
            Row::new(3, vec![Value::Int64(3)]),
            Row::new(4, vec![Value::Int64(2)]), // duplicate
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Distinct, Some(0))]);
        let result = executor.execute(input);

        assert_eq!(result.len(), 1);
        // Should return count of distinct values: 1, 2, 3 = 3
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(3)));
    }

    #[test]
    fn test_distinct_with_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(1)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Int64(1)]),
            Row::new(3, vec![Value::Null]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Distinct, Some(0))]);
        let result = executor.execute(input);

        // Distinct values: 1, null = 2
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(2)));
    }

    #[test]
    fn test_geomean() {
        // Geometric mean of [2, 8] = sqrt(2 * 8) = sqrt(16) = 4
        let rows = vec![
            Row::new(0, vec![Value::Float64(2.0)]),
            Row::new(1, vec![Value::Float64(8.0)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::GeoMean, Some(0))]);
        let result = executor.execute(input);

        if let Some(Value::Float64(geomean)) = result.entries[0].get_field(0) {
            assert!((geomean - 4.0).abs() < 0.001);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_geomean_single_value() {
        let rows = vec![Row::new(0, vec![Value::Float64(5.0)])];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::GeoMean, Some(0))]);
        let result = executor.execute(input);

        if let Some(Value::Float64(geomean)) = result.entries[0].get_field(0) {
            assert!((geomean - 5.0).abs() < 0.001);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_geomean_with_zero_and_negative() {
        // GeoMean filters out non-positive values
        let rows = vec![
            Row::new(0, vec![Value::Float64(2.0)]),
            Row::new(1, vec![Value::Float64(0.0)]), // filtered
            Row::new(2, vec![Value::Float64(-1.0)]), // filtered
            Row::new(3, vec![Value::Float64(8.0)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::GeoMean, Some(0))]);
        let result = executor.execute(input);

        // Only [2, 8] are used, geomean = 4
        if let Some(Value::Float64(geomean)) = result.entries[0].get_field(0) {
            assert!((geomean - 4.0).abs() < 0.001);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_geomean_all_non_positive() {
        let rows = vec![
            Row::new(0, vec![Value::Float64(0.0)]),
            Row::new(1, vec![Value::Float64(-1.0)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::GeoMean, Some(0))]);
        let result = executor.execute(input);

        // All values filtered, should return Null
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Null));
    }

    #[test]
    fn test_sum_with_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(10)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Int64(20)]),
            Row::new(3, vec![Value::Null]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Sum, Some(0))]);
        let result = executor.execute(input);

        // Sum should ignore nulls: 10 + 20 = 30
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_sum_mixed_types() {
        let rows = vec![
            Row::new(0, vec![Value::Int32(10)]),
            Row::new(1, vec![Value::Int64(20)]),
            Row::new(2, vec![Value::Float64(30.5)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Sum, Some(0))]);
        let result = executor.execute(input);

        // Mixed types should return Float64
        if let Some(Value::Float64(sum)) = result.entries[0].get_field(0) {
            assert!((sum - 60.5).abs() < 0.001);
        } else {
            panic!("Expected Float64 value for mixed types");
        }
    }

    #[test]
    fn test_min_max_with_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(30)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Int64(10)]),
            Row::new(3, vec![Value::Null]),
            Row::new(4, vec![Value::Int64(20)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![
            (AggregateFunc::Min, Some(0)),
            (AggregateFunc::Max, Some(0)),
        ]);
        let result = executor.execute(input);

        // Min/Max should ignore nulls
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(10)));
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Int64(30)));
    }

    #[test]
    fn test_min_max_all_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Null]),
            Row::new(1, vec![Value::Null]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![
            (AggregateFunc::Min, Some(0)),
            (AggregateFunc::Max, Some(0)),
        ]);
        let result = executor.execute(input);

        // All nulls should return Null
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Null));
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Null));
    }

    #[test]
    fn test_stddev_single_value() {
        let rows = vec![Row::new(0, vec![Value::Float64(5.0)])];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::StdDev, Some(0))]);
        let result = executor.execute(input);

        // StdDev of single value is 0
        if let Some(Value::Float64(stddev)) = result.entries[0].get_field(0) {
            assert!((stddev - 0.0).abs() < 0.001);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_stddev_empty() {
        let input = Relation::from_rows_owned(Vec::new(), vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::StdDev, Some(0))]);
        let result = executor.execute(input);

        // Empty set should return Null
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Null));
    }

    #[test]
    fn test_stddev_with_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Float64(2.0)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Float64(4.0)]),
            Row::new(3, vec![Value::Null]),
            Row::new(4, vec![Value::Float64(6.0)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::StdDev, Some(0))]);
        let result = executor.execute(input);

        // Values: [2, 4, 6], mean = 4, variance = ((2-4)^2 + (4-4)^2 + (6-4)^2) / 3 = 8/3
        // stddev = sqrt(8/3) ≈ 1.633
        if let Some(Value::Float64(stddev)) = result.entries[0].get_field(0) {
            assert!((stddev - 1.633).abs() < 0.01);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_avg_with_nulls() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(10)]),
            Row::new(1, vec![Value::Null]),
            Row::new(2, vec![Value::Int64(20)]),
            Row::new(3, vec![Value::Null]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![(AggregateFunc::Avg, Some(0))]);
        let result = executor.execute(input);

        // Avg should ignore nulls: (10 + 20) / 2 = 15
        assert_eq!(result.entries[0].get_field(0), Some(&Value::Float64(15.0)));
    }

    #[test]
    fn test_multiple_aggregates() {
        let rows = vec![
            Row::new(0, vec![Value::Int64(10)]),
            Row::new(1, vec![Value::Int64(20)]),
            Row::new(2, vec![Value::Int64(30)]),
            Row::new(3, vec![Value::Int64(40)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::no_group(vec![
            (AggregateFunc::Count, None),
            (AggregateFunc::Sum, Some(0)),
            (AggregateFunc::Avg, Some(0)),
            (AggregateFunc::Min, Some(0)),
            (AggregateFunc::Max, Some(0)),
        ]);
        let result = executor.execute(input);

        assert_eq!(result.entries[0].get_field(0), Some(&Value::Int64(4))); // COUNT
        assert_eq!(result.entries[0].get_field(1), Some(&Value::Int64(100))); // SUM
        assert_eq!(result.entries[0].get_field(2), Some(&Value::Float64(25.0))); // AVG
        assert_eq!(result.entries[0].get_field(3), Some(&Value::Int64(10))); // MIN
        assert_eq!(result.entries[0].get_field(4), Some(&Value::Int64(40))); // MAX
    }

    #[test]
    fn test_group_by_with_multiple_aggregates() {
        let rows = vec![
            Row::new(0, vec![Value::String("A".into()), Value::Int64(10)]),
            Row::new(1, vec![Value::String("A".into()), Value::Int64(20)]),
            Row::new(2, vec![Value::String("A".into()), Value::Int64(30)]),
            Row::new(3, vec![Value::String("B".into()), Value::Int64(100)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::new(
            vec![0],
            vec![
                (AggregateFunc::Count, None),
                (AggregateFunc::Sum, Some(1)),
                (AggregateFunc::Avg, Some(1)),
            ],
        );
        let result = executor.execute(input);

        assert_eq!(result.len(), 2);

        // Find group A and B by checking the group key
        for entry in &result.entries {
            let group_key = entry.get_field(0);
            match group_key {
                Some(Value::String(s)) if s == "A" => {
                    assert_eq!(entry.get_field(1), Some(&Value::Int64(3))); // COUNT
                    assert_eq!(entry.get_field(2), Some(&Value::Int64(60))); // SUM
                    assert_eq!(entry.get_field(3), Some(&Value::Float64(20.0)));
                    // AVG
                }
                Some(Value::String(s)) if s == "B" => {
                    assert_eq!(entry.get_field(1), Some(&Value::Int64(1))); // COUNT
                    assert_eq!(entry.get_field(2), Some(&Value::Int64(100))); // SUM
                    assert_eq!(entry.get_field(3), Some(&Value::Float64(100.0)));
                    // AVG
                }
                _ => panic!("Unexpected group key"),
            }
        }
    }

    // ==================== Bug 3 Test: GROUP BY key collision with | separator ====================
    // This test demonstrates Bug 3: GROUP BY key collision when string values contain the separator

    #[test]
    fn test_group_by_separator_collision_bug() {
        // Two different groups that should NOT be merged:
        // Group 1: ("a|b", "c") - first column is "a|b", second is "c"
        // Group 2: ("a", "b|c") - first column is "a", second is "b|c"
        // BUG: Both generate the same key "a|b|c" and get incorrectly merged
        let rows = vec![
            Row::new(
                0,
                vec![
                    Value::String("a|b".into()),
                    Value::String("c".into()),
                    Value::Int64(10),
                ],
            ),
            Row::new(
                1,
                vec![
                    Value::String("a".into()),
                    Value::String("b|c".into()),
                    Value::Int64(20),
                ],
            ),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::new(
            vec![0, 1], // group by first two columns
            vec![(AggregateFunc::Sum, Some(2))],
        );
        let result = executor.execute(input);

        // Should have 2 distinct groups, not 1
        assert_eq!(
            result.len(), 2,
            "Bug 3: Groups with separator in values should NOT be merged. Expected 2 groups, got {}",
            result.len()
        );
    }

    // ==================== Bug 4 Test: GROUP BY type confusion ====================
    // This test demonstrates Bug 4: Different types with same numeric value get merged

    #[test]
    fn test_group_by_type_confusion_bug() {
        // Three different groups that should NOT be merged:
        // Group 1: Int32(42)
        // Group 2: Int64(42)
        // Group 3: DateTime(42)
        // BUG: All three generate the same key "42" and get incorrectly merged
        let rows = vec![
            Row::new(0, vec![Value::Int32(42), Value::Int64(10)]),
            Row::new(1, vec![Value::Int64(42), Value::Int64(20)]),
            Row::new(2, vec![Value::DateTime(42), Value::Int64(30)]),
        ];
        let input = Relation::from_rows_owned(rows, vec!["t".into()]);

        let executor = AggregateExecutor::new(
            vec![0], // group by first column
            vec![(AggregateFunc::Sum, Some(1))],
        );
        let result = executor.execute(input);

        // Should have 3 distinct groups (different types), not 1
        assert_eq!(
            result.len(), 3,
            "Bug 4: Different types with same numeric value should NOT be merged. Expected 3 groups, got {}",
            result.len()
        );
    }
}
