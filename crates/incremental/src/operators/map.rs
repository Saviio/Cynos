//! Incremental map and project operators.

use crate::delta::Delta;
use alloc::vec::Vec;
use cynos_core::{Row, Value};

/// Applies a mapper function to a batch of deltas.
///
/// Each delta's data is transformed using the mapper function.
/// The diff values are preserved.
pub fn map_incremental<T, U, F>(input: &[Delta<T>], mapper: F) -> Vec<Delta<U>>
where
    F: Fn(&T) -> U,
{
    input
        .iter()
        .map(|d| Delta::new(mapper(&d.data), d.diff))
        .collect()
}

/// Applies a mapper function to a batch of deltas, consuming the input.
#[allow(dead_code)]
pub fn map_incremental_owned<T, U, F>(input: Vec<Delta<T>>, mapper: F) -> Vec<Delta<U>>
where
    F: Fn(T) -> U,
{
    input
        .into_iter()
        .map(|d| Delta::new(mapper(d.data), d.diff))
        .collect()
}

/// Projects specific columns from row deltas.
///
/// Creates new rows containing only the specified columns.
pub fn project_incremental(input: &[Delta<Row>], columns: &[usize]) -> Vec<Delta<Row>> {
    input
        .iter()
        .map(|d| {
            let projected_values: Vec<Value> = columns
                .iter()
                .filter_map(|&col| d.data.get(col).cloned())
                .collect();
            Delta::new(
                Row::new_with_version(d.data.id(), d.data.version(), projected_values),
                d.diff,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec;

    #[test]
    fn test_map_incremental_basic() {
        let deltas = vec![Delta::insert(1), Delta::insert(2), Delta::delete(3)];

        let mapped = map_incremental(&deltas, |&x| x * 2);

        assert_eq!(mapped.len(), 3);
        assert_eq!(mapped[0].data, 2);
        assert_eq!(mapped[1].data, 4);
        assert_eq!(mapped[2].data, 6);
    }

    #[test]
    fn test_map_incremental_preserves_diff() {
        let deltas = vec![Delta::insert(1i32), Delta::delete(2i32)];

        let mapped = map_incremental(&deltas, |&x| x.to_string());

        assert!(mapped[0].is_insert());
        assert!(mapped[1].is_delete());
    }

    #[test]
    fn test_project_incremental() {
        let row = Row::new(
            1,
            vec![
                Value::Int64(1),
                Value::String("Alice".into()),
                Value::Int32(25),
            ],
        );
        let deltas = vec![Delta::insert(row)];

        let projected = project_incremental(&deltas, &[0, 2]);

        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].data.len(), 2);
        assert_eq!(projected[0].data.get(0), Some(&Value::Int64(1)));
        assert_eq!(projected[0].data.get(1), Some(&Value::Int32(25)));
        assert_eq!(projected[0].data.id(), 1);
        assert_eq!(projected[0].data.version(), 1);
    }
}
