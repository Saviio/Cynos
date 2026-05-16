//! JSONPath evaluation for JSONB values.
//!
//! This module provides the evaluation logic for JSONPath expressions
//! against JsonbValue instances.

use crate::path::parser::{CompareOp, JsonPath, JsonPathPredicate, PredicateValue};
use crate::value::JsonbValue;
use alloc::vec::Vec;

impl JsonbValue {
    /// Evaluates a JSONPath expression and returns all matching values.
    pub fn query<'a>(&'a self, path: &JsonPath) -> Vec<&'a JsonbValue> {
        let mut results = Vec::new();
        eval_path(self, path, &mut results);
        results
    }

    /// Evaluates a JSONPath expression and returns the first matching value.
    pub fn query_first<'a>(&'a self, path: &JsonPath) -> Option<&'a JsonbValue> {
        eval_path_first(self, path)
    }
}

fn eval_path<'a>(value: &'a JsonbValue, path: &JsonPath, results: &mut Vec<&'a JsonbValue>) {
    match path {
        JsonPath::Root => {
            results.push(value);
        }
        JsonPath::Field(parent, field) => {
            let parent_results = eval_path_collect(value, parent);
            for v in parent_results {
                if let Some(obj) = v.as_object() {
                    if let Some(field_value) = obj.get(field) {
                        results.push(field_value);
                    }
                }
            }
        }
        JsonPath::Index(parent, index) => {
            let parent_results = eval_path_collect(value, parent);
            for v in parent_results {
                if let Some(arr) = v.as_array() {
                    if let Some(item) = arr.get(*index) {
                        results.push(item);
                    }
                }
            }
        }
        JsonPath::Slice(parent, start, end) => {
            let parent_results = eval_path_collect(value, parent);
            for v in parent_results {
                if let Some(arr) = v.as_array() {
                    let start_idx = start.unwrap_or(0);
                    let end_idx = end.unwrap_or(arr.len());
                    for item in arr.iter().skip(start_idx).take(end_idx - start_idx) {
                        results.push(item);
                    }
                }
            }
        }
        JsonPath::Wildcard(parent) => {
            let parent_results = eval_path_collect(value, parent);
            for v in parent_results {
                match v {
                    JsonbValue::Array(arr) => {
                        for item in arr {
                            results.push(item);
                        }
                    }
                    JsonbValue::Object(obj) => {
                        for (_, val) in obj.iter() {
                            results.push(val);
                        }
                    }
                    _ => {}
                }
            }
        }
        JsonPath::RecursiveField(parent, field) => {
            let parent_results = eval_path_collect(value, parent);
            for v in parent_results {
                recursive_field_search(v, field, results);
            }
        }
        JsonPath::Filter(parent, predicate) => {
            let parent_results = eval_path_collect(value, parent);
            for v in parent_results {
                if let Some(arr) = v.as_array() {
                    for item in arr {
                        if eval_predicate(item, predicate) {
                            results.push(item);
                        }
                    }
                }
            }
        }
    }
}

fn eval_path_collect<'a>(value: &'a JsonbValue, path: &JsonPath) -> Vec<&'a JsonbValue> {
    let mut results = Vec::new();
    eval_path(value, path, &mut results);
    results
}

fn eval_path_first<'a>(value: &'a JsonbValue, path: &JsonPath) -> Option<&'a JsonbValue> {
    let mut first = None;
    eval_path_until(value, path, &mut |candidate| {
        first = Some(candidate);
        true
    });
    first
}

fn eval_path_until<'a>(
    value: &'a JsonbValue,
    path: &JsonPath,
    visit: &mut dyn FnMut(&'a JsonbValue) -> bool,
) -> bool {
    match path {
        JsonPath::Root => visit(value),
        JsonPath::Field(parent, field) => eval_path_until(value, parent, &mut |v| {
            if let Some(obj) = v.as_object() {
                if let Some(field_value) = obj.get(field) {
                    return visit(field_value);
                }
            }
            false
        }),
        JsonPath::Index(parent, index) => eval_path_until(value, parent, &mut |v| {
            if let Some(arr) = v.as_array() {
                if let Some(item) = arr.get(*index) {
                    return visit(item);
                }
            }
            false
        }),
        JsonPath::Slice(parent, start, end) => eval_path_until(value, parent, &mut |v| {
            if let Some(arr) = v.as_array() {
                let start_idx = start.unwrap_or(0);
                let end_idx = end.unwrap_or(arr.len());
                for item in arr.iter().skip(start_idx).take(end_idx - start_idx) {
                    if visit(item) {
                        return true;
                    }
                }
            }
            false
        }),
        JsonPath::Wildcard(parent) => eval_path_until(value, parent, &mut |v| {
            match v {
                JsonbValue::Array(arr) => {
                    for item in arr {
                        if visit(item) {
                            return true;
                        }
                    }
                }
                JsonbValue::Object(obj) => {
                    for (_, val) in obj.iter() {
                        if visit(val) {
                            return true;
                        }
                    }
                }
                _ => {}
            }
            false
        }),
        JsonPath::RecursiveField(parent, field) => eval_path_until(value, parent, &mut |v| {
            recursive_field_search_until(v, field, visit)
        }),
        JsonPath::Filter(parent, predicate) => eval_path_until(value, parent, &mut |v| {
            if let Some(arr) = v.as_array() {
                for item in arr {
                    if eval_predicate(item, predicate) && visit(item) {
                        return true;
                    }
                }
            }
            false
        }),
    }
}

fn recursive_field_search<'a>(
    value: &'a JsonbValue,
    field: &str,
    results: &mut Vec<&'a JsonbValue>,
) {
    match value {
        JsonbValue::Object(obj) => {
            if let Some(v) = obj.get(field) {
                results.push(v);
            }
            for (_, v) in obj.iter() {
                recursive_field_search(v, field, results);
            }
        }
        JsonbValue::Array(arr) => {
            for item in arr {
                recursive_field_search(item, field, results);
            }
        }
        _ => {}
    }
}

fn recursive_field_search_until<'a>(
    value: &'a JsonbValue,
    field: &str,
    visit: &mut dyn FnMut(&'a JsonbValue) -> bool,
) -> bool {
    match value {
        JsonbValue::Object(obj) => {
            if let Some(v) = obj.get(field) {
                if visit(v) {
                    return true;
                }
            }
            for (_, v) in obj.iter() {
                if recursive_field_search_until(v, field, visit) {
                    return true;
                }
            }
        }
        JsonbValue::Array(arr) => {
            for item in arr {
                if recursive_field_search_until(item, field, visit) {
                    return true;
                }
            }
        }
        _ => {}
    }
    false
}

fn eval_predicate(value: &JsonbValue, predicate: &JsonPathPredicate) -> bool {
    match predicate {
        JsonPathPredicate::Exists(field) => {
            if let Some(obj) = value.as_object() {
                obj.contains_key(field)
            } else {
                false
            }
        }
        JsonPathPredicate::Compare(field, op, expected) => {
            if let Some(obj) = value.as_object() {
                if let Some(actual) = obj.get(field) {
                    compare_values(actual, op, expected)
                } else {
                    false
                }
            } else {
                false
            }
        }
        JsonPathPredicate::And(left, right) => {
            eval_predicate(value, left) && eval_predicate(value, right)
        }
        JsonPathPredicate::Or(left, right) => {
            eval_predicate(value, left) || eval_predicate(value, right)
        }
        JsonPathPredicate::Not(inner) => !eval_predicate(value, inner),
    }
}

fn compare_values(actual: &JsonbValue, op: &CompareOp, expected: &PredicateValue) -> bool {
    match (actual, expected) {
        (JsonbValue::Null, PredicateValue::Null) => matches!(op, CompareOp::Eq),
        (JsonbValue::Bool(a), PredicateValue::Bool(b)) => match op {
            CompareOp::Eq => a == b,
            CompareOp::Ne => a != b,
            _ => false,
        },
        (JsonbValue::Number(a), PredicateValue::Number(b)) => match op {
            CompareOp::Eq => (a - b).abs() < f64::EPSILON,
            CompareOp::Ne => (a - b).abs() >= f64::EPSILON,
            CompareOp::Lt => a < b,
            CompareOp::Le => a <= b,
            CompareOp::Gt => a > b,
            CompareOp::Ge => a >= b,
        },
        (JsonbValue::String(a), PredicateValue::String(b)) => match op {
            CompareOp::Eq => a == b,
            CompareOp::Ne => a != b,
            CompareOp::Lt => a < b,
            CompareOp::Le => a <= b,
            CompareOp::Gt => a > b,
            CompareOp::Ge => a >= b,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::JsonbObject;
    use alloc::vec;

    fn make_test_json() -> JsonbValue {
        let mut user = JsonbObject::new();
        user.insert("name".into(), JsonbValue::String("Alice".into()));
        user.insert(
            "tags".into(),
            JsonbValue::Array(vec![
                JsonbValue::String("admin".into()),
                JsonbValue::String("developer".into()),
            ]),
        );

        let mut root = JsonbObject::new();
        root.insert("user".into(), JsonbValue::Object(user));
        JsonbValue::Object(root)
    }

    #[test]
    fn test_query_root() {
        let json = make_test_json();
        let path = JsonPath::parse("$").unwrap();
        let results = json.query(&path);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_query_field() {
        let json = make_test_json();
        let path = JsonPath::parse("$.user.name").unwrap();
        let results = json.query(&path);
        assert_eq!(results, vec![&JsonbValue::String("Alice".into())]);
    }

    #[test]
    fn test_query_array_index() {
        let json = make_test_json();
        let path = JsonPath::parse("$.user.tags[0]").unwrap();
        let results = json.query(&path);
        assert_eq!(results, vec![&JsonbValue::String("admin".into())]);
    }

    #[test]
    fn test_query_array_slice() {
        let json = make_test_json();
        let path = JsonPath::parse("$.user.tags[0:2]").unwrap();
        let results = json.query(&path);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], &JsonbValue::String("admin".into()));
        assert_eq!(results[1], &JsonbValue::String("developer".into()));
    }

    #[test]
    fn test_query_wildcard() {
        let json = make_test_json();
        let path = JsonPath::parse("$.user.tags[*]").unwrap();
        let results = json.query(&path);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_recursive() {
        let mut inner = JsonbObject::new();
        inner.insert("name".into(), JsonbValue::String("inner".into()));

        let mut outer = JsonbObject::new();
        outer.insert("name".into(), JsonbValue::String("outer".into()));
        outer.insert("child".into(), JsonbValue::Object(inner));

        let json = JsonbValue::Object(outer);
        let path = JsonPath::parse("$..name").unwrap();
        let results = json.query(&path);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_filter() {
        let mut item1 = JsonbObject::new();
        item1.insert("price".into(), JsonbValue::Number(5.0));

        let mut item2 = JsonbObject::new();
        item2.insert("price".into(), JsonbValue::Number(15.0));

        let mut item3 = JsonbObject::new();
        item3.insert("price".into(), JsonbValue::Number(8.0));

        let mut root = JsonbObject::new();
        root.insert(
            "items".into(),
            JsonbValue::Array(vec![
                JsonbValue::Object(item1),
                JsonbValue::Object(item2),
                JsonbValue::Object(item3),
            ]),
        );

        let json = JsonbValue::Object(root);
        let path = JsonPath::parse("$.items[?(@.price < 10)]").unwrap();
        let results = json.query(&path);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_first() {
        let json = make_test_json();
        let path = JsonPath::parse("$.user.name").unwrap();
        let result = json.query_first(&path);
        assert_eq!(result, Some(&JsonbValue::String("Alice".into())));
    }

    #[test]
    fn test_query_first_checks_later_parent_matches() {
        let mut first = JsonbObject::new();
        first.insert("other".into(), JsonbValue::String("skip".into()));

        let mut second = JsonbObject::new();
        second.insert("name".into(), JsonbValue::String("Alice".into()));

        let json = JsonbValue::Array(vec![JsonbValue::Object(first), JsonbValue::Object(second)]);
        let path = JsonPath::parse("$[*].name").unwrap();

        assert_eq!(
            json.query_first(&path),
            Some(&JsonbValue::String("Alice".into()))
        );
    }

    #[test]
    fn test_query_missing_field() {
        let json = make_test_json();
        let path = JsonPath::parse("$.user.email").unwrap();
        let results = json.query(&path);
        assert!(results.is_empty());
    }
}
