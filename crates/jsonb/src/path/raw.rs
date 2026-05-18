//! Raw JSON helpers for simple JSONPath probes.
//!
//! These helpers are intentionally limited to root/field/index paths. They are
//! used by hot paths that need to answer scalar predicates without constructing
//! a full `JsonbValue`, while preserving the same path parser as the normal
//! JSONB evaluator.

use crate::path::parser::JsonPath;
use alloc::string::String;
use alloc::vec::Vec;
use cynos_core::Value;

/// A JSONPath subset that can be evaluated by scanning JSON text directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimpleJsonPath {
    segments: Vec<SimpleJsonPathSegment>,
}

/// A segment in a [`SimpleJsonPath`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimpleJsonPathSegment {
    Field(String),
    Index(usize),
}

impl SimpleJsonPath {
    /// Parses and compiles a JSONPath into the direct-scannable subset.
    pub fn parse(path: &str) -> Option<Self> {
        let parsed = JsonPath::parse(path).ok()?;
        Self::from_json_path(&parsed)
    }

    /// Compiles an already parsed JSONPath into the direct-scannable subset.
    pub fn from_json_path(path: &JsonPath) -> Option<Self> {
        let mut segments = Vec::new();
        collect_simple_json_path_segments(path, &mut segments).then_some(Self { segments })
    }

    /// Returns this path's compiled segments.
    pub fn segments(&self) -> &[SimpleJsonPathSegment] {
        &self.segments
    }

    /// Extracts the raw JSON text slice at this path.
    pub fn extract<'json>(&self, json: &'json [u8]) -> Option<&'json [u8]> {
        extract_simple_json_path(json, self)
    }
}

/// Compares a raw JSON value slice with a scalar Cynos value.
///
/// This helper is deliberately limited to scalar equality. Composite JSON
/// values should keep using the full JSONB evaluator so object/array semantics
/// remain centralized in the normal JSONB layer.
pub fn raw_json_eq_value(raw: &[u8], expected: &Value) -> bool {
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
        Value::Int32(value) => raw_json_number_eq(raw, *value as f64),
        Value::Int64(value) => raw_json_number_eq(raw, *value as f64),
        Value::Float64(value) => raw_json_number_eq(raw, *value),
        Value::String(value) => decode_json_string_literal(raw)
            .map(|actual| actual == *value)
            .unwrap_or(false),
        _ => false,
    }
}

/// Evaluates the scalar subset of JSONB contains on a raw JSON value slice.
///
/// For string needles this preserves the existing query semantics: string JSON
/// values are decoded before substring matching, while non-string JSON slices
/// fall back to textual containment.
pub fn raw_json_contains_value(raw: &[u8], expected: &Value) -> bool {
    match expected {
        Value::String(needle) => {
            let raw = trim_json_bytes(raw);
            if raw.first() == Some(&b'"') {
                return decode_json_string_literal(raw)
                    .map(|actual| actual.contains(needle.as_str()))
                    .unwrap_or(false);
            }

            core::str::from_utf8(raw)
                .map(|actual| actual.contains(needle.as_str()))
                .unwrap_or(false)
        }
        _ => raw_json_eq_value(raw, expected),
    }
}

fn raw_json_number_eq(raw: &[u8], expected: f64) -> bool {
    core::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .map(|actual| (actual - expected).abs() < f64::EPSILON)
        .unwrap_or(false)
}

fn collect_simple_json_path_segments(
    path: &JsonPath,
    segments: &mut Vec<SimpleJsonPathSegment>,
) -> bool {
    match path {
        JsonPath::Root => true,
        JsonPath::Field(parent, field) => {
            if !collect_simple_json_path_segments(parent, segments) {
                return false;
            }
            segments.push(SimpleJsonPathSegment::Field(field.clone()));
            true
        }
        JsonPath::Index(parent, index) => {
            if !collect_simple_json_path_segments(parent, segments) {
                return false;
            }
            segments.push(SimpleJsonPathSegment::Index(*index));
            true
        }
        JsonPath::Slice(_, _, _)
        | JsonPath::RecursiveField(_, _)
        | JsonPath::Wildcard(_)
        | JsonPath::Filter(_, _) => false,
    }
}

fn extract_simple_json_path<'json>(
    json: &'json [u8],
    path: &SimpleJsonPath,
) -> Option<&'json [u8]> {
    let mut current = trim_json_bytes(json);
    for segment in path.segments() {
        current = match segment {
            SimpleJsonPathSegment::Field(field) => extract_json_field(current, field)?,
            SimpleJsonPathSegment::Index(index) => extract_json_index(current, *index)?,
        };
    }
    Some(trim_json_bytes(current))
}

fn extract_json_field<'json>(object: &'json [u8], field: &str) -> Option<&'json [u8]> {
    let bytes = trim_json_bytes(object);
    if bytes.first() != Some(&b'{') || bytes.last() != Some(&b'}') {
        return None;
    }

    let mut pos = 1usize;
    loop {
        pos = skip_json_whitespace(bytes, pos);
        match bytes.get(pos) {
            Some(b'}') => return None,
            Some(b'"') => {}
            _ => return None,
        }

        let key_start = pos;
        let key_end = scan_json_string_end(bytes, key_start)?;
        pos = skip_json_whitespace(bytes, key_end);
        if bytes.get(pos) != Some(&b':') {
            return None;
        }

        pos = skip_json_whitespace(bytes, pos + 1);
        let value_start = pos;
        let value_end = scan_json_value_end(bytes, value_start)?;
        if json_string_literal_eq(&bytes[key_start..key_end], field) {
            return Some(trim_json_bytes(&bytes[value_start..value_end]));
        }

        pos = skip_json_whitespace(bytes, value_end);
        match bytes.get(pos) {
            Some(b',') => pos += 1,
            Some(b'}') => return None,
            _ => return None,
        }
    }
}

fn extract_json_index(array: &[u8], target_index: usize) -> Option<&[u8]> {
    let bytes = trim_json_bytes(array);
    if bytes.first() != Some(&b'[') || bytes.last() != Some(&b']') {
        return None;
    }

    let mut pos = 1usize;
    let mut current_index = 0usize;
    loop {
        pos = skip_json_whitespace(bytes, pos);
        match bytes.get(pos) {
            Some(b']') | None => return None,
            Some(_) => {}
        }

        let value_start = pos;
        let value_end = scan_json_value_end(bytes, value_start)?;
        if current_index == target_index {
            return Some(trim_json_bytes(&bytes[value_start..value_end]));
        }

        current_index += 1;
        pos = skip_json_whitespace(bytes, value_end);
        match bytes.get(pos) {
            Some(b',') => pos += 1,
            Some(b']') => return None,
            _ => return None,
        }
    }
}

/// Trims ASCII JSON whitespace around a raw JSON slice.
#[inline]
pub fn trim_json_bytes(bytes: &[u8]) -> &[u8] {
    let mut start = 0usize;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

/// Skips ASCII JSON whitespace from `pos`.
#[inline]
pub fn skip_json_whitespace(bytes: &[u8], mut pos: usize) -> usize {
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

/// Finds the exclusive end offset of a JSON string literal.
pub fn scan_json_string_end(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'"') {
        return None;
    }

    let mut pos = start + 1;
    let mut escaped = false;
    while pos < bytes.len() {
        let byte = bytes[pos];
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            return Some(pos + 1);
        }
        pos += 1;
    }

    None
}

/// Finds the exclusive end offset of the JSON value starting at `start`.
pub fn scan_json_value_end(bytes: &[u8], start: usize) -> Option<usize> {
    let start = skip_json_whitespace(bytes, start);
    match bytes.get(start) {
        Some(b'"') => scan_json_string_end(bytes, start),
        Some(b'{') | Some(b'[') => scan_json_composite_end(bytes, start),
        Some(_) => {
            let mut pos = start;
            while pos < bytes.len() {
                match bytes[pos] {
                    b',' | b']' | b'}' => break,
                    _ => pos += 1,
                }
            }
            Some(pos)
        }
        None => None,
    }
}

fn scan_json_composite_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (offset, byte) in bytes[start..].iter().copied().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    return Some(start + offset + 1);
                }
            }
            _ => {}
        }
    }

    None
}

/// Decodes a JSON string literal using the same lightweight escape semantics as
/// the existing text JSON parser.
pub fn decode_json_string_literal(bytes: &[u8]) -> Option<String> {
    let bytes = trim_json_bytes(bytes);
    if bytes.len() < 2 || bytes.first() != Some(&b'"') || bytes.last() != Some(&b'"') {
        return None;
    }

    decode_json_string_inner(core::str::from_utf8(&bytes[1..bytes.len() - 1]).ok()?)
}

fn json_string_literal_eq(bytes: &[u8], expected: &str) -> bool {
    let bytes = trim_json_bytes(bytes);
    if bytes.len() < 2 || bytes.first() != Some(&b'"') || bytes.last() != Some(&b'"') {
        return false;
    }

    let inner = &bytes[1..bytes.len() - 1];
    if !inner.contains(&b'\\') {
        return inner == expected.as_bytes();
    }

    core::str::from_utf8(inner)
        .ok()
        .and_then(decode_json_string_inner)
        .map(|actual| actual == expected)
        .unwrap_or(false)
}

fn decode_json_string_inner(s: &str) -> Option<String> {
    let mut result = String::new();
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            result.push(ch);
            continue;
        }

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
    }

    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec;

    #[test]
    fn extracts_nested_object_and_array_values() {
        let path = SimpleJsonPath::parse("$.risk.history[1].bucket").unwrap();
        let json = br#"{"risk":{"history":[{"bucket":"low"},{"bucket":"high"}]}}"#;

        assert_eq!(path.extract(json), Some(br#""high""#.as_slice()));
    }

    #[test]
    fn matches_escaped_object_key_without_full_json_parse() {
        let path = SimpleJsonPath {
            segments: vec![SimpleJsonPathSegment::Field("st\"atus".to_string())],
        };
        let json = br#"{"st\"atus":"active"}"#;

        assert_eq!(path.extract(json), Some(br#""active""#.as_slice()));
    }

    #[test]
    fn preserves_lightweight_unknown_escape_semantics() {
        assert_eq!(
            decode_json_string_literal(br#""a\qb""#),
            Some("a\\qb".to_string())
        );
    }

    #[test]
    fn rejects_non_simple_paths() {
        assert!(SimpleJsonPath::parse("$..name").is_none());
        assert!(SimpleJsonPath::parse("$.items[*]").is_none());
        assert!(SimpleJsonPath::parse("$.items[0:2]").is_none());
    }

    #[test]
    fn compares_raw_scalar_values_without_full_parse() {
        assert!(raw_json_eq_value(
            br#" "enterprise" "#,
            &Value::String("enterprise".into())
        ));
        assert!(raw_json_eq_value(b"42", &Value::Int64(42)));
        assert!(raw_json_eq_value(b"true", &Value::Boolean(true)));
        assert!(raw_json_contains_value(
            br#""high-priority""#,
            &Value::String("priority".into())
        ));
    }
}
