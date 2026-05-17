//! JSONPath module for JSONB queries.

pub mod eval;
pub mod parser;
#[doc(hidden)]
pub mod raw;

pub use parser::{CompareOp, JsonPath, JsonPathPredicate, ParseError, PredicateValue};
#[doc(hidden)]
pub use raw::{
    decode_json_string_literal, raw_json_contains_value, raw_json_eq_value, scan_json_string_end,
    scan_json_value_end, skip_json_whitespace, trim_json_bytes, SimpleJsonPath,
    SimpleJsonPathSegment,
};
