# cynos-binary

Compact binary row encoding for WASM and high-throughput host interop.

## Overview

`cynos-binary` turns rows into a compact buffer that is cheap to pass across the WASM boundary. The higher-level JS API uses this crate under `execBinary()` and decodes the result with `ResultSet`.

Main pieces:

- `SchemaLayout`: precomputed column offsets, null-mask size, and row stride.
- `BinaryEncoder`: row encoder that writes a header, fixed section, and variable section.
- `BinaryResult`: WASM-friendly owned result buffer.
- `SchemaLayoutCache`: cache used by the host-facing API for full-table layouts.

## Format Summary

- 16-byte header: `row_count`, `row_stride`, `var_offset`, `flags`
- Per-row null mask: `ceil(column_count / 8)` bytes
- Fixed-width columns stored inline
- Variable-width columns (`String`, `Bytes`, `Jsonb`) stored as `(offset, len)` pairs into a trailing variable section

Interop notes:

- `Int64` and `DateTime` are encoded as `f64` values for JavaScript interop.
- The encoder treats JSONB payloads as opaque bytes. In the JS/WASM stack those bytes are UTF-8 JSON text.

See `PROTOCOL.md` for the full wire-format design notes.

## Example

```rust
use std::rc::Rc;

use cynos_binary::{BinaryEncoder, SchemaLayout};
use cynos_core::schema::TableBuilder;
use cynos_core::{DataType, Row, Value};

fn main() -> cynos_core::Result<()> {
    let schema = TableBuilder::new("users")?
        .add_column("id", DataType::Int64)?
        .add_column("name", DataType::String)?
        .build()?;

    let layout = SchemaLayout::from_schema(&schema);
    let mut encoder = BinaryEncoder::new(layout, 1);

    let rows = vec![Rc::new(Row::new(
        1,
        vec![Value::Int64(1), Value::String("Alice".into())],
    ))];

    encoder.encode_rows(&rows);
    let buffer = encoder.finish();

    assert!(!buffer.is_empty());
    Ok(())
}
```

## Notes

- Use `SchemaLayout::from_projection()` for projected queries.
- Use `SchemaLayout::from_schemas()` when encoding joined rows.
- Enable the `wasm` feature if you want `wasm-bindgen` exports such as `BinaryResult::asView()` and `BinaryResult::intoTransferable()`.

## License

Apache-2.0
