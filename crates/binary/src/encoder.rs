//! Binary encoder for high-performance row serialization.
//!
//! Encodes rows into a compact binary format that can be read directly
//! from WASM linear memory using JS DataView.

use super::{flags, BinaryDataType, SchemaLayout, HEADER_SIZE};
use alloc::rc::Rc;
use alloc::vec::Vec;
use cynos_core::{Row, Value};

/// High-performance binary encoder
pub struct BinaryEncoder {
    layout: SchemaLayout,
    /// Fixed section buffer (header + rows)
    buffer: Vec<u8>,
    /// Variable-length data buffer
    var_buffer: Vec<u8>,
    /// Number of rows encoded
    row_count: usize,
    /// Whether any NULL values were encountered
    has_nulls: bool,
}

impl BinaryEncoder {
    /// Create a new encoder with pre-allocated buffers
    pub fn new(layout: SchemaLayout, estimated_rows: usize) -> Self {
        let fixed_size = layout.calculate_fixed_size(estimated_rows);
        // Estimate variable data: ~32 bytes per row average
        let var_estimate = estimated_rows * 32;

        Self {
            layout,
            buffer: Vec::with_capacity(fixed_size),
            var_buffer: Vec::with_capacity(var_estimate),
            row_count: 0,
            has_nulls: false,
        }
    }

    /// Encode a batch of rows
    pub fn encode_rows(&mut self, rows: &[Rc<Row>]) {
        self.encode_rows_iter(rows.iter());
    }

    /// Encode a batch of rows from any iterator over shared rows.
    pub fn encode_rows_iter<'a, I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = &'a Rc<Row>>,
    {
        self.encode_row_refs_iter(rows.into_iter().map(|row| row.as_ref()));
    }

    /// Encode a batch of rows from any iterator over borrowed rows.
    pub fn encode_row_refs_iter<'a, I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = &'a Row>,
    {
        // Reserve space for header (will be written at the end)
        if self.buffer.is_empty() {
            self.buffer.resize(HEADER_SIZE, 0);
        }

        for row in rows {
            self.encode_row(row);
            self.row_count += 1;
        }
    }

    /// Encode a single row
    #[inline(always)]
    fn encode_row(&mut self, row: &Row) {
        let null_mask_size = self.layout.null_mask_size();
        let num_columns = self.layout.columns().len();

        // Reserve space for null_mask
        let null_mask_start = self.buffer.len();
        self.buffer.resize(null_mask_start + null_mask_size, 0);

        // Encode each column value
        for col_idx in 0..num_columns {
            let col_layout = &self.layout.columns()[col_idx];
            let data_type = col_layout.data_type;
            let fixed_size = col_layout.fixed_size;

            let value = row.get(col_idx);

            // Handle NULL
            if matches!(value, Some(Value::Null) | None) {
                // Set null bit
                let byte_idx = col_idx / 8;
                let bit_idx = col_idx % 8;
                self.buffer[null_mask_start + byte_idx] |= 1 << bit_idx;
                self.has_nulls = true;

                // Write zero bytes for the column
                self.buffer.extend(core::iter::repeat(0).take(fixed_size));
                continue;
            }

            let value = value.unwrap();
            self.encode_value_fast(value, data_type);
        }
    }

    /// Encode a single value - optimized with direct memory writes
    #[inline(always)]
    fn encode_value_fast(&mut self, value: &Value, data_type: BinaryDataType) {
        match (value, data_type) {
            (Value::Boolean(b), BinaryDataType::Boolean) => {
                self.buffer.push(if *b { 1 } else { 0 });
            }
            (Value::Int32(i), BinaryDataType::Int32) => {
                self.write_bytes_fast(&i.to_le_bytes());
            }
            (Value::Int64(i), BinaryDataType::Int64) => {
                // Store as f64 for JS Number compatibility
                let f = *i as f64;
                self.write_bytes_fast(&f.to_le_bytes());
            }
            (Value::Float64(f), BinaryDataType::Float64) => {
                self.write_bytes_fast(&f.to_le_bytes());
            }
            (Value::DateTime(ts), BinaryDataType::DateTime) => {
                // Store as f64 (milliseconds)
                let f = *ts as f64;
                self.write_bytes_fast(&f.to_le_bytes());
            }
            (Value::String(s), BinaryDataType::String) => {
                self.write_varlen_fast(s.as_bytes());
            }
            (Value::Bytes(b), BinaryDataType::Bytes) => {
                self.write_varlen_fast(b);
            }
            (Value::Jsonb(j), BinaryDataType::Jsonb) => {
                // JsonbValue stores JSON as bytes already
                self.write_varlen_fast(&j.0);
            }
            // Type mismatch - write zeros
            _ => {
                let size = data_type.fixed_size();
                self.buffer.extend(core::iter::repeat(0).take(size));
            }
        }
    }

    /// Write bytes directly using copy_nonoverlapping for maximum performance
    #[inline(always)]
    fn write_bytes_fast(&mut self, bytes: &[u8]) {
        let len = bytes.len();
        let old_len = self.buffer.len();
        self.buffer.reserve(len);

        // SAFETY: We just reserved enough space, and we're writing valid bytes
        unsafe {
            let dst = self.buffer.as_mut_ptr().add(old_len);
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, len);
            self.buffer.set_len(old_len + len);
        }
    }

    /// Write variable-length data and store (offset, length) in fixed section
    #[inline(always)]
    fn write_varlen_fast(&mut self, data: &[u8]) {
        let offset = self.var_buffer.len() as u32;
        let length = data.len() as u32;

        // Write offset and length to fixed section using fast path
        self.write_bytes_fast(&offset.to_le_bytes());
        self.write_bytes_fast(&length.to_le_bytes());

        // Append data to variable buffer using fast path
        let old_len = self.var_buffer.len();
        self.var_buffer.reserve(data.len());

        // SAFETY: We just reserved enough space
        unsafe {
            let dst = self.var_buffer.as_mut_ptr().add(old_len);
            core::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
            self.var_buffer.set_len(old_len + data.len());
        }
    }

    /// Finalize encoding and return the complete buffer
    pub fn finish(mut self) -> Vec<u8> {
        // Calculate var_offset (where variable section starts)
        let var_offset = self.buffer.len() as u32;

        // Write header
        let row_count = self.row_count as u32;
        let row_stride = self.layout.row_stride() as u32;
        let flags = if self.has_nulls { flags::HAS_NULLS } else { 0 };

        // Header: row_count (4) + row_stride (4) + var_offset (4) + flags (4)
        self.buffer[0..4].copy_from_slice(&row_count.to_le_bytes());
        self.buffer[4..8].copy_from_slice(&row_stride.to_le_bytes());
        self.buffer[8..12].copy_from_slice(&var_offset.to_le_bytes());
        self.buffer[12..16].copy_from_slice(&flags.to_le_bytes());

        // Append variable section
        self.buffer.append(&mut self.var_buffer);

        self.buffer
    }

    /// Get the schema layout
    pub fn layout(&self) -> &SchemaLayout {
        &self.layout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec;
    use cynos_core::schema::{Column, Table};
    use cynos_core::DataType;

    fn create_test_schema() -> Table {
        Table::new(
            "test",
            vec![
                Column::new("id", DataType::Int64),
                Column::new("name", DataType::String).nullable(true),
                Column::new("value", DataType::Float64),
            ],
        )
    }

    #[test]
    fn test_schema_layout() {
        let schema = create_test_schema();
        let layout = SchemaLayout::from_schema(&schema);

        assert_eq!(layout.columns().len(), 3);
        assert_eq!(layout.null_mask_size(), 1); // ceil(3/8) = 1
                                                // row_stride = null_mask(1) + id(8) + name(8) + value(8) = 25
        assert_eq!(layout.row_stride(), 25);
    }

    #[test]
    fn test_encode_simple_row() {
        let schema = create_test_schema();
        let layout = SchemaLayout::from_schema(&schema);
        let mut encoder = BinaryEncoder::new(layout, 1);

        let row = Rc::new(Row::new(
            1,
            vec![
                Value::Int64(42),
                Value::String("hello".to_string()),
                Value::Float64(3.14),
            ],
        ));

        encoder.encode_rows(&[row]);
        let buffer = encoder.finish();

        // Verify header
        let row_count = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        assert_eq!(row_count, 1);

        let row_stride = u32::from_le_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
        assert_eq!(row_stride, 25);
    }
}
