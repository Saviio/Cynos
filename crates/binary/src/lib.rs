//! Binary Protocol for high-performance row serialization.
//!
//! Provides a compact binary encoding format for transferring row data.
//! When the `wasm` feature is enabled, includes zero-copy WASM bindings
//! for direct JS access to encoded buffers.
//!
//! ## Binary Format (Row-Major)
//!
//! ```text
//! Header: 16 bytes
//! +----------+----------+------------+-------+
//! | row_count| row_stride| var_offset | flags |
//! | u32      | u32       | u32        | u32   |
//! +----------+----------+------------+-------+
//!
//! Fixed Section (row-major):
//! Row 0: [null_mask: ceil(cols/8) bytes][col0][col1][col2]
//! Row 1: [null_mask][col0][col1][col2]
//! ...
//!
//! Variable Section:
//! [string bytes][bytes data][jsonb data]
//! ```

#![no_std]

extern crate alloc;

mod encoder;
mod layout_cache;
mod schema_layout;

pub use encoder::BinaryEncoder;
pub use layout_cache::SchemaLayoutCache;
pub use schema_layout::{ColumnLayout, SchemaLayout};

use alloc::vec::Vec;

/// Header size in bytes
pub const HEADER_SIZE: usize = 16;

/// Header flags
pub mod flags {
    pub const HAS_NULLS: u32 = 1 << 0;
}

/// Data type IDs for binary encoding
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryDataType {
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Float64 = 3,
    String = 4,
    DateTime = 5,
    Bytes = 6,
    Jsonb = 7,
}

impl BinaryDataType {
    /// Get the fixed size in bytes for this data type
    pub fn fixed_size(self) -> usize {
        match self {
            BinaryDataType::Boolean => 1,
            BinaryDataType::Int32 => 4,
            BinaryDataType::Int64 => 8, // stored as f64 for JS compatibility
            BinaryDataType::Float64 => 8,
            BinaryDataType::String => 8, // (offset: u32, len: u32)
            BinaryDataType::DateTime => 8,
            BinaryDataType::Bytes => 8, // (offset: u32, len: u32)
            BinaryDataType::Jsonb => 8, // (offset: u32, len: u32)
        }
    }

    /// Check if this type uses variable-length storage
    pub fn is_variable_length(self) -> bool {
        matches!(
            self,
            BinaryDataType::String | BinaryDataType::Bytes | BinaryDataType::Jsonb
        )
    }
}

impl From<cynos_core::DataType> for BinaryDataType {
    fn from(dt: cynos_core::DataType) -> Self {
        match dt {
            cynos_core::DataType::Boolean => BinaryDataType::Boolean,
            cynos_core::DataType::Int32 => BinaryDataType::Int32,
            cynos_core::DataType::Int64 => BinaryDataType::Int64,
            cynos_core::DataType::Float64 => BinaryDataType::Float64,
            cynos_core::DataType::String => BinaryDataType::String,
            cynos_core::DataType::DateTime => BinaryDataType::DateTime,
            cynos_core::DataType::Bytes => BinaryDataType::Bytes,
            cynos_core::DataType::Jsonb => BinaryDataType::Jsonb,
        }
    }
}

/// Binary result buffer returned from execBinary()
#[cfg_attr(feature = "wasm", wasm_bindgen::prelude::wasm_bindgen)]
pub struct BinaryResult {
    buffer: Vec<u8>,
}

#[cfg(feature = "wasm")]
use wasm_bindgen::JsCast;

#[cfg(feature = "wasm")]
#[wasm_bindgen::prelude::wasm_bindgen]
impl BinaryResult {
    /// Get pointer to the buffer data (as usize for JS)
    pub fn ptr(&self) -> usize {
        self.buffer.as_ptr() as usize
    }

    /// Get buffer length
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if buffer is empty
    #[wasm_bindgen(js_name = isEmpty)]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Get buffer as Uint8Array (copies data to JS)
    /// Use asView() for zero-copy access instead.
    #[wasm_bindgen(js_name = toUint8Array)]
    pub fn to_uint8_array(&self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(&self.buffer[..])
    }

    /// Copy the buffer into a standalone Uint8Array suitable for `postMessage`
    /// transfer lists and other ownership-taking APIs.
    ///
    /// Unlike `asView()`, the returned bytes are no longer tied to WASM memory.
    #[wasm_bindgen(js_name = intoTransferable)]
    pub fn into_transferable(self) -> js_sys::Uint8Array {
        js_sys::Uint8Array::from(&self.buffer[..])
    }

    /// Get a zero-copy Uint8Array view into WASM memory.
    /// WARNING: This view becomes invalid if WASM memory grows or if this BinaryResult is freed.
    /// The caller must ensure the BinaryResult outlives any use of the returned view.
    #[wasm_bindgen(js_name = asView)]
    pub fn as_view(&self) -> js_sys::Uint8Array {
        let memory = wasm_bindgen::memory();
        let buffer = memory
            .dyn_ref::<js_sys::WebAssembly::Memory>()
            .expect("wasm_bindgen::memory() should return WebAssembly.Memory")
            .buffer();

        js_sys::Uint8Array::new_with_byte_offset_and_length(
            &buffer,
            self.buffer.as_ptr() as u32,
            self.buffer.len() as u32,
        )
    }

    /// Free the buffer memory
    pub fn free(self) {
        drop(self);
    }
}

#[cfg(not(feature = "wasm"))]
impl BinaryResult {
    /// Get pointer to the buffer data
    pub fn ptr(&self) -> usize {
        self.buffer.as_ptr() as usize
    }

    /// Get buffer length
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

impl BinaryResult {
    /// Create a new BinaryResult from a buffer
    pub fn new(buffer: Vec<u8>) -> Self {
        Self { buffer }
    }
}
