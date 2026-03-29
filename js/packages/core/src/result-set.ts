/**
 * ResultSet - Zero-copy binary result set for high-performance queries.
 *
 * Provides lazy decoding of query results directly from WASM linear memory
 * using DataView, eliminating serialization overhead.
 */

import type { BinaryResult, SchemaLayout } from './wasm.js';

type SchemaLayoutMethods = Pick<
  SchemaLayout,
  'columnCount' | 'columnName' | 'columnOffset' | 'columnType' | 'nullMaskSize' | 'rowStride'
>;

export interface ResultSetLayoutSnapshot {
  columnNames: string[];
  columnOffsets: number[];
  columnTypes: number[];
  nullMaskSize: number;
  rowStride: number;
}

export type ResultSetLayoutInput = SchemaLayoutMethods | ResultSetLayoutSnapshot;
export type ResultSetBufferInput = BinaryResult | Uint8Array | ArrayBuffer;

// Data type constants (must match Rust BinaryDataType)
const DataType = {
  Boolean: 0,
  Int32: 1,
  Int64: 2,
  Float64: 3,
  String: 4,
  DateTime: 5,
  Bytes: 6,
  Jsonb: 7,
} as const;

// Header size in bytes
const HEADER_SIZE = 16;

// Shared TextDecoder instance for performance
const textDecoder = new TextDecoder();

function isLayoutSnapshot(layout: ResultSetLayoutInput): layout is ResultSetLayoutSnapshot {
  return 'columnNames' in layout;
}

function normalizeLayout(layout: ResultSetLayoutInput): ResultSetLayoutSnapshot {
  if (isLayoutSnapshot(layout)) {
    return {
      columnNames: [...layout.columnNames],
      columnOffsets: [...layout.columnOffsets],
      columnTypes: [...layout.columnTypes],
      nullMaskSize: layout.nullMaskSize,
      rowStride: layout.rowStride,
    };
  }

  const columnCount = layout.columnCount();
  const columnNames: string[] = new Array(columnCount);
  const columnOffsets: number[] = new Array(columnCount);
  const columnTypes: number[] = new Array(columnCount);

  for (let i = 0; i < columnCount; i++) {
    columnNames[i] = layout.columnName(i) ?? '';
    columnOffsets[i] = layout.columnOffset(i) ?? 0;
    columnTypes[i] = layout.columnType(i) ?? 0;
  }

  return {
    columnNames,
    columnOffsets,
    columnTypes,
    nullMaskSize: layout.nullMaskSize(),
    rowStride: layout.rowStride(),
  };
}

function normalizeBuffer(
  buffer: ResultSetBufferInput
): { view: Uint8Array; release: () => void } {
  if (buffer instanceof Uint8Array) {
    return { view: buffer, release: () => {} };
  }

  if (buffer instanceof ArrayBuffer) {
    return { view: new Uint8Array(buffer), release: () => {} };
  }

  return {
    view: (buffer as any).asView(),
    release: () => buffer.free(),
  };
}

export function snapshotSchemaLayout(layout: SchemaLayoutMethods): ResultSetLayoutSnapshot {
  return normalizeLayout(layout);
}

/**
 * Zero-copy result set for binary protocol queries.
 * Provides lazy decoding of values directly from WASM linear memory.
 */
export class ResultSet<T = Record<string, unknown>> implements Iterable<T> {
  private readonly releaseBuffer: () => void;
  private readonly layout: ResultSetLayoutSnapshot;
  private readonly uint8Array: Uint8Array;
  private readonly dataView: DataView;

  // Cached header values
  private readonly _rowCount: number;
  private readonly _rowStride: number;
  private readonly _varOffset: number;
  private readonly _flags: number;
  private readonly _nullMaskSize: number;

  // Cached column info
  private readonly _columnNames: string[];
  private readonly _columnTypes: number[];
  private readonly _columnOffsets: number[];

  // Compiled row decoder for fast toArray() (null if CSP blocks new Function)
  private readonly _compiledGet: ((rowIndex: number) => T) | null;

  /**
   * Create a ResultSet from a binary buffer and schema layout.
   *
   * @param buffer - Binary result from execBinary(), or a transferred Uint8Array / ArrayBuffer
   * @param layout - Schema layout from getSchemaLayout(), or a serialized layout snapshot
   */
  constructor(buffer: ResultSetBufferInput, layout: ResultSetLayoutInput) {
    this.layout = normalizeLayout(layout);

    const normalizedBuffer = normalizeBuffer(buffer);
    this.releaseBuffer = normalizedBuffer.release;
    this.uint8Array = normalizedBuffer.view;
    this.dataView = new DataView(
      this.uint8Array.buffer,
      this.uint8Array.byteOffset,
      this.uint8Array.byteLength
    );

    // Parse header
    this._rowCount = this.dataView.getUint32(0, true);
    this._rowStride = this.dataView.getUint32(4, true);
    this._varOffset = this.dataView.getUint32(8, true);
    this._flags = this.dataView.getUint32(12, true);
    this._nullMaskSize = this.layout.nullMaskSize;

    // Cache column info
    this._columnNames = [...this.layout.columnNames];
    this._columnTypes = [...this.layout.columnTypes];
    this._columnOffsets = [...this.layout.columnOffsets];

    // Compile a specialized row decoder function for this schema.
    // This generates code with literal property names and inlined offsets,
    // enabling V8 hidden class optimization and eliminating per-row branching.
    // Falls back to generic get() if CSP blocks new Function().
    let compiled: ((rowIndex: number) => T) | null = null;
    try {
      compiled = this._compileRowDecoder();
    } catch (_) {
      // CSP or other restriction — fall back to generic decoder
    }
    this._compiledGet = compiled;
  }

  /** Number of rows in result */
  get length(): number {
    return this._rowCount;
  }

  /** Column names */
  get columns(): string[] {
    return this._columnNames;
  }

  /** Number of columns */
  get columnCount(): number {
    return this._columnNames.length;
  }

  /**
   * Check if a value is null.
   */
  isNull(rowIndex: number, columnIndex: number): boolean {
    if (this._flags === 0) return false; // No nulls in result

    const rowOffset = HEADER_SIZE + rowIndex * this._rowStride;
    const byteIndex = Math.floor(columnIndex / 8);
    const bitIndex = columnIndex % 8;
    const byte = this.dataView.getUint8(rowOffset + byteIndex);
    return (byte & (1 << bitIndex)) !== 0;
  }

  /**
   * Get the byte offset for a column value in a row.
   */
  private getValueOffset(rowIndex: number, columnIndex: number): number {
    return HEADER_SIZE + rowIndex * this._rowStride + this._nullMaskSize + this._columnOffsets[columnIndex];
  }

  // Type-specific getters

  getBoolean(rowIndex: number, columnIndex: number): boolean | null {
    if (this.isNull(rowIndex, columnIndex)) return null;
    const offset = this.getValueOffset(rowIndex, columnIndex);
    return this.dataView.getUint8(offset) !== 0;
  }

  getInt32(rowIndex: number, columnIndex: number): number | null {
    if (this.isNull(rowIndex, columnIndex)) return null;
    const offset = this.getValueOffset(rowIndex, columnIndex);
    return this.dataView.getInt32(offset, true);
  }

  getNumber(rowIndex: number, columnIndex: number): number | null {
    if (this.isNull(rowIndex, columnIndex)) return null;
    const offset = this.getValueOffset(rowIndex, columnIndex);
    return this.dataView.getFloat64(offset, true);
  }

  getString(rowIndex: number, columnIndex: number): string | null {
    if (this.isNull(rowIndex, columnIndex)) return null;
    const offset = this.getValueOffset(rowIndex, columnIndex);
    const strOffset = this.dataView.getUint32(offset, true);
    const strLength = this.dataView.getUint32(offset + 4, true);

    if (strLength === 0) return '';

    // Decode UTF-8 from variable section using shared decoder
    const bytes = this.uint8Array.subarray(
      this._varOffset + strOffset,
      this._varOffset + strOffset + strLength
    );
    return textDecoder.decode(bytes);
  }

  getDate(rowIndex: number, columnIndex: number): Date | null {
    if (this.isNull(rowIndex, columnIndex)) return null;
    const offset = this.getValueOffset(rowIndex, columnIndex);
    const timestamp = this.dataView.getFloat64(offset, true);
    return new Date(timestamp);
  }

  getBytes(rowIndex: number, columnIndex: number): Uint8Array | null {
    if (this.isNull(rowIndex, columnIndex)) return null;
    const offset = this.getValueOffset(rowIndex, columnIndex);
    const bytesOffset = this.dataView.getUint32(offset, true);
    const bytesLength = this.dataView.getUint32(offset + 4, true);

    if (bytesLength === 0) return new Uint8Array(0);

    // Return copy of bytes
    return this.uint8Array.slice(
      this._varOffset + bytesOffset,
      this._varOffset + bytesOffset + bytesLength
    );
  }

  getJsonb(rowIndex: number, columnIndex: number): unknown | null {
    if (this.isNull(rowIndex, columnIndex)) return null;
    const offset = this.getValueOffset(rowIndex, columnIndex);
    const jsonOffset = this.dataView.getUint32(offset, true);
    const jsonLength = this.dataView.getUint32(offset + 4, true);

    if (jsonLength === 0) return null;

    // Decode JSON from variable section using shared decoder
    const bytes = this.uint8Array.subarray(
      this._varOffset + jsonOffset,
      this._varOffset + jsonOffset + jsonLength
    );
    const jsonStr = textDecoder.decode(bytes);
    return JSON.parse(jsonStr);
  }

  /**
   * Get value by row and column index (auto-typed).
   */
  getValue(rowIndex: number, columnIndex: number): unknown {
    const colType = this._columnTypes[columnIndex];
    switch (colType) {
      case DataType.Boolean:
        return this.getBoolean(rowIndex, columnIndex);
      case DataType.Int32:
        return this.getInt32(rowIndex, columnIndex);
      case DataType.Int64:
      case DataType.Float64:
        return this.getNumber(rowIndex, columnIndex);
      case DataType.DateTime:
        return this.getDate(rowIndex, columnIndex);
      case DataType.String:
        return this.getString(rowIndex, columnIndex);
      case DataType.Bytes:
        return this.getBytes(rowIndex, columnIndex);
      case DataType.Jsonb:
        return this.getJsonb(rowIndex, columnIndex);
      default:
        return null;
    }
  }

  /**
   * Compile a specialized row decoder function for this schema.
   * Generates code with literal property names and inlined type dispatch,
   * so V8 creates a single hidden class for all row objects.
   */
  private _compileRowDecoder(): (rowIndex: number) => T {
    const hasNulls = this._flags !== 0;
    const colCount = this._columnNames.length;
    const nullMaskSize = this._nullMaskSize;
    const varOffset = this._varOffset;

    // Build function body lines
    const lines: string[] = [];
    lines.push(`var rowOff = ${HEADER_SIZE} + rowIndex * ${this._rowStride};`);
    lines.push(`var dOff = rowOff + ${nullMaskSize};`);
    lines.push(`var dv = dataView;`);
    lines.push(`var u8 = uint8Array;`);

    // Build object literal with all properties in fixed order (hidden class friendly)
    // First pass: compute each column value into a local variable
    for (let i = 0; i < colCount; i++) {
      const colOffset = this._columnOffsets[i];
      const colType = this._columnTypes[i];
      const vName = `v${i}`;

      if (hasNulls) {
        const byteIdx = i >> 3;
        const bitMask = 1 << (i & 7);
        lines.push(`var ${vName};`);
        lines.push(`if (dv.getUint8(rowOff + ${byteIdx}) & ${bitMask}) { ${vName} = null; } else {`);
      } else {
        lines.push(`var ${vName};`);
      }

      const off = `dOff + ${colOffset}`;
      switch (colType) {
        case DataType.Boolean:
          lines.push(`${vName} = dv.getUint8(${off}) !== 0;`);
          break;
        case DataType.Int32:
          lines.push(`${vName} = dv.getInt32(${off}, true);`);
          break;
        case DataType.Int64:
        case DataType.Float64:
          lines.push(`${vName} = dv.getFloat64(${off}, true);`);
          break;
        case DataType.DateTime:
          lines.push(`${vName} = new Date(dv.getFloat64(${off}, true));`);
          break;
        case DataType.String:
          lines.push(`{ var so = dv.getUint32(${off}, true), sl = dv.getUint32(${off} + 4, true);`);
          lines.push(`${vName} = sl === 0 ? '' : td.decode(u8.subarray(${varOffset} + so, ${varOffset} + so + sl)); }`);
          break;
        case DataType.Bytes:
          lines.push(`{ var bo = dv.getUint32(${off}, true), bl = dv.getUint32(${off} + 4, true);`);
          lines.push(`${vName} = bl === 0 ? new Uint8Array(0) : u8.slice(${varOffset} + bo, ${varOffset} + bo + bl); }`);
          break;
        case DataType.Jsonb:
          lines.push(`{ var jo = dv.getUint32(${off}, true), jl = dv.getUint32(${off} + 4, true);`);
          lines.push(`${vName} = jl === 0 ? null : JSON.parse(td.decode(u8.subarray(${varOffset} + jo, ${varOffset} + jo + jl))); }`);
          break;
        default:
          lines.push(`${vName} = null;`);
      }

      if (hasNulls) {
        lines.push(`}`); // close else
      }
    }

    // Build object literal — all properties in consistent order for hidden class
    const props = this._columnNames.map((name, i) => {
      // Escape property name for safety
      const safeName = JSON.stringify(name);
      return `${safeName}: v${i}`;
    });
    lines.push(`return {${props.join(',')}};`);

    const body = lines.join('\n');
    // Create function with closed-over references
    const fn = new Function('dataView', 'uint8Array', 'td',
      `return function getRow(rowIndex) {\n${body}\n};`
    );
    return fn(this.dataView, this.uint8Array, textDecoder) as (rowIndex: number) => T;
  }

  /**
   * Get a row as an object (lazy creation).
   * Uses compiled decoder when available, falls back to generic decoder.
   */
  get(rowIndex: number): T {
    if (this._compiledGet !== null) return this._compiledGet(rowIndex);
    return this._genericGet(rowIndex);
  }

  /**
   * Generic row decoder — fallback when compiled decoder is unavailable (CSP).
   */
  private _genericGet(rowIndex: number): T {
    const row: Record<string, unknown> = {};
    const rowOffset = HEADER_SIZE + rowIndex * this._rowStride;
    const dataOffset = rowOffset + this._nullMaskSize;
    const hasNulls = this._flags !== 0;
    const colCount = this._columnNames.length;

    for (let i = 0; i < colCount; i++) {
      if (hasNulls) {
        const byteIndex = i >> 3;
        const bitIndex = i & 7;
        if ((this.dataView.getUint8(rowOffset + byteIndex) & (1 << bitIndex)) !== 0) {
          row[this._columnNames[i]] = null;
          continue;
        }
      }

      const offset = dataOffset + this._columnOffsets[i];
      const colType = this._columnTypes[i];

      switch (colType) {
        case DataType.Boolean:
          row[this._columnNames[i]] = this.dataView.getUint8(offset) !== 0;
          break;
        case DataType.Int32:
          row[this._columnNames[i]] = this.dataView.getInt32(offset, true);
          break;
        case DataType.Int64:
        case DataType.Float64:
          row[this._columnNames[i]] = this.dataView.getFloat64(offset, true);
          break;
        case DataType.DateTime:
          row[this._columnNames[i]] = new Date(this.dataView.getFloat64(offset, true));
          break;
        case DataType.String: {
          const strOffset = this.dataView.getUint32(offset, true);
          const strLength = this.dataView.getUint32(offset + 4, true);
          if (strLength === 0) {
            row[this._columnNames[i]] = '';
          } else {
            row[this._columnNames[i]] = textDecoder.decode(
              this.uint8Array.subarray(
                this._varOffset + strOffset,
                this._varOffset + strOffset + strLength
              )
            );
          }
          break;
        }
        case DataType.Bytes: {
          const bytesOffset = this.dataView.getUint32(offset, true);
          const bytesLength = this.dataView.getUint32(offset + 4, true);
          if (bytesLength === 0) {
            row[this._columnNames[i]] = new Uint8Array(0);
          } else {
            row[this._columnNames[i]] = this.uint8Array.slice(
              this._varOffset + bytesOffset,
              this._varOffset + bytesOffset + bytesLength
            );
          }
          break;
        }
        case DataType.Jsonb: {
          const jsonOffset = this.dataView.getUint32(offset, true);
          const jsonLength = this.dataView.getUint32(offset + 4, true);
          if (jsonLength === 0) {
            row[this._columnNames[i]] = null;
          } else {
            row[this._columnNames[i]] = JSON.parse(
              textDecoder.decode(
                this.uint8Array.subarray(
                  this._varOffset + jsonOffset,
                  this._varOffset + jsonOffset + jsonLength
                )
              )
            );
          }
          break;
        }
        default:
          row[this._columnNames[i]] = null;
      }
    }
    return row as T;
  }

  // Array-compatible API

  /**
   * Convert to array (compatibility with old API).
   */
  toArray(): T[] {
    const result: T[] = new Array(this._rowCount);
    for (let i = 0; i < this._rowCount; i++) {
      result[i] = this.get(i);
    }
    return result;
  }

  /**
   * Map over rows.
   */
  map<U>(fn: (row: T, index: number) => U): U[] {
    const result: U[] = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = fn(this.get(i), i);
    }
    return result;
  }

  /**
   * Filter rows.
   */
  filter(fn: (row: T, index: number) => boolean): T[] {
    const result: T[] = [];
    for (let i = 0; i < this.length; i++) {
      const row = this.get(i);
      if (fn(row, i)) {
        result.push(row);
      }
    }
    return result;
  }

  /**
   * Find first matching row.
   */
  find(fn: (row: T, index: number) => boolean): T | undefined {
    for (let i = 0; i < this.length; i++) {
      const row = this.get(i);
      if (fn(row, i)) {
        return row;
      }
    }
    return undefined;
  }

  /**
   * Iterate over rows.
   */
  forEach(fn: (row: T, index: number) => void): void {
    for (let i = 0; i < this.length; i++) {
      fn(this.get(i), i);
    }
  }

  /**
   * Iterator protocol.
   */
  *[Symbol.iterator](): Iterator<T> {
    for (let i = 0; i < this.length; i++) {
      yield this.get(i);
    }
  }

  /**
   * Free the underlying WASM memory.
   */
  free(): void {
    this.releaseBuffer();
  }

  /**
   * Disposable support (using statement).
   */
  [Symbol.dispose](): void {
    this.free();
  }
}
