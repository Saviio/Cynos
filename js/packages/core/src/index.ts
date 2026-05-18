/**
 * Cynos Database - Main Entry Point
 *
 * High-performance in-memory database for JavaScript/TypeScript with:
 * - Reactive queries with incremental view maintenance
 * - JSONB support
 * - Transaction support
 * - Type-safe query builders
 */

// Import WASM module
import init, {
  Database as WasmDatabase,
  JsTableBuilder,
  JsTable,
  SelectBuilder,
  PreparedSelectQuery,
  PreparedGraphqlQuery,
  InsertBuilder,
  UpdateBuilder,
  DeleteBuilder,
  JsTransaction,
  JsObservableQuery,
  JsGraphqlSubscription,
  JsIvmObservableQuery,
  JsChangesStream,
  Column,
  Expr,
  JsDataType,
  JsSortOrder,
  ColumnOptions,
  ForeignKeyOptions,
  col,
  SchemaLayout,
  BinaryResult,
} from './wasm.js';

// Import ResultSet helpers
import { ResultSet, snapshotSchemaLayout } from './result-set.js';
export type {
  ResultSetBufferInput,
  ResultSetLayoutInput,
  ResultSetLayoutSnapshot,
} from './result-set.js';

export type {
  JsTableBuilder,
  JsTable,
  SelectBuilder,
  PreparedSelectQuery,
  PreparedGraphqlQuery,
  InsertBuilder,
  UpdateBuilder,
  DeleteBuilder,
  JsTransaction,
  JsObservableQuery,
  JsGraphqlSubscription,
  JsIvmObservableQuery,
  JsChangesStream,
  Column,
  Expr,
  SchemaLayout,
  BinaryResult,
};

export { JsDataType, JsSortOrder, ColumnOptions, ForeignKeyOptions, col, ResultSet, snapshotSchemaLayout };

export type { DataType, SortOrder, ChangeSet, Row, SubscriptionCallback, Unsubscribe } from './types.js';

let initialized = false;

/**
 * Initialize the Cynos WASM module.
 * Must be called before using any database functionality.
 */
export async function initCynos(): Promise<void> {
  if (initialized) return;
  await init();
  initialized = true;
}

/**
 * Create a new Cynos database instance.
 *
 * @param name - Database name
 * @returns Database instance
 *
 * @example
 * ```typescript
 * await initCynos();
 * const db = createDatabase('mydb');
 *
 * // Create a table
 * const users = db.createTable('users')
 *   .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
 *   .column('name', JsDataType.String)
 *   .column('age', JsDataType.Int32);
 * db.registerTable(users);
 *
 * // Insert data
 * await db.insert('users').values([
 *   { id: 1, name: 'Alice', age: 25 },
 *   { id: 2, name: 'Bob', age: 30 }
 * ]).exec();
 *
 * // Query data
 * const results = await db.select('*').from('users').exec();
 * ```
 */
export function createDatabase(name: string): WasmDatabase {
  if (!initialized) {
    throw new Error('Cynos not initialized. Call initCynos() first.');
  }
  return new WasmDatabase(name);
}

// Re-export Database class for direct usage
export { WasmDatabase as Database };
