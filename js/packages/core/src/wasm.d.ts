/* tslint:disable */
/* eslint-disable */

/**
 * Binary result buffer returned from execBinary()
 */
export class BinaryResult {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Get a zero-copy Uint8Array view into WASM memory.
     * WARNING: This view becomes invalid if WASM memory grows or if this BinaryResult is freed.
     * The caller must ensure the BinaryResult outlives any use of the returned view.
     */
    asView(): Uint8Array;
    /**
     * Free the buffer memory
     */
    free(): void;
    /**
     * Copy the buffer into a standalone Uint8Array suitable for `postMessage`
     * transfer lists and other ownership-taking APIs.
     *
     * Unlike `asView()`, the returned bytes are no longer tied to WASM memory.
     */
    intoTransferable(): Uint8Array;
    /**
     * Check if buffer is empty
     */
    isEmpty(): boolean;
    /**
     * Get buffer length
     */
    len(): number;
    /**
     * Get pointer to the buffer data (as usize for JS)
     */
    ptr(): number;
    /**
     * Get buffer as Uint8Array (copies data to JS)
     * Use asView() for zero-copy access instead.
     */
    toUint8Array(): Uint8Array;
}

/**
 * A column reference for building expressions.
 */
export class Column {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Creates a BETWEEN expression: column BETWEEN low AND high
     */
    between(low: any, high: any): Expr;
    /**
     * Creates an equality expression: column = value
     */
    eq(value: any): Expr;
    /**
     * Creates a JSONB path access expression
     */
    get(path: string): JsonbColumn;
    /**
     * Creates a greater-than expression: column > value
     */
    gt(value: any): Expr;
    /**
     * Creates a greater-than-or-equal expression: column >= value
     */
    gte(value: any): Expr;
    /**
     * Creates an IN expression: column IN (values)
     */
    in(values: any): Expr;
    /**
     * Creates an IS NOT NULL expression
     */
    isNotNull(): Expr;
    /**
     * Creates an IS NULL expression
     */
    isNull(): Expr;
    /**
     * Creates a LIKE expression: column LIKE pattern
     */
    like(pattern: string): Expr;
    /**
     * Creates a less-than expression: column < value
     */
    lt(value: any): Expr;
    /**
     * Creates a less-than-or-equal expression: column <= value
     */
    lte(value: any): Expr;
    /**
     * Creates a MATCH (regex) expression: column MATCH pattern
     */
    match(pattern: string): Expr;
    /**
     * Creates a not-equal expression: column != value
     */
    ne(value: any): Expr;
    /**
     * Creates a new column reference with table name.
     */
    constructor(table: string, name: string);
    /**
     * Creates a simple column reference without table name.
     * If the name contains a dot (e.g., "orders.year"), it will be parsed
     * as table.column.
     */
    static new_simple(name: string): Column;
    /**
     * Creates a NOT BETWEEN expression: column NOT BETWEEN low AND high
     */
    notBetween(low: any, high: any): Expr;
    /**
     * Creates a NOT IN expression: column NOT IN (values)
     */
    notIn(values: any): Expr;
    /**
     * Creates a NOT LIKE expression: column NOT LIKE pattern
     */
    notLike(pattern: string): Expr;
    /**
     * Creates a NOT MATCH (regex) expression: column NOT MATCH pattern
     */
    notMatch(pattern: string): Expr;
    /**
     * Sets the column index.
     */
    with_index(index: number): Column;
    /**
     * Returns the column name.
     */
    readonly name: string;
    /**
     * Returns the table name if set.
     */
    readonly tableName: string | undefined;
}

/**
 * Column options for table creation.
 */
export class ColumnOptions {
    free(): void;
    [Symbol.dispose](): void;
    constructor();
    primaryKey(value: boolean): ColumnOptions;
    setAutoIncrement(value: boolean): ColumnOptions;
    setNullable(value: boolean): ColumnOptions;
    setUnique(value: boolean): ColumnOptions;
    auto_increment: boolean;
    nullable: boolean;
    primary_key: boolean;
    unique: boolean;
}

/**
 * The main database interface.
 *
 * Provides methods for:
 * - Creating and dropping tables
 * - CRUD operations (insert, select, update, delete)
 * - Transaction management
 * - Observable queries
 */
export class Database {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Clears all data from all tables.
     */
    clear(): void;
    /**
     * Clears data from a specific table.
     */
    clearTable(name: string): void;
    /**
     * Async factory method for creating a database (for WASM compatibility).
     */
    static create(name: string): Promise<Database>;
    /**
     * Creates a new table builder.
     */
    createTable(name: string): JsTableBuilder;
    /**
     * Starts a DELETE operation.
     */
    delete(table: string): DeleteBuilder;
    /**
     * Drops a table from the database.
     */
    dropTable(name: string): void;
    /**
     * Executes a GraphQL query against the current database snapshot.
     *
     * Returns a standard GraphQL payload object with a single `data` property.
     */
    graphql(query: string, variables?: any | null, operation_name?: string | null): any;
    /**
     * Renders the current GraphQL schema as SDL.
     */
    graphqlSchema(): string;
    /**
     * Checks if a table exists.
     */
    hasTable(name: string): boolean;
    /**
     * Starts an INSERT operation.
     */
    insert(table: string): InsertBuilder;
    /**
     * Creates a new database instance.
     */
    constructor(name: string);
    /**
     * Parses and prepares a GraphQL query for repeated execution.
     */
    prepareGraphql(query: string, operation_name?: string | null): PreparedGraphqlQuery;
    /**
     * Registers a table schema with the database.
     */
    registerTable(builder: JsTableBuilder): void;
    /**
     * Starts a SELECT query.
     * Accepts either:
     * - A single string: select('*') or select('name')
     * - Multiple strings: select('name', 'score') - passed as variadic args
     */
    select(...columns: any): SelectBuilder;
    /**
     * Creates a live GraphQL subscription backed by the root query planner path.
     */
    subscribeGraphql(query: string, variables?: any | null, operation_name?: string | null): JsGraphqlSubscription;
    /**
     * Gets a table reference by name.
     */
    table(name: string): JsTable | undefined;
    /**
     * Returns the number of tables.
     */
    tableCount(): number;
    /**
     * Returns all table names.
     */
    tableNames(): Array<any>;
    takeLastCommitProfile(): any;
    takeLastDeltaFlushProfile(): any;
    takeLastIvmBridgeProfile(): any;
    takeLastSnapshotFlushProfile(): any;
    takeLastTraceInitProfile(): any;
    /**
     * Returns the total row count across all tables.
     */
    totalRowCount(): number;
    /**
     * Begins a new transaction.
     */
    transaction(): JsTransaction;
    /**
     * Starts an UPDATE operation.
     */
    update(table: string): UpdateBuilder;
    /**
     * Returns the database name.
     */
    readonly name: string;
}

/**
 * DELETE query builder.
 */
export class DeleteBuilder {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Executes the delete operation.
     */
    exec(): Promise<any>;
    /**
     * Sets or extends the WHERE clause.
     * Multiple calls to where_() are combined with AND.
     */
    where(predicate: Expr): DeleteBuilder;
}

/**
 * Expression type for query predicates.
 */
export class Expr {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Creates an AND expression: self AND other
     */
    and(other: Expr): Expr;
    /**
     * Creates a NOT expression: NOT self
     */
    not(): Expr;
    /**
     * Creates an OR expression: self OR other
     */
    or(other: Expr): Expr;
}

/**
 * Foreign-key options for GraphQL relation naming.
 */
export class ForeignKeyOptions {
    free(): void;
    [Symbol.dispose](): void;
    fieldName(value: string): ForeignKeyOptions;
    constructor();
    reverseFieldName(value: string): ForeignKeyOptions;
}

/**
 * INSERT query builder.
 */
export class InsertBuilder {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Executes the insert operation.
     */
    exec(): Promise<any>;
    /**
     * Sets the values to insert.
     */
    values(data: any): InsertBuilder;
}

/**
 * JavaScript-friendly changes stream.
 *
 * This provides the `changes()` API that yields the complete result set
 * whenever data changes. The callback receives the full current data,
 * not incremental changes - perfect for React's setState pattern.
 */
export class JsChangesStream {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Returns the current result.
     */
    getResult(): any;
    /**
     * Returns the current result as a binary buffer for zero-copy access.
     */
    getResultBinary(): BinaryResult;
    /**
     * Returns the schema layout for decoding binary results.
     */
    getSchemaLayout(): SchemaLayout;
    /**
     * Subscribes to the changes stream.
     *
     * The callback receives the complete current result set as a JavaScript array.
     * It is called immediately with the initial data, and again whenever data changes.
     * Perfect for React: `stream.subscribe(data => setUsers(data))`
     *
     * Returns an unsubscribe function.
     */
    subscribe(callback: Function): Function;
    /**
     * Subscribes to the changes stream using binary snapshots.
     *
     * The callback receives a `BinaryResult` for the full current result set.
     * It is called immediately with the initial data, and again whenever data changes.
     * Use `getSchemaLayout()` once and decode with `ResultSet` on the JS side.
     *
     * Returns an unsubscribe function.
     */
    subscribeBinary(callback: Function): Function;
}

/**
 * Data types supported by Cynos.
 */
export enum JsDataType {
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Float64 = 3,
    String = 4,
    DateTime = 5,
    Bytes = 6,
    Jsonb = 7,
}

/**
 * JavaScript-friendly GraphQL subscription wrapper.
 *
 * The callback receives a standard GraphQL payload object with a single `data`
 * property. The payload is emitted immediately on subscribe and again whenever
 * the rendered GraphQL response changes.
 */
export class JsGraphqlSubscription {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Returns the current GraphQL payload.
     */
    getResult(): any;
    /**
     * Subscribes to GraphQL payload changes and emits the initial value immediately.
     */
    subscribe(callback: Function): Function;
    /**
     * Returns the number of active subscriptions.
     */
    subscriptionCount(): number;
}

/**
 * JavaScript-friendly IVM observable query wrapper.
 * Uses DBSP-based incremental view maintenance for O(delta) updates.
 */
export class JsIvmObservableQuery {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Returns the current result as a JavaScript array.
     */
    getResult(): any;
    /**
     * Returns the current result as a binary buffer for zero-copy access.
     */
    getResultBinary(): BinaryResult;
    /**
     * Returns the schema layout for decoding binary results.
     */
    getSchemaLayout(): SchemaLayout;
    /**
     * Returns whether the result is empty.
     */
    isEmpty(): boolean;
    /**
     * Subscribes to IVM query changes.
     *
     * The callback receives a delta object `{ added: Row[], removed: Row[] }`
     * instead of the full result set. This is the true O(delta) path —
     * the UI side should apply the delta to its own state.
     *
     * Use `getResult()` to get the initial full result before subscribing.
     * Returns an unsubscribe function.
     */
    subscribe(callback: Function): Function;
    /**
     * Returns the number of active subscriptions.
     */
    subscriptionCount(): number;
    /**
     * Returns the number of rows in the result.
     */
    readonly length: number;
}

/**
 * JavaScript-friendly observable query wrapper.
 * Uses re-query strategy for optimal performance with indexes.
 */
export class JsObservableQuery {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Returns the current result as a JavaScript array.
     */
    getResult(): any;
    /**
     * Returns the current result as a binary buffer for zero-copy access.
     */
    getResultBinary(): BinaryResult;
    /**
     * Returns the schema layout for decoding binary results.
     */
    getSchemaLayout(): SchemaLayout;
    /**
     * Returns whether the result is empty.
     */
    isEmpty(): boolean;
    /**
     * Subscribes to query changes.
     *
     * The callback receives the complete current result set as a JavaScript array.
     * It is called whenever data changes (not immediately - use getResult for initial data).
     * Returns an unsubscribe function.
     */
    subscribe(callback: Function): Function;
    /**
     * Subscribes to query changes using binary snapshots.
     *
     * The callback receives a `BinaryResult` for the complete current result set.
     * This avoids per-update JS object materialization inside the WASM bridge.
     * Call `getSchemaLayout()` once and decode with `ResultSet` on the JS side.
     *
     * It is called whenever data changes (not immediately - use getResultBinary for initial data).
     * Returns an unsubscribe function.
     */
    subscribeBinary(callback: Function): Function;
    /**
     * Returns the number of active subscriptions.
     */
    subscriptionCount(): number;
    /**
     * Returns the number of rows in the result.
     */
    readonly length: number;
}

/**
 * Sort order for ORDER BY clauses.
 */
export enum JsSortOrder {
    Asc = 0,
    Desc = 1,
}

/**
 * JavaScript-friendly table reference.
 */
export class JsTable {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Returns a column reference.
     */
    col(name: string): Column | undefined;
    /**
     * Returns the number of columns.
     */
    columnCount(): number;
    /**
     * Returns the column names.
     */
    columnNames(): Array<any>;
    /**
     * Returns the column data type.
     */
    getColumnType(name: string): JsDataType | undefined;
    /**
     * Returns whether a column is nullable.
     */
    isColumnNullable(name: string): boolean;
    /**
     * Returns the primary key column names.
     */
    primaryKeyColumns(): Array<any>;
    /**
     * Returns the table name.
     */
    readonly name: string;
}

/**
 * JavaScript-friendly table builder.
 */
export class JsTableBuilder {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Builds the table schema and returns a JsTable.
     */
    build(): JsTable;
    /**
     * Adds a column to the table.
     */
    column(name: string, data_type: JsDataType, options?: ColumnOptions | null): JsTableBuilder;
    /**
     * Adds a foreign key and optional GraphQL relation names.
     */
    foreignKey(name: string, child_column: string, parent_table: string, parent_column: string, options?: ForeignKeyOptions | null): JsTableBuilder;
    /**
     * Adds an index to the table.
     */
    index(name: string, columns: any): JsTableBuilder;
    /**
     * Adds a JSONB index for specific paths.
     */
    jsonbIndex(column: string, paths: any): JsTableBuilder;
    /**
     * Creates a new table builder.
     */
    constructor(name: string);
    /**
     * Sets the primary key columns.
     */
    primaryKey(columns: any): JsTableBuilder;
    /**
     * Adds a unique index to the table.
     */
    uniqueIndex(name: string, columns: any): JsTableBuilder;
    /**
     * Returns the table name.
     */
    readonly name: string;
}

/**
 * JavaScript-friendly transaction wrapper.
 */
export class JsTransaction {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Commits the transaction.
     */
    commit(): void;
    /**
     * Deletes rows from a table within the transaction.
     */
    delete(table: string, predicate?: Expr | null): number;
    /**
     * Inserts rows into a table within the transaction.
     */
    insert(table: string, values: any): void;
    /**
     * Rolls back the transaction.
     */
    rollback(): void;
    /**
     * Updates rows in a table within the transaction.
     */
    update(table: string, set_values: any, predicate?: Expr | null): number;
    /**
     * Returns whether the transaction is still active.
     */
    readonly active: boolean;
    /**
     * Returns the transaction state.
     */
    readonly state: string;
}

/**
 * A JSONB column with path access.
 */
export class JsonbColumn {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Creates a contains expression for the JSONB path.
     */
    contains(value: any): Expr;
    /**
     * Creates an equality expression for the JSONB path value.
     */
    eq(value: any): Expr;
    /**
     * Creates an exists expression for the JSONB path.
     */
    exists(): Expr;
}

/**
 * A prepared GraphQL query that reuses the parsed document across executions.
 */
export class PreparedGraphqlQuery {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Executes the prepared GraphQL query with an optional variables object.
     */
    exec(variables?: any | null): any;
    /**
     * Creates a live subscription from a prepared GraphQL subscription document.
     */
    subscribe(variables?: any | null): JsGraphqlSubscription;
}

export class PreparedSelectQuery {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Executes the prepared query and returns JS objects.
     */
    exec(): Promise<any>;
    /**
     * Executes the prepared query and returns a binary result buffer.
     */
    execBinary(): Promise<BinaryResult>;
    /**
     * Gets the schema layout for binary decoding.
     */
    getSchemaLayout(): SchemaLayout;
}

/**
 * Pre-computed layout for binary encoding/decoding
 */
export class SchemaLayout {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Get the number of columns
     */
    columnCount(): number;
    /**
     * Get column fixed size by index
     */
    columnFixedSize(idx: number): number | undefined;
    /**
     * Get column name by index
     */
    columnName(idx: number): string | undefined;
    /**
     * Check if column is nullable
     */
    columnNullable(idx: number): boolean | undefined;
    /**
     * Get column offset by index (offset within row, after null_mask)
     */
    columnOffset(idx: number): number | undefined;
    /**
     * Get column type by index (returns BinaryDataType as u8)
     */
    columnType(idx: number): number | undefined;
    /**
     * Get null mask size in bytes
     */
    nullMaskSize(): number;
    /**
     * Get row stride (total bytes per row)
     */
    rowStride(): number;
}

/**
 * SELECT query builder.
 */
export class SelectBuilder {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Adds an AVG(column) aggregate.
     */
    avg(column: string): SelectBuilder;
    /**
     * Creates a changes stream (initial + incremental).
     */
    changes(): JsChangesStream;
    /**
     * Adds a COUNT(*) aggregate.
     */
    count(): SelectBuilder;
    /**
     * Adds a COUNT(column) aggregate.
     */
    countCol(column: string): SelectBuilder;
    /**
     * Adds a DISTINCT(column) aggregate (returns count of distinct values).
     */
    distinct(column: string): SelectBuilder;
    /**
     * Executes the query and returns results.
     */
    exec(): Promise<any>;
    /**
     * Executes the query and returns a binary result buffer.
     * Use with getSchemaLayout() for zero-copy decoding in JS.
     */
    execBinary(): Promise<BinaryResult>;
    /**
     * Explains the query plan without executing it.
     *
     * Returns an object with:
     * - `logical`: The original logical plan
     * - `optimized`: The optimized logical plan (after index selection, etc.)
     * - `physical`: The final physical execution plan
     */
    explain(): any;
    /**
     * Sets the FROM table.
     */
    from(table: string): SelectBuilder;
    /**
     * Adds a GEOMEAN(column) aggregate.
     */
    geomean(column: string): SelectBuilder;
    /**
     * Gets the schema layout for binary decoding.
     * The layout can be cached by JS for repeated queries on the same table.
     */
    getSchemaLayout(): SchemaLayout;
    /**
     * Adds a GROUP BY clause.
     */
    groupBy(columns: any): SelectBuilder;
    /**
     * Adds an INNER JOIN.
     */
    innerJoin(table: string, condition: Expr): SelectBuilder;
    /**
     * Adds a LEFT JOIN.
     */
    leftJoin(table: string, condition: Expr): SelectBuilder;
    /**
     * Sets the LIMIT.
     */
    limit(n: number): SelectBuilder;
    /**
     * Adds a MAX(column) aggregate.
     */
    max(column: string): SelectBuilder;
    /**
     * Adds a MIN(column) aggregate.
     */
    min(column: string): SelectBuilder;
    /**
     * Creates an observable query using the cached execution path.
     * When data changes, the engine reuses the compiled plan and can apply
     * row-local patches for simple single-table pipelines instead of always
     * re-executing the full query.
     */
    observe(): JsObservableQuery;
    /**
     * Sets the OFFSET.
     */
    offset(n: number): SelectBuilder;
    /**
     * Adds an ORDER BY clause.
     */
    orderBy(column: string, order: JsSortOrder): SelectBuilder;
    /**
     * Compiles the current query into a reusable prepared handle.
     */
    prepare(): PreparedSelectQuery;
    /**
     * Adds a STDDEV(column) aggregate.
     */
    stddev(column: string): SelectBuilder;
    /**
     * Adds a SUM(column) aggregate.
     */
    sum(column: string): SelectBuilder;
    /**
     * Creates an IVM-based observable query using DBSP incremental dataflow.
     *
     * Unlike `observe()` which re-executes the full query on every change (O(result_set)),
     * `trace()` compiles the query into a dataflow graph and propagates only deltas (O(delta)).
     *
     * Returns an error if the query is not incrementalizable (e.g. contains ORDER BY / LIMIT).
     */
    trace(): JsIvmObservableQuery;
    /**
     * Combines this query with another query using UNION (distinct).
     */
    union(other: SelectBuilder): SelectBuilder;
    /**
     * Combines this query with another query using UNION ALL.
     */
    unionAll(other: SelectBuilder): SelectBuilder;
    /**
     * Sets or extends the WHERE clause.
     * Multiple calls to where_() are combined with AND.
     */
    where(predicate: Expr): SelectBuilder;
}

/**
 * UPDATE query builder.
 */
export class UpdateBuilder {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Executes the update operation.
     */
    exec(): Promise<any>;
    /**
     * Sets column values.
     * Can be called with either:
     * - An object: set({ column: value, ... })
     * - Two arguments: set(column, value)
     */
    set(column_or_obj: any, value?: any | null): UpdateBuilder;
    /**
     * Sets or extends the WHERE clause.
     * Multiple calls to where_() are combined with AND.
     */
    where(predicate: Expr): UpdateBuilder;
}

/**
 * Helper function to create a column reference.
 */
export function col(name: string): Column;

/**
 * Initialize the WASM module.
 */
export function init(): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_column_free: (a: number, b: number) => void;
    readonly __wbg_columnoptions_free: (a: number, b: number) => void;
    readonly __wbg_database_free: (a: number, b: number) => void;
    readonly __wbg_deletebuilder_free: (a: number, b: number) => void;
    readonly __wbg_expr_free: (a: number, b: number) => void;
    readonly __wbg_foreignkeyoptions_free: (a: number, b: number) => void;
    readonly __wbg_get_columnoptions_auto_increment: (a: number) => number;
    readonly __wbg_get_columnoptions_nullable: (a: number) => number;
    readonly __wbg_get_columnoptions_primary_key: (a: number) => number;
    readonly __wbg_get_columnoptions_unique: (a: number) => number;
    readonly __wbg_insertbuilder_free: (a: number, b: number) => void;
    readonly __wbg_jschangesstream_free: (a: number, b: number) => void;
    readonly __wbg_jsgraphqlsubscription_free: (a: number, b: number) => void;
    readonly __wbg_jsivmobservablequery_free: (a: number, b: number) => void;
    readonly __wbg_jsobservablequery_free: (a: number, b: number) => void;
    readonly __wbg_jsonbcolumn_free: (a: number, b: number) => void;
    readonly __wbg_jstable_free: (a: number, b: number) => void;
    readonly __wbg_jstablebuilder_free: (a: number, b: number) => void;
    readonly __wbg_jstransaction_free: (a: number, b: number) => void;
    readonly __wbg_preparedgraphqlquery_free: (a: number, b: number) => void;
    readonly __wbg_preparedselectquery_free: (a: number, b: number) => void;
    readonly __wbg_selectbuilder_free: (a: number, b: number) => void;
    readonly __wbg_set_columnoptions_auto_increment: (a: number, b: number) => void;
    readonly __wbg_set_columnoptions_nullable: (a: number, b: number) => void;
    readonly __wbg_set_columnoptions_primary_key: (a: number, b: number) => void;
    readonly __wbg_set_columnoptions_unique: (a: number, b: number) => void;
    readonly __wbg_updatebuilder_free: (a: number, b: number) => void;
    readonly col: (a: number, b: number) => number;
    readonly column_between: (a: number, b: number, c: number) => number;
    readonly column_eq: (a: number, b: number) => number;
    readonly column_get: (a: number, b: number, c: number) => number;
    readonly column_gt: (a: number, b: number) => number;
    readonly column_gte: (a: number, b: number) => number;
    readonly column_in: (a: number, b: number) => number;
    readonly column_isNotNull: (a: number) => number;
    readonly column_isNull: (a: number) => number;
    readonly column_like: (a: number, b: number, c: number) => number;
    readonly column_lt: (a: number, b: number) => number;
    readonly column_lte: (a: number, b: number) => number;
    readonly column_match: (a: number, b: number, c: number) => number;
    readonly column_name: (a: number, b: number) => void;
    readonly column_ne: (a: number, b: number) => number;
    readonly column_new: (a: number, b: number, c: number, d: number) => number;
    readonly column_notBetween: (a: number, b: number, c: number) => number;
    readonly column_notIn: (a: number, b: number) => number;
    readonly column_notLike: (a: number, b: number, c: number) => number;
    readonly column_notMatch: (a: number, b: number, c: number) => number;
    readonly column_tableName: (a: number, b: number) => void;
    readonly column_with_index: (a: number, b: number) => number;
    readonly columnoptions_new: () => number;
    readonly columnoptions_primaryKey: (a: number, b: number) => number;
    readonly columnoptions_setAutoIncrement: (a: number, b: number) => number;
    readonly columnoptions_setNullable: (a: number, b: number) => number;
    readonly columnoptions_setUnique: (a: number, b: number) => number;
    readonly database_clear: (a: number) => void;
    readonly database_clearTable: (a: number, b: number, c: number, d: number) => void;
    readonly database_create: (a: number, b: number) => number;
    readonly database_createTable: (a: number, b: number, c: number) => number;
    readonly database_delete: (a: number, b: number, c: number) => number;
    readonly database_dropTable: (a: number, b: number, c: number, d: number) => void;
    readonly database_graphql: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => void;
    readonly database_graphqlSchema: (a: number, b: number) => void;
    readonly database_hasTable: (a: number, b: number, c: number) => number;
    readonly database_insert: (a: number, b: number, c: number) => number;
    readonly database_name: (a: number, b: number) => void;
    readonly database_new: (a: number, b: number) => number;
    readonly database_prepareGraphql: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly database_registerTable: (a: number, b: number, c: number) => void;
    readonly database_select: (a: number, b: number) => number;
    readonly database_subscribeGraphql: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => void;
    readonly database_table: (a: number, b: number, c: number) => number;
    readonly database_tableCount: (a: number) => number;
    readonly database_tableNames: (a: number) => number;
    readonly database_takeLastCommitProfile: (a: number) => number;
    readonly database_takeLastDeltaFlushProfile: (a: number) => number;
    readonly database_takeLastIvmBridgeProfile: (a: number) => number;
    readonly database_takeLastSnapshotFlushProfile: (a: number) => number;
    readonly database_takeLastTraceInitProfile: (a: number) => number;
    readonly database_totalRowCount: (a: number) => number;
    readonly database_transaction: (a: number) => number;
    readonly database_update: (a: number, b: number, c: number) => number;
    readonly deletebuilder_exec: (a: number) => number;
    readonly deletebuilder_where: (a: number, b: number) => number;
    readonly expr_and: (a: number, b: number) => number;
    readonly expr_not: (a: number) => number;
    readonly expr_or: (a: number, b: number) => number;
    readonly foreignkeyoptions_fieldName: (a: number, b: number, c: number) => number;
    readonly foreignkeyoptions_new: () => number;
    readonly foreignkeyoptions_reverseFieldName: (a: number, b: number, c: number) => number;
    readonly init: () => void;
    readonly insertbuilder_exec: (a: number) => number;
    readonly insertbuilder_values: (a: number, b: number) => number;
    readonly jschangesstream_getResult: (a: number) => number;
    readonly jschangesstream_getResultBinary: (a: number) => number;
    readonly jschangesstream_getSchemaLayout: (a: number) => number;
    readonly jschangesstream_subscribe: (a: number, b: number) => number;
    readonly jschangesstream_subscribeBinary: (a: number, b: number) => number;
    readonly jsgraphqlsubscription_getResult: (a: number) => number;
    readonly jsgraphqlsubscription_subscribe: (a: number, b: number) => number;
    readonly jsgraphqlsubscription_subscriptionCount: (a: number) => number;
    readonly jsivmobservablequery_getResult: (a: number) => number;
    readonly jsivmobservablequery_getResultBinary: (a: number) => number;
    readonly jsivmobservablequery_getSchemaLayout: (a: number) => number;
    readonly jsivmobservablequery_isEmpty: (a: number) => number;
    readonly jsivmobservablequery_length: (a: number) => number;
    readonly jsivmobservablequery_subscribe: (a: number, b: number) => number;
    readonly jsivmobservablequery_subscriptionCount: (a: number) => number;
    readonly jsobservablequery_getResult: (a: number) => number;
    readonly jsobservablequery_getResultBinary: (a: number) => number;
    readonly jsobservablequery_getSchemaLayout: (a: number) => number;
    readonly jsobservablequery_isEmpty: (a: number) => number;
    readonly jsobservablequery_length: (a: number) => number;
    readonly jsobservablequery_subscribe: (a: number, b: number) => number;
    readonly jsobservablequery_subscribeBinary: (a: number, b: number) => number;
    readonly jsobservablequery_subscriptionCount: (a: number) => number;
    readonly jsonbcolumn_contains: (a: number, b: number) => number;
    readonly jsonbcolumn_eq: (a: number, b: number) => number;
    readonly jsonbcolumn_exists: (a: number) => number;
    readonly jstable_col: (a: number, b: number, c: number) => number;
    readonly jstable_columnCount: (a: number) => number;
    readonly jstable_columnNames: (a: number) => number;
    readonly jstable_getColumnType: (a: number, b: number, c: number) => number;
    readonly jstable_isColumnNullable: (a: number, b: number, c: number) => number;
    readonly jstable_name: (a: number, b: number) => void;
    readonly jstable_primaryKeyColumns: (a: number) => number;
    readonly jstablebuilder_build: (a: number, b: number) => void;
    readonly jstablebuilder_column: (a: number, b: number, c: number, d: number, e: number) => number;
    readonly jstablebuilder_foreignKey: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number) => number;
    readonly jstablebuilder_index: (a: number, b: number, c: number, d: number) => number;
    readonly jstablebuilder_jsonbIndex: (a: number, b: number, c: number, d: number) => number;
    readonly jstablebuilder_name: (a: number, b: number) => void;
    readonly jstablebuilder_new: (a: number, b: number) => number;
    readonly jstablebuilder_primaryKey: (a: number, b: number) => number;
    readonly jstablebuilder_uniqueIndex: (a: number, b: number, c: number, d: number) => number;
    readonly jstransaction_active: (a: number) => number;
    readonly jstransaction_commit: (a: number, b: number) => void;
    readonly jstransaction_delete: (a: number, b: number, c: number, d: number, e: number) => void;
    readonly jstransaction_insert: (a: number, b: number, c: number, d: number, e: number) => void;
    readonly jstransaction_rollback: (a: number, b: number) => void;
    readonly jstransaction_state: (a: number, b: number) => void;
    readonly jstransaction_update: (a: number, b: number, c: number, d: number, e: number, f: number) => void;
    readonly preparedgraphqlquery_exec: (a: number, b: number, c: number) => void;
    readonly preparedgraphqlquery_subscribe: (a: number, b: number, c: number) => void;
    readonly preparedselectquery_exec: (a: number) => number;
    readonly preparedselectquery_execBinary: (a: number) => number;
    readonly preparedselectquery_getSchemaLayout: (a: number) => number;
    readonly selectbuilder_avg: (a: number, b: number, c: number) => number;
    readonly selectbuilder_changes: (a: number, b: number) => void;
    readonly selectbuilder_count: (a: number) => number;
    readonly selectbuilder_countCol: (a: number, b: number, c: number) => number;
    readonly selectbuilder_distinct: (a: number, b: number, c: number) => number;
    readonly selectbuilder_exec: (a: number) => number;
    readonly selectbuilder_execBinary: (a: number) => number;
    readonly selectbuilder_explain: (a: number, b: number) => void;
    readonly selectbuilder_from: (a: number, b: number, c: number) => number;
    readonly selectbuilder_geomean: (a: number, b: number, c: number) => number;
    readonly selectbuilder_getSchemaLayout: (a: number, b: number) => void;
    readonly selectbuilder_groupBy: (a: number, b: number) => number;
    readonly selectbuilder_innerJoin: (a: number, b: number, c: number, d: number) => number;
    readonly selectbuilder_leftJoin: (a: number, b: number, c: number, d: number) => number;
    readonly selectbuilder_limit: (a: number, b: number) => number;
    readonly selectbuilder_max: (a: number, b: number, c: number) => number;
    readonly selectbuilder_min: (a: number, b: number, c: number) => number;
    readonly selectbuilder_observe: (a: number, b: number) => void;
    readonly selectbuilder_offset: (a: number, b: number) => number;
    readonly selectbuilder_orderBy: (a: number, b: number, c: number, d: number) => number;
    readonly selectbuilder_prepare: (a: number, b: number) => void;
    readonly selectbuilder_stddev: (a: number, b: number, c: number) => number;
    readonly selectbuilder_sum: (a: number, b: number, c: number) => number;
    readonly selectbuilder_trace: (a: number, b: number) => void;
    readonly selectbuilder_union: (a: number, b: number, c: number) => void;
    readonly selectbuilder_unionAll: (a: number, b: number, c: number) => void;
    readonly selectbuilder_where: (a: number, b: number) => number;
    readonly updatebuilder_exec: (a: number) => number;
    readonly updatebuilder_set: (a: number, b: number, c: number) => number;
    readonly updatebuilder_where: (a: number, b: number) => number;
    readonly column_new_simple: (a: number, b: number) => number;
    readonly __wbg_binaryresult_free: (a: number, b: number) => void;
    readonly __wbg_schemalayout_free: (a: number, b: number) => void;
    readonly binaryresult_asView: (a: number) => number;
    readonly binaryresult_free: (a: number) => void;
    readonly binaryresult_intoTransferable: (a: number) => number;
    readonly binaryresult_isEmpty: (a: number) => number;
    readonly binaryresult_len: (a: number) => number;
    readonly binaryresult_ptr: (a: number) => number;
    readonly binaryresult_toUint8Array: (a: number) => number;
    readonly schemalayout_columnCount: (a: number) => number;
    readonly schemalayout_columnFixedSize: (a: number, b: number) => number;
    readonly schemalayout_columnName: (a: number, b: number, c: number) => void;
    readonly schemalayout_columnNullable: (a: number, b: number) => number;
    readonly schemalayout_columnOffset: (a: number, b: number) => number;
    readonly schemalayout_columnType: (a: number, b: number) => number;
    readonly schemalayout_nullMaskSize: (a: number) => number;
    readonly schemalayout_rowStride: (a: number) => number;
    readonly __wasm_bindgen_func_elem_84: (a: number, b: number) => void;
    readonly __wasm_bindgen_func_elem_3522: (a: number, b: number) => void;
    readonly __wasm_bindgen_func_elem_8513: (a: number, b: number, c: number, d: number) => void;
    readonly __wasm_bindgen_func_elem_3529: (a: number, b: number, c: number) => void;
    readonly __wasm_bindgen_func_elem_1536: (a: number, b: number) => void;
    readonly __wbindgen_export: (a: number, b: number) => number;
    readonly __wbindgen_export2: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_export3: (a: number) => void;
    readonly __wbindgen_add_to_stack_pointer: (a: number) => number;
    readonly __wbindgen_export4: (a: number, b: number, c: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
