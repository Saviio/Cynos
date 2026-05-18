/* @ts-self-types="./cynos_database.d.ts" */

/**
 * Binary result buffer returned from execBinary()
 */
export class BinaryResult {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(BinaryResult.prototype);
        obj.__wbg_ptr = ptr;
        BinaryResultFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        BinaryResultFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_binaryresult_free(ptr, 0);
    }
    /**
     * Get a zero-copy Uint8Array view into WASM memory.
     * WARNING: This view becomes invalid if WASM memory grows or if this BinaryResult is freed.
     * The caller must ensure the BinaryResult outlives any use of the returned view.
     * @returns {Uint8Array}
     */
    asView() {
        const ret = wasm.binaryresult_asView(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Free the buffer memory
     */
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.binaryresult_free(ptr);
    }
    /**
     * Copy the buffer into a standalone Uint8Array suitable for `postMessage`
     * transfer lists and other ownership-taking APIs.
     *
     * Unlike `asView()`, the returned bytes are no longer tied to WASM memory.
     * @returns {Uint8Array}
     */
    intoTransferable() {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.binaryresult_intoTransferable(ptr);
        return takeObject(ret);
    }
    /**
     * Check if buffer is empty
     * @returns {boolean}
     */
    isEmpty() {
        const ret = wasm.binaryresult_isEmpty(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * Get buffer length
     * @returns {number}
     */
    len() {
        const ret = wasm.binaryresult_len(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Get pointer to the buffer data (as usize for JS)
     * @returns {number}
     */
    ptr() {
        const ret = wasm.binaryresult_ptr(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Get buffer as Uint8Array (copies data to JS)
     * Use asView() for zero-copy access instead.
     * @returns {Uint8Array}
     */
    toUint8Array() {
        const ret = wasm.binaryresult_toUint8Array(this.__wbg_ptr);
        return takeObject(ret);
    }
}
if (Symbol.dispose) BinaryResult.prototype[Symbol.dispose] = BinaryResult.prototype.free;

/**
 * A column reference for building expressions.
 */
export class Column {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(Column.prototype);
        obj.__wbg_ptr = ptr;
        ColumnFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        ColumnFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_column_free(ptr, 0);
    }
    /**
     * Creates a BETWEEN expression: column BETWEEN low AND high
     * @param {any} low
     * @param {any} high
     * @returns {Expr}
     */
    between(low, high) {
        try {
            const ret = wasm.column_between(this.__wbg_ptr, addBorrowedObject(low), addBorrowedObject(high));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates an equality expression: column = value
     * @param {any} value
     * @returns {Expr}
     */
    eq(value) {
        try {
            const ret = wasm.column_eq(this.__wbg_ptr, addBorrowedObject(value));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates a JSONB path access expression
     * @param {string} path
     * @returns {JsonbColumn}
     */
    get(path) {
        const ptr0 = passStringToWasm0(path, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.column_get(this.__wbg_ptr, ptr0, len0);
        return JsonbColumn.__wrap(ret);
    }
    /**
     * Creates a greater-than expression: column > value
     * @param {any} value
     * @returns {Expr}
     */
    gt(value) {
        try {
            const ret = wasm.column_gt(this.__wbg_ptr, addBorrowedObject(value));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates a greater-than-or-equal expression: column >= value
     * @param {any} value
     * @returns {Expr}
     */
    gte(value) {
        try {
            const ret = wasm.column_gte(this.__wbg_ptr, addBorrowedObject(value));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates an IN expression: column IN (values)
     * @param {any} values
     * @returns {Expr}
     */
    in(values) {
        try {
            const ret = wasm.column_in(this.__wbg_ptr, addBorrowedObject(values));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates an IS NOT NULL expression
     * @returns {Expr}
     */
    isNotNull() {
        const ret = wasm.column_isNotNull(this.__wbg_ptr);
        return Expr.__wrap(ret);
    }
    /**
     * Creates an IS NULL expression
     * @returns {Expr}
     */
    isNull() {
        const ret = wasm.column_isNull(this.__wbg_ptr);
        return Expr.__wrap(ret);
    }
    /**
     * Creates a LIKE expression: column LIKE pattern
     * @param {string} pattern
     * @returns {Expr}
     */
    like(pattern) {
        const ptr0 = passStringToWasm0(pattern, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.column_like(this.__wbg_ptr, ptr0, len0);
        return Expr.__wrap(ret);
    }
    /**
     * Creates a less-than expression: column < value
     * @param {any} value
     * @returns {Expr}
     */
    lt(value) {
        try {
            const ret = wasm.column_lt(this.__wbg_ptr, addBorrowedObject(value));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates a less-than-or-equal expression: column <= value
     * @param {any} value
     * @returns {Expr}
     */
    lte(value) {
        try {
            const ret = wasm.column_lte(this.__wbg_ptr, addBorrowedObject(value));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates a MATCH (regex) expression: column MATCH pattern
     * @param {string} pattern
     * @returns {Expr}
     */
    match(pattern) {
        const ptr0 = passStringToWasm0(pattern, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.column_match(this.__wbg_ptr, ptr0, len0);
        return Expr.__wrap(ret);
    }
    /**
     * Returns the column name.
     * @returns {string}
     */
    get name() {
        let deferred1_0;
        let deferred1_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.column_name(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            deferred1_0 = r0;
            deferred1_1 = r1;
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Creates a not-equal expression: column != value
     * @param {any} value
     * @returns {Expr}
     */
    ne(value) {
        try {
            const ret = wasm.column_ne(this.__wbg_ptr, addBorrowedObject(value));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates a new column reference with table name.
     * @param {string} table
     * @param {string} name
     */
    constructor(table, name) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len1 = WASM_VECTOR_LEN;
        const ret = wasm.column_new(ptr0, len0, ptr1, len1);
        this.__wbg_ptr = ret >>> 0;
        ColumnFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Creates a simple column reference without table name.
     * If the name contains a dot (e.g., "orders.year"), it will be parsed
     * as table.column.
     * @param {string} name
     * @returns {Column}
     */
    static new_simple(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.col(ptr0, len0);
        return Column.__wrap(ret);
    }
    /**
     * Creates a NOT BETWEEN expression: column NOT BETWEEN low AND high
     * @param {any} low
     * @param {any} high
     * @returns {Expr}
     */
    notBetween(low, high) {
        try {
            const ret = wasm.column_notBetween(this.__wbg_ptr, addBorrowedObject(low), addBorrowedObject(high));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates a NOT IN expression: column NOT IN (values)
     * @param {any} values
     * @returns {Expr}
     */
    notIn(values) {
        try {
            const ret = wasm.column_notIn(this.__wbg_ptr, addBorrowedObject(values));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates a NOT LIKE expression: column NOT LIKE pattern
     * @param {string} pattern
     * @returns {Expr}
     */
    notLike(pattern) {
        const ptr0 = passStringToWasm0(pattern, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.column_notLike(this.__wbg_ptr, ptr0, len0);
        return Expr.__wrap(ret);
    }
    /**
     * Creates a NOT MATCH (regex) expression: column NOT MATCH pattern
     * @param {string} pattern
     * @returns {Expr}
     */
    notMatch(pattern) {
        const ptr0 = passStringToWasm0(pattern, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.column_notMatch(this.__wbg_ptr, ptr0, len0);
        return Expr.__wrap(ret);
    }
    /**
     * Returns the table name if set.
     * @returns {string | undefined}
     */
    get tableName() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.column_tableName(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            let v1;
            if (r0 !== 0) {
                v1 = getStringFromWasm0(r0, r1).slice();
                wasm.__wbindgen_export4(r0, r1 * 1, 1);
            }
            return v1;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Sets the column index.
     * @param {number} index
     * @returns {Column}
     */
    with_index(index) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.column_with_index(ptr, index);
        return Column.__wrap(ret);
    }
}
if (Symbol.dispose) Column.prototype[Symbol.dispose] = Column.prototype.free;

/**
 * Column options for table creation.
 */
export class ColumnOptions {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(ColumnOptions.prototype);
        obj.__wbg_ptr = ptr;
        ColumnOptionsFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        ColumnOptionsFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_columnoptions_free(ptr, 0);
    }
    constructor() {
        const ret = wasm.columnoptions_new();
        this.__wbg_ptr = ret >>> 0;
        ColumnOptionsFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * @param {boolean} value
     * @returns {ColumnOptions}
     */
    primaryKey(value) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.columnoptions_primaryKey(ptr, value);
        return ColumnOptions.__wrap(ret);
    }
    /**
     * @param {boolean} value
     * @returns {ColumnOptions}
     */
    setAutoIncrement(value) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.columnoptions_setAutoIncrement(ptr, value);
        return ColumnOptions.__wrap(ret);
    }
    /**
     * @param {boolean} value
     * @returns {ColumnOptions}
     */
    setNullable(value) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.columnoptions_setNullable(ptr, value);
        return ColumnOptions.__wrap(ret);
    }
    /**
     * @param {boolean} value
     * @returns {ColumnOptions}
     */
    setUnique(value) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.columnoptions_setUnique(ptr, value);
        return ColumnOptions.__wrap(ret);
    }
    /**
     * @returns {boolean}
     */
    get auto_increment() {
        const ret = wasm.__wbg_get_columnoptions_auto_increment(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * @returns {boolean}
     */
    get nullable() {
        const ret = wasm.__wbg_get_columnoptions_nullable(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * @returns {boolean}
     */
    get primary_key() {
        const ret = wasm.__wbg_get_columnoptions_primary_key(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * @returns {boolean}
     */
    get unique() {
        const ret = wasm.__wbg_get_columnoptions_unique(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * @param {boolean} arg0
     */
    set auto_increment(arg0) {
        wasm.__wbg_set_columnoptions_auto_increment(this.__wbg_ptr, arg0);
    }
    /**
     * @param {boolean} arg0
     */
    set nullable(arg0) {
        wasm.__wbg_set_columnoptions_nullable(this.__wbg_ptr, arg0);
    }
    /**
     * @param {boolean} arg0
     */
    set primary_key(arg0) {
        wasm.__wbg_set_columnoptions_primary_key(this.__wbg_ptr, arg0);
    }
    /**
     * @param {boolean} arg0
     */
    set unique(arg0) {
        wasm.__wbg_set_columnoptions_unique(this.__wbg_ptr, arg0);
    }
}
if (Symbol.dispose) ColumnOptions.prototype[Symbol.dispose] = ColumnOptions.prototype.free;

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
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(Database.prototype);
        obj.__wbg_ptr = ptr;
        DatabaseFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        DatabaseFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_database_free(ptr, 0);
    }
    /**
     * Clears all data from all tables.
     */
    clear() {
        wasm.database_clear(this.__wbg_ptr);
    }
    /**
     * Clears data from a specific table.
     * @param {string} name
     */
    clearTable(name) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            wasm.database_clearTable(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Async factory method for creating a database (for WASM compatibility).
     * @param {string} name
     * @returns {Promise<Database>}
     */
    static create(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.database_create(ptr0, len0);
        return takeObject(ret);
    }
    /**
     * Creates a new table builder.
     * @param {string} name
     * @returns {JsTableBuilder}
     */
    createTable(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.database_createTable(this.__wbg_ptr, ptr0, len0);
        return JsTableBuilder.__wrap(ret);
    }
    /**
     * Starts a DELETE operation.
     * @param {string} table
     * @returns {DeleteBuilder}
     */
    delete(table) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.database_delete(this.__wbg_ptr, ptr0, len0);
        return DeleteBuilder.__wrap(ret);
    }
    /**
     * Drops a table from the database.
     * @param {string} name
     */
    dropTable(name) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            wasm.database_dropTable(retptr, this.__wbg_ptr, ptr0, len0);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Executes a GraphQL query against the current database snapshot.
     *
     * Returns a standard GraphQL payload object with a single `data` property.
     * @param {string} query
     * @param {any | null} [variables]
     * @param {string | null} [operation_name]
     * @returns {any}
     */
    graphql(query, variables, operation_name) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(query, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            var ptr1 = isLikeNone(operation_name) ? 0 : passStringToWasm0(operation_name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            var len1 = WASM_VECTOR_LEN;
            wasm.database_graphql(retptr, this.__wbg_ptr, ptr0, len0, isLikeNone(variables) ? 0 : addHeapObject(variables), ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Renders the current GraphQL schema as SDL.
     * @returns {string}
     */
    graphqlSchema() {
        let deferred1_0;
        let deferred1_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.database_graphqlSchema(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            deferred1_0 = r0;
            deferred1_1 = r1;
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Checks if a table exists.
     * @param {string} name
     * @returns {boolean}
     */
    hasTable(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.database_hasTable(this.__wbg_ptr, ptr0, len0);
        return ret !== 0;
    }
    /**
     * Starts an INSERT operation.
     * @param {string} table
     * @returns {InsertBuilder}
     */
    insert(table) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.database_insert(this.__wbg_ptr, ptr0, len0);
        return InsertBuilder.__wrap(ret);
    }
    /**
     * Returns the database name.
     * @returns {string}
     */
    get name() {
        let deferred1_0;
        let deferred1_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.database_name(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            deferred1_0 = r0;
            deferred1_1 = r1;
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Creates a new database instance.
     * @param {string} name
     */
    constructor(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.database_new(ptr0, len0);
        this.__wbg_ptr = ret >>> 0;
        DatabaseFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Parses and prepares a GraphQL query for repeated execution.
     * @param {string} query
     * @param {string | null} [operation_name]
     * @returns {PreparedGraphqlQuery}
     */
    prepareGraphql(query, operation_name) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(query, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            var ptr1 = isLikeNone(operation_name) ? 0 : passStringToWasm0(operation_name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            var len1 = WASM_VECTOR_LEN;
            wasm.database_prepareGraphql(retptr, this.__wbg_ptr, ptr0, len0, ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return PreparedGraphqlQuery.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Registers a table schema with the database.
     * @param {JsTableBuilder} builder
     */
    registerTable(builder) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            _assertClass(builder, JsTableBuilder);
            wasm.database_registerTable(retptr, this.__wbg_ptr, builder.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Starts a SELECT query.
     * Accepts either:
     * - A single string: select('*') or select('name')
     * - Multiple strings: select('name', 'score') - passed as variadic args
     * @param {...any} columns
     * @returns {SelectBuilder}
     */
    select(...columns) {
        try {
            const ret = wasm.database_select(this.__wbg_ptr, addBorrowedObject(columns));
            return SelectBuilder.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates a live GraphQL subscription backed by the root query planner path.
     * @param {string} query
     * @param {any | null} [variables]
     * @param {string | null} [operation_name]
     * @returns {JsGraphqlSubscription}
     */
    subscribeGraphql(query, variables, operation_name) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(query, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            var ptr1 = isLikeNone(operation_name) ? 0 : passStringToWasm0(operation_name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            var len1 = WASM_VECTOR_LEN;
            wasm.database_subscribeGraphql(retptr, this.__wbg_ptr, ptr0, len0, isLikeNone(variables) ? 0 : addHeapObject(variables), ptr1, len1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return JsGraphqlSubscription.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Gets a table reference by name.
     * @param {string} name
     * @returns {JsTable | undefined}
     */
    table(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.database_table(this.__wbg_ptr, ptr0, len0);
        return ret === 0 ? undefined : JsTable.__wrap(ret);
    }
    /**
     * Returns the number of tables.
     * @returns {number}
     */
    tableCount() {
        const ret = wasm.database_tableCount(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Returns all table names.
     * @returns {Array<any>}
     */
    tableNames() {
        const ret = wasm.database_tableNames(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * @returns {any}
     */
    takeLastCommitProfile() {
        const ret = wasm.database_takeLastCommitProfile(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * @returns {any}
     */
    takeLastDeltaFlushProfile() {
        const ret = wasm.database_takeLastDeltaFlushProfile(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * @returns {any}
     */
    takeLastIvmBridgeProfile() {
        const ret = wasm.database_takeLastIvmBridgeProfile(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * @returns {any}
     */
    takeLastSnapshotFlushProfile() {
        const ret = wasm.database_takeLastSnapshotFlushProfile(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * @returns {any}
     */
    takeLastTraceInitProfile() {
        const ret = wasm.database_takeLastTraceInitProfile(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Returns the total row count across all tables.
     * @returns {number}
     */
    totalRowCount() {
        const ret = wasm.database_totalRowCount(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Begins a new transaction.
     * @returns {JsTransaction}
     */
    transaction() {
        const ret = wasm.database_transaction(this.__wbg_ptr);
        return JsTransaction.__wrap(ret);
    }
    /**
     * Starts an UPDATE operation.
     * @param {string} table
     * @returns {UpdateBuilder}
     */
    update(table) {
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.database_update(this.__wbg_ptr, ptr0, len0);
        return UpdateBuilder.__wrap(ret);
    }
}
if (Symbol.dispose) Database.prototype[Symbol.dispose] = Database.prototype.free;

/**
 * DELETE query builder.
 */
export class DeleteBuilder {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(DeleteBuilder.prototype);
        obj.__wbg_ptr = ptr;
        DeleteBuilderFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        DeleteBuilderFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_deletebuilder_free(ptr, 0);
    }
    /**
     * Executes the delete operation.
     * @returns {Promise<any>}
     */
    exec() {
        const ret = wasm.deletebuilder_exec(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Sets or extends the WHERE clause.
     * Multiple calls to where_() are combined with AND.
     * @param {Expr} predicate
     * @returns {DeleteBuilder}
     */
    where(predicate) {
        const ptr = this.__destroy_into_raw();
        _assertClass(predicate, Expr);
        const ret = wasm.deletebuilder_where(ptr, predicate.__wbg_ptr);
        return DeleteBuilder.__wrap(ret);
    }
}
if (Symbol.dispose) DeleteBuilder.prototype[Symbol.dispose] = DeleteBuilder.prototype.free;

/**
 * Expression type for query predicates.
 */
export class Expr {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(Expr.prototype);
        obj.__wbg_ptr = ptr;
        ExprFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        ExprFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_expr_free(ptr, 0);
    }
    /**
     * Creates an AND expression: self AND other
     * @param {Expr} other
     * @returns {Expr}
     */
    and(other) {
        _assertClass(other, Expr);
        const ret = wasm.expr_and(this.__wbg_ptr, other.__wbg_ptr);
        return Expr.__wrap(ret);
    }
    /**
     * Creates a NOT expression: NOT self
     * @returns {Expr}
     */
    not() {
        const ret = wasm.expr_not(this.__wbg_ptr);
        return Expr.__wrap(ret);
    }
    /**
     * Creates an OR expression: self OR other
     * @param {Expr} other
     * @returns {Expr}
     */
    or(other) {
        _assertClass(other, Expr);
        const ret = wasm.expr_or(this.__wbg_ptr, other.__wbg_ptr);
        return Expr.__wrap(ret);
    }
}
if (Symbol.dispose) Expr.prototype[Symbol.dispose] = Expr.prototype.free;

/**
 * Foreign-key options for GraphQL relation naming.
 */
export class ForeignKeyOptions {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(ForeignKeyOptions.prototype);
        obj.__wbg_ptr = ptr;
        ForeignKeyOptionsFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        ForeignKeyOptionsFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_foreignkeyoptions_free(ptr, 0);
    }
    /**
     * @param {string} value
     * @returns {ForeignKeyOptions}
     */
    fieldName(value) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(value, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.foreignkeyoptions_fieldName(ptr, ptr0, len0);
        return ForeignKeyOptions.__wrap(ret);
    }
    constructor() {
        const ret = wasm.foreignkeyoptions_new();
        this.__wbg_ptr = ret >>> 0;
        ForeignKeyOptionsFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * @param {string} value
     * @returns {ForeignKeyOptions}
     */
    reverseFieldName(value) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(value, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.foreignkeyoptions_reverseFieldName(ptr, ptr0, len0);
        return ForeignKeyOptions.__wrap(ret);
    }
}
if (Symbol.dispose) ForeignKeyOptions.prototype[Symbol.dispose] = ForeignKeyOptions.prototype.free;

/**
 * INSERT query builder.
 */
export class InsertBuilder {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(InsertBuilder.prototype);
        obj.__wbg_ptr = ptr;
        InsertBuilderFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        InsertBuilderFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_insertbuilder_free(ptr, 0);
    }
    /**
     * Executes the insert operation.
     * @returns {Promise<any>}
     */
    exec() {
        const ret = wasm.insertbuilder_exec(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Sets the values to insert.
     * @param {any} data
     * @returns {InsertBuilder}
     */
    values(data) {
        try {
            const ptr = this.__destroy_into_raw();
            const ret = wasm.insertbuilder_values(ptr, addBorrowedObject(data));
            return InsertBuilder.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
}
if (Symbol.dispose) InsertBuilder.prototype[Symbol.dispose] = InsertBuilder.prototype.free;

/**
 * JavaScript-friendly changes stream.
 *
 * This provides the `changes()` API that yields the complete result set
 * whenever data changes. The callback receives the full current data,
 * not incremental changes - perfect for React's setState pattern.
 */
export class JsChangesStream {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(JsChangesStream.prototype);
        obj.__wbg_ptr = ptr;
        JsChangesStreamFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        JsChangesStreamFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_jschangesstream_free(ptr, 0);
    }
    /**
     * Returns the current result.
     * @returns {any}
     */
    getResult() {
        const ret = wasm.jschangesstream_getResult(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Returns the current result as a binary buffer for zero-copy access.
     * @returns {BinaryResult}
     */
    getResultBinary() {
        const ret = wasm.jschangesstream_getResultBinary(this.__wbg_ptr);
        return BinaryResult.__wrap(ret);
    }
    /**
     * Returns the schema layout for decoding binary results.
     * @returns {SchemaLayout}
     */
    getSchemaLayout() {
        const ret = wasm.jschangesstream_getSchemaLayout(this.__wbg_ptr);
        return SchemaLayout.__wrap(ret);
    }
    /**
     * Subscribes to the changes stream.
     *
     * The callback receives the complete current result set as a JavaScript array.
     * It is called immediately with the initial data, and again whenever data changes.
     * Perfect for React: `stream.subscribe(data => setUsers(data))`
     *
     * Returns an unsubscribe function.
     * @param {Function} callback
     * @returns {Function}
     */
    subscribe(callback) {
        const ret = wasm.jschangesstream_subscribe(this.__wbg_ptr, addHeapObject(callback));
        return takeObject(ret);
    }
    /**
     * Subscribes to the changes stream using binary snapshots.
     *
     * The callback receives a `BinaryResult` for the full current result set.
     * It is called immediately with the initial data, and again whenever data changes.
     * Use `getSchemaLayout()` once and decode with `ResultSet` on the JS side.
     *
     * Returns an unsubscribe function.
     * @param {Function} callback
     * @returns {Function}
     */
    subscribeBinary(callback) {
        const ret = wasm.jschangesstream_subscribeBinary(this.__wbg_ptr, addHeapObject(callback));
        return takeObject(ret);
    }
}
if (Symbol.dispose) JsChangesStream.prototype[Symbol.dispose] = JsChangesStream.prototype.free;

/**
 * Data types supported by Cynos.
 * @enum {0 | 1 | 2 | 3 | 4 | 5 | 6 | 7}
 */
export const JsDataType = Object.freeze({
    Boolean: 0, "0": "Boolean",
    Int32: 1, "1": "Int32",
    Int64: 2, "2": "Int64",
    Float64: 3, "3": "Float64",
    String: 4, "4": "String",
    DateTime: 5, "5": "DateTime",
    Bytes: 6, "6": "Bytes",
    Jsonb: 7, "7": "Jsonb",
});

/**
 * JavaScript-friendly GraphQL subscription wrapper.
 *
 * The callback receives a standard GraphQL payload object with a single `data`
 * property. The payload is emitted immediately on subscribe and again whenever
 * the rendered GraphQL response changes.
 */
export class JsGraphqlSubscription {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(JsGraphqlSubscription.prototype);
        obj.__wbg_ptr = ptr;
        JsGraphqlSubscriptionFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        JsGraphqlSubscriptionFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_jsgraphqlsubscription_free(ptr, 0);
    }
    /**
     * Returns the current GraphQL payload.
     * @returns {any}
     */
    getResult() {
        const ret = wasm.jsgraphqlsubscription_getResult(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Subscribes to GraphQL payload changes and emits the initial value immediately.
     * @param {Function} callback
     * @returns {Function}
     */
    subscribe(callback) {
        const ret = wasm.jsgraphqlsubscription_subscribe(this.__wbg_ptr, addHeapObject(callback));
        return takeObject(ret);
    }
    /**
     * Returns the number of active subscriptions.
     * @returns {number}
     */
    subscriptionCount() {
        const ret = wasm.jsgraphqlsubscription_subscriptionCount(this.__wbg_ptr);
        return ret >>> 0;
    }
}
if (Symbol.dispose) JsGraphqlSubscription.prototype[Symbol.dispose] = JsGraphqlSubscription.prototype.free;

/**
 * JavaScript-friendly IVM observable query wrapper.
 * Uses DBSP-based incremental view maintenance for O(delta) updates.
 */
export class JsIvmObservableQuery {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(JsIvmObservableQuery.prototype);
        obj.__wbg_ptr = ptr;
        JsIvmObservableQueryFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        JsIvmObservableQueryFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_jsivmobservablequery_free(ptr, 0);
    }
    /**
     * Returns the current result as a JavaScript array.
     * @returns {any}
     */
    getResult() {
        const ret = wasm.jsivmobservablequery_getResult(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Returns the current result as a binary buffer for zero-copy access.
     * @returns {BinaryResult}
     */
    getResultBinary() {
        const ret = wasm.jsivmobservablequery_getResultBinary(this.__wbg_ptr);
        return BinaryResult.__wrap(ret);
    }
    /**
     * Returns the schema layout for decoding binary results.
     * @returns {SchemaLayout}
     */
    getSchemaLayout() {
        const ret = wasm.jsivmobservablequery_getSchemaLayout(this.__wbg_ptr);
        return SchemaLayout.__wrap(ret);
    }
    /**
     * Returns whether the result is empty.
     * @returns {boolean}
     */
    isEmpty() {
        const ret = wasm.jsivmobservablequery_isEmpty(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * Returns the number of rows in the result.
     * @returns {number}
     */
    get length() {
        const ret = wasm.jsivmobservablequery_length(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Subscribes to IVM query changes.
     *
     * The callback receives a delta object `{ added: Row[], removed: Row[] }`
     * instead of the full result set. This is the true O(delta) path —
     * the UI side should apply the delta to its own state.
     *
     * Use `getResult()` to get the initial full result before subscribing.
     * Returns an unsubscribe function.
     * @param {Function} callback
     * @returns {Function}
     */
    subscribe(callback) {
        const ret = wasm.jsivmobservablequery_subscribe(this.__wbg_ptr, addHeapObject(callback));
        return takeObject(ret);
    }
    /**
     * Returns the number of active subscriptions.
     * @returns {number}
     */
    subscriptionCount() {
        const ret = wasm.jsivmobservablequery_subscriptionCount(this.__wbg_ptr);
        return ret >>> 0;
    }
}
if (Symbol.dispose) JsIvmObservableQuery.prototype[Symbol.dispose] = JsIvmObservableQuery.prototype.free;

/**
 * JavaScript-friendly observable query wrapper.
 * Uses re-query strategy for optimal performance with indexes.
 */
export class JsObservableQuery {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(JsObservableQuery.prototype);
        obj.__wbg_ptr = ptr;
        JsObservableQueryFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        JsObservableQueryFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_jsobservablequery_free(ptr, 0);
    }
    /**
     * Returns the current result as a JavaScript array.
     * @returns {any}
     */
    getResult() {
        const ret = wasm.jsobservablequery_getResult(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Returns the current result as a binary buffer for zero-copy access.
     * @returns {BinaryResult}
     */
    getResultBinary() {
        const ret = wasm.jsobservablequery_getResultBinary(this.__wbg_ptr);
        return BinaryResult.__wrap(ret);
    }
    /**
     * Returns the schema layout for decoding binary results.
     * @returns {SchemaLayout}
     */
    getSchemaLayout() {
        const ret = wasm.jsobservablequery_getSchemaLayout(this.__wbg_ptr);
        return SchemaLayout.__wrap(ret);
    }
    /**
     * Returns whether the result is empty.
     * @returns {boolean}
     */
    isEmpty() {
        const ret = wasm.jsobservablequery_isEmpty(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * Returns the number of rows in the result.
     * @returns {number}
     */
    get length() {
        const ret = wasm.jsobservablequery_length(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Subscribes to query changes.
     *
     * The callback receives the complete current result set as a JavaScript array.
     * It is called whenever data changes (not immediately - use getResult for initial data).
     * Returns an unsubscribe function.
     * @param {Function} callback
     * @returns {Function}
     */
    subscribe(callback) {
        const ret = wasm.jsobservablequery_subscribe(this.__wbg_ptr, addHeapObject(callback));
        return takeObject(ret);
    }
    /**
     * Subscribes to query changes using binary snapshots.
     *
     * The callback receives a `BinaryResult` for the complete current result set.
     * This avoids per-update JS object materialization inside the WASM bridge.
     * Call `getSchemaLayout()` once and decode with `ResultSet` on the JS side.
     *
     * It is called whenever data changes (not immediately - use getResultBinary for initial data).
     * Returns an unsubscribe function.
     * @param {Function} callback
     * @returns {Function}
     */
    subscribeBinary(callback) {
        const ret = wasm.jsobservablequery_subscribeBinary(this.__wbg_ptr, addHeapObject(callback));
        return takeObject(ret);
    }
    /**
     * Returns the number of active subscriptions.
     * @returns {number}
     */
    subscriptionCount() {
        const ret = wasm.jsobservablequery_subscriptionCount(this.__wbg_ptr);
        return ret >>> 0;
    }
}
if (Symbol.dispose) JsObservableQuery.prototype[Symbol.dispose] = JsObservableQuery.prototype.free;

/**
 * Sort order for ORDER BY clauses.
 * @enum {0 | 1}
 */
export const JsSortOrder = Object.freeze({
    Asc: 0, "0": "Asc",
    Desc: 1, "1": "Desc",
});

/**
 * JavaScript-friendly table reference.
 */
export class JsTable {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(JsTable.prototype);
        obj.__wbg_ptr = ptr;
        JsTableFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        JsTableFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_jstable_free(ptr, 0);
    }
    /**
     * Returns a column reference.
     * @param {string} name
     * @returns {Column | undefined}
     */
    col(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.jstable_col(this.__wbg_ptr, ptr0, len0);
        return ret === 0 ? undefined : Column.__wrap(ret);
    }
    /**
     * Returns the number of columns.
     * @returns {number}
     */
    columnCount() {
        const ret = wasm.jstable_columnCount(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Returns the column names.
     * @returns {Array<any>}
     */
    columnNames() {
        const ret = wasm.jstable_columnNames(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Returns the column data type.
     * @param {string} name
     * @returns {JsDataType | undefined}
     */
    getColumnType(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.jstable_getColumnType(this.__wbg_ptr, ptr0, len0);
        return ret === 8 ? undefined : ret;
    }
    /**
     * Returns whether a column is nullable.
     * @param {string} name
     * @returns {boolean}
     */
    isColumnNullable(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.jstable_isColumnNullable(this.__wbg_ptr, ptr0, len0);
        return ret !== 0;
    }
    /**
     * Returns the table name.
     * @returns {string}
     */
    get name() {
        let deferred1_0;
        let deferred1_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.jstable_name(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            deferred1_0 = r0;
            deferred1_1 = r1;
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Returns the primary key column names.
     * @returns {Array<any>}
     */
    primaryKeyColumns() {
        const ret = wasm.jstable_primaryKeyColumns(this.__wbg_ptr);
        return takeObject(ret);
    }
}
if (Symbol.dispose) JsTable.prototype[Symbol.dispose] = JsTable.prototype.free;

/**
 * JavaScript-friendly table builder.
 */
export class JsTableBuilder {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(JsTableBuilder.prototype);
        obj.__wbg_ptr = ptr;
        JsTableBuilderFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        JsTableBuilderFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_jstablebuilder_free(ptr, 0);
    }
    /**
     * Builds the table schema and returns a JsTable.
     * @returns {JsTable}
     */
    build() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.jstablebuilder_build(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return JsTable.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Adds a column to the table.
     * @param {string} name
     * @param {JsDataType} data_type
     * @param {ColumnOptions | null} [options]
     * @returns {JsTableBuilder}
     */
    column(name, data_type, options) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        let ptr1 = 0;
        if (!isLikeNone(options)) {
            _assertClass(options, ColumnOptions);
            ptr1 = options.__destroy_into_raw();
        }
        const ret = wasm.jstablebuilder_column(ptr, ptr0, len0, data_type, ptr1);
        return JsTableBuilder.__wrap(ret);
    }
    /**
     * Adds a foreign key and optional GraphQL relation names.
     * @param {string} name
     * @param {string} child_column
     * @param {string} parent_table
     * @param {string} parent_column
     * @param {ForeignKeyOptions | null} [options]
     * @returns {JsTableBuilder}
     */
    foreignKey(name, child_column, parent_table, parent_column, options) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ptr1 = passStringToWasm0(child_column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len1 = WASM_VECTOR_LEN;
        const ptr2 = passStringToWasm0(parent_table, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len2 = WASM_VECTOR_LEN;
        const ptr3 = passStringToWasm0(parent_column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len3 = WASM_VECTOR_LEN;
        let ptr4 = 0;
        if (!isLikeNone(options)) {
            _assertClass(options, ForeignKeyOptions);
            ptr4 = options.__destroy_into_raw();
        }
        const ret = wasm.jstablebuilder_foreignKey(ptr, ptr0, len0, ptr1, len1, ptr2, len2, ptr3, len3, ptr4);
        return JsTableBuilder.__wrap(ret);
    }
    /**
     * Adds an index to the table.
     * @param {string} name
     * @param {any} columns
     * @returns {JsTableBuilder}
     */
    index(name, columns) {
        try {
            const ptr = this.__destroy_into_raw();
            const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            const ret = wasm.jstablebuilder_index(ptr, ptr0, len0, addBorrowedObject(columns));
            return JsTableBuilder.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Adds a JSONB index for specific paths.
     * @param {string} column
     * @param {any} paths
     * @returns {JsTableBuilder}
     */
    jsonbIndex(column, paths) {
        try {
            const ptr = this.__destroy_into_raw();
            const ptr0 = passStringToWasm0(column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            const ret = wasm.jstablebuilder_jsonbIndex(ptr, ptr0, len0, addBorrowedObject(paths));
            return JsTableBuilder.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Returns the table name.
     * @returns {string}
     */
    get name() {
        let deferred1_0;
        let deferred1_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.jstablebuilder_name(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            deferred1_0 = r0;
            deferred1_1 = r1;
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Creates a new table builder.
     * @param {string} name
     */
    constructor(name) {
        const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.jstablebuilder_new(ptr0, len0);
        this.__wbg_ptr = ret >>> 0;
        JsTableBuilderFinalization.register(this, this.__wbg_ptr, this);
        return this;
    }
    /**
     * Sets the primary key columns.
     * @param {any} columns
     * @returns {JsTableBuilder}
     */
    primaryKey(columns) {
        try {
            const ptr = this.__destroy_into_raw();
            const ret = wasm.jstablebuilder_primaryKey(ptr, addBorrowedObject(columns));
            return JsTableBuilder.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Adds a unique index to the table.
     * @param {string} name
     * @param {any} columns
     * @returns {JsTableBuilder}
     */
    uniqueIndex(name, columns) {
        try {
            const ptr = this.__destroy_into_raw();
            const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            const ret = wasm.jstablebuilder_uniqueIndex(ptr, ptr0, len0, addBorrowedObject(columns));
            return JsTableBuilder.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
}
if (Symbol.dispose) JsTableBuilder.prototype[Symbol.dispose] = JsTableBuilder.prototype.free;

/**
 * JavaScript-friendly transaction wrapper.
 */
export class JsTransaction {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(JsTransaction.prototype);
        obj.__wbg_ptr = ptr;
        JsTransactionFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        JsTransactionFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_jstransaction_free(ptr, 0);
    }
    /**
     * Returns whether the transaction is still active.
     * @returns {boolean}
     */
    get active() {
        const ret = wasm.jstransaction_active(this.__wbg_ptr);
        return ret !== 0;
    }
    /**
     * Commits the transaction.
     */
    commit() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.jstransaction_commit(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Deletes rows from a table within the transaction.
     * @param {string} table
     * @param {Expr | null} [predicate]
     * @returns {number}
     */
    delete(table, predicate) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(table, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            let ptr1 = 0;
            if (!isLikeNone(predicate)) {
                _assertClass(predicate, Expr);
                ptr1 = predicate.__destroy_into_raw();
            }
            wasm.jstransaction_delete(retptr, this.__wbg_ptr, ptr0, len0, ptr1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return r0 >>> 0;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Inserts rows into a table within the transaction.
     * @param {string} table
     * @param {any} values
     */
    insert(table, values) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(table, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            wasm.jstransaction_insert(retptr, this.__wbg_ptr, ptr0, len0, addBorrowedObject(values));
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Rolls back the transaction.
     */
    rollback() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.jstransaction_rollback(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            if (r1) {
                throw takeObject(r0);
            }
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Returns the transaction state.
     * @returns {string}
     */
    get state() {
        let deferred1_0;
        let deferred1_1;
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.jstransaction_state(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            deferred1_0 = r0;
            deferred1_1 = r1;
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_export4(deferred1_0, deferred1_1, 1);
        }
    }
    /**
     * Updates rows in a table within the transaction.
     * @param {string} table
     * @param {any} set_values
     * @param {Expr | null} [predicate]
     * @returns {number}
     */
    update(table, set_values, predicate) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            const ptr0 = passStringToWasm0(table, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            const len0 = WASM_VECTOR_LEN;
            let ptr1 = 0;
            if (!isLikeNone(predicate)) {
                _assertClass(predicate, Expr);
                ptr1 = predicate.__destroy_into_raw();
            }
            wasm.jstransaction_update(retptr, this.__wbg_ptr, ptr0, len0, addBorrowedObject(set_values), ptr1);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return r0 >>> 0;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            heap[stack_pointer++] = undefined;
        }
    }
}
if (Symbol.dispose) JsTransaction.prototype[Symbol.dispose] = JsTransaction.prototype.free;

/**
 * A JSONB column with path access.
 */
export class JsonbColumn {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(JsonbColumn.prototype);
        obj.__wbg_ptr = ptr;
        JsonbColumnFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        JsonbColumnFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_jsonbcolumn_free(ptr, 0);
    }
    /**
     * Creates a contains expression for the JSONB path.
     * @param {any} value
     * @returns {Expr}
     */
    contains(value) {
        try {
            const ret = wasm.jsonbcolumn_contains(this.__wbg_ptr, addBorrowedObject(value));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates an equality expression for the JSONB path value.
     * @param {any} value
     * @returns {Expr}
     */
    eq(value) {
        try {
            const ret = wasm.jsonbcolumn_eq(this.__wbg_ptr, addBorrowedObject(value));
            return Expr.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Creates an exists expression for the JSONB path.
     * @returns {Expr}
     */
    exists() {
        const ret = wasm.jsonbcolumn_exists(this.__wbg_ptr);
        return Expr.__wrap(ret);
    }
}
if (Symbol.dispose) JsonbColumn.prototype[Symbol.dispose] = JsonbColumn.prototype.free;

/**
 * A prepared GraphQL query that reuses the parsed document across executions.
 */
export class PreparedGraphqlQuery {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(PreparedGraphqlQuery.prototype);
        obj.__wbg_ptr = ptr;
        PreparedGraphqlQueryFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        PreparedGraphqlQueryFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_preparedgraphqlquery_free(ptr, 0);
    }
    /**
     * Executes the prepared GraphQL query with an optional variables object.
     * @param {any | null} [variables]
     * @returns {any}
     */
    exec(variables) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.preparedgraphqlquery_exec(retptr, this.__wbg_ptr, isLikeNone(variables) ? 0 : addHeapObject(variables));
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Creates a live subscription from a prepared GraphQL subscription document.
     * @param {any | null} [variables]
     * @returns {JsGraphqlSubscription}
     */
    subscribe(variables) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.preparedgraphqlquery_subscribe(retptr, this.__wbg_ptr, isLikeNone(variables) ? 0 : addHeapObject(variables));
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return JsGraphqlSubscription.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
}
if (Symbol.dispose) PreparedGraphqlQuery.prototype[Symbol.dispose] = PreparedGraphqlQuery.prototype.free;

export class PreparedSelectQuery {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(PreparedSelectQuery.prototype);
        obj.__wbg_ptr = ptr;
        PreparedSelectQueryFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        PreparedSelectQueryFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_preparedselectquery_free(ptr, 0);
    }
    /**
     * Executes the prepared query and returns JS objects.
     * @returns {Promise<any>}
     */
    exec() {
        const ret = wasm.preparedselectquery_exec(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Executes the prepared query and returns a binary result buffer.
     * @returns {Promise<BinaryResult>}
     */
    execBinary() {
        const ret = wasm.preparedselectquery_execBinary(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Gets the schema layout for binary decoding.
     * @returns {SchemaLayout}
     */
    getSchemaLayout() {
        const ret = wasm.preparedselectquery_getSchemaLayout(this.__wbg_ptr);
        return SchemaLayout.__wrap(ret);
    }
}
if (Symbol.dispose) PreparedSelectQuery.prototype[Symbol.dispose] = PreparedSelectQuery.prototype.free;

/**
 * Pre-computed layout for binary encoding/decoding
 */
export class SchemaLayout {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(SchemaLayout.prototype);
        obj.__wbg_ptr = ptr;
        SchemaLayoutFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        SchemaLayoutFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_schemalayout_free(ptr, 0);
    }
    /**
     * Get the number of columns
     * @returns {number}
     */
    columnCount() {
        const ret = wasm.schemalayout_columnCount(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Get column fixed size by index
     * @param {number} idx
     * @returns {number | undefined}
     */
    columnFixedSize(idx) {
        const ret = wasm.schemalayout_columnFixedSize(this.__wbg_ptr, idx);
        return ret === 0x100000001 ? undefined : ret;
    }
    /**
     * Get column name by index
     * @param {number} idx
     * @returns {string | undefined}
     */
    columnName(idx) {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.schemalayout_columnName(retptr, this.__wbg_ptr, idx);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            let v1;
            if (r0 !== 0) {
                v1 = getStringFromWasm0(r0, r1).slice();
                wasm.__wbindgen_export4(r0, r1 * 1, 1);
            }
            return v1;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Check if column is nullable
     * @param {number} idx
     * @returns {boolean | undefined}
     */
    columnNullable(idx) {
        const ret = wasm.schemalayout_columnNullable(this.__wbg_ptr, idx);
        return ret === 0xFFFFFF ? undefined : ret !== 0;
    }
    /**
     * Get column offset by index (offset within row, after null_mask)
     * @param {number} idx
     * @returns {number | undefined}
     */
    columnOffset(idx) {
        const ret = wasm.schemalayout_columnOffset(this.__wbg_ptr, idx);
        return ret === 0x100000001 ? undefined : ret;
    }
    /**
     * Get column type by index (returns BinaryDataType as u8)
     * @param {number} idx
     * @returns {number | undefined}
     */
    columnType(idx) {
        const ret = wasm.schemalayout_columnType(this.__wbg_ptr, idx);
        return ret === 0xFFFFFF ? undefined : ret;
    }
    /**
     * Get null mask size in bytes
     * @returns {number}
     */
    nullMaskSize() {
        const ret = wasm.schemalayout_nullMaskSize(this.__wbg_ptr);
        return ret >>> 0;
    }
    /**
     * Get row stride (total bytes per row)
     * @returns {number}
     */
    rowStride() {
        const ret = wasm.schemalayout_rowStride(this.__wbg_ptr);
        return ret >>> 0;
    }
}
if (Symbol.dispose) SchemaLayout.prototype[Symbol.dispose] = SchemaLayout.prototype.free;

/**
 * SELECT query builder.
 */
export class SelectBuilder {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(SelectBuilder.prototype);
        obj.__wbg_ptr = ptr;
        SelectBuilderFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        SelectBuilderFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_selectbuilder_free(ptr, 0);
    }
    /**
     * Adds an AVG(column) aggregate.
     * @param {string} column
     * @returns {SelectBuilder}
     */
    avg(column) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.selectbuilder_avg(ptr, ptr0, len0);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Creates a changes stream (initial + incremental).
     * @returns {JsChangesStream}
     */
    changes() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.selectbuilder_changes(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return JsChangesStream.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Adds a COUNT(*) aggregate.
     * @returns {SelectBuilder}
     */
    count() {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.selectbuilder_count(ptr);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Adds a COUNT(column) aggregate.
     * @param {string} column
     * @returns {SelectBuilder}
     */
    countCol(column) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.selectbuilder_countCol(ptr, ptr0, len0);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Adds a DISTINCT(column) aggregate (returns count of distinct values).
     * @param {string} column
     * @returns {SelectBuilder}
     */
    distinct(column) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.selectbuilder_distinct(ptr, ptr0, len0);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Executes the query and returns results.
     * @returns {Promise<any>}
     */
    exec() {
        const ret = wasm.selectbuilder_exec(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Executes the query and returns a binary result buffer.
     * Use with getSchemaLayout() for zero-copy decoding in JS.
     * @returns {Promise<BinaryResult>}
     */
    execBinary() {
        const ret = wasm.selectbuilder_execBinary(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Explains the query plan without executing it.
     *
     * Returns an object with:
     * - `logical`: The original logical plan
     * - `optimized`: The optimized logical plan (after index selection, etc.)
     * - `physical`: The final physical execution plan
     * @returns {any}
     */
    explain() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.selectbuilder_explain(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return takeObject(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Sets the FROM table.
     * @param {string} table
     * @returns {SelectBuilder}
     */
    from(table) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.selectbuilder_from(ptr, ptr0, len0);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Adds a GEOMEAN(column) aggregate.
     * @param {string} column
     * @returns {SelectBuilder}
     */
    geomean(column) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.selectbuilder_geomean(ptr, ptr0, len0);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Gets the schema layout for binary decoding.
     * The layout can be cached by JS for repeated queries on the same table.
     * @returns {SchemaLayout}
     */
    getSchemaLayout() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.selectbuilder_getSchemaLayout(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return SchemaLayout.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Adds a GROUP BY clause.
     * @param {any} columns
     * @returns {SelectBuilder}
     */
    groupBy(columns) {
        try {
            const ptr = this.__destroy_into_raw();
            const ret = wasm.selectbuilder_groupBy(ptr, addBorrowedObject(columns));
            return SelectBuilder.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Adds an INNER JOIN.
     * @param {string} table
     * @param {Expr} condition
     * @returns {SelectBuilder}
     */
    innerJoin(table, condition) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        _assertClass(condition, Expr);
        const ret = wasm.selectbuilder_innerJoin(ptr, ptr0, len0, condition.__wbg_ptr);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Adds a LEFT JOIN.
     * @param {string} table
     * @param {Expr} condition
     * @returns {SelectBuilder}
     */
    leftJoin(table, condition) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(table, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        _assertClass(condition, Expr);
        const ret = wasm.selectbuilder_leftJoin(ptr, ptr0, len0, condition.__wbg_ptr);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Sets the LIMIT.
     * @param {number} n
     * @returns {SelectBuilder}
     */
    limit(n) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.selectbuilder_limit(ptr, n);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Adds a MAX(column) aggregate.
     * @param {string} column
     * @returns {SelectBuilder}
     */
    max(column) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.selectbuilder_max(ptr, ptr0, len0);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Adds a MIN(column) aggregate.
     * @param {string} column
     * @returns {SelectBuilder}
     */
    min(column) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.selectbuilder_min(ptr, ptr0, len0);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Creates an observable query using the cached execution path.
     * When data changes, the engine reuses the compiled plan and can apply
     * row-local patches for simple single-table pipelines instead of always
     * re-executing the full query.
     * @returns {JsObservableQuery}
     */
    observe() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.selectbuilder_observe(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return JsObservableQuery.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Sets the OFFSET.
     * @param {number} n
     * @returns {SelectBuilder}
     */
    offset(n) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.selectbuilder_offset(ptr, n);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Adds an ORDER BY clause.
     * @param {string} column
     * @param {JsSortOrder} order
     * @returns {SelectBuilder}
     */
    orderBy(column, order) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.selectbuilder_orderBy(ptr, ptr0, len0, order);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Compiles the current query into a reusable prepared handle.
     * @returns {PreparedSelectQuery}
     */
    prepare() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.selectbuilder_prepare(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return PreparedSelectQuery.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Adds a STDDEV(column) aggregate.
     * @param {string} column
     * @returns {SelectBuilder}
     */
    stddev(column) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.selectbuilder_stddev(ptr, ptr0, len0);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Adds a SUM(column) aggregate.
     * @param {string} column
     * @returns {SelectBuilder}
     */
    sum(column) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(column, wasm.__wbindgen_export, wasm.__wbindgen_export2);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.selectbuilder_sum(ptr, ptr0, len0);
        return SelectBuilder.__wrap(ret);
    }
    /**
     * Creates an IVM-based observable query using DBSP incremental dataflow.
     *
     * Unlike `observe()` which re-executes the full query on every change (O(result_set)),
     * `trace()` compiles the query into a dataflow graph and propagates only deltas (O(delta)).
     *
     * Returns an error if the query is not incrementalizable (e.g. contains ORDER BY / LIMIT).
     * @returns {JsIvmObservableQuery}
     */
    trace() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.selectbuilder_trace(retptr, this.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return JsIvmObservableQuery.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Combines this query with another query using UNION (distinct).
     * @param {SelectBuilder} other
     * @returns {SelectBuilder}
     */
    union(other) {
        try {
            const ptr = this.__destroy_into_raw();
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            _assertClass(other, SelectBuilder);
            wasm.selectbuilder_union(retptr, ptr, other.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return SelectBuilder.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Combines this query with another query using UNION ALL.
     * @param {SelectBuilder} other
     * @returns {SelectBuilder}
     */
    unionAll(other) {
        try {
            const ptr = this.__destroy_into_raw();
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            _assertClass(other, SelectBuilder);
            wasm.selectbuilder_unionAll(retptr, ptr, other.__wbg_ptr);
            var r0 = getDataViewMemory0().getInt32(retptr + 4 * 0, true);
            var r1 = getDataViewMemory0().getInt32(retptr + 4 * 1, true);
            var r2 = getDataViewMemory0().getInt32(retptr + 4 * 2, true);
            if (r2) {
                throw takeObject(r1);
            }
            return SelectBuilder.__wrap(r0);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
     * Sets or extends the WHERE clause.
     * Multiple calls to where_() are combined with AND.
     * @param {Expr} predicate
     * @returns {SelectBuilder}
     */
    where(predicate) {
        const ptr = this.__destroy_into_raw();
        _assertClass(predicate, Expr);
        const ret = wasm.selectbuilder_where(ptr, predicate.__wbg_ptr);
        return SelectBuilder.__wrap(ret);
    }
}
if (Symbol.dispose) SelectBuilder.prototype[Symbol.dispose] = SelectBuilder.prototype.free;

/**
 * UPDATE query builder.
 */
export class UpdateBuilder {
    static __wrap(ptr) {
        ptr = ptr >>> 0;
        const obj = Object.create(UpdateBuilder.prototype);
        obj.__wbg_ptr = ptr;
        UpdateBuilderFinalization.register(obj, obj.__wbg_ptr, obj);
        return obj;
    }
    __destroy_into_raw() {
        const ptr = this.__wbg_ptr;
        this.__wbg_ptr = 0;
        UpdateBuilderFinalization.unregister(this);
        return ptr;
    }
    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_updatebuilder_free(ptr, 0);
    }
    /**
     * Executes the update operation.
     * @returns {Promise<any>}
     */
    exec() {
        const ret = wasm.updatebuilder_exec(this.__wbg_ptr);
        return takeObject(ret);
    }
    /**
     * Sets column values.
     * Can be called with either:
     * - An object: set({ column: value, ... })
     * - Two arguments: set(column, value)
     * @param {any} column_or_obj
     * @param {any | null} [value]
     * @returns {UpdateBuilder}
     */
    set(column_or_obj, value) {
        try {
            const ptr = this.__destroy_into_raw();
            const ret = wasm.updatebuilder_set(ptr, addBorrowedObject(column_or_obj), isLikeNone(value) ? 0 : addHeapObject(value));
            return UpdateBuilder.__wrap(ret);
        } finally {
            heap[stack_pointer++] = undefined;
        }
    }
    /**
     * Sets or extends the WHERE clause.
     * Multiple calls to where_() are combined with AND.
     * @param {Expr} predicate
     * @returns {UpdateBuilder}
     */
    where(predicate) {
        const ptr = this.__destroy_into_raw();
        _assertClass(predicate, Expr);
        const ret = wasm.updatebuilder_where(ptr, predicate.__wbg_ptr);
        return UpdateBuilder.__wrap(ret);
    }
}
if (Symbol.dispose) UpdateBuilder.prototype[Symbol.dispose] = UpdateBuilder.prototype.free;

/**
 * Helper function to create a column reference.
 * @param {string} name
 * @returns {Column}
 */
export function col(name) {
    const ptr0 = passStringToWasm0(name, wasm.__wbindgen_export, wasm.__wbindgen_export2);
    const len0 = WASM_VECTOR_LEN;
    const ret = wasm.col(ptr0, len0);
    return Column.__wrap(ret);
}

/**
 * Initialize the WASM module.
 */
export function init() {
    wasm.init();
}

function __wbg_get_imports() {
    const import0 = {
        __proto__: null,
        __wbg___wbindgen_boolean_get_bbbb1c18aa2f5e25: function(arg0) {
            const v = getObject(arg0);
            const ret = typeof(v) === 'boolean' ? v : undefined;
            return isLikeNone(ret) ? 0xFFFFFF : ret ? 1 : 0;
        },
        __wbg___wbindgen_is_bigint_31b12575b56f32fc: function(arg0) {
            const ret = typeof(getObject(arg0)) === 'bigint';
            return ret;
        },
        __wbg___wbindgen_is_function_0095a73b8b156f76: function(arg0) {
            const ret = typeof(getObject(arg0)) === 'function';
            return ret;
        },
        __wbg___wbindgen_is_null_ac34f5003991759a: function(arg0) {
            const ret = getObject(arg0) === null;
            return ret;
        },
        __wbg___wbindgen_is_object_5ae8e5880f2c1fbd: function(arg0) {
            const val = getObject(arg0);
            const ret = typeof(val) === 'object' && val !== null;
            return ret;
        },
        __wbg___wbindgen_is_undefined_9e4d92534c42d778: function(arg0) {
            const ret = getObject(arg0) === undefined;
            return ret;
        },
        __wbg___wbindgen_memory_bd1fbcf21fbef3c8: function() {
            const ret = wasm.memory;
            return addHeapObject(ret);
        },
        __wbg___wbindgen_number_get_8ff4255516ccad3e: function(arg0, arg1) {
            const obj = getObject(arg1);
            const ret = typeof(obj) === 'number' ? obj : undefined;
            getDataViewMemory0().setFloat64(arg0 + 8 * 1, isLikeNone(ret) ? 0 : ret, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, !isLikeNone(ret), true);
        },
        __wbg___wbindgen_string_get_72fb696202c56729: function(arg0, arg1) {
            const obj = getObject(arg1);
            const ret = typeof(obj) === 'string' ? obj : undefined;
            var ptr1 = isLikeNone(ret) ? 0 : passStringToWasm0(ret, wasm.__wbindgen_export, wasm.__wbindgen_export2);
            var len1 = WASM_VECTOR_LEN;
            getDataViewMemory0().setInt32(arg0 + 4 * 1, len1, true);
            getDataViewMemory0().setInt32(arg0 + 4 * 0, ptr1, true);
        },
        __wbg___wbindgen_throw_be289d5034ed271b: function(arg0, arg1) {
            throw new Error(getStringFromWasm0(arg0, arg1));
        },
        __wbg__wbg_cb_unref_d9b87ff7982e3b21: function(arg0) {
            getObject(arg0)._wbg_cb_unref();
        },
        __wbg_apply_ada2ee1a60ac7b3c: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = getObject(arg0).apply(getObject(arg1), getObject(arg2));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_binaryresult_new: function(arg0) {
            const ret = BinaryResult.__wrap(arg0);
            return addHeapObject(ret);
        },
        __wbg_buffer_7b5f53e46557d8f1: function(arg0) {
            const ret = getObject(arg0).buffer;
            return addHeapObject(ret);
        },
        __wbg_call_389efe28435a9388: function() { return handleError(function (arg0, arg1) {
            const ret = getObject(arg0).call(getObject(arg1));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_call_4708e0c13bdc8e95: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = getObject(arg0).call(getObject(arg1), getObject(arg2));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_database_new: function(arg0) {
            const ret = Database.__wrap(arg0);
            return addHeapObject(ret);
        },
        __wbg_from_bddd64e7d5ff6941: function(arg0) {
            const ret = Array.from(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_getTime_1e3cd1391c5c3995: function(arg0) {
            const ret = getObject(arg0).getTime();
            return ret;
        },
        __wbg_get_9b94d73e6221f75c: function(arg0, arg1) {
            const ret = getObject(arg0)[arg1 >>> 0];
            return addHeapObject(ret);
        },
        __wbg_get_b3ed3ad4be2bc8ac: function() { return handleError(function (arg0, arg1) {
            const ret = Reflect.get(getObject(arg0), getObject(arg1));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_instanceof_Date_1b9f15b87f10aa4c: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof Date;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Memory_dc8c61e3f831ee37: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof WebAssembly.Memory;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Object_1c6af87502b733ed: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof Object;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_instanceof_Uint8Array_9b9075935c74707c: function(arg0) {
            let result;
            try {
                result = getObject(arg0) instanceof Uint8Array;
            } catch (_) {
                result = false;
            }
            const ret = result;
            return ret;
        },
        __wbg_isArray_d314bb98fcf08331: function(arg0) {
            const ret = Array.isArray(getObject(arg0));
            return ret;
        },
        __wbg_keys_b50a709a76add04e: function(arg0) {
            const ret = Object.keys(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_length_32ed9a279acd054c: function(arg0) {
            const ret = getObject(arg0).length;
            return ret;
        },
        __wbg_length_35a7bace40f36eac: function(arg0) {
            const ret = getObject(arg0).length;
            return ret;
        },
        __wbg_new_245cd5c49157e602: function(arg0) {
            const ret = new Date(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_new_361308b2356cecd0: function() {
            const ret = new Object();
            return addHeapObject(ret);
        },
        __wbg_new_3eb36ae241fe6f44: function() {
            const ret = new Array();
            return addHeapObject(ret);
        },
        __wbg_new_b5d9e2fb389fef91: function(arg0, arg1) {
            try {
                var state0 = {a: arg0, b: arg1};
                var cb0 = (arg0, arg1) => {
                    const a = state0.a;
                    state0.a = 0;
                    try {
                        return __wasm_bindgen_func_elem_9140(a, state0.b, arg0, arg1);
                    } finally {
                        state0.a = a;
                    }
                };
                const ret = new Promise(cb0);
                return addHeapObject(ret);
            } finally {
                state0.a = state0.b = 0;
            }
        },
        __wbg_new_dd2b680c8bf6ae29: function(arg0) {
            const ret = new Uint8Array(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_new_from_slice_a3d2629dc1826784: function(arg0, arg1) {
            const ret = new Uint8Array(getArrayU8FromWasm0(arg0, arg1));
            return addHeapObject(ret);
        },
        __wbg_new_no_args_1c7c842f08d00ebb: function(arg0, arg1) {
            const ret = new Function(getStringFromWasm0(arg0, arg1));
            return addHeapObject(ret);
        },
        __wbg_new_with_byte_offset_and_length_aa261d9c9da49eb1: function(arg0, arg1, arg2) {
            const ret = new Uint8Array(getObject(arg0), arg1 >>> 0, arg2 >>> 0);
            return addHeapObject(ret);
        },
        __wbg_new_with_length_1763c527b2923202: function(arg0) {
            const ret = new Array(arg0 >>> 0);
            return addHeapObject(ret);
        },
        __wbg_new_with_length_a2c39cbe88fd8ff1: function(arg0) {
            const ret = new Uint8Array(arg0 >>> 0);
            return addHeapObject(ret);
        },
        __wbg_now_a3af9a2f4bbaa4d1: function() {
            const ret = Date.now();
            return ret;
        },
        __wbg_ownKeys_c7100fb5fa376c6f: function() { return handleError(function (arg0) {
            const ret = Reflect.ownKeys(getObject(arg0));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_parse_708461a1feddfb38: function() { return handleError(function (arg0, arg1) {
            const ret = JSON.parse(getStringFromWasm0(arg0, arg1));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_prototypesetcall_bdcdcc5842e4d77d: function(arg0, arg1, arg2) {
            Uint8Array.prototype.set.call(getArrayU8FromWasm0(arg0, arg1), getObject(arg2));
        },
        __wbg_push_8ffdcb2063340ba5: function(arg0, arg1) {
            const ret = getObject(arg0).push(getObject(arg1));
            return ret;
        },
        __wbg_queueMicrotask_0aa0a927f78f5d98: function(arg0) {
            const ret = getObject(arg0).queueMicrotask;
            return addHeapObject(ret);
        },
        __wbg_queueMicrotask_5bb536982f78a56f: function(arg0) {
            queueMicrotask(getObject(arg0));
        },
        __wbg_resolve_002c4b7d9d8f6b64: function(arg0) {
            const ret = Promise.resolve(getObject(arg0));
            return addHeapObject(ret);
        },
        __wbg_set_6cb8631f80447a67: function() { return handleError(function (arg0, arg1, arg2) {
            const ret = Reflect.set(getObject(arg0), getObject(arg1), getObject(arg2));
            return ret;
        }, arguments); },
        __wbg_set_cc56eefd2dd91957: function(arg0, arg1, arg2) {
            getObject(arg0).set(getArrayU8FromWasm0(arg1, arg2));
        },
        __wbg_set_f43e577aea94465b: function(arg0, arg1, arg2) {
            getObject(arg0)[arg1 >>> 0] = takeObject(arg2);
        },
        __wbg_slice_78ac081c3e25cc60: function(arg0, arg1, arg2) {
            const ret = getObject(arg0).slice(arg1 >>> 0, arg2 >>> 0);
            return addHeapObject(ret);
        },
        __wbg_static_accessor_GLOBAL_12837167ad935116: function() {
            const ret = typeof global === 'undefined' ? null : global;
            return isLikeNone(ret) ? 0 : addHeapObject(ret);
        },
        __wbg_static_accessor_GLOBAL_THIS_e628e89ab3b1c95f: function() {
            const ret = typeof globalThis === 'undefined' ? null : globalThis;
            return isLikeNone(ret) ? 0 : addHeapObject(ret);
        },
        __wbg_static_accessor_SELF_a621d3dfbb60d0ce: function() {
            const ret = typeof self === 'undefined' ? null : self;
            return isLikeNone(ret) ? 0 : addHeapObject(ret);
        },
        __wbg_static_accessor_WINDOW_f8727f0cf888e0bd: function() {
            const ret = typeof window === 'undefined' ? null : window;
            return isLikeNone(ret) ? 0 : addHeapObject(ret);
        },
        __wbg_stringify_8d1cc6ff383e8bae: function() { return handleError(function (arg0) {
            const ret = JSON.stringify(getObject(arg0));
            return addHeapObject(ret);
        }, arguments); },
        __wbg_then_b9e7b3b5f1a9e1b5: function(arg0, arg1) {
            const ret = getObject(arg0).then(getObject(arg1));
            return addHeapObject(ret);
        },
        __wbg_toString_3cadee6e7c22b39e: function() { return handleError(function (arg0, arg1) {
            const ret = getObject(arg0).toString(arg1);
            return addHeapObject(ret);
        }, arguments); },
        __wbindgen_cast_0000000000000001: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { dtor_idx: 1, function: Function { arguments: [], shim_idx: 2, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm.__wasm_bindgen_func_elem_90, __wasm_bindgen_func_elem_1547);
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000002: function(arg0, arg1) {
            // Cast intrinsic for `Closure(Closure { dtor_idx: 297, function: Function { arguments: [Externref], shim_idx: 298, ret: Unit, inner_ret: Some(Unit) }, mutable: true }) -> Externref`.
            const ret = makeMutClosure(arg0, arg1, wasm.__wasm_bindgen_func_elem_3680, __wasm_bindgen_func_elem_3687);
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000003: function(arg0) {
            // Cast intrinsic for `F64 -> Externref`.
            const ret = arg0;
            return addHeapObject(ret);
        },
        __wbindgen_cast_0000000000000004: function(arg0, arg1) {
            // Cast intrinsic for `Ref(String) -> Externref`.
            const ret = getStringFromWasm0(arg0, arg1);
            return addHeapObject(ret);
        },
        __wbindgen_object_clone_ref: function(arg0) {
            const ret = getObject(arg0);
            return addHeapObject(ret);
        },
        __wbindgen_object_drop_ref: function(arg0) {
            takeObject(arg0);
        },
        __wbindgen_object_is_undefined: function(arg0) {
            const ret = getObject(arg0) === undefined;
            return ret;
        },
    };
    return {
        __proto__: null,
        "./cynos_database_bg.js": import0,
    };
}

function __wasm_bindgen_func_elem_1547(arg0, arg1) {
    wasm.__wasm_bindgen_func_elem_1547(arg0, arg1);
}

function __wasm_bindgen_func_elem_3687(arg0, arg1, arg2) {
    wasm.__wasm_bindgen_func_elem_3687(arg0, arg1, addHeapObject(arg2));
}

function __wasm_bindgen_func_elem_9140(arg0, arg1, arg2, arg3) {
    wasm.__wasm_bindgen_func_elem_9140(arg0, arg1, addHeapObject(arg2), addHeapObject(arg3));
}

const BinaryResultFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_binaryresult_free(ptr >>> 0, 1));
const ColumnFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_column_free(ptr >>> 0, 1));
const ColumnOptionsFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_columnoptions_free(ptr >>> 0, 1));
const DatabaseFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_database_free(ptr >>> 0, 1));
const DeleteBuilderFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_deletebuilder_free(ptr >>> 0, 1));
const ExprFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_expr_free(ptr >>> 0, 1));
const ForeignKeyOptionsFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_foreignkeyoptions_free(ptr >>> 0, 1));
const InsertBuilderFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_insertbuilder_free(ptr >>> 0, 1));
const JsChangesStreamFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_jschangesstream_free(ptr >>> 0, 1));
const JsGraphqlSubscriptionFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_jsgraphqlsubscription_free(ptr >>> 0, 1));
const JsIvmObservableQueryFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_jsivmobservablequery_free(ptr >>> 0, 1));
const JsObservableQueryFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_jsobservablequery_free(ptr >>> 0, 1));
const JsTableFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_jstable_free(ptr >>> 0, 1));
const JsTableBuilderFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_jstablebuilder_free(ptr >>> 0, 1));
const JsTransactionFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_jstransaction_free(ptr >>> 0, 1));
const JsonbColumnFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_jsonbcolumn_free(ptr >>> 0, 1));
const PreparedGraphqlQueryFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_preparedgraphqlquery_free(ptr >>> 0, 1));
const PreparedSelectQueryFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_preparedselectquery_free(ptr >>> 0, 1));
const SchemaLayoutFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_schemalayout_free(ptr >>> 0, 1));
const SelectBuilderFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_selectbuilder_free(ptr >>> 0, 1));
const UpdateBuilderFinalization = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(ptr => wasm.__wbg_updatebuilder_free(ptr >>> 0, 1));

function addHeapObject(obj) {
    if (heap_next === heap.length) heap.push(heap.length + 1);
    const idx = heap_next;
    heap_next = heap[idx];

    heap[idx] = obj;
    return idx;
}

function _assertClass(instance, klass) {
    if (!(instance instanceof klass)) {
        throw new Error(`expected instance of ${klass.name}`);
    }
}

function addBorrowedObject(obj) {
    if (stack_pointer == 1) throw new Error('out of js stack');
    heap[--stack_pointer] = obj;
    return stack_pointer;
}

const CLOSURE_DTORS = (typeof FinalizationRegistry === 'undefined')
    ? { register: () => {}, unregister: () => {} }
    : new FinalizationRegistry(state => state.dtor(state.a, state.b));

function dropObject(idx) {
    if (idx < 132) return;
    heap[idx] = heap_next;
    heap_next = idx;
}

function getArrayU8FromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return getUint8ArrayMemory0().subarray(ptr / 1, ptr / 1 + len);
}

let cachedDataViewMemory0 = null;
function getDataViewMemory0() {
    if (cachedDataViewMemory0 === null || cachedDataViewMemory0.buffer.detached === true || (cachedDataViewMemory0.buffer.detached === undefined && cachedDataViewMemory0.buffer !== wasm.memory.buffer)) {
        cachedDataViewMemory0 = new DataView(wasm.memory.buffer);
    }
    return cachedDataViewMemory0;
}

function getStringFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return decodeText(ptr, len);
}

let cachedUint8ArrayMemory0 = null;
function getUint8ArrayMemory0() {
    if (cachedUint8ArrayMemory0 === null || cachedUint8ArrayMemory0.byteLength === 0) {
        cachedUint8ArrayMemory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8ArrayMemory0;
}

function getObject(idx) { return heap[idx]; }

function handleError(f, args) {
    try {
        return f.apply(this, args);
    } catch (e) {
        wasm.__wbindgen_export3(addHeapObject(e));
    }
}

let heap = new Array(128).fill(undefined);
heap.push(undefined, null, true, false);

let heap_next = heap.length;

function isLikeNone(x) {
    return x === undefined || x === null;
}

function makeMutClosure(arg0, arg1, dtor, f) {
    const state = { a: arg0, b: arg1, cnt: 1, dtor };
    const real = (...args) => {

        // First up with a closure we increment the internal reference
        // count. This ensures that the Rust closure environment won't
        // be deallocated while we're invoking it.
        state.cnt++;
        const a = state.a;
        state.a = 0;
        try {
            return f(a, state.b, ...args);
        } finally {
            state.a = a;
            real._wbg_cb_unref();
        }
    };
    real._wbg_cb_unref = () => {
        if (--state.cnt === 0) {
            state.dtor(state.a, state.b);
            state.a = 0;
            CLOSURE_DTORS.unregister(state);
        }
    };
    CLOSURE_DTORS.register(real, state, state);
    return real;
}

function passStringToWasm0(arg, malloc, realloc) {
    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length, 1) >>> 0;
        getUint8ArrayMemory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len, 1) >>> 0;

    const mem = getUint8ArrayMemory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }
    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3, 1) >>> 0;
        const view = getUint8ArrayMemory0().subarray(ptr + offset, ptr + len);
        const ret = cachedTextEncoder.encodeInto(arg, view);

        offset += ret.written;
        ptr = realloc(ptr, len, offset, 1) >>> 0;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

let stack_pointer = 128;

function takeObject(idx) {
    const ret = getObject(idx);
    dropObject(idx);
    return ret;
}

let cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
cachedTextDecoder.decode();
const MAX_SAFARI_DECODE_BYTES = 2146435072;
let numBytesDecoded = 0;
function decodeText(ptr, len) {
    numBytesDecoded += len;
    if (numBytesDecoded >= MAX_SAFARI_DECODE_BYTES) {
        cachedTextDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
        cachedTextDecoder.decode();
        numBytesDecoded = len;
    }
    return cachedTextDecoder.decode(getUint8ArrayMemory0().subarray(ptr, ptr + len));
}

const cachedTextEncoder = new TextEncoder();

if (!('encodeInto' in cachedTextEncoder)) {
    cachedTextEncoder.encodeInto = function (arg, view) {
        const buf = cachedTextEncoder.encode(arg);
        view.set(buf);
        return {
            read: arg.length,
            written: buf.length
        };
    };
}

let WASM_VECTOR_LEN = 0;

let wasmModule, wasm;
function __wbg_finalize_init(instance, module) {
    wasm = instance.exports;
    wasmModule = module;
    cachedDataViewMemory0 = null;
    cachedUint8ArrayMemory0 = null;
    wasm.__wbindgen_start();
    return wasm;
}

async function __wbg_load(module, imports) {
    if (typeof Response === 'function' && module instanceof Response) {
        if (typeof WebAssembly.instantiateStreaming === 'function') {
            try {
                return await WebAssembly.instantiateStreaming(module, imports);
            } catch (e) {
                const validResponse = module.ok && expectedResponseType(module.type);

                if (validResponse && module.headers.get('Content-Type') !== 'application/wasm') {
                    console.warn("`WebAssembly.instantiateStreaming` failed because your server does not serve Wasm with `application/wasm` MIME type. Falling back to `WebAssembly.instantiate` which is slower. Original error:\n", e);

                } else { throw e; }
            }
        }

        const bytes = await module.arrayBuffer();
        return await WebAssembly.instantiate(bytes, imports);
    } else {
        const instance = await WebAssembly.instantiate(module, imports);

        if (instance instanceof WebAssembly.Instance) {
            return { instance, module };
        } else {
            return instance;
        }
    }

    function expectedResponseType(type) {
        switch (type) {
            case 'basic': case 'cors': case 'default': return true;
        }
        return false;
    }
}

function initSync(module) {
    if (wasm !== undefined) return wasm;


    if (module !== undefined) {
        if (Object.getPrototypeOf(module) === Object.prototype) {
            ({module} = module)
        } else {
            console.warn('using deprecated parameters for `initSync()`; pass a single object instead')
        }
    }

    const imports = __wbg_get_imports();
    if (!(module instanceof WebAssembly.Module)) {
        module = new WebAssembly.Module(module);
    }
    const instance = new WebAssembly.Instance(module, imports);
    return __wbg_finalize_init(instance, module);
}

async function __wbg_init(module_or_path) {
    if (wasm !== undefined) return wasm;


    if (module_or_path !== undefined) {
        if (Object.getPrototypeOf(module_or_path) === Object.prototype) {
            ({module_or_path} = module_or_path)
        } else {
            console.warn('using deprecated parameters for the initialization function; pass a single object instead')
        }
    }

    if (module_or_path === undefined) {
        module_or_path = new URL('cynos.wasm', import.meta.url);
    }
    const imports = __wbg_get_imports();

    if (typeof module_or_path === 'string' || (typeof Request === 'function' && module_or_path instanceof Request) || (typeof URL === 'function' && module_or_path instanceof URL)) {
        module_or_path = fetch(module_or_path);
    }

    const { instance, module } = await __wbg_load(await module_or_path, imports);

    return __wbg_finalize_init(instance, module);
}

export { initSync, __wbg_init as default };
