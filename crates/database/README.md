# cynos-database

WASM-facing database API that stitches together storage, query planning, reactive delivery, and binary encoding.

## Overview

`cynos-database` is the host-facing crate in the Rust workspace. It exposes `wasm-bindgen` types such as:

- `Database`
- `JsTableBuilder` / `JsTable`
- `SelectBuilder`, `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder`
- `JsObservableQuery`, `JsChangesStream`, `JsIvmObservableQuery`
- `BinaryResult` and `SchemaLayout`

The published JS package `@cynos/core` in `js/packages/core` is built on top of these WASM exports.

## Query Surface

The select builder currently supports:

- Projection (`'*'`, single column, arrays, or variadic columns)
- `where(...)`
- `orderBy(...)`, `limit(...)`, `offset(...)`
- `innerJoin(...)` and `leftJoin(...)`
- `groupBy(...)`
- Aggregates: `count`, `countCol`, `sum`, `avg`, `min`, `max`, `stddev`, `geomean`, `distinct`
- `explain()`, `getSchemaLayout()`, and `execBinary()`

## Reactive Modes

| API | Engine path | Callback payload | Typical delivery cost | Notes |
| --- | --- | --- | --- | --- |
| `observe()` | Re-query | Full current result set on change | Re-executes the query and rematerializes the current result | Call `getResult()` yourself for the initial state |
| `changes()` | Re-query | Full current result set immediately and on later changes | Same as `observe()`, but with an eager initial emission | Good fit for UI state |
| `trace()` | Incremental dataflow | `{ added, removed }` | Scales with delta propagation after the plan is compiled to dataflow | Fails for non-incrementalizable plans such as `ORDER BY` / `LIMIT` / `TopN` |

## JavaScript Example

```ts
import {
  ColumnOptions,
  JsDataType,
  ResultSet,
  col,
  createDatabase,
  initCynos,
  snapshotSchemaLayout,
} from '@cynos/core';

await initCynos();
const db = createDatabase('demo');

const users = db.createTable('users')
  .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
  .column('name', JsDataType.String)
  .column('age', JsDataType.Int32)
  .index('idx_age', 'age');

db.registerTable(users);

await db.insert('users').values([
  { id: 1, name: 'Alice', age: 25 },
  { id: 2, name: 'Bob', age: 30 },
]).exec();

const current = await db
  .select('*')
  .from('users')
  .where(col('age').gt(18))
  .exec();

const stream = db.select('*').from('users').changes();
const stop = stream.subscribe((rows) => {
  console.log('current result', rows);
});

const binaryLayout = snapshotSchemaLayout(stream.getSchemaLayout());
const stopBinary = stream.subscribeBinary((binary) => {
  const transferable = binary.intoTransferable();
  const rs = new ResultSet(transferable, binaryLayout);
  console.log('binary snapshot', rs.toArray());
});

const trace = db
  .select('*')
  .from('users')
  .where(col('age').gt(18))
  .trace();

const stopTrace = trace.subscribe((delta) => {
  console.log(delta.added, delta.removed);
});

const query = db.select('*').from('users');
const layout = query.getSchemaLayout();
const binary = await query.execBinary();
const rs = new ResultSet(binary, layout);
console.log(rs.toArray());
rs.free();
```

## Transactions

`db.transaction()` returns a `JsTransaction` wrapper with `insert`, `update`, `delete`, `commit`, and `rollback` methods.

Current behavior:

- Transactions are journal-based and operate on in-memory state.
- Commit/rollback integrate with the re-query notification path.
- For direct delta-driven IVM notifications, the non-transactional CRUD builders (`insert` / `update` / `delete`) are the fully wired path today.

## Build Notes

```bash
# Raw WASM build
cd crates/database
wasm-pack build --target web

# Or build the JS package workspace that wraps this crate
cd ../../js
pnpm install
pnpm build
```

## License

Apache-2.0
