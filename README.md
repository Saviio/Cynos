# Cynos
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Saviio/cynos)

> Warning: Cynos is still under active development and is not production-ready. APIs, crate boundaries, and performance characteristics may change.

Cynos is an embedded in-memory relational engine built for low-latency live queries, prepared execution, incremental maintenance, and efficient WASM/JS delivery.

Cynos is a Rust workspace for an in-memory relational engine, a rule/heuristic query planner, incremental dataflow, reactive subscriptions, and a WASM-backed JavaScript/TypeScript package.

The public JS package lives in `js/packages/core` as `@cynos/core`. The Rust crates are organized so the lower layers stay reusable and mostly `#![no_std]`, while the host-facing WASM and benchmark crates depend on a full runtime.

## What Exists Today

- In-memory row storage with schema enforcement, primary-key lookups, secondary B+Tree indexes, and JSONB-oriented GIN indexes.
- Rule/heuristic query planning with predicate pushdown, implicit join recognition, join reordering by row-count estimates, index selection, TopN pushdown, order-by-index rewriting, limit/offset pushdown into scans, and cached compiled execution artifacts.
- Reactive query APIs with three distinct modes:
  - `observe()`: cached query execution, callback receives the full current result set when it changes.
  - `changes()`: cached query execution, callback receives the full current result set immediately and on later changes.
  - `trace()`: incremental view maintenance (IVM), callback receives `{ added, removed }` deltas for incrementalizable queries.
- A shared live runtime abstraction in `cynos-database` that batches table changes once and fans them out to row subscriptions and GraphQL subscriptions across snapshot and delta backends.
- Prepared query handles via `prepare()`, which reuse the compiled physical plan and expose `exec()`, `execBinary()`, and `getSchemaLayout()` for repeated execution.
- An engine-native GraphQL surface via `cynos-gql`, including generated SDL, root `query` / `mutation` / `subscription` fields, prepared GraphQL operations, planner-backed root-table execution for `where` / `orderBy` / `limit` / `offset`, shared live-runtime integration with `observe()` / `changes()` / `trace()`, and batched nested relation rendering for live payload assembly.
- Compiled single-table execution fast paths that can fuse scan/filter/project work and apply row-local reactive patches for simple subscriptions instead of always re-running the full query.
- Binary query results via `execBinary()` + `getSchemaLayout()` + `ResultSet` for low-overhead WASM-to-JS transfer.
- JSONB building blocks including a compact binary codec, a JSONPath subset parser/evaluator, JSONB operators, and extraction helpers for GIN indexing.
- Journaled in-memory transactions with commit/rollback APIs.

## Advantages

Cynos is designed for embedded, query-heavy applications where the hot working set lives in memory and live queries are part of the runtime, not layered on later.

That gives it a different design target from SQLite, RxDB, and PGlite:

| Compared with | Better known for | Cynos is typically stronger when... |
| --- | --- | --- |
| SQLite | Durable embedded SQL storage, broad compatibility, and tiny operational footprint | the data is already memory-resident and engine-native live queries or low-overhead WASM result delivery matter more than persistence |
| RxDB | Local-first sync, reactive document collections, `RxQuery`, query cache, and EventReduce-based updates | the workload needs relational joins, aggregates, JSONB-style querying, and reactive delivery from one execution engine |
| PGlite | PostgreSQL compatibility in WASM with ecosystem reuse | a tighter embedded execution path and purpose-built reactive APIs matter more than PostgreSQL compatibility |

Core advantages:

- Live queries are first-class: `observe()` / `changes()` return full current results, while `trace()` returns DBSP-style `{ added, removed }` deltas.
- Hot paths are compiled and cached: physical plans, execution artifacts, fused single-table fast paths, and row-local reactive patching reduce repeat-query overhead.
- The WASM/JS boundary is optimized too: `execBinary()` plus `SchemaLayout` can avoid most JS object materialization cost on larger results.
- The workspace stays modular: storage, planner, indexes, JSONB, incremental dataflow, and host bindings are separated, with lower layers kept largely `no_std + alloc` friendly.

Representative recent local measurements, on Node.js + WASM with both 10K and 100K row/document datasets, are shown below as workload-specific reference points rather than universal claims. The main relational table uses aligned semantics across Cynos, PGlite, and SQLite: prepared query reuse plus full JS object materialization.

| Workload | Cynos | Representative comparison | What it suggests |
| --- | ---: | ---: | --- |
| Point lookup (`id` near the 90th percentile) | `0.013 ms` at 10K, `0.006 ms` at 100K | PGlite `0.209 ms` at 10K, `1.35 ms` at 100K; SQLite `0.020 ms` at 10K, `0.012 ms` at 100K | Cynos is already very competitive on repeated embedded reads, not only on subscriptions |
| Relational join (`users JOIN departments ... LIMIT 1000`) | `6.10 ms` at 10K, `6.37 ms` at 100K | PGlite `8.11 ms` at 10K, `10.60 ms` at 100K; SQLite `3.22 ms` at 10K, `5.07 ms` at 100K | Cynos is competitive on embedded relational work while staying purpose-built for reactive delivery |
| Wide scan (`LIMIT 5000`) via object rows vs `execBinary()` only | `16.45 ms -> 0.836 ms` at 10K, `17.16 ms -> 0.810 ms` at 100K | same Cynos query, different transport/materialization path | The WASM/JS boundary is a major part of end-to-end cost, and `execBinary()` remains one of Cynos's clearest advantages |
| Live update latency | `changes(): 0.353 ms`, `trace(): 0.021 ms` at 10K; `changes(): 4.35 ms`, `trace(): 0.031 ms` at 100K | PGlite live query `2.63 ms` at 10K, `9.69 ms` at 100K | Cynos is especially strong when live queries are part of the hot path, and `trace()` stays close to constant here |
| JSON filter (`metadata.category = tech LIMIT 100`) | `0.229 ms` at 10K, `0.483 ms` at 100K | PGlite `0.760 ms` at 10K, `1.76 ms` at 100K; SQLite `0.219 ms` at 10K, `0.282 ms` at 100K | Cynos already handles structured JSON predicates in the same practical range as embedded SQL/WASM peers |
| Mutation-driven requery (`insert + complex JSON query`) | `0.318 ms` at 10K, `0.326 ms` at 100K | PGlite `0.422 ms` at 10K, `0.637 ms` at 100K; SQLite `0.118 ms` at 10K, `0.205 ms` at 100K | Cynos's re-query path remains competitive even when the query includes nested JSON predicates and ordering |

A few important caveats to keep the comparison fair:

- In the same local harness, SQLite (`sql.js`) remained faster on some wide scans, aggregate-heavy paths, and several JSON query shapes.
- RxDB remained faster on warm cached document reads; Cynos's advantage is the unified relational + JSONB + reactive engine, not cache-hit document lookup latency alone.
- The benchmark now separates "prepared query + object rows" from lower-overhead array/binary paths, because otherwise result materialization can dominate the comparison and blur what is actually executor cost.
- On the current 10K/100K document benchmark, Cynos's metadata index is still workload-sensitive: it helps some 10K JSON filters materially, but the current 100K compound/indexed paths are not yet wins and remain useful optimizer/index-planning regression checks.
- PGlite remains the better fit when PostgreSQL compatibility and ecosystem reuse are the primary requirements.

The cross-engine WASM comparison harness that produced these numbers lives in `scripts/engine_compare.mjs` and currently runs both 10K and 100K scenarios with aligned prepared-query semantics for the main relational comparisons.

Best fit:

- in-memory dashboards, local analytics, collaborative views, and client-side derived state;
- browser / edge / WASM applications where query latency and JS materialization cost both matter;
- workloads that repeatedly execute the same query shapes and benefit from cached execution or incremental maintenance.

Less ideal when durability, SQL compatibility, or sync ecosystem breadth matter more than in-memory reactive performance:

- choose SQLite when durable embedded SQL storage is the main requirement;
- choose RxDB when offline sync and document-centric replication are the main requirement;
- choose PGlite when PostgreSQL compatibility in WASM is the main requirement.

## Repository Layout

| Path | Purpose |
| --- | --- |
| `crates/core` | Shared types, schema definitions, rows, and errors |
| `crates/index` | Standalone B+Tree, hash, nullable, and GIN index primitives |
| `crates/storage` | Row store, table cache, constraint checking, and transaction journal |
| `crates/query` | AST, planner, optimizer passes, physical plans, and executors |
| `crates/incremental` | DBSP-style incremental dataflow and materialized views |
| `crates/reactive` | Delta-based observable queries on top of incremental views |
| `crates/jsonb` | JSONB value model, binary codec, JSONPath subset, and operators |
| `crates/binary` | Compact binary row encoding for WASM/JS transfer |
| `crates/gql` | GraphQL AST, parser, schema generation, binding, planner lowering, and execution |
| `crates/database` | WASM/JS-facing database API that stitches the other crates together |
| `crates/perf` | Custom benchmark runner for the workspace |
| `js/packages/core` | Published NPM package `@cynos/core` |
| `scripts` | Cross-engine benchmark and comparison scripts |
| `example` | Vite demo app for live queries, binary protocol, and performance experiments |

## Architecture Overview

At a high level, Cynos is layered so the storage, planning, incremental, and host-facing pieces stay separable:

```text
Application code
  -> @cynos/core (js/packages/core)
      -> cynos-database (wasm-bindgen host API)
          -> cynos-gql          -> GraphQL schema / parser / binder / planner lowering
          -> cynos-query        -> logical plan / physical plan / executors
          -> cynos-storage      -> row store / constraints / transaction journal
          -> cynos-index        -> B+Tree / hash / GIN primitives
          -> cynos-jsonb        -> JSONB values / JSONPath / operators / key extraction
          -> cynos-binary       -> compact result encoding for WASM -> JS
          -> cynos-incremental  -> incremental dataflow for eligible plans
          -> cynos-reactive     -> subscriptions on top of incremental views
```

Operationally there are two query delivery paths:

Both paths sit under one live runtime control plane in `cynos-database`: row subscriptions and GraphQL subscriptions share the same change registry / flush machinery, while the GraphQL layer stays an adapter that selects the snapshot or delta kernel per query shape.

1. Cached execution path:
   - `SelectBuilder` produces a logical plan.
   - `cynos-query` rewrites and lowers it to a physical plan plus a cached `PlanExecutionArtifact`.
   - The compiled artifact runs against `cynos-storage`, using fused single-table fast paths where possible and a compiled executor elsewhere.
   - `observe()` / `changes()` reuse that cached artifact. For simple single-table pipelines without blocking operators, the reactive layer can patch the current result in place from `changed_ids` instead of forcing a full re-execution.
   - Results are delivered either as JS objects or as a binary buffer decoded by `ResultSet`.
2. Incremental path:
   - If `PhysicalPlan::is_incrementalizable()` is true, `cynos-database` lowers the plan into `cynos-incremental` dataflow.
   - `cynos-reactive` subscribes to the materialized view and emits row-level deltas.
   - If the plan is not incrementalizable, callers should use the cached execution APIs instead.

Cross-cutting responsibilities:

- `cynos-core` defines the shared schema/value/error model used everywhere else.
- `cynos-jsonb` feeds both query semantics and GIN-style indexing.
- `cynos-perf` exercises the full stack with custom benchmark scenarios.

## JavaScript / TypeScript Quick Start

```ts
import {
  ColumnOptions,
  JsDataType,
  col,
  createDatabase,
  initCynos,
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

const rows = await db
  .select('*')
  .from('users')
  .where(col('age').gt(25))
  .exec();

console.log(rows);
```

Notes:

- Table builders must be passed to `db.registerTable(...)`. Calling `createTable(...).column(...);` alone does not install the schema.
- `select()` accepts `'*'`, a single column name, an array of column names, or multiple variadic column arguments.
- `prepare()` returns a reusable handle with `exec()`, `execBinary()`, and `getSchemaLayout()` when the same query shape runs repeatedly.
- For joins, use column references on both sides, for example `col('orders.user_id').eq(col('users.id'))`.

## GraphQL Quick Start

Cynos can expose the current table cache as a GraphQL schema directly from Rust/WASM. More
importantly, GraphQL in Cynos is attached at the engine boundary: schema is derived from the
current database metadata, root reads lower into the native planner, mutations execute through
native row operations, and subscriptions reuse the same live runtime control plane as
`observe()` / `changes()` / `trace()`.

For readers familiar with systems such as Hasura or PostGraphile, the main difference here is the
integration boundary. In Cynos, GraphQL is not primarily a separate service tier over the
database; it is one of the native query surfaces of the engine itself.

The JS package exposes that surface through:

- `db.graphqlSchema()`: render the current schema as SDL
- `db.graphql(query, variables?, operationName?)`: execute a query or mutation and return `{ data }`
- `db.subscribeGraphql(query, variables?, operationName?)`: create a live GraphQL subscription
- `db.prepareGraphql(query, operationName?)`: reuse a parsed GraphQL document across executions

Operationally, that means:

- schema comes from the current `TableCache`
- root `query` / `subscription` fields lower into `cynos-query`
- mutations run through native row operations
- GraphQL subscriptions share the same live runtime control plane as the row-oriented APIs
- live nested relation payloads are assembled with Rust-side batching rather than a
  resolver-per-parent-row execution pattern

Example schema setup with explicit relation names:

```ts
import {
  ColumnOptions,
  ForeignKeyOptions,
  JsDataType,
  createDatabase,
  initCynos,
} from '@cynos/core';

await initCynos();
const db = createDatabase('graphql-demo');

db.registerTable(
  db.createTable('users')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('name', JsDataType.String)
);

db.registerTable(
  db.createTable('posts')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('author_id', JsDataType.Int64)
    .column('title', JsDataType.String)
    .foreignKey(
      'fk_posts_author',
      'author_id',
      'users',
      'id',
      new ForeignKeyOptions()
        .fieldName('author')
        .reverseFieldName('posts')
    )
);

console.log(db.graphqlSchema());
```

### Query syntax

Root query fields are generated per table:

- collection query: `users`, `posts`, ...
- primary-key query: `usersByPk`, `postsByPk`, ...

Collection fields accept `where`, `orderBy`, `limit`, and `offset`.

```graphql
query GetUsers($minId: Long!) {
  users(
    where: { id: { gte: $minId } }
    orderBy: [{ field: ID, direction: ASC }]
    limit: 10
  ) {
    id
    name
    posts(orderBy: [{ field: ID, direction: DESC }], limit: 5) {
      id
      title
    }
  }
}
```

Primary-key lookup:

```graphql
{
  usersByPk(pk: { id: 1 }) {
    id
    name
  }
}
```

Planner note: root collection queries and `ByPk` queries are lowered into the existing `cynos-query` planner path, so root `where` / `orderBy` / `limit` / `offset` can benefit from the same index-aware execution as the native query builder.

### Mutation syntax

Each table generates three root mutations based on the GraphQL type name:

- `insertUsers`
- `updateUsers`
- `deleteUsers`

Examples:

```graphql
mutation {
  insertUsers(input: [{ id: 1, name: "Alice" }]) {
    id
    name
  }
}
```

```graphql
mutation {
  updateUsers(
    where: { id: { eq: 1 } }
    set: { name: "Alicia" }
  ) {
    id
    name
  }
}
```

```graphql
mutation {
  deleteUsers(where: { id: { eq: 1 } }) {
    id
    name
  }
}
```

`update<Type>` and `delete<Type>` reuse the same collection arguments as root queries, so `where`, `orderBy`, `limit`, and `offset` also shape the mutation target set.

### Subscription syntax

Subscriptions use the same root field shapes as queries and return standard GraphQL `{ data }` payloads in JS:

```ts
const sub = db.subscribeGraphql(`
  subscription {
    users(orderBy: [{ field: ID, direction: ASC }]) {
      id
      name
    }
  }
`);

console.log(sub.getResult());

const stop = sub.subscribe((payload) => {
  console.log(payload.data.users);
});
```

Prepared GraphQL documents also support live subscriptions:

```ts
const prepared = db.prepareGraphql(
  `
    subscription UserFeed {
      users(orderBy: [{ field: ID, direction: ASC }]) {
        id
        name
      }
    }
  `,
  'UserFeed'
);

const sub = prepared.subscribe();
```

Runtime note: GraphQL subscriptions compile the root field into the existing planner path, then
choose a snapshot/re-query or delta/IVM backend depending on query shape. This does not expose
`trace()` row deltas directly; GraphQL subscriptions still deliver GraphQL `{ data }` snapshots.
But the lower live kernels are shared, and nested relation payloads are assembled with Rust-side
batching so multi-level live payloads do not degrade into row-by-row in-memory N+1 fetch
patterns.

### Filters, ordering, and scalars

The generated schema includes table-specific inputs such as:

- `UsersWhereInput`
- `UsersOrderByInput`
- `UsersPkInput`
- `UsersInsertInput`
- `UsersPatchInput`

Common filter operators:

- Boolean: `eq`, `ne`, `isNull`
- Numeric / DateTime: `eq`, `ne`, `in`, `notIn`, `gt`, `gte`, `lt`, `lte`, `between`, `isNull`
- String / Bytes: `eq`, `ne`, `in`, `notIn`, `gt`, `gte`, `lt`, `lte`, `like`, `isNull`
- JSON: `path`, `eq`, `contains`, `exists`, `isNull`
- Logical composition: `AND`, `OR`

Example:

```graphql
{
  users(
    where: {
      AND: [
        { id: { between: [1, 100] } }
        { name: { like: "Al%" } }
      ]
    }
    orderBy: [{ field: ID, direction: DESC }]
  ) {
    id
    name
  }
}
```

Current scalar mapping:

- `Int32 -> Int`
- `Int64 -> Long`
- `Float64 -> Float`
- `DateTime -> DateTime`
- `Bytes -> Bytes`
- `Jsonb -> JSON`

### Current GraphQL scope boundaries

The current GraphQL surface is intentionally focused on table access and live queries. A few
areas are better understood as deliberate scope boundaries than as accidental omissions:

- fragments are not implemented; in the current Cynos usage model, GraphQL operations are often
  prepared close to the database boundary with concrete selections
- full GraphQL introspection is not implemented; Cynos already owns the schema metadata and can
  render SDL directly via `db.graphqlSchema()`, which is often the more relevant capability in an
  embedded setting
- broader directive support is intentionally minimal; `@include` and `@skip` map cleanly to
  bind-time selection pruning, while other directive families do not currently map as naturally to
  the planner and live-query model
- multi-root subscriptions are intentionally unsupported; they add significant dependency-tracking,
  backend-selection, invalidation, and materialization complexity for relatively low practical ROI

`@include`, `@skip`, and `__typename` are supported. GraphQL subscriptions must select exactly one
concrete root field.

## Reactive Modes

| API | Callback payload | Best for | Notes |
| --- | --- | --- | --- |
| `observe()` | Full current result set | Imperative listeners that can fetch the initial state manually | No initial callback; call `getResult()` first if needed. Uses cached execution artifacts and may patch simple single-table results in place |
| `changes()` | Full current result set | UI state updates such as React `setState` | Emits the initial result immediately. Uses the same cached execution path as `observe()` |
| `trace()` | `{ added, removed }` delta object | Incremental UIs or downstream consumers that want O(delta) updates | Only works for incrementalizable plans; `ORDER BY`, `LIMIT`, `OFFSET`, and other non-streamable operators fall back to `observe()`/`changes()` |

```ts
import { col } from '@cynos/core';

const stream = db
  .select('*')
  .from('users')
  .where(col('age').gte(18))
  .changes();

const stopStream = stream.subscribe((currentRows) => {
  console.log('current result', currentRows);
});

const trace = db
  .select('*')
  .from('users')
  .where(col('age').gte(18))
  .trace();

console.log('initial trace result', trace.getResult());

const stopTrace = trace.subscribe((delta) => {
  console.log('added', delta.added);
  console.log('removed', delta.removed);
});
```

## DBSP-Style IVM

`trace()` is Cynos's DBSP-style incremental view maintenance path. It is not a separate query language; it reuses the normal planner pipeline and then lowers an eligible physical plan into a delta-oriented dataflow graph.

`observe()` / `changes()` now cover an important middle ground: they still expose full-result semantics, but simple single-table subscriptions can avoid full re-query work by applying row-local patches through the cached execution artifact.

How it works today:

1. Build the normal logical plan from the query builder.
2. Run the regular planner/optimizer pipeline and produce a physical plan.
3. Execute that physical plan once to bootstrap the initial result.
4. If `PhysicalPlan::is_incrementalizable()` is true, compile the plan into `cynos-incremental::DataflowNode`.
5. Route table-level `Delta<Row>` updates through `cynos-reactive::ObservableQuery`, which emits `{ added, removed }` changes to subscribers.

That design means the incremental path shares the same query-building surface as the re-query path, but uses a different maintenance strategy after the initial snapshot.

Current capabilities that are grounded in the implemented compiler/runtime:

- Incrementalizable scan family: table scans, index scans/gets, `IN` index gets, and GIN scan variants can participate in the bootstrap plan and dependency graph.
- Incrementalizable relational operators: filter, projection, map-style computed projections, joins, cross product, and hash aggregate.
- Current aggregate coverage on the incremental path: `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.
- `MIN` and `MAX` are maintained with ordered multisets internally, so deletes do not force a full recomputation of the group.
- Join plans are lowered into incremental join state, so multi-table live queries can stay on the delta path when the rest of the plan is eligible.

Current boundaries:

- `ORDER BY`, `LIMIT`, and `TopN` are explicit blockers for `trace()`.
- When a query cannot be incrementalized, `trace()` returns an error and the caller should use `observe()` or `changes()` instead.
- The callback payload is a delta object, not a fully materialized result set, so consumers must maintain local state or re-read with `getResult()`.

## Choosing A Live Query Strategy

Cynos intentionally exposes two live-query families because they optimize for different things.

| Strategy | API | Strengths | Tradeoffs | Good fit |
| --- | --- | --- | --- | --- |
| Cached full-result path | `observe()` / `changes()` | Works with the full query surface, easy to consume, always delivers the full current state, naturally fits UI state setters, and can use row-local patching for simple single-table subscriptions | Still ships full current results to subscribers; complex plans may still fall back to deterministic re-execution and full result comparison | Dashboards, CRUD tables, simple filtered lists, queries with `ORDER BY` / `LIMIT`, correctness-first integrations |
| Incremental plan | `trace()` | Reuses the planned query shape but propagates deltas after bootstrap; lower steady-state cost on frequent writes; payload is small when output churn is small | Restricted to incrementalizable plans; consumer must apply `{ added, removed }`; less convenient for naive UI binding | High-frequency subscriptions, collaborative/live views, pipelines where the query is mostly filter/join/group-by without ordering/windowing |

Practical guidance:

- Use `changes()` when you want the simplest "always give me the current rows" API, especially in React/Vue-style state updates.
- Use `observe()` when you want the same re-query semantics but prefer to fetch the initial state manually.
- Use `trace()` when the query stays within the supported incremental subset and update frequency is high enough that even the cached full-result path becomes the bottleneck, or when you explicitly want row-level deltas.
- If you need sorted, paginated, or otherwise non-streamable output, treat the cached full-result path as the default strategy.

## Local Bench Snapshot

To make this tradeoff less abstract, benchmarks were conducted natively at the Rust layer on a Mac mini M4, before cross-compilation to WASM. The absolute numbers will differ in WASM environments (browsers, runtimes), but the relative performance characteristics shown here reflect the underlying algorithmic tradeoffs.

```bash
cargo bench -p cynos-database --bench requery_microbench
```

Recent `requery_microbench` runs show the current cached execution path behaves very differently depending on the query shape. The `observe()` / `changes()` APIs are still full-result APIs, but simple single-table subscriptions now use a row-local patch path instead of a forced full re-query on every change.

| Scenario | 10K rows | 100K rows | Notes |
| --- | --- | --- | --- |
| `single_query_filter_execute` | `226.85 us` | `6.07 ms` | Compiled single-query execution |
| `requery_observe_create` | `318.29 us` | `7.02 ms` | Initial snapshot + subscription setup |
| `requery_on_change/result_changes` | `925 ns` | `1.27 us` | Simple single-table filter, row-local patch path |
| `requery_on_change/result_unchanged` | `810 ns` | `812 ns` | Same shape, unchanged result |
| `requery_on_change_filter_project_limit/result_changes` | `19.71 us` | `17.98 us` | `LIMIT` keeps this on the generic cached execution path |
| `requery_on_change_compound_filter/result_changes` | `948 ns` | `1.31 us` | Compound single-table filter still benefits from row-local patching |

What these numbers mean in practice:

- Initial query execution still costs what you would expect for a compiled in-memory query over 10K / 100K rows.
- Simple single-table live queries are now much cheaper on steady-state updates because the engine can patch only the touched rows into the current result.
- Blocking operators such as `LIMIT` still matter: they can keep a subscription on the generic cached execution path even when the rest of the query is simple.
- `trace()` remains the true delta-native API for joins, grouped subscriptions, and consumers that want `{ added, removed }` rather than a refreshed full result.

Important caveats:

- These are Rust-side harness numbers, not browser end-to-end UI timings.
- The `requery_on_change` rows above are no longer measuring a forced full re-query for simple single-table plans; they measure the current reactive update path, which may use row-local patching.
- The callback payload for `observe()` / `changes()` is still the full current result set even when the engine-side maintenance work is tiny.
- If your real UI needs sorted/paginated output, or the query shape is multi-table / aggregate-heavy, the generic cached path or `trace()` may still be the better mental model than the single-table patch fast path.

## Complexity At A Glance

These are the typical asymptotics for the current implementation, not hard real-time guarantees:

| Area | Typical cost | Notes |
| --- | --- | --- |
| Primary-key / B+Tree lookup | `O(log n)` | Backed by `RowStore` + B+Tree indexes |
| B+Tree range scan | `O(log n + k)` | `k` is the number of returned row IDs |
| Standalone hash-index lookup | Average `O(1)` | Implemented in `cynos-index`, but not the default secondary-index path in `RowStore` today |
| GIN-style key lookup | Proportional to extracted keys + posting-list work | Best thought of as inverted-index style rather than a plain `O(1)` hash lookup |
| `observe()` / `changes()` simple single-table path | Roughly `O(changed_rows * per-row filter/project) + O(log r)` | Applies row-local patches into the current result when the plan is a patchable single-table pipeline |
| `observe()` / `changes()` generic path | Re-executes the cached plan and compares/materializes the current result | Used for joins, aggregates, `LIMIT`, sorting, and any plan outside the patchable subset |
| `trace()` | `O(Δoutput)` delivery after incremental compilation | Only for plans that can be lowered to incremental dataflow |
| Incremental `COUNT` / `SUM` / `AVG` | `O(Δinput)` over affected groups | Maintains running aggregate state |
| Incremental `MIN` / `MAX` | `O(log group_size)` per delta | Implemented with ordered multisets in `cynos-incremental` |

## Binary Results

The JS package can read query results through a compact binary buffer instead of JSON object materialization.

```ts
import { ResultSet } from '@cynos/core';

const query = db.select('*').from('users');
const layout = query.getSchemaLayout();
const buffer = await query.execBinary();

const rs = new ResultSet(buffer, layout);
console.log(rs.length);
console.log(rs.getString(0, 1));

const materialized = rs.toArray();
rs.free();
```

Implementation details:

- The binary format is row-major with a 16-byte header plus a fixed-width section and a variable-width section.
- `Int64` and `DateTime` are encoded as `f64` values in the binary payload for JS interop.
- `ResultSet` uses a compiled decoder when `new Function()` is allowed and falls back to an interpreted path under stricter CSP settings.

## Rust Quick Start

```rust
use cynos_core::schema::TableBuilder;
use cynos_core::{DataType, Row, Value};
use cynos_storage::RowStore;

fn main() -> cynos_core::Result<()> {
    let schema = TableBuilder::new("users")?
        .add_column("id", DataType::Int64)?
        .add_column("name", DataType::String)?
        .add_primary_key(&["id"], false)?
        .build()?;

    let mut store = RowStore::new(schema);
    store.insert(Row::new(
        1,
        vec![Value::Int64(1), Value::String("Alice".into())],
    ))?;

    assert_eq!(store.len(), 1);
    Ok(())
}
```

## Build And Test

```bash
# Rust workspace
cargo test --workspace

# Benchmark binary
cargo run -p cynos-perf --release

# Raw WASM build for the database crate
cd crates/database
wasm-pack build --target web

# JS package workspace
cd ../../js
pnpm install
pnpm build
```

## Browser / Runtime Compatibility

Compilation to WASM and execution in browser-like environments are first-class features of cynos. To provide a complete picture, this section presents additional compatibility data.

The current JS package and generated glue code rely on these platform features:

| Capability | Where it is used | Requirement |
| --- | --- | --- |
| `WebAssembly` | Loading and instantiating `cynos.wasm` | Required |
| `Promise` | `initCynos()` and async query APIs | Required |
| `TypedArray` / `DataView` | Binary protocol and WASM memory access | Required |
| `TextDecoder` | String and JSONB decoding | Required |
| `BigInt` | Exact `Int64` inputs outside the JS safe-integer range | Optional for basic usage, required for precise 64-bit literals |
| `new Function()` | Fast compiled decoder in `ResultSet` | Optional; there is an interpreted fallback |

Notes:

- The generated wasm-bindgen loader already falls back from `WebAssembly.instantiateStreaming(...)` to byte-buffer instantiation when streaming or MIME setup is unavailable.
- The current WASM target does not depend on threads or SIMD-specific browser features.
- `@cynos/core` is ESM-first and targets modern browsers plus recent Node.js runtimes with standard WebAssembly APIs.

## Current Limits

- Cynos is in-memory only. There is no durable on-disk storage engine yet.
- Transactions are journaled commit/rollback over in-memory state; this is not durable storage in the traditional ACID database sense.
- `trace()` only works for plans that the physical planner can lower to incremental dataflow.
- The GraphQL layer is intentionally focused on table/query semantics and live queries: fragments and full introspection are not implemented, subscriptions require a single concrete root field, and multi-root subscriptions are intentionally left out because they add substantial complexity for relatively low payoff in the current model.
- Storage/query integration currently materializes B+Tree and GIN indexes from schema definitions. A standalone hash index implementation also exists in the workspace, but it is not the default secondary-index path in `RowStore` today.
- JavaScript `Int64` values are exposed through JS-friendly paths as numbers, so values outside the safe integer range lose precision unless the calling pattern is designed around that limitation.

## License

Apache-2.0
