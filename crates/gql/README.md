# cynos-gql

Native GraphQL schema, compiler, execution, and live-query rendering for Cynos.

`cynos-gql` gives Cynos a GraphQL surface that is built from the same schema metadata, planner,
mutation pipeline, and live-query runtime as the rest of the database.

This yields a more tightly integrated model than the usual "GraphQL in front of a database" shape.
Table metadata becomes SDL. Root GraphQL fields lower into the native query planner. Mutations act
on native rows. Subscriptions ride the same live-query kernels that already power Cynos elsewhere.
Nested payloads are rendered by a batched engine in Rust/WASM rather than by a separate resolver
tree.

In Cynos, GraphQL is treated as a native query surface of the engine itself.

## Integration model

There are many ways to combine GraphQL with a database. Most of them treat GraphQL as a layer that
sits above the engine:

- schema is maintained separately from database metadata
- root fields are translated into another query language
- nested relations are assembled field-by-field
- live behavior is implemented by a GraphQL-specific runtime

`cynos-gql` takes a more integrated direction.

It compiles GraphQL into Cynos's own execution model:

- schema is derived from tables, columns, and foreign keys
- root `query` and `subscription` fields lower into `cynos-query`
- mutation roots execute through native row mutation paths
- nested relation payloads are materialized through a compiled batched renderer
- live subscriptions are driven by Cynos live-query kernels

This can be viewed as a more tightly integrated GraphQL/database model. In practice, GraphQL lives
much closer to the engine, so it can reuse planning, indexing, mutation, and live-query
infrastructure directly.

## Key properties

- **Schema-native**
  - GraphQL types and root fields come directly from the current `TableCache`.
- **Planner-native**
  - root GraphQL reads compile into the same planner/executor stack used by the rest of Cynos.
- **Live-native**
  - GraphQL subscriptions are adapters over Cynos live-query kernels, not a parallel runtime.
- **Render-native**
  - nested GraphQL payloads are rendered through batched relation fetching and invalidation-aware
    caches.
- **WASM-friendly**
  - the full GraphQL pipeline stays in Rust/WASM close to the data and execution hot paths.

## High-level architecture

At a high level, `cynos-gql` provides:

- schema generation from a `TableCache`
- GraphQL SDL rendering
- a native parser and AST
- variable resolution and binding against a `GraphqlCatalog`
- planner lowering for root collection / by-pk / subscription fields
- native execution for queries and mutations
- batched nested relation rendering
- stateful invalidation for live GraphQL payloads

The public exports in `crates/gql/src/lib.rs` include:

- `GraphqlSchema`, `render_schema_sdl`
- `GraphqlCatalog`
- `PreparedQuery`
- `execute_query`, `execute_operation`
- `compile_batch_plan`, `GraphqlBatchPlan`
- `GraphqlBatchState`, `GraphqlInvalidation`

## Native compilation pipeline

The GraphQL pipeline in Cynos is compiler-shaped from end to end:

```text
TableCache / schema metadata
  -> GraphqlSchema / GraphqlCatalog
      -> parser.rs        : GraphQL text -> AST
      -> bind.rs          : AST + variables -> BoundOperation
      -> plan.rs          : root fields -> cynos-query logical plan
      -> execute.rs       : query/mutation execution
      -> render_plan.rs   : nested selection -> GraphqlBatchPlan
      -> batch_render.rs  : batched payload materialization
      -> reactive bridge  : live invalidation on top of Cynos live kernels
```

Two properties are worth noting:

- there is no SQL text generation stage in the middle
- nested rendering is compiled too, not left to generic resolver callbacks

## Schema model

`cynos-gql` derives GraphQL schema objects directly from the current table cache.

For each table, it generates:

- a collection root field, such as `users`
- a primary-key root field, such as `usersByPk`, when the table has a primary key
- mutation root fields:
  - `insertUsers`
  - `updateUsers`
  - `deleteUsers`
- a subscription root field with the same collection surface as the query root
- object fields for columns
- forward relation fields from foreign keys
- reverse relation fields from foreign keys
- typed filter/order/insert/patch/pk input objects

This keeps the GraphQL surface closely aligned with the underlying database schema.

### Relation model

Relations come from foreign-key metadata.

- forward relation fields point from child -> parent
- reverse relation fields point from parent -> children
- custom GraphQL relation names can be derived from FK metadata

Nested queries are therefore understood structurally by the compiler instead of being delegated to
ad hoc field resolvers.

Example:

```graphql
query {
  posts {
    id
    title
    author {
      id
      name
    }
  }
}
```

and:

```graphql
query {
  users {
    id
    name
    posts(orderBy: [{ field: ID, direction: DESC }], limit: 5) {
      id
      title
    }
  }
}
```

are both compiled relation traversals over native schema metadata.

## Query semantics

GraphQL `query` is treated as a first-class one-shot read surface.

Important properties:

- root fields are bound against the generated GraphQL catalog
- collection / by-pk roots lower into `cynos-query` plans
- `where` / `orderBy` / `limit` / `offset` are pushed into planner-backed execution
- nested relation payloads are rendered through a compiled `GraphqlBatchPlan`

In practice, GraphQL reads inherit the same kinds of engine capabilities that matter elsewhere in
Cynos:

- predicate pushdown
- index-aware execution
- order-by pushdown when possible
- limit/offset pushdown

This means the GraphQL syntax is not only a convenience wrapper; it also maps onto the planner as a
native query surface.

## Mutation semantics

GraphQL `mutation` is also mapped onto native engine behavior rather than being treated as a special
transport layer.

Mutation roots execute as real row mutations and return payloads selected from the affected rows.
Those payloads use the same nested selection semantics and the same batched rendering engine as
queries.

Example:

```graphql
mutation CreateOrder {
  insertOrders(input: [{ id: 13, user_id: 2, total: 42 }]) {
    id
    total
    buyer {
      id
      name
    }
  }
}
```

This keeps GraphQL mutation behavior close to the database instead of splitting "write semantics"
and "response shaping" into separate layers.

## Subscription semantics

GraphQL `subscription` is treated as a live query surface.

This is a core part of the crate's design.

In Cynos:

- subscription roots compile against the same root-field planner machinery as queries
- the lower runtime chooses the appropriate live backend
- GraphQL payload rendering sits above that backend as an adapter layer

Today a GraphQL subscription:

- must select exactly one concrete root field
- may use the same root collection surface as the query root
- may contain nested forward and reverse relations
- may be driven by either snapshot/requery or delta/IVM backend depending on query shape

Example:

```graphql
subscription WatchUsers {
  users(orderBy: [{ field: ID, direction: ASC }]) {
    id
    name
    posts(orderBy: [{ field: ID, direction: DESC }], limit: 3) {
      id
      title
    }
  }
}
```

This keeps GraphQL live behavior aligned with Cynos's broader live-query model instead of creating a
separate GraphQL-only execution path.

## Batched nested rendering

One practical characteristic of `cynos-gql` is that nested payload assembly is compiled and batched.

Instead of the classic field-by-field pattern:

- visit parent row
- resolve one relation
- visit next parent row
- resolve the same relation again

the renderer works set-wise:

- compile the nested selection into a `GraphqlBatchPlan`
- collect relation keys for a whole parent frontier
- fetch missing relation buckets in batches
- cache rendered row objects and relation buckets
- invalidate only the affected cached subtrees for live subscriptions

This affects several paths:

- one-shot queries avoid in-memory N+1 work
- mutation payloads avoid repeated relation fetches
- live subscriptions can preserve and invalidate render state incrementally

The current production implementation uses the batched renderer everywhere:

- one-shot query payloads
- mutation payloads
- live snapshot payloads
- live delta payloads

The current batched rendering and live adapter architecture is documented in
`crates/gql/batched-rendering-design.md`.

## Supported GraphQL surface

The implementation intentionally tracks a large and practical subset of GraphQL.

### Operations

- `query`
- `mutation`
- `subscription`

### Field features

- nested selections
- aliases
- variables
- operation names
- default variable values
- `__typename`

### Directives

Currently supported field directives:

- `@include(if: ...)`
- `@skip(if: ...)`

These are resolved during binding, so pruned fields do not enter the execution or render plan.

### Root collection arguments

Collection roots and reverse relation collections support:

- `where`
- `orderBy`
- `limit`
- `offset`

### Mutation shapes

- `insert<Type>(input: ...)`
- `update<Type>(set: ..., where: ..., orderBy: ..., limit: ..., offset: ...)`
- `delete<Type>(where: ..., orderBy: ..., limit: ..., offset: ...)`

### Filter surface

Scalar filters currently include combinations of:

- `eq`, `ne`
- `in`, `notIn`
- `gt`, `gte`
- `lt`, `lte`
- `between`
- `like`
- `isNull`
- logical `AND` / `OR`

The exact operator set depends on the underlying scalar type.

## JSON / JSONB support

JSON columns are mapped to the `JSON` scalar and support native filter shapes including:

- `path`
- `eq`
- `contains`
- `exists`
- `isNull`

Example:

```graphql
query TechPosts {
  posts(
    where: {
      metadata: {
        path: "$.category"
        eq: "tech"
      }
    }
  ) {
    id
    title
  }
}
```

Another example:

```graphql
query TaggedPosts {
  posts(
    where: {
      metadata: {
        path: "$.tags"
        contains: "db"
      }
    }
  ) {
    id
    title
  }
}
```

This allows GraphQL filters to reach native JSONB behavior through the same planner/executor stack
rather than falling back to opaque string handling.

## Integration implications

From an architectural perspective, `cynos-gql` shows one possible way to integrate GraphQL with a
database engine more tightly.

It suggests that GraphQL can be:

- schema-derived rather than manually mirrored
- planner-backed rather than translated at arm's length
- mutation-aware at the engine boundary
- live-query-aware without inventing a separate runtime
- handled efficiently for nested payloads because rendering is compiled and batched

Within Cynos, this crate is part of a broader attempt to keep query surface, live delivery, and
execution model closely connected inside one embedded engine.

## Prepared operations

`PreparedQuery` lets callers parse once and execute or bind repeatedly:

- `PreparedQuery::parse(...)`
- `PreparedQuery::parse_with_operation(...)`
- `PreparedQuery::bind(...)`
- `PreparedQuery::execute(...)`
- `PreparedQuery::execute_mut(...)`

This is useful when:

- the same GraphQL document is reused many times
- variables change between executions
- callers want explicit binding before handing the operation to other layers

## Example: schema + execution

At the Rust level, the typical usage shape is:

```rust
use cynos_gql::{GraphqlCatalog, PreparedQuery, render_schema_sdl};
use cynos_storage::TableCache;

fn run(cache: &TableCache) {
    let schema = render_schema_sdl(cache);
    let catalog = GraphqlCatalog::from_table_cache(cache);

    let prepared = PreparedQuery::parse(
        "query GetUsers { \
           users(orderBy: [{ field: ID, direction: ASC }], limit: 10) { \
             id \
             name \
           } \
         }",
    )
    .unwrap();

    let response = prepared.execute(cache, &catalog, None).unwrap();
    let _ = (schema, response);
}
```

In higher-level JS/WASM usage, this crate is typically consumed through `cynos-database` and the
published `@cynos/core` package.

## Current scope boundaries

`cynos-gql` stays close to GraphQL semantics, but its scope is defined by what maps cleanly onto
the native Cynos schema, planner, mutation, and live-query model. Some omissions are therefore
deliberate design choices rather than unplanned gaps.

- fragments are not supported today
  - in the current design context, queries are usually authored close to the database boundary and
    compiled from concrete selections, so fragment support has not been a priority and may remain
    unnecessary
- directives are currently field-only, and only `@include` / `@skip` are supported
  - those two directives map cleanly to bind-time selection pruning; other directive families do
    not currently have an equally natural query primitive to map onto
- full GraphQL introspection is not implemented, though `__typename` is supported
  - in the Cynos model, schema information is already derived from and available through the
    database itself, so full remote-style introspection has lower priority than in a decoupled
    GraphQL server
- subscriptions currently require exactly one concrete root field
  - this is intentional: multi-root subscriptions appear to offer relatively low ROI in the current
    design while adding substantial complexity to planning, invalidation, and live execution

## Related docs

- `crates/gql/batched-rendering-design.md`
- `crates/database/live-runtime-architecture.md`

## License

Apache-2.0
