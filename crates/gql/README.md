# cynos-gql

Engine-native GraphQL schema generation, binding, planning, execution, and live-query integration
for Cynos.

`cynos-gql` turns the current Cynos schema into a GraphQL surface and maps GraphQL operations onto
the same planner, row-mutation, and live-query machinery that the engine already uses. In Cynos,
GraphQL is not treated as a separate service tier over the database; it is one of the native query
surfaces of the engine itself.

That distinction is the main thing to notice when reading this crate.

## Integration boundary

Hasura and PostGraphile are useful comparison points because they also expose GraphQL from database
structure. The important difference in Cynos is not simply that it "supports GraphQL", but where
GraphQL is attached to the stack.

In a typical generated-GraphQL system, the shape is roughly:

- an existing database
- a GraphQL service or execution layer over that database
- translation from GraphQL operations into lower-level database work

In Cynos, the same embedded runtime already owns:

- schema metadata (`TableCache`)
- native root planning (`cynos-query`)
- row mutations
- snapshot and delta live kernels
- GraphQL binding and response shaping

So `cynos-gql` is better understood as the part of Cynos that maps GraphQL semantics onto
engine-native planning, execution, and live-query primitives.

This is a statement about architecture, not a blanket claim that Cynos is always faster than
Hasura, PostGraphile, or any other system. Real performance still depends on workload, schema
shape, indexing, deployment model, and application behavior.

## What the crate does

At a high level, `cynos-gql` provides:

- schema generation from database metadata
- SDL rendering
- a native parser and AST
- variable binding and semantic validation against a `GraphqlCatalog`
- planner lowering for root query and subscription reads
- execution for queries and mutations
- live GraphQL subscription integration on top of Cynos live runtime
- batched nested rendering for live GraphQL payloads

The schema is derived directly from the database model. There is no parallel hand-maintained
GraphQL schema layer inside the engine.

## Schema model

`cynos-gql` derives GraphQL types and fields from the current table cache.

For each table it generates:

- collection roots such as `users`
- primary-key roots such as `usersByPk` when a primary key exists
- mutation roots such as `insertUsers`, `updateUsers`, and `deleteUsers`
- subscription roots on the same collection surface as query roots
- object fields for columns
- forward and reverse relation fields from foreign keys
- typed input objects for filtering, ordering, inserts, patches, and primary-key lookups

This keeps the GraphQL surface tightly aligned with the actual database schema that Cynos is
running.

## Execution model

The GraphQL pipeline in Cynos is compiler-shaped:

```text
TableCache / schema metadata
  -> GraphqlSchema / GraphqlCatalog
      -> parser.rs        : GraphQL text -> AST
      -> bind.rs          : AST + variables -> BoundOperation
      -> plan.rs          : root query/subscription fields -> LogicalPlan
      -> execute.rs       : query / mutation execution
      -> reactive_bridge  : GraphQL live subscriptions on top of live runtime
```

Two distinctions matter:

- root GraphQL reads lower into the native query planner instead of being rewritten into SQL text
- GraphQL subscriptions are integrated into the live runtime instead of being implemented as an
  independent GraphQL-specific execution stack

## Queries and mutations

GraphQL `query` and `mutation` map onto native Cynos operations.

### Query roots

Collection and by-primary-key roots lower into `cynos-query` plans. That means root-level GraphQL
reads can reuse the same planner behavior that Cynos uses elsewhere, including:

- predicate pushdown
- index-aware planning
- order-by pushdown when possible
- limit/offset pushdown

### Mutation roots

Mutation roots execute through native row operations and then shape the affected rows back into
GraphQL responses.

The GraphQL layer therefore controls request and response semantics, while the actual mutation work
still happens through Cynos's own data model and execution path.

## Live runtime integration

One of the more distinctive parts of the current implementation is how GraphQL subscriptions relate
to the rest of the live system.

GraphQL subscriptions do not introduce a third, GraphQL-only live kernel. Instead:

- all live surfaces register through the same live runtime control plane
- `observe()` and `changes()` remain snapshot / re-query based
- `trace()` remains delta / IVM based
- GraphQL subscriptions choose between those same lower backend families per query shape

So the live runtime is unified at the subscription-control level, while still preserving the two
existing lower execution styles already present in Cynos.

### Backend selection

GraphQL subscriptions are compiled one root field at a time. The runtime then selects the lower
backend based on the shape of that root field:

- delta / IVM is used only for delta-capable subscription shapes
- otherwise the subscription falls back to the snapshot / re-query path

This keeps the business-facing GraphQL subscription surface stable while allowing the engine to use
the lower live kernel that best matches the query.

### Relation to `observe()`, `changes()`, and `trace()`

It is useful to be precise here:

- GraphQL subscriptions and row-oriented live APIs share the same live runtime control plane
- GraphQL delta subscriptions reuse the same lower delta kernel family as `trace()`
- GraphQL snapshot subscriptions reuse the same lower snapshot kernel family as `observe()` and
  `changes()`

GraphQL subscriptions still emit GraphQL response snapshots rather than row-delta streams. So
GraphQL does not expose `trace()` directly, but it does reuse the same lower incremental machinery
when the query shape allows it.

## Nested relations and N+1 behavior

Nested relations are one of the places where Cynos GraphQL differs structurally from
resolver-per-field GraphQL stacks.

The current live GraphQL path contains a dedicated batched rendering layer:

- `GraphqlBatchPlan`
- `GraphqlBatchState`
- `GraphqlInvalidation`

That layer batches nested relation fetches and invalidation work for live GraphQL payload
materialization. In practice, the live path can:

- collect relation keys across a parent frontier
- fetch relation buckets in batches
- cache rendered nested payloads
- invalidate only the affected cached subtrees when dependency tables change

The dedicated batched renderer is currently concentrated in the live subscription path. Its purpose
is to avoid the classic resolver-per-parent-row N+1 shape during repeated live materialization.

This does not by itself prove a universal performance advantage over other systems. What it does
mean is that Cynos's live GraphQL path is explicitly designed to keep nested relation work
set-oriented and close to the engine, rather than resolving nested fields independently per parent
row.

## Supported GraphQL surface

The implemented surface is intentionally a practical subset of GraphQL.

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

Currently supported:

- `@include(if: ...)`
- `@skip(if: ...)`

These are resolved during binding, so pruned fields do not enter later planning or execution.

### Collection arguments

Collection roots and reverse relation collections support:

- `where`
- `orderBy`
- `limit`
- `offset`

### JSON / JSONB filters

JSON columns map to the `JSON` scalar and currently support filter shapes including:

- `path`
- `eq`
- `contains`
- `exists`
- `isNull`

## Deliberate scope boundaries

In a generic GraphQL server README, the following items might be listed simply as "missing
features". In Cynos, they are better understood as deliberate scope boundaries chosen to keep the
mapping between GraphQL semantics and engine primitives tight.

### Fragments

Fragments are not currently supported.

In Cynos's current usage model, GraphQL operations are often authored close to the database
boundary, frequently as prepared operations with concrete selections. In that context, fragments
have had lower priority than planner lowering, live runtime integration, and nested relation
materialization.

This is a real surface limitation, but it is not currently the main bottleneck for the kind of
embedded, engine-adjacent GraphQL usage Cynos is targeting.

### Full introspection

Full GraphQL introspection is not currently implemented, though `__typename` is supported.

For Cynos, this is also partly a product-context choice. The engine already owns the schema
metadata, derives the GraphQL catalog from it, and can render SDL directly via `graphqlSchema()`.
In an embedded setting where the application owns the database instance, that is often more useful
than remote-server-style introspection.

The trade-off is still real: some GraphQL tooling assumes full introspection support, and Cynos
does not currently center that workflow.

### Multi-root subscriptions

GraphQL subscriptions currently require exactly one concrete root field.

This is an intentional constraint. Multi-root subscriptions have relatively low payoff in the
current Cynos workload model, while significantly increasing complexity in:

- dependency tracking
- backend selection
- invalidation
- live result materialization

Given that trade-off, Cynos keeps subscriptions single-root and focuses on making that path map
cleanly onto the underlying live runtime.

### Broader directive surface

Only `@include` and `@skip` are currently supported.

These two directives have a direct and useful mapping to bind-time selection pruning. Other
directive families do not currently map as naturally onto Cynos's planner and live-query model, so
they have not been added merely for GraphQL surface completeness.

## How to read the comparison with Hasura and PostGraphile

The comparison point is not that Cynos is the only system that can derive GraphQL from relational
schema, or that other systems cannot optimize execution. The comparison point is narrower:

- Cynos places GraphQL inside the embedded engine runtime
- the same runtime owns schema metadata, planning, mutations, live kernels, and GraphQL adaptation
- GraphQL subscriptions reuse the same lower live runtime families as the row-oriented APIs

That integration boundary is what makes Cynos GraphQL feel different in practice.

## Prepared operations

`PreparedQuery` allows callers to parse once and reuse a document across executions:

- `PreparedQuery::parse(...)`
- `PreparedQuery::parse_with_operation(...)`
- `PreparedQuery::bind(...)`
- `PreparedQuery::execute(...)`
- `PreparedQuery::execute_mut(...)`

At the JS/WASM boundary, the related surface is `prepareGraphql(...)`.

## Related docs

- `crates/gql/live-batching-design.md`
- `crates/database/live-runtime-unification.md`

## License

Apache-2.0
