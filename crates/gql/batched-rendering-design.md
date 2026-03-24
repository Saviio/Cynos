# GraphQL Batched Rendering and Live Adapter Design

Status: implemented (current architecture)  
Owner: `cynos-gql` / `cynos-database`  
Primary scope: `crates/gql`, `crates/database`

## 1. Why this document exists

The original version of this document described a proposed optimization for GraphQL live
subscriptions. That is no longer accurate.

The current implementation has already moved to a stronger shape:

- batched GraphQL payload rendering is the production path
- the same batched renderer is used by one-shot query payloads
- the same batched renderer is used by mutation payloads
- the same batched renderer is used by GraphQL live snapshot payloads
- the same batched renderer is used by GraphQL live delta payloads

In other words, batching is no longer a live-only optimization. It is now the canonical GraphQL
payload materialization layer.

This document describes that implemented architecture.

## 2. Architectural boundary

The key boundary remains the same as the one agreed for Cynos overall:

- `query` and `subscription` are not semantically unified
- GraphQL remains an adapter/compiler layer above the database runtime
- live capability still comes from lower live-query kernels
- the GraphQL layer only unifies payload planning, relation fetching, rendering, and invalidation

What is unified:

- GraphQL selection lowering into a batched render plan
- GraphQL relation fetch orchestration
- GraphQL object/list materialization
- GraphQL cache invalidation for nested payload state

What is intentionally not unified:

- one-shot execution semantics vs live execution semantics
- snapshot live kernel vs delta/IVM live kernel
- JS-visible GraphQL result delivery vs SQL row/delta delivery

So the current design should be read as:

- one GraphQL payload/rendering abstraction
- two lower live kernels
- one GraphQL adapter layer that can sit on top of either kernel

For the broader runtime/control-plane direction, see `crates/database/live-runtime-unification.md`.

## 3. Current end-to-end shape

### 3.1 One-shot query and mutation payloads

Production one-shot rendering now goes through:

- bind GraphQL into `BoundRootField`
- compile a `GraphqlBatchPlan`
- render through `render_root_field_batched_stateless()`

This path is entered from `crates/gql/src/execute.rs` via `render_root_field_rows()`.

Important consequence:

- there is no longer a production recursive renderer for one-shot GraphQL row payloads

Queries and mutation result payloads both use the same batched renderer. The only difference is
how the root rows are obtained.

### 3.2 Live snapshot subscriptions

`Database::subscribe_graphql()` in `crates/database/src/database.rs` compiles:

- the planner-backed root field plan
- the immutable `GraphqlBatchPlan`
- the dependency table bindings used by the live runtime

If GraphQL delta execution is not selected, the runtime materializes a
`GraphqlSubscriptionObservable`:

- root rows are maintained by the snapshot/requery kernel
- the GraphQL payload is materialized by the batched renderer
- nested relation payload state is preserved in `GraphqlBatchState`
- nested changes invalidate only the relevant cached render state when possible

### 3.3 Live delta subscriptions

When the root field is delta-capable and the physical plan can be compiled to dataflow, the same
subscription compilation step chooses a delta live plan instead.

That path materializes a `GraphqlDeltaObservable`:

- root rows are maintained by `MaterializedView`
- GraphQL payload rendering still uses the same `GraphqlBatchPlan`
- nested invalidation still uses the same `GraphqlBatchState`
- only the root-row maintenance kernel changes

This is the intended architecture boundary:

- lower layer chooses snapshot vs delta
- GraphQL payload assembly stays on one shared path above that choice

## 4. Production rendering surfaces

The current renderer exposes two surfaces.

### 4.1 Stateless batched render

Used by one-shot query and mutation payload materialization:

- `render_root_field_batched_stateless()`
- internally creates a fresh `GraphqlBatchState`
- uses batching and deduplication within the call
- does not retain caches after the call returns

This gives one-shot operations the same set-oriented relation fetching benefits without introducing
request-to-request state.

### 4.2 Stateful batched render

Used by live GraphQL observables:

- `render_graphql_response()`
- `render_root_field_batched()`

This surface receives a mutable `GraphqlBatchState`, allowing the live runtime to preserve:

- rendered row objects
- relation bucket caches
- parent/child membership information
- invalidation metadata across flushes

That preserved state is what makes incremental nested re-rendering possible.

## 5. Core compiled artifact: `GraphqlBatchPlan`

`crates/gql/src/render_plan.rs` defines the immutable plan used by every production GraphQL render.

At a high level, the plan contains:

- a root render node
- a render-node graph for nested selections
- relation edges between render nodes
- lookup tables keyed by table name for invalidation
- incoming-edge lookup for upward invalidation walks

### 5.1 Render nodes

Each `RenderNodePlan` represents one selected table/object shape:

- `table_name`
- ordered `fields`
- `dependency_tables`

Each field is compiled into a `RenderFieldKind`:

- `Typename`
- `Column`
- `ForwardRelation`
- `ReverseRelation`

This means field-shape decisions are made once at bind/plan time, not during each render.

### 5.2 Relation edges

Each `RelationEdgePlan` describes one GraphQL relation traversal:

- parent node id
- child node id
- forward vs reverse direction
- relation metadata
- optional nested collection query
- chosen fetch strategy
- `direct_table`

`direct_table` is especially important for live invalidation. It records the table whose row/key
changes can directly dirty the edge cache.

### 5.3 Plan-level lookup tables

The plan also precomputes:

- `table_node_lookup`: which render nodes materialize rows from a table
- `table_edge_lookup`: which relation edges are directly sourced by a table
- `incoming_edges`: which edges point into a node

These lookups are used heavily by `GraphqlBatchState::apply_invalidation()`.

### 5.4 Relation strategy selection

Compile-time strategy selection is intentionally simple and predictable.

Forward relations:

- use `IndexedProbeBatch` when the relation targets a single-column primary key
- otherwise use `PlannerBatch`

Reverse relations:

- use `IndexedProbeBatch` only for the simple case with no nested filter, no nested ordering, no
  nested limit, and no nested offset
- otherwise use `PlannerBatch`

`ScanAndBucket` exists as a correctness-preserving fallback strategy inside the renderer. The
important production property is:

- even when a relation fetch falls back, it still stays inside the batched renderer
- there is no fallback from batched rendering back to the old recursive payload path

## 6. Render-time state: `GraphqlBatchState`

`crates/gql/src/batch_render.rs` stores all reusable render-time state in `GraphqlBatchState`.

The important caches are:

- `row_cache`
  - memoized rendered objects keyed by `(node_id, row_id, row_version)`
- `row_sources`
  - source rows for cached render entries
- `row_dependencies`
  - which relation-edge keys a rendered row depends on
- `node_row_index`
  - fast lookup from `(node_id, row_id)` to cached row entries
- `edge_bucket_cache`
  - cached relation buckets keyed by `(edge_id, relation_key)`
- `edge_parent_membership`
  - reverse membership from relation buckets back to parent rendered rows

This state is per render invocation in stateless mode, and per subscription in live mode. There is
currently no cross-subscription/global GraphQL render cache.

## 7. Batched rendering model

The renderer is still recursive in shape, but no longer row-by-row in data access.

The execution pattern is:

1. take a frontier of parent rows for one render node
2. collect all relation keys for each outgoing edge
3. fetch only the missing buckets for that edge
4. store the fetched rows in `edge_bucket_cache`
5. render row objects using cached buckets
6. recurse into child nodes after the edge buckets for the current frontier are ready

The critical operation is `prefetch_node_edges()`:

- it inspects all relation fields on the current node
- gathers keys across the entire row frontier
- identifies keys that are not already cached
- fetches the missing relation buckets in one batched step per edge

This removes the old in-memory N+1 shape where each parent row resolved its nested relations
independently.

## 8. Relation fetch strategies

The renderer currently supports three relation fetch strategies.

### 8.1 `PlannerBatch`

This is the most semantically complete strategy.

It works by:

- building one relation-key `IN (...)` predicate for the whole frontier
- combining that predicate with any nested reverse-relation filter
- lowering the combined query through the existing planner
- executing the logical plan once
- bucketing the result rows by relation key

For reverse relations, the renderer intentionally clears per-parent `limit` and `offset` before the
batched planner query is executed, and then re-applies the per-parent window after bucketing. This
preserves GraphQL semantics while still batching the fetch.

This is the strategy used for:

- nested filters
- nested ordering
- nested pagination
- any forward relation that is not a trivial single-column PK lookup

### 8.2 `IndexedProbeBatch`

This is the fast path for simple relation traversals.

It works by:

- probing the target PK/index for each distinct relation key
- collecting the matched rows into buckets
- applying `apply_collection_query()` for simple reverse collections when needed

For reverse relations, this strategy is only chosen when the nested query is structurally simple,
so it never has to emulate semantics it cannot preserve.

### 8.3 `ScanAndBucket`

This is the correctness fallback:

- scan the target table
- keep rows whose relation key is in the current key set
- bucket them by relation key
- optionally apply the nested collection query

It is not the preferred path, but it ensures the batched renderer remains total even when planner
or index assumptions do not hold.

## 9. Directives and selection pruning

The currently implemented GraphQL directives are:

- `@include(if: ...)`
- `@skip(if: ...)`

These are handled during binding in `crates/gql/src/bind.rs`, before the batch plan is compiled.

That has two important consequences:

- directive-pruned fields do not appear in `GraphqlBatchPlan`
- directive-pruned relations do not participate in render-time invalidation

So the batch plan already represents the post-directive selection tree.

## 10. Live invalidation model

The live runtime talks to the GraphQL renderer through `GraphqlInvalidation`:

- `root_changed`
- `changed_tables`
- `dirty_edge_keys`
- `dirty_table_rows`

This struct is the contract between lower live-query change tracking and upper GraphQL render-state
invalidation.

### 10.1 Snapshot-backed invalidation

`GraphqlSubscriptionObservable` uses the snapshot/requery kernel for root rows.

On change:

- root tables may be updated via reactive patch or full re-query
- nested-only table changes do not force root-row recomputation
- the runtime builds a `GraphqlInvalidation` from changed table ids and row ids
- `GraphqlBatchState::apply_invalidation()` drops only the render cache entries affected by the
  change set when it can identify them

Snapshot invalidation typically has less precise edge-key information than the delta path, but it
still reuses the same render-state invalidation mechanism.

### 10.2 Delta-backed invalidation

`GraphqlDeltaObservable` uses `MaterializedView` for root rows and additionally derives relation-key
invalidations from the incoming row deltas.

On change:

- the live runtime extracts changed row ids
- it also extracts dirty relation keys for all edges directly sourced by the changed table
- those dirty keys are recorded in `dirty_edge_keys`
- `MaterializedView` updates the root result
- the same `GraphqlBatchState::apply_invalidation()` removes only the affected cached buckets and
  row objects

This makes nested relation updates much more targeted than a full cache reset, especially for
multi-level subscriptions.

### 10.3 Upward invalidation across nested relations

The important property of `GraphqlBatchState::apply_invalidation()` is that invalidation does not
stop at the modified child bucket.

Because the state keeps:

- row source rows
- parent membership by edge/key
- incoming edges per node

the invalidation walk can:

- remove the dirty child bucket
- find cached parent rows that depended on that bucket
- remove those parent row render entries
- continue walking upward through incoming relation edges

That is how nested subscriptions such as:

```graphql
subscription WatchUsersWithPosts {
  users {
    id
    posts {
      id
      comments {
        id
      }
    }
  }
}
```

can invalidate the affected cached subtrees without resetting the entire GraphQL render state.

## 11. Live backend selection

GraphQL subscription compilation in `crates/database/src/database.rs` follows this rule:

1. always compile the planner-backed root field plan
2. always compile the immutable `GraphqlBatchPlan`
3. attempt delta/IVM only when the root field is delta-capable
4. attempt delta/IVM only when the physical plan can be compiled to dataflow
5. otherwise fall back to snapshot live

So there is one GraphQL payload/rendering stack, but backend selection still happens at live-plan
compile time.

This preserves the intended layering:

- backend selection is a database/runtime concern
- payload rendering is a GraphQL adapter concern

## 12. What changed relative to the old design

The current implementation is stronger and simpler than the original proposal:

- batching is no longer subscription-only
- production one-shot GraphQL rendering also uses the batched path
- live GraphQL no longer has an optional batch-plan branch
- `GraphqlSubscriptionObservable` always owns a `GraphqlBatchPlan`
- `GraphqlDeltaObservable` always owns a `GraphqlBatchPlan`
- the old recursive renderer is no longer production code

The only remaining recursive renderer lives behind `#[cfg(test)]` and is kept as a semantic oracle
for tests.

## 13. Testing strategy

The current test strategy is:

- compare batched rendering with the legacy recursive renderer in `crates/gql/src/batch_render.rs`
- test directive pruning and query semantics in `crates/gql/src/query.rs`
- test live snapshot and live delta behavior in `crates/database`

The recursive renderer remains useful precisely because it is no longer on the production path: it
acts as a reference implementation for correctness checks.

## 14. Performance and semantic contracts

The current implementation is built around the following contracts.

### 14.1 Contracts that must hold

- no production regression back to per-row recursive relation fetching
- no new GraphQL-specific live kernel
- no regression to `observe()` / `changes()` hot paths
- no regression to delta/IVM hot paths from adding generic GraphQL branching
- final GraphQL responses preserve existing selection semantics
- live subscribers only emit when the final GraphQL response actually changes

### 14.2 Semantics preserved by the current renderer

- nested forward and reverse relation traversal
- nested filters
- nested ordering
- nested limit/offset for snapshot and one-shot rendering
- directive-pruned selections
- full response snapshots for GraphQL subscriptions

### 14.3 What is not promised by this layer

- JS-visible GraphQL tree deltas
- cross-subscription shared batching
- multi-root GraphQL subscriptions
- making every GraphQL subscription delta-capable

## 15. Current limitations and future work

The current architecture is intentionally strong on the payload/rendering boundary, but several
possible extensions remain outside today’s scope:

- broader GraphQL-on-IVM coverage for more root query shapes
- shared subscription-plan or render-state reuse across identical subscriptions
- a JS-visible GraphQL delta protocol
- additional GraphQL directives beyond `@include` and `@skip`
- higher-level relation synthesis beyond explicit FK-based relation edges

Those are future extensions. They should be added on top of the current split:

- lower layer owns live execution kernels
- upper GraphQL layer owns batched payload materialization

## 16. Summary

The current Cynos GraphQL architecture is:

- one immutable `GraphqlBatchPlan` per root field
- one batched renderer used everywhere in production
- one reusable `GraphqlBatchState` for stateful/live payloads
- one GraphQL invalidation contract shared by snapshot and delta live backends
- two lower live kernels, chosen independently of GraphQL payload assembly

That is the important architectural result:

- GraphQL is now a thin but powerful bridge layer
- batching is a first-class production capability
- live invalidation works through the shared batched render state instead of a separate GraphQL
  runtime model
