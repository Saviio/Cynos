# GraphQL Live Batching Design

Status: active design
Owner: cynos-gql / cynos-database
Scope: `crates/gql`, `crates/database`, `crates/storage`

## 1. Context

Cynos already has one important batching layer in the live runtime:

- table changes are collected in `LiveRegistry`
- pending changes and pending deltas are coalesced before flush
- the same observable is invoked once per flush rather than once per row change

That part lives in `crates/database/src/live_runtime.rs` and is the correct control-plane shape for:

- `observe()`
- `changes()`
- `trace()`
- `subscribeGraphql()`

The remaining performance problem is not event batching. It is GraphQL payload assembly.

Today GraphQL live subscriptions already reuse the existing root-plan machinery:

- root fields are lowered through `crates/gql/src/plan.rs`
- `subscribeGraphql()` chooses `Snapshot` or `Delta` backend in `crates/database/src/database.rs`
- `GraphqlSubscriptionObservable` and `GraphqlDeltaObservable` live on top of the shared runtime in `crates/database/src/reactive_bridge.rs`

However, once root rows are available, GraphQL payload rendering still follows a row-by-row recursive execution shape:

- `render_root_field_rows()`
- `render_row_list()`
- `execute_row_selection()`
- `execute_forward_relation()`
- `execute_reverse_relation()`

That shape is correct, but it is not set-oriented:

- one parent row can trigger one relation lookup
- nested reverse relations can perform repeated index probes
- nested forward relations can repeatedly fetch the same parent row
- if an index is missing, the fallback can degenerate into repeated scans

This is a classic in-memory N+1 shape. It does not create extra network round-trips, but it still creates unnecessary CPU work, allocations, and repeated index traversal.

The long-term architectural direction remains unchanged:

- exactly two live execution kernels
  - `SnapshotKernel`
  - `DeltaKernel`
- one shared runtime control plane
- GraphQL remains an adapter/compiler layer
- GraphQL live should be able to choose `Snapshot` or `Delta` per query shape

This design adds the missing batching layer inside the GraphQL adapter without changing those principles.

## 2. Problem Statement

We need a GraphQL batching mechanism that:

- removes row-by-row nested relation fetching from GraphQL live payload assembly
- preserves the current live runtime abstraction and does not create a fourth runtime lane
- keeps `observe()` / `changes()` performance intact
- keeps `trace()` / DBSP-IVM performance intact
- works for both GraphQL snapshot-backed and delta-backed subscriptions
- supports multi-level nested relations
- preserves existing GraphQL semantics for:
  - nested filters
  - nested ordering
  - nested limit/offset in snapshot mode
  - directive-pruned selections
- allows the GraphQL layer to keep acting as a bridge above live query kernels instead of owning an independent execution model

The most important constraint is:

- batching must live in the GraphQL adapter layer, not in the SQL live kernels

## 3. Goals

### 3.1 Primary goals

- Introduce a set-oriented GraphQL relation rendering path for live subscriptions.
- Eliminate N+1-style relation fetch patterns inside a single subscription render.
- Reuse the existing root query planner path and backend selector.
- Keep GraphQL live output as full payload snapshots for JS consumers.
- Make nested relation invalidation explicit and efficient.

### 3.2 Secondary goals

- Reuse planner/index capabilities for nested GraphQL relation fetches when possible.
- Minimize repeated row-to-GraphQL materialization work with per-subscription caches.
- Make batching behavior explicit and testable instead of implicit recursion.
- Provide a path for future subtree-delta or shared-subscription optimizations.

## 4. Non-Goals

This design does not require the first implementation to deliver all of the following:

- a JS-visible GraphQL delta protocol (`path/op/value`)
- cross-subscription batching across unrelated GraphQL documents
- multi-root GraphQL subscriptions
- fragment execution support
- a global shared live-plan cache for identical GraphQL subscriptions

The first complete implementation may still emit:

- full GraphQL payload snapshots externally
- one subscription-local batching state per GraphQL subscription

## 5. Current Execution Shape

### 5.1 What already works well

Current live GraphQL creation already has the right high-level structure:

- `crates/database/src/database.rs`
  - binds GraphQL documents
  - builds the planner-backed root field plan
  - collects dependency tables
  - selects `Snapshot` vs `Delta`
- `crates/database/src/live_runtime.rs`
  - registers GraphQL observables into the shared live registry
  - keeps one control-plane batching mechanism for all live APIs
- `crates/database/src/reactive_bridge.rs`
  - keeps GraphQL snapshot and delta adapters on top of the shared control plane

This should remain intact.

### 5.2 Where the N+1 shape comes from

The current GraphQL render path is still row-recursive:

- `render_row_list()` walks rows one by one
- `execute_row_selection()` walks fields one by one
- each relation field resolves itself independently

Specifically:

- forward relation execution fetches the target row for one source row at a time
- reverse relation execution fetches child rows for one parent row at a time
- reverse relation filtering, ordering, and pagination are applied after the fetch

The fallback helpers amplify this cost:

- `fetch_rows_by_column()`
- `fetch_rows_by_index_or_scan()`
- `apply_collection_query()`

As a result:

- `posts { author { ... } }` can repeatedly fetch the same author
- `users { posts { ... } }` can repeatedly probe the same foreign-key index
- `users { posts { comments { ... } } }` multiplies this cost by depth

### 5.3 Why the current delta path still needs batching

`GraphqlDeltaObservable` already benefits from incremental maintenance of root rows. That is valuable and should not be regressed.

But today the delta path still does this after root-row maintenance:

- mark payload dirty
- re-render the GraphQL payload tree from current rows

So the current delta advantage is mostly:

- better root-row maintenance

not yet:

- better nested payload maintenance

This design addresses the missing nested layer.

## 6. Design Principles

1. Keep the two-kernel architecture intact
   - `SnapshotKernel` remains cached-plan/requery/patch
   - `DeltaKernel` remains DBSP-style dataflow/IVM

2. Put batching in the GraphQL adapter
   - no GraphQL-specific branching in SQL hot loops
   - no GraphQL-specific state inside `ObservableQuery`

3. Batch by relation edge and by frontier
   - resolve one edge for many parents at once
   - move level-by-level through the selection tree

4. Planner first, storage fallback second
   - use planner-backed nested fetches when semantics allow
   - fall back to storage/index probing only when necessary

5. Preserve API semantics
   - same JS-facing payload shape
   - same subscription lifecycle
   - notify only when the final payload actually changes

6. Keep GraphQL bridge-only
   - GraphQL compiles plans
   - GraphQL renders payloads
   - live kernels remain owned by lower layers

## 7. Proposed Architecture

The new batching layer introduces three main pieces.

### 7.1 `GraphqlBatchPlan`

A compiled, immutable description of how to render a bound GraphQL root field in a set-oriented way.

Responsibilities:

- describe the selection tree as render nodes and relation edges
- precompute relation fetch strategy hints
- precompute dependency edges by table
- define cache and invalidation boundaries

Location:

- new module in `crates/gql`, for example `crates/gql/src/render_plan.rs`

### 7.2 `GraphqlBatchRenderer`

A runtime renderer that takes:

- root rows
- a `GraphqlBatchPlan`
- subscription-local state
- invalidation information

and produces:

- a full `GraphqlResponse`

Responsibilities:

- perform relation fetches in batches
- bucket results by relation key
- reuse cached buckets and cached row materializations
- walk nested selections level-by-level instead of row-by-row

Location:

- new module in `crates/gql`, for example `crates/gql/src/batch_render.rs`

### 7.3 `GraphqlBatchState`

A mutable, subscription-local state object held by `GraphqlSubscriptionObservable` and `GraphqlDeltaObservable`.

Responsibilities:

- cache rendered node values
- cache relation buckets
- remember parent membership for buckets
- track dirty edges and dirty keys
- retain the previous rendered response for equality checks

Location:

- new module in `crates/gql` or `crates/database`, depending on whether ownership remains purely GraphQL-side
- preferred: state type lives in `crates/gql`, adapter holds it from `crates/database`

## 8. Data Structures

### 8.1 Batch plan

The batch plan should represent the bound selection tree explicitly.

```rust
pub struct GraphqlBatchPlan {
    pub root_node: NodeId,
    pub nodes: Vec<RenderNodePlan>,
    pub edges: Vec<RelationEdgePlan>,
    pub table_to_edges: HashMap<TableId, SmallVec<[EdgeId; 4]>>,
}

pub struct RenderNodePlan {
    pub id: NodeId,
    pub table_name: String,
    pub scalar_fields: Vec<ScalarFieldPlan>,
    pub relation_edges: Vec<EdgeId>,
}

pub struct RelationEdgePlan {
    pub id: EdgeId,
    pub parent_node: NodeId,
    pub child_node: NodeId,
    pub relation: RelationMeta,
    pub query: Option<BoundCollectionQuery>,
    pub strategy: RelationFetchStrategy,
    pub cardinality: RelationCardinality,
}
```

This plan is compiled once at subscription creation time from:

- `GraphqlCatalog`
- `BoundRootField`
- `BoundSelectionSet`
- table-id metadata from `cynos-database`

### 8.2 Fetch strategies

```rust
pub enum RelationFetchStrategy {
    PlannerBatch,
    IndexedProbeBatch,
    ScanAndBucket,
}
```

Meaning:

- `PlannerBatch`
  - build one planner-backed fetch over a set of relation keys
- `IndexedProbeBatch`
  - deduplicate relation keys and probe by unique key
- `ScanAndBucket`
  - one scan filtered by a key set, then bucketize

### 8.3 Batch state

```rust
pub struct GraphqlBatchState {
    pub response: Option<GraphqlResponse>,
    pub row_value_cache: HashMap<(NodeId, u64, u64), ResponseValue>,
    pub edge_bucket_cache: HashMap<(EdgeId, RelationKey), BucketRows>,
    pub edge_parent_membership: HashMap<(EdgeId, RelationKey), SmallVec<[ParentSlot; 4]>>,
    pub dirty_edges: HashSet<EdgeId>,
    pub dirty_keys: HashMap<EdgeId, HashSet<RelationKey>>,
}
```

Key ideas:

- `row_value_cache`
  - key includes row id and row version
  - repeated references to the same row can reuse the same rendered scalar object
- `edge_bucket_cache`
  - one bucket per relation key per edge
  - forward relation buckets hold at most one row
  - reverse relation buckets hold lists of rows
- `edge_parent_membership`
  - maps one edge key to the parent slots currently depending on it
  - lets invalidation target only affected subtrees

### 8.4 Invalidation envelope

The GraphQL adapter should consume a normalized invalidation description instead of ad hoc booleans.

```rust
pub struct GraphqlInvalidation {
    pub root_changed: bool,
    pub changed_tables: SmallVec<[TableId; 4]>,
    pub dirty_edges: SmallVec<[EdgeId; 8]>,
    pub dirty_keys: HashMap<EdgeId, HashSet<RelationKey>>,
}
```

The snapshot and delta adapters can both produce this shape, with different precision levels.

## 9. Compile-Time Strategy Selection

### 9.1 Root plan stays unchanged

The current root path remains the same:

- root field is lowered to a planner-backed logical plan
- GraphQL live still selects snapshot or delta backend in `crates/database/src/database.rs`

This design does not replace the current root-plan path.

### 9.2 Relation-edge strategy rules

Each relation edge in the nested selection tree chooses a fetch strategy once at compile time.

#### `PlannerBatch`

Preferred when:

- the edge query can be expressed as one set-oriented planner query
- the relation filter can be combined with `relation_key IN (...)`
- nested ordering should reuse planner/index selection

Typical use:

- reverse relations with filter and ordering
- forward relations when planner-backed fetch is simpler than repeated probes

#### `IndexedProbeBatch`

Preferred when:

- key lookups are highly selective
- the relation has a suitable single-column index
- per-key probing is cheaper than a broad batch query

Important detail:

- this is still batched by unique keys
- it is not one probe per parent row

#### `ScanAndBucket`

Used when:

- no useful index exists
- planner batching is unavailable
- a scan over the child table plus key filtering is cheaper or simpler than repeated probes

This is a correctness-preserving fallback and should remain available.

### 9.3 Strategy heuristics

The initial selector should be conservative and explicit.

Recommended heuristics:

- forward relation
  - if parent column is the target table primary key or a unique single-column index exists:
    - prefer `IndexedProbeBatch`
  - otherwise:
    - use `PlannerBatch` if supported
    - else `ScanAndBucket`

- reverse relation without `limit/offset`
  - prefer `PlannerBatch`

- reverse relation with `limit/offset`
  - still batch fetch by relation key
  - if planner can apply filter and order but not per-parent limit:
    - fetch filtered and ordered rows once
    - bucketize by relation key
    - apply limit/offset per bucket after bucketization
  - if planner path is unavailable:
    - use `IndexedProbeBatch` or `ScanAndBucket`

This rule preserves semantics while still removing the row-by-row execution shape.

## 10. Runtime Pipeline

### 10.1 Subscription creation

At GraphQL subscription creation time:

1. bind the GraphQL document
2. compile the root field plan as today
3. compile a `GraphqlBatchPlan`
4. create the live plan:
   - `SnapshotKernel` or `DeltaKernel`
5. initialize `GraphqlBatchState`
6. render the initial response through the batch renderer

This changes the adapter payload path, not the kernel selection path.

### 10.2 Snapshot-backed subscription flow

`GraphqlSubscriptionObservable` continues to:

- patch or re-execute root rows on root-table changes
- mark payloads dirty when nested tables change

With batching, the adapter then:

- builds a `GraphqlInvalidation`
- reuses cached buckets for untouched edges
- re-fetches only dirty relation edges
- re-renders only affected node values when possible
- recomputes the final payload
- emits only if the final payload changed

This keeps the current snapshot semantics but removes nested row-by-row relation execution.

### 10.3 Delta-backed subscription flow

`GraphqlDeltaObservable` continues to:

- push deltas into `MaterializedView`
- maintain root rows incrementally

With batching, the adapter additionally:

- converts table deltas into edge/key invalidation
- invalidates only the touched buckets when possible
- re-renders only affected subtrees
- leaves untouched relation buckets and untouched rendered nodes intact

This gives the delta path a second incremental layer:

- incremental root rows
- batched, key-targeted nested rendering

## 11. Batching Algorithm

### 11.1 Frontier-based rendering

The renderer must traverse the selection tree by frontier, not by row recursion.

High-level algorithm:

1. start with the root frontier of root rows
2. render scalar fields for all rows in the frontier
3. group relation work by edge
4. for each edge:
   - collect unique relation keys from all parent rows in the frontier
   - remove already-cached and not-dirty keys
   - fetch missing buckets in one batched operation
   - bucketize results by relation key
5. attach child rows to parent slots
6. build the next frontier from unique child rows
7. repeat until the deepest nested level is processed

Pseudo-code:

```rust
for frontier in frontier_queue {
    render_scalars(frontier.rows, frontier.node_id, state);

    for edge_id in plan.nodes[frontier.node_id].relation_edges.iter().copied() {
        let keys = collect_unique_keys(frontier.rows, edge_id);
        let fetch_keys = subtract_cached_clean_keys(keys, state, invalidation);
        let new_buckets = fetch_many(cache, plan.edge(edge_id), &fetch_keys);
        state.merge_buckets(edge_id, new_buckets);
        let next_rows = attach_children(frontier.rows, edge_id, state);
        frontier_queue.push(next_rows);
    }
}
```

### 11.2 Why this removes N+1

For a query like:

```graphql
subscription {
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

the current shape is approximately:

- one `posts` fetch per `user`
- one `comments` fetch per `post`

The new shape becomes:

- one batched `posts` fetch for all visible `user.id`
- one batched `comments` fetch for all visible `post.id`

The cost grows with:

- number of relation edges
- number of unique relation keys
- number of matched rows

not with:

- number of parent rows multiplied by depth

## 12. Fetch Strategy Details

### 12.1 Planner-backed relation batching

`PlannerBatch` is the preferred path for nested reverse relations because it lets Cynos reuse:

- filter planning
- index selection
- sort planning
- execution artifact reuse

Conceptually, the relation fetch becomes:

- `child.relation_column IN (:keys)`
- plus the original nested GraphQL filter
- plus planner-backed ordering when applicable

That means GraphQL nested relations can continue to benefit from the existing planner instead of reimplementing query logic inside the adapter.

Recommended implementation pieces:

- add a new planner helper in `crates/gql/src/plan.rs`
- for example:
  - `build_relation_batch_plan(...)`
- allow the nested relation query builder to synthesize:
  - a relation-key `IN` predicate
  - combined with the user-specified nested filter

The fetched row stream is then bucketized by relation key.

### 12.2 Indexed probe batching

For very selective keyed lookups, one planner batch may not be the best trade-off.

In those cases:

- deduplicate relation keys first
- probe once per unique key, not once per parent row
- prefer `visit_index_scan_with_options()` from `crates/storage/src/row_store.rs`

Benefits:

- avoids repeated work for repeated keys
- avoids materializing intermediate vectors when the visitor API is enough
- retains storage-level limit/offset/reverse support where useful

### 12.3 Scan-and-bucket fallback

When no useful index exists, the adapter should still avoid row-by-row scans.

Instead:

- build a `HashSet` of relation keys
- scan the child table once
- keep rows whose relation column is in the key set
- bucketize them

This is the correct fallback because it replaces:

- many probes or many scans

with:

- one scan plus bucketization

## 13. Invalidation Model

### 13.1 Snapshot path invalidation

Snapshot-backed GraphQL subscriptions do not always have old/new row values for every nested-table change. Their invalidation is therefore coarser.

Recommended behavior:

- root-table changes
  - keep existing patch-or-requery logic for root rows
- non-root dependency changes
  - map changed tables to dependent relation edges
  - mark those edges dirty
  - keep root rows intact unless the root plan itself changed

This already avoids the worst current behavior:

- full root requery for pure nested relation churn

### 13.2 Delta path invalidation

Delta-backed GraphQL subscriptions receive concrete row deltas. They can therefore invalidate with much higher precision.

For each changed row:

- extract old and new relation keys for affected edges
- dirty both old and new buckets
- invalidate row render cache entries tied to the changed row id/version

Examples:

- reverse relation `users.posts`
  - a post insert/update/delete dirties `author_id` buckets
- forward relation `posts.author`
  - a user update dirties the `id` bucket
  - a post update changing `author_id` dirties both the old and new author buckets

This is what allows the delta path to become truly relation-aware instead of merely root-aware.

### 13.3 Table-to-edge indexing

`GraphqlBatchPlan` should precompute:

- which relation edges depend on which tables

This lets the adapters translate:

- changed table ids

into:

- candidate dirty edges

without walking the whole selection tree on every flush.

## 14. Caching Model

### 14.1 Row render cache

Cache rendered values by:

- node id
- row id
- row version

This is especially important for:

- shared forward relations
- multi-level graphs where the same row appears from multiple parents

### 14.2 Relation bucket cache

Cache the result of one edge for one relation key.

Examples:

- `(posts.author, author_id=2) -> Some(user#2)`
- `(users.posts, user_id=1) -> [post#10, post#11]`

This ensures repeated keys are resolved once per subscription state.

### 14.3 Parent membership cache

Track which parent slots depend on which relation-key buckets.

This is necessary for:

- targeted subtree invalidation
- efficient recomposition after key-local changes

Without it, the renderer would still have to re-walk too much of the tree after every nested update.

## 15. Semantics

### 15.1 External API semantics

This design preserves:

- `subscribeGraphql()` output shape
- `PreparedGraphqlQuery.subscribe()` output shape
- full payload snapshots returned by `get_result()`
- callback emission only when final payload changes

### 15.2 GraphQL query semantics

The batched renderer must preserve the semantics of:

- nested scalar selections
- nested forward and reverse relations
- nested filters
- nested ordering
- nested limit/offset in snapshot mode
- `@include` / `@skip` pruned selections after binding

Directive handling remains naturally compatible because batching happens after binding, when the active selection tree is already known.

### 15.3 Internal Payload Adapter Boundary

The JS-facing GraphQL subscription surface continues to emit full GraphQL response objects.
That external shape is intentionally stable.

Internally, the database bridge routes GraphQL responses through a small output adapter boundary:

- `GraphqlResponsePayloadCache`
  - owns the last semantic `GraphqlResponse`
  - owns the cached JS payload
  - owns the per-subscription encode caches
- `GraphqlOutputAdapter`
  - chooses how the current response should be encoded
  - owns the payload-level change decision
  - currently supports `FullPayload`
- `GraphqlResponseEncoding`
  - distinguishes plain full-payload encoding from batched root-list patch-assisted full-payload encoding

This boundary is deliberately internal. It is not a public GraphQL delta API.

The important architectural rule is:

- GraphQL output mode is selected at the adapter boundary
- snapshot and delta observables do not hand-roll separate payload encoding paths
- future delta-native payload modes must plug into the same adapter boundary
- public `subscribeGraphql()` and `PreparedGraphqlQuery.subscribe()` semantics stay unchanged unless a separate API design explicitly changes them

The likely future internal modes are:

- `FullPayload`
  - current public behavior
  - emits a normal `{ data: ... }` response object
- `FullPayloadWithRootPatch`
  - still public full payload
  - internally reuses root-list JS array/object structure when root positions are stable or splice-compatible
- `DeltaNativeInternal`
  - internal-only representation of GraphQL tree mutations
  - may feed a full-payload adapter, a binary transport, or a future opt-in public API

The last mode should not be exposed directly until the API surface answers:

- path addressing format
- nullability and list splice semantics
- interaction with aliases and directives
- object identity expectations on the JS side
- fallback behavior for unsupported query shapes

### 15.4 Cache Lifecycle Semantics

Batch rendering and response encoding both use long-lived per-subscription caches. These caches are necessary for performance, but they must have explicit lifecycle rules:

- rendered row cache is keyed by `(node_id, row_id, row_version)`
- relation bucket cache is keyed by `(edge_id, relation_key)`
- parent membership tracks which rendered parents depend on which buckets
- root-list cache owns only the current root list
- JS encode caches share one central capacity policy in `crates/database/src/convert.rs`

Current batch-state cache pruning is intentionally lazy:

- normal renders pay only a size check
- when the row cache exceeds the policy ceiling, pruning starts from the current root list
- reachable nested rows are preserved by following registered relation dependencies and bucket contents
- unreachable row render entries are removed
- edge buckets without any parent membership are removed

This gives long-lived subscriptions a bounded degradation path without adding expensive GC work to normal small and medium renders.

### 15.5 Delta capability semantics

This design does not change the current delta eligibility gate in `crates/gql/src/bind.rs`.

That means:

- delta-backed GraphQL remains restricted to query shapes already deemed delta-capable
- batching improves rendering work inside that supported subset
- snapshot batching still handles the broader GraphQL surface

## 16. Module Ownership and Code Placement

Recommended file additions:

- `crates/gql/src/render_plan.rs`
  - compile `BoundRootField` into `GraphqlBatchPlan`
- `crates/gql/src/batch_fetch.rs`
  - planner-backed and storage-backed batched relation fetch helpers
- `crates/gql/src/batch_render.rs`
  - frontier renderer, bucketization, caches, invalidation application

Recommended integration points:

- `crates/gql/src/lib.rs`
  - export the new plan and renderer types
- `crates/database/src/database.rs`
  - compile `GraphqlBatchPlan` alongside the existing root plan
- `crates/database/src/live_runtime.rs`
  - extend GraphQL adapter plans to carry batch-plan metadata
- `crates/database/src/reactive_bridge.rs`
  - replace recursive payload rendering with `GraphqlBatchRenderer`

No changes should be required to:

- `observe()` external API
- `changes()` external API
- `trace()` external API
- `ObservableQuery`
- `MaterializedView`

## 17. Performance Contract

The following contracts must hold after the refactor.

### 17.1 SQL live paths

- `observe()` must not regress beyond noise
- `changes()` must not regress beyond noise
- `trace()` must not regress beyond noise

Reason:

- GraphQL batching must stay entirely outside SQL live hot loops

### 17.2 GraphQL live paths

- nested relation renders should scale with:
  - number of unique relation keys
  - number of matched rows
  - number of dirty edges/keys

not with:

- number of parent rows times depth

### 17.3 Snapshot-backed GraphQL

Pure nested relation churn should no longer imply:

- row-by-row relation re-fetching
- repeated fetches for repeated keys

### 17.4 Delta-backed GraphQL

Delta-backed GraphQL should retain:

- incremental root row maintenance

and additionally gain:

- relation-key-local nested invalidation
- relation bucket reuse

## 18. Testing and Verification

### 18.1 Correctness tests

Add or extend tests for:

- forward relation batching
- reverse relation batching
- multi-level nested relations
- repeated keys on forward relations
- repeated keys on reverse relations
- nested filter correctness
- nested order-by correctness
- nested limit/offset correctness in snapshot mode
- delta invalidation for old/new relation keys

Recommended locations:

- `crates/gql` unit tests for plan compilation and bucketization
- `crates/database/src/database.rs` tests for live subscription correctness
- `js/packages/core/tests/graphql.test.ts` for wasm-facing end-to-end cases

### 18.2 Performance tests

Add dedicated GraphQL batching benchmarks that compare:

- old recursive render shape vs batched render shape
- snapshot-backed nested subscriptions
- delta-backed nested subscriptions
- repeated-key forward relation graphs
- large-fanout reverse relation graphs
- multi-level relation graphs

Recommended benchmark scenarios:

- `users -> posts`
- `posts -> author`
- `users -> posts -> comments`
- many posts sharing the same author
- many comments sharing the same post

### 18.3 Regression suite

The following must continue to pass:

- existing Rust unit tests
- existing wasm/browser tests
- existing performance suite in `cynos-perf`

## 19. Risks and Mitigations

### Risk 1: batching becomes a second query engine

Mitigation:

- use planner-backed nested fetches wherever possible
- keep batching focused on relation-key set execution and bucketization
- do not duplicate optimizer logic in the GraphQL adapter

### Risk 2: cache invalidation complexity grows too fast

Mitigation:

- compile explicit `table_to_edges` mappings
- keep the invalidation envelope formal and typed
- allow snapshot mode to use coarse invalidation when precise key invalidation is not available

### Risk 3: GraphQL code leaks into SQL hot paths

Mitigation:

- keep new batching state and rendering code entirely in GraphQL adapter modules
- keep `LiveRegistry`, `ObservableQuery`, and `MaterializedView` free of GraphQL-specific logic

### Risk 4: per-parent limit/offset semantics are accidentally changed

Mitigation:

- treat per-parent limit/offset as bucket post-processing semantics
- add explicit tests for nested order + limit + offset combinations

## 20. Acceptance Criteria

This design is considered implemented when all of the following are true:

- GraphQL live no longer resolves nested relations row-by-row in the general case
- one subscription render batches relation resolution by edge and by unique relation key
- snapshot-backed GraphQL subscriptions reuse batched relation fetches
- delta-backed GraphQL subscriptions reuse batched relation fetches and key-local invalidation
- existing live runtime abstractions remain intact
- `observe()` / `changes()` / `trace()` do not regress
- existing unit, wasm, and performance suites continue to pass

## 21. Summary

The correct place to solve GraphQL live N+1 in Cynos is not the live runtime kernel. It is the GraphQL adapter layer.

The live runtime already batches change delivery correctly. What is missing is a second batching layer:

- relation batching during GraphQL payload assembly

This design introduces that layer by:

- compiling a `GraphqlBatchPlan`
- rendering with a frontier-based `GraphqlBatchRenderer`
- caching relation buckets and rendered node values in `GraphqlBatchState`
- using coarse invalidation for snapshot mode and key-targeted invalidation for delta mode

The result keeps Cynos aligned with the intended architecture:

- two live kernels
- one shared runtime
- GraphQL as an upper-layer bridge
- better nested live performance without sacrificing current SQL live behavior
