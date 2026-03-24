# Live Runtime Architecture

Status: implemented (current architecture)  
Owner: `cynos-database` / `cynos-gql`  
Primary scope: `crates/database`, `crates/gql`, `js/packages/core`

## 1. Purpose

This document describes the live runtime as it exists today.

## 2. Architectural summary

Cynos currently exposes four live-oriented API surfaces:

- `observe()`
- `changes()`
- `trace()`
- `subscribeGraphql()` / prepared GraphQL subscriptions

At the execution level, these APIs are backed by two kernels:

- `Snapshot`
  - cached plan execution, re-query, and row-local reactive patch
- `Delta`
  - DBSP-style incremental view maintenance via dataflow and deltas

At the control-plane level, these APIs share one runtime abstraction:

- one `LivePlan` model
- one dependency registration mechanism
- one `LiveRegistry`
- one batching/flush mechanism
- one GC/keepalive model

At the adapter/output level, the runtime currently supports four surfaces:

- rows snapshot
- rows delta
- GraphQL snapshot
- GraphQL delta

At the runtime level, the structure is:

- exactly two execution kernels
- one shared runtime control plane
- multiple adapters above that control plane

GraphQL uses the same snapshot and delta live machinery as the row-oriented APIs. It adds adapter
logic for binding, rendering, and invalidation, but not a third execution kernel.

## 3. Shared and specialized parts

### 3.1 Unified

The following concerns are unified in the current runtime:

- dependency registration by table id
- pending change accumulation
- pending delta accumulation
- flush scheduling
- flush ordering
- subscription lifecycle cleanup
- runtime-level routing from table changes to subscribed observables

### 3.2 Intentionally specialized

The following parts remain specialized:

- snapshot vs delta execution
- rows output shaping vs GraphQL output shaping
- root-row maintenance for GraphQL snapshot vs GraphQL delta
- JS wrappers for row snapshots, row deltas, and GraphQL subscriptions

The control plane is shared, while hot execution paths remain concrete and specialized.

## 4. Core runtime types

The shared runtime model lives primarily in `crates/database/src/live_runtime.rs`.

### 4.1 `LiveEngineKind`

`LiveEngineKind` identifies which execution kernel a live plan uses:

- `Snapshot`
- `Delta`

This choice is made when the live plan is compiled and registered.

### 4.2 `LiveOutputKind`

`LiveOutputKind` identifies the adapter/output surface:

- `RowsSnapshot`
- `RowsDelta`
- `GraphqlSnapshot`
- `GraphqlDelta`

For GraphQL, `GraphqlDelta` means the subscription is driven by the delta kernel internally. It does
not imply a JS-visible GraphQL tree-delta protocol. The external GraphQL subscription payload is
still a GraphQL response snapshot.

### 4.3 `LiveDependencySet`

`LiveDependencySet` records the tables that drive invalidation:

- `tables`
  - complete dependency set
- `root_tables`
  - root-result tables, currently especially relevant for GraphQL snapshot subscriptions

Rows-oriented live plans typically only need `tables`. GraphQL snapshot subscriptions also use
`root_tables` to distinguish root-row changes from nested dependency changes.

### 4.4 `KernelPlan`

The runtime keeps the kernel plan separate from the adapter plan.

Snapshot kernel plan:

- compiled physical plan
- initial rows
- initial result summary

Delta kernel plan:

- compiled dataflow
- initial owned rows

Execution and output shaping are modeled independently.

### 4.5 `AdapterPlan`

The current adapter plan variants are:

- `RowsSnapshot`
- `RowsDelta`
- `GraphqlSnapshot`
- `GraphqlDelta`

Rows adapters carry:

- row projection metadata
- binary layout metadata

GraphQL adapters carry:

- `GraphqlCatalog`
- bound root field
- compiled `GraphqlBatchPlan`
- dependency table bindings

### 4.6 `LivePlan`

`LivePlan` is the runtime-level product of live query compilation.

It contains:

- `LivePlanDescriptor`
- `KernelPlan`
- `AdapterPlan`

This is the runtime boundary between compilation and materialization:

- compile once into a typed live plan
- materialize later into the appropriate observable and JS wrapper

## 5. Materialization paths

`LivePlan` materialization methods turn the abstract plan into concrete observables and register them
with the shared registry.

### 5.1 Rows snapshot

`materialize_rows_snapshot()`:

- creates a `ReQueryObservable`
- registers it as a snapshot subscription
- wraps it in `JsObservableQuery`

This is the path used by `observe()` and by `changes()` through its rows-snapshot base.

### 5.2 Rows delta

`materialize_rows_delta()`:

- creates an `ObservableQuery`
- registers it as a delta subscription
- wraps it in `JsIvmObservableQuery`

This is the `trace()` path.

### 5.3 GraphQL snapshot

`materialize_graphql_snapshot()`:

- creates a `GraphqlSubscriptionObservable`
- passes the compiled physical plan, initial rows, summary, catalog, field, batch plan, and
  dependency bindings
- registers it as a snapshot subscription
- wraps it in `JsGraphqlSubscription`

### 5.4 GraphQL delta

`materialize_graphql_delta()`:

- creates a `GraphqlDeltaObservable`
- passes the dataflow, initial rows, catalog, field, batch plan, and dependency bindings
- registers it as a delta subscription
- wraps it in `JsGraphqlSubscription`

This is the current GraphQL-on-delta path.

## 6. `LiveRegistry` control plane

`LiveRegistry` is the shared routing and batching layer for live subscriptions.

Its core state includes:

- snapshot subscriptions indexed by `TableId`
- delta subscriptions indexed by `TableId`
- pending changed row ids
- pending row deltas
- one flush-scheduled flag

### 6.1 Registration

Snapshot and delta subscriptions are registered independently:

- `register_snapshot(...)`
- `register_delta(...)`

Registration expands a subscription across all dependency tables in its `LiveDependencySet`. The
runtime then routes future table changes by table id.

### 6.2 Change accumulation

The registry accumulates two kinds of pending work:

- `pending_changes`
  - row ids grouped by table id
- `pending_deltas`
  - deltas grouped by table id

Snapshot-only updates fill `pending_changes`.

Delta-aware updates fill both:

- `pending_deltas`, so the delta lane can propagate true row deltas
- `pending_changes`, so snapshot subscriptions depending on the same tables can still be invalidated

### 6.3 Flush scheduling

The runtime batches change processing once per flush cycle.

On `wasm32`:

- flush is scheduled through a resolved `Promise`
- multiple writes in the same tick are coalesced

Outside `wasm32`:

- flush executes synchronously

This batching mechanism is shared across row and GraphQL live APIs.

### 6.4 Flush order

Flush order is currently:

1. delta lane
2. snapshot lane
3. dead-subscription GC

This order is encoded in both the async and sync flush paths.

### 6.5 Delta lane behavior

`flush_delta_lane()` routes table deltas to registered delta subscriptions:

- rows delta subscriptions receive `ObservableQuery::on_table_change(...)`
- GraphQL delta subscriptions receive `GraphqlDeltaObservable::on_table_change(...)`

The delta lane stays separate from the snapshot observable path.

### 6.6 Snapshot lane behavior

`flush_snapshot_lane()` does an additional merge step before dispatch:

- row snapshot observables are merged by observable identity
- GraphQL snapshot observables are also merged by observable identity

This avoids repeated callback and invalidation work when one observable depends on multiple changed
tables in the same flush.

For rows snapshot subscriptions:

- changed row ids are unioned into one set per observable

For GraphQL snapshot subscriptions:

- changed row ids are preserved per table

This table-aware shape lets GraphQL snapshot subscriptions distinguish root-table changes from
nested dependency changes.

### 6.7 Lifecycle cleanup

After each flush, `gc_dead_queries()` removes subscriptions whose listener/keepalive count dropped
to zero.

This cleanup is shared across:

- rows snapshot subscriptions
- rows delta subscriptions
- GraphQL snapshot subscriptions
- GraphQL delta subscriptions

## 7. Rows live surfaces

### 7.1 `observe()` / `changes()`

Rows snapshot live remains built on:

- cached compiled physical plans
- `ReQueryObservable`
- row-local reactive patch when available
- full current-result delivery

The runtime adds a shared plan descriptor, registration model, and flush control plane around that
existing execution path.

### 7.2 `trace()`

Rows delta live remains built on:

- dataflow compilation
- `ObservableQuery`
- delta propagation over `MaterializedView`
- `{ added, removed }` delivery

The control plane is shared, and the delta kernel remains specialized.

## 8. GraphQL live surfaces

GraphQL live planning begins in `crates/database/src/database.rs`.

The current compile path:

1. requires a GraphQL subscription operation
2. requires exactly one concrete root field
3. lowers the root field through the planner-backed GraphQL root plan
4. always compiles a `GraphqlBatchPlan`
5. collects dependency tables and root tables
6. attempts delta compilation when the root field is delta-capable
7. falls back to snapshot otherwise

### 8.1 Snapshot GraphQL live

GraphQL snapshot subscriptions use:

- snapshot kernel for root rows
- `GraphqlSubscriptionObservable` as the adapter/runtime object
- `GraphqlBatchState` and `GraphqlBatchPlan` for payload rendering and invalidation

They can respond to:

- root-row changes
- nested dependency changes

through the shared registry.

### 8.2 Delta GraphQL live

GraphQL delta subscriptions use:

- delta kernel for root rows
- `GraphqlDeltaObservable` as the adapter/runtime object
- the same `GraphqlBatchPlan` and `GraphqlBatchState` used by GraphQL snapshot subscriptions

In the current implementation:

- delta vs snapshot changes root-row maintenance
- GraphQL payload rendering remains on one shared batched path

### 8.3 External payload semantics

Even on the delta backend, GraphQL subscriptions currently emit full GraphQL response snapshots to
JS consumers.

The runtime does not currently expose:

- GraphQL tree deltas
- path-level patch streams

That is an intentional scope boundary for the current architecture.

## 9. GraphQL backend selection

Backend selection happens when the live plan is compiled:

- if the root field is delta-capable
- and if the physical plan can be compiled to dataflow
- then GraphQL live uses the delta kernel
- otherwise it uses the snapshot kernel

This keeps backend selection on the cold path:

- backend selection is cold-path work
- hot execution stays on specialized snapshot or delta paths

## 10. Runtime properties

The current runtime has the following operational properties.

### 10.1 Kernel count

GraphQL uses:

- snapshot kernel
- delta kernel

### 10.2 Specialized hot paths

Rows snapshot, rows delta, GraphQL snapshot, and GraphQL delta share control-plane concepts, but
their hot execution objects remain concrete and specialized.

### 10.3 Flush batching

Table changes are coalesced once per flush cycle before routing to subscriptions. This avoids
per-row subscription dispatch and applies across the whole live system.

### 10.4 GraphQL responsibilities

GraphQL owns:

- binding
- root-field lowering
- payload rendering
- payload invalidation

The live runtime owns:

- dependency routing
- batching
- flush scheduling
- lifecycle cleanup

## 11. Current scope boundaries

The current runtime does not provide all of the following:

- GraphQL tree-delta delivery to JS
- delta/IVM support for every GraphQL query shape
- multi-root GraphQL subscriptions
- moving every GraphQL-specific runtime type out of `reactive_bridge.rs`

These are separate extensions and do not change the current control-plane structure.

## 12. Summary

The current Cynos live runtime can be summarized as:

- one shared control plane in `LiveRegistry`
- two execution kernels: snapshot and delta
- one typed `LivePlan` boundary between compilation and materialization
- rows and GraphQL adapters above the same runtime model
- compile-time backend selection for GraphQL live
- batched flush and shared lifecycle management across all live surfaces

At the architectural level:

- the control plane is unified
- the kernels remain specialized
- GraphQL participates as a first-class adapter rather than as a separate live runtime
