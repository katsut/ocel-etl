# Architecture

How ocel-etl turns raw connector output into trustworthy OCEL.

## The staging gate

Connectors never construct an `Ocel` directly. They accumulate
`StagingEvent`s and object upserts in a `StagingLog` — an order-tolerant,
duplicate-tolerant working area — and call `into_ocel()`, which declares
types from what it saw, deduplicates identical attribute observations, and
**validates**. A connector bug surfaces as a gate error at build time, not as
a corrupt log downstream.

## Transformations are reference-safe

`map_events` / `retain_events` rewrite or drop events and rebuild the event
schema from what remains. `map_object_ids` re-keys objects while chasing
every E2O/O2O reference; two ids mapped to the same target merge (attributes
append, the first non-empty type wins). These primitives are what
ocel-transform's recipe steps and the identity-resolution handoff are built
from.

## Round-tripping for incremental sync

`from_ocel` lifts a gated log back into staging as the identity, so
incremental connectors can prune a key-prefixed slice, re-map only what
changed, and re-gate. The correctness bar is equality: an incremental run
must produce the same log as a full re-pull.
