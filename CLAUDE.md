# CLAUDE.md — ocel-etl

The ETL engine every connector builds on: accumulate raw material in a
`StagingLog`, then pass the `into_ocel()` **validation gate** — a connector
cannot emit an invalid OCEL. Concepts in [ARCHITECTURE.md](ARCHITECTURE.md).

## Build, test, verify

```sh
cargo test
cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

## Map

- `src/staging.rs` — everything: `StagingLog` / `StagingEvent`,
  `upsert_object`, `add_event`, `map_events` / `retain_events` /
  `map_object_ids` (re-key with E2O/O2O reference chasing and same-target
  merge), `from_ocel` (identity after the gate), `into_ocel` (validate +
  dedupe of identical `(name, value, time)` observations), and
  `sync::repair_parent_links` helpers used by incremental connectors

## Invariants and traps

- `into_ocel()` is the only exit; if it returns violations, the caller's
  mapping is wrong — never bypass the gate.
- `map_events` rebuilds the event-type declarations: renames drop the old
  type, emptied types disappear, and declarations seeded via `from_ocel`
  that lose all events disappear too (documented semantics).
- `map_object_ids` chases every E2O/O2O reference and merges objects mapped
  to the same id (attributes append, first non-empty type wins).
- Incremental-sync semantics live with the connectors (prune by event-id
  prefix, re-map, re-gate); the equality test "incremental == full re-pull"
  is the bar.

## Conventions

Issue → branch → PR → CI green → squash-merge. Published on crates.io
(`ocel-etl`); publish needs the owner's GO. Design docs live in the private
ocel-workspace, not here.
