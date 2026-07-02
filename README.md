# ocel-etl

[![crates.io](https://img.shields.io/crates/v/ocel-etl.svg)](https://crates.io/crates/ocel-etl)
[![docs.rs](https://img.shields.io/docsrs/ocel-etl)](https://docs.rs/ocel-etl)

ETL engine for [OCEL 2.0](https://www.ocel-standard.org/) process mining, built
on the [`ocel`](https://crates.io/crates/ocel) crate.

Connectors extract source data into a `StagingLog` — a loose working
representation that tolerates out-of-order ingestion, dangling references, and
growing schemas — and convert it into a validated `ocel::Ocel` through the
single `into_ocel()` gate.

```rust
use ocel_etl::{StagingEvent, StagingLog};

let mut staging = StagingLog::new();
// events may arrive before the objects they reference (pagination order)
staging.add_event(StagingEvent { /* ... */ });
staging.upsert_object("task-1", "task");

let log = staging.into_ocel()?;       // the validation gate
ocel::io::sqlite::write_path(&log, "out.sqlite")?;
```

## License

MIT
