//! Generic ETL engine for OCEL 2.0 process mining.
//!
//! Connectors extract source data into a [`StagingLog`] — a loose working
//! representation that tolerates out-of-order ingestion, dangling references,
//! and growing schemas — and convert it into a validated [`ocel::Ocel`]
//! through the single [`StagingLog::into_ocel`] gate.

mod staging;

pub use staging::{StagingEvent, StagingLog};
