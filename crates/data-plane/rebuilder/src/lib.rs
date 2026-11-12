//! Deterministic candle rebuild from archived market events.
//!
//! The rebuilder reads compacted event partitions plus the raw tail that has
//! not been compacted yet, streaming one bounded Parquet batch at a time.
//! Events are staged per decoded trading pair in a bounded external sort and
//! replayed through the aggregator's pure candle processor, so all streams for
//! a pair update the same candles and the same archived input always produces
//! the same candle revisions. Regenerated candles are themselves spilled in
//! the sink's object order and uploaded one object file at a time, so neither
//! the event stream nor the candle output is ever fully materialized in
//! memory. Output can be written to either the raw or the compacted layer.

mod config;
mod iceberg_event_reader;
mod rebuild;
mod runner;
mod sink;

#[cfg(test)]
mod iceberg_event_reader_tests;

pub use config::{RebuildConfig, RebuildSourceLayer};
pub use iceberg_event_reader::*;
