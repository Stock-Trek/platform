//! The explicit boundary between the compacted and raw archive layers.
//!
//! The raw archive is the append-only source of truth. Compaction copies
//! completed raw objects into de-duplicated, sorted partitions and records the
//! source objects in the compacted Iceberg snapshot. That snapshot is the
//! handoff contract: every raw object named by the snapshot is fully represented
//! by the compacted files in the same snapshot, and every other raw object is
//! still part of the raw tail.
//!
//! A recovery reader can therefore replay the compacted files followed by the
//! raw tail and see every archived record exactly once. It never has to choose
//! between a partially compacted table (which would miss the tail) and the full
//! raw history (which would duplicate the compacted prefix).

mod handoff;

#[cfg(test)]
mod tests;

pub use handoff::*;
