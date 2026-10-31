//! Durable checkpoints for aggregator shards.
//!
//! A checkpoint contains the source position *and* the serialized keyed state
//! whose effects are included up to that position. The source message is
//! acknowledged only after this record has been written, so a crash can replay
//! work but cannot silently skip it.

mod checkpoint;

#[cfg(test)]
pub(crate) mod tests;

pub use checkpoint::*;
