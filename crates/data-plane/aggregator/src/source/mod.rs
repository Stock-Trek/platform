//! Input sources for the aggregator.
//!
//! The router consumes the ingester's market events and gap markers and
//! reshards them onto shard subjects. Each shard writer then consumes
//! only its own shard subject, which gives every candle key a single writer.

mod source;

#[cfg(test)]
mod tests;

pub use source::*;
