//! Shared market data messages, candle aggregation types, exchange frame
//! decoders, and the NATS message contract.
//!
//! The ingester and aggregator both consume exchange frames, so the codecs and
//! wire-level event types live here rather than in either service. Candle types
//! are shared so other data-plane crates can reuse the same representation. The
//! subject layout and JSON envelope codec are also defined here so every
//! producer and consumer uses the same contract.

pub mod candle;
pub mod decoders;
pub mod envelope;
pub mod event;
pub mod subject;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
