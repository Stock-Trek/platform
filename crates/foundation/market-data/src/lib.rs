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
pub mod envelope_codec;

#[cfg(test)]
mod envelope_codec_tests;

pub use envelope_codec::*;
pub mod event;
pub mod market_event_subject;

#[cfg(test)]
mod market_event_subject_tests;

pub use market_event_subject::*;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
