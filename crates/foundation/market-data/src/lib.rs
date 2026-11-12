//! Shared market data messages, candle aggregation types, exchange frame
//! decoders, and the NATS message contract.
//!
//! The ingester and aggregator both consume exchange frames, so the codecs and
//! wire-level event types live here rather than in either service. Candle types
//! are shared so other data-plane crates can reuse the same representation. The
//! subject layout and JSON envelope codec are also defined here so every
//! producer and consumer uses the same contract.

mod candle;
mod decoders;
mod envelope_codec;
mod event;
mod market_event_subject;

#[cfg(test)]
mod envelope_codec_tests;
#[cfg(test)]
mod market_event_subject_tests;
#[cfg(any(test, feature = "test-utils"))]
mod test_utils;

pub use candle::*;
pub use decoders::*;
pub use envelope_codec::*;
pub use event::*;
pub use market_event_subject::*;
