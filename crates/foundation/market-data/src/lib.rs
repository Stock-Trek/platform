//! Shared market data messages, candle aggregation types, and exchange frame
//! decoders.
//!
//! The ingester and aggregator both consume exchange frames, so the codecs and
//! wire-level event types live here rather than in either service. Candle types
//! are shared so other data-plane crates can reuse the same representation.

pub mod candle;
pub mod decoders;
pub mod event;
