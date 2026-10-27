//! Shared market data messages and exchange frame decoders.
//!
//! The ingester and aggregator both consume exchange frames, so the codecs and
//! wire-level event types live here rather than in either service.

pub mod decoders;
pub mod event;
