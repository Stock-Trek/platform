//! Reshard/router stage.
//!
//! The router turns the ingester's exchange/stream subjects into a shard stream
//! keyed by `(exchange, trading pair)`. Downstream shard writers therefore
//! see every event for a candle key in order, on exactly one shard.

mod implementation;

#[cfg(test)]
mod tests;

pub use implementation::*;
