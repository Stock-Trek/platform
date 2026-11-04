//! Candle aggregation types.
//!
//! The candle model is split by responsibility: stock-trek's [`AlignedWindow`]
//! defines candle intervals and alignment, `key` identifies a candle, `body`
//! folds trades into stock-trek's `MarketOhlcv` body, `revision` assigns deterministic revisions
//! to closed candles, and `closed` pairs a candle with its revision.

mod body;
mod closed;
mod key;
mod revision;

#[cfg(test)]
mod tests;

pub use body::Candle;
pub use closed::ClosedCandle;
pub use key::CandleKey;
pub use revision::CandleRevision;
pub use stock_trek::markets::{AlignedWindow, MarketOhlcv};
