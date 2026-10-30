//! Candle aggregation types.
//!
//! The candle model is split by responsibility: `interval` parses and
//! canonicalizes candle windows, `key` identifies a candle, `body` folds
//! trades into an OHLCV body, `revision` assigns deterministic revisions to
//! closed candles, and `closed` pairs a candle with its revision.

mod body;
mod closed;
mod interval;
mod key;
mod revision;

#[cfg(test)]
mod tests;

pub use body::Candle;
pub use closed::ClosedCandle;
pub use interval::{CandleInterval, CandleIntervalError};
pub use key::CandleKey;
pub use revision::CandleRevision;
