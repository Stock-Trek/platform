//! Candle window helpers backed by stock-trek's [`AlignedWindow`].
//!
//! [`AlignedWindow`] is the source of truth for candle intervals, window
//! alignment, and label parsing and serialization.

pub use stock_trek::markets::AlignedWindow;
use strum::IntoEnumIterator;

/// Every candle interval supported by stock-trek.
pub fn all_intervals() -> Vec<AlignedWindow> {
    AlignedWindow::iter().collect()
}
