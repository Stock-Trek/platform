//! Candle window helpers backed by stock-trek's [`AlignedWindow`].
//!
//! [`AlignedWindow`] is the source of truth for candle intervals, window
//! alignment, and label-based serialization. This module only adds the label
//! parsing helper that platform needs.

pub use stock_trek::markets::AlignedWindow;
use strum::IntoEnumIterator;
use thiserror::Error;

/// Errors produced while parsing a candle window label.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlignedWindowError {
    #[error("invalid candle interval label {0:?}")]
    InvalidLabel(String),
}

/// Parse a candle interval label into its stock-trek aligned window.
///
/// Labels are matched case-insensitively. Only the canonical labels emitted by
/// [`AlignedWindow::label`] are accepted; unknown labels are rejected.
pub fn parse_interval(label: &str) -> Result<AlignedWindow, AlignedWindowError> {
    let original = label.trim();
    let normalized = original.to_ascii_lowercase();
    AlignedWindow::try_from(normalized.as_str())
        .map_err(|_| AlignedWindowError::InvalidLabel(original.to_string()))
}

/// Every candle interval supported by stock-trek.
pub fn all_intervals() -> Vec<AlignedWindow> {
    AlignedWindow::iter().collect()
}
