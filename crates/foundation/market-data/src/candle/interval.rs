//! Candle window helpers backed by stock-trek's [`AlignedWindow`].
//!
//! [`AlignedWindow`] is the source of truth for candle intervals and window
//! alignment. This module only adds the label parsing and serialization
//! helpers that platform needs.

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

/// Serialize an aligned window using its canonical label.
///
/// This keeps candle keys and revisions compatible with the label-based format
/// used by archive records while the in-memory type is stock-trek's
/// [`AlignedWindow`].
pub(crate) mod serde_label {
    use super::{AlignedWindow, parse_interval};
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S>(window: &AlignedWindow, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&window.label())
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<AlignedWindow, D::Error>
    where
        D: Deserializer<'de>,
    {
        let label = String::deserialize(deserializer)?;
        parse_interval(&label).map_err(serde::de::Error::custom)
    }
}
