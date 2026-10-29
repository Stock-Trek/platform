//! Builders for market data values used in tests.

use crate::event::GapMarker;
use platform_types::new_types::TimestampMilliseconds;
use stock_trek_types::cex::cex_id::CexId;

/// Builds a [`GapMarker`] covering the inclusive `missing_from..=missing_to`
/// range.
///
/// The previous and observed sequences are derived from the missing range, and
/// the marker is attributed to a Binance `btcusdt@trade` stream.
pub fn gap(missing_from: u64, missing_to: u64) -> GapMarker {
    GapMarker {
        exchange: CexId::Binance,
        stream: "btcusdt@trade".to_string(),
        previous_sequence: missing_from.saturating_sub(1),
        observed_sequence: missing_to.saturating_add(1),
        missing_from,
        missing_to,
        detected_at: TimestampMilliseconds(1),
    }
}
