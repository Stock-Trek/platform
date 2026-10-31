//! Interval and retention settings shared by live processing and recovery.

use platform_market_data::candle::CandleInterval;
use platform_types::new_types::DurationMilliseconds;

/// Interval and retention settings shared by live processing and recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorConfig {
    pub intervals: Vec<CandleInterval>,
    pub allowed_lateness: DurationMilliseconds,
    /// Maximum number of closed candles retained per candle key.
    pub max_closed_candles_per_key: usize,
    /// Event-time reach for closed-candle corrections.
    pub correction_cutoff: DurationMilliseconds,
    /// Schema version new state is stamped with and restored state must match.
    pub state_schema_version: u32,
}

impl ProcessorConfig {
    pub fn new(
        intervals: Vec<CandleInterval>,
        allowed_lateness: DurationMilliseconds,
        max_closed_candles_per_key: usize,
        correction_cutoff: DurationMilliseconds,
    ) -> Self {
        Self {
            intervals,
            allowed_lateness,
            max_closed_candles_per_key,
            correction_cutoff,
            state_schema_version: crate::state::STATE_SCHEMA_VERSION,
        }
    }

    /// Configuration for tests and non-retention callers that intentionally
    /// keep no closed-candle cap and no event-time correction cutoff.
    pub fn retention_disabled(
        intervals: Vec<CandleInterval>,
        allowed_lateness: DurationMilliseconds,
    ) -> Self {
        Self::new(
            intervals,
            allowed_lateness,
            usize::MAX,
            DurationMilliseconds(i64::MAX),
        )
    }

    pub fn with_intervals(intervals: Vec<CandleInterval>) -> Self {
        Self::retention_disabled(intervals, DurationMilliseconds::ZERO)
    }

    pub fn with_state_schema_version(mut self, state_schema_version: u32) -> Self {
        self.state_schema_version = state_schema_version;
        self
    }
}
