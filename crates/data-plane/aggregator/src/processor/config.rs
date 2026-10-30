//! Interval and lateness settings shared by live processing and recovery.

use platform_market_data::candle::CandleInterval;
use platform_types::new_types::DurationMilliseconds;

/// Interval and lateness settings shared by live processing and recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorConfig {
    pub intervals: Vec<CandleInterval>,
    pub allowed_lateness: DurationMilliseconds,
    /// Schema version new state is stamped with and restored state must match.
    pub state_schema_version: u32,
}

impl ProcessorConfig {
    pub fn new(intervals: Vec<CandleInterval>, allowed_lateness: DurationMilliseconds) -> Self {
        Self {
            intervals,
            allowed_lateness,
            state_schema_version: crate::state::STATE_SCHEMA_VERSION,
        }
    }

    pub fn with_intervals(intervals: Vec<CandleInterval>) -> Self {
        Self::new(intervals, DurationMilliseconds::ZERO)
    }

    pub fn with_state_schema_version(mut self, state_schema_version: u32) -> Self {
        self.state_schema_version = state_schema_version;
        self
    }
}
