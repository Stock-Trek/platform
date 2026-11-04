//! Interval and lateness settings shared by live processing and recovery.

use platform_market_data::candle::AlignedWindow;
use platform_types::new_types::DurationMilliseconds;
use std::collections::BTreeMap;

/// Interval and lateness settings shared by live processing and recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorConfig {
    pub intervals: Vec<AlignedWindow>,
    pub allowed_lateness: DurationMilliseconds,
    pub max_closed_candles_per_interval: BTreeMap<AlignedWindow, usize>,
    pub correction_cutoff: DurationMilliseconds,
    /// Schema version new state is stamped with and restored state must match.
    pub state_schema_version: u32,
}

impl ProcessorConfig {
    pub fn new(
        intervals: Vec<AlignedWindow>,
        allowed_lateness: DurationMilliseconds,
        max_closed_candles_per_interval: BTreeMap<AlignedWindow, usize>,
        correction_cutoff: DurationMilliseconds,
    ) -> Self {
        Self {
            intervals,
            allowed_lateness,
            max_closed_candles_per_interval,
            correction_cutoff,
            state_schema_version: crate::state::STATE_SCHEMA_VERSION,
        }
    }

    pub fn without_retention(
        intervals: Vec<AlignedWindow>,
        allowed_lateness: DurationMilliseconds,
    ) -> Self {
        let max_closed_candles_per_interval = intervals
            .iter()
            .copied()
            .map(|interval| (interval, usize::MAX))
            .collect();
        Self::new(
            intervals,
            allowed_lateness,
            max_closed_candles_per_interval,
            DurationMilliseconds(i64::MAX),
        )
    }

    pub fn with_intervals(intervals: Vec<AlignedWindow>) -> Self {
        Self::without_retention(intervals, DurationMilliseconds::ZERO)
    }

    pub fn with_state_schema_version(mut self, state_schema_version: u32) -> Self {
        self.state_schema_version = state_schema_version;
        self
    }

    pub fn max_closed_candles(&self, interval: AlignedWindow) -> Option<usize> {
        self.max_closed_candles_per_interval.get(&interval).copied()
    }
}
