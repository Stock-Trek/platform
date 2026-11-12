//! Shared result and input types for aggregator shard checkpoint restore and
//! archive-backed state rebuild.

use crate::config::{AggregatorConfig, ReplayStartPolicy};
use crate::shard_id::ShardId;
use crate::state::AggregatorState;
use platform_market_data::AlignedWindow;
use platform_nats::ConsumerStart;
use platform_types::{DurationMilliseconds, TimestampMilliseconds};
use std::collections::BTreeMap;

/// The state and source position a shard should start from.
#[derive(Debug, Clone)]
pub struct RestoredShard {
    pub state: AggregatorState,
    /// Where the live source should begin after recovery.
    pub start: ConsumerStart,
    /// Which recovery path produced the state.
    pub restored_from: RestoreSource,
    /// Whether the state was rebuilt by replaying archived raw events.
    pub rebuilt_from_archive: bool,
}

/// Recovery path that produced a shard's starting state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreSource {
    Checkpoint,
    Archive,
    Empty,
}

impl RestoreSource {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::Archive => "archive",
            Self::Empty => "empty",
        }
    }
}

impl RestoredShard {
    pub fn replay_watermark(&self) -> Option<TimestampMilliseconds> {
        if self.rebuilt_from_archive {
            self.state.watermark
        } else {
            None
        }
    }

    pub fn start_sequence(&self) -> Option<u64> {
        match self.start {
            ConsumerStart::Sequence(sequence) => Some(sequence),
            ConsumerStart::Earliest | ConsumerStart::Latest => None,
        }
    }
}

/// Inputs that control checkpoint restore and archive rebuild for one writer.
#[derive(Debug, Clone)]
pub struct RestoreOptions {
    pub shard: ShardId,
    pub source_stream: String,
    pub intervals: Vec<AlignedWindow>,
    pub shard_count: usize,
    pub allowed_lateness: DurationMilliseconds,
    pub max_closed_candles_per_interval: BTreeMap<AlignedWindow, usize>,
    pub correction_cutoff: DurationMilliseconds,
    pub replay_start_policy: ReplayStartPolicy,
    pub expected_schema_version: u32,
}

impl RestoreOptions {
    pub fn from_config(
        config: &AggregatorConfig,
        shard: ShardId,
        source_stream: impl Into<String>,
    ) -> Self {
        Self {
            shard,
            source_stream: source_stream.into(),
            intervals: config.intervals.clone(),
            shard_count: config.shard_count,
            allowed_lateness: config.processor_config().allowed_lateness,
            max_closed_candles_per_interval: config
                .processor_config()
                .max_closed_candles_per_interval,
            correction_cutoff: config.processor_config().correction_cutoff,
            replay_start_policy: config.replay_start_policy,
            expected_schema_version: config.state_schema_version,
        }
    }
}
