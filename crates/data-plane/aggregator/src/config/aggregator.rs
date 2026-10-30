use super::replay::ReplayStartPolicy;
use crate::processor::ProcessorConfig;
use crate::shard::{ShardId, all_shards};
use platform_market_data::candle::CandleInterval;
use platform_types::new_types::DurationMilliseconds;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    pub log_filter: String,
    pub nats_url: String,
    pub event_subject_prefix: String,
    /// Subject prefix carrying gap markers from the ingester.
    pub gap_subject_prefix: String,
    pub open_candle_subject_prefix: String,
    pub closed_candle_subject_prefix: String,
    /// Subject prefix owned by the aggregator for routed shard inputs.
    ///
    /// This intentionally lives outside the market-data subject tree so the
    /// router stream cannot overlap the ingester stream.
    pub shard_subject_prefix: String,
    /// Total number of key shards.
    pub shard_count: usize,
    /// When set, this process only owns that single shard. Otherwise it runs
    /// every shard (useful for local development and tests).
    pub shard_index: Option<usize>,
    pub intervals: Vec<CandleInterval>,
    /// How long after a candle window ends a late event may still update it.
    pub allowed_lateness: Duration,
    /// How often keyed state is checkpointed while processing.
    pub checkpoint_interval: Duration,
    /// KV bucket for checkpoints and writer leases.
    pub checkpoint_bucket: String,
    /// Prefix of checkpoint and lease keys inside the checkpoint bucket.
    pub checkpoint_key_prefix: String,
    /// How long the input may be idle before expired candle windows are
    /// flushed and state is checkpointed. Idle flushing uses the current
    /// event-time watermark and never advances it with the wall clock.
    pub watermark_idle_timeout: Duration,
    /// What state a shard starts from.
    pub replay_start_policy: ReplayStartPolicy,
    /// State schema version written into new state and required in checkpoints.
    pub state_schema_version: u32,
    /// Prefix used for durable consumer names. The router uses
    /// `{prefix}-router` and each shard uses `{prefix}-shard-{n}`.
    pub consumer_prefix: String,
    /// Lifetime of the single-writer shard lease.
    pub lease_ttl: Duration,
}

impl AggregatorConfig {
    /// The KV bucket that stores source checkpoints for routed shards.
    pub fn checkpoint_bucket(&self) -> String {
        self.checkpoint_bucket.clone()
    }

    /// Durable name used by the reshard router.
    pub fn router_durable_name(&self) -> String {
        format!("{}-router", self.consumer_prefix)
    }

    /// Durable name used by one shard writer.
    pub fn shard_durable_name(&self, shard: ShardId) -> String {
        format!("{}-shard-{}", self.consumer_prefix, shard.get())
    }

    /// The deterministic processing configuration used by live processing and
    /// archive rebuilds.
    pub fn processor_config(&self) -> ProcessorConfig {
        ProcessorConfig::new(
            self.intervals.clone(),
            DurationMilliseconds(
                i64::try_from(self.allowed_lateness.as_millis()).unwrap_or(i64::MAX),
            ),
        )
        .with_state_schema_version(self.state_schema_version)
    }

    /// The shards this process is responsible for.
    pub fn shards(&self) -> Vec<ShardId> {
        match self.shard_index {
            Some(index) => vec![ShardId::new(index as u32)],
            None => all_shards(self.shard_count),
        }
    }
}
