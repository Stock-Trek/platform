use crate::partition::{PartitionId, all_partitions};
use crate::processor::ProcessorConfig;
use platform_archive::RoutingBucket;
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::CandleInterval;
use platform_nats::NatsSubject;
use platform_runtime::Config;
use platform_types::new_types::DurationMilliseconds;
use std::fmt;
use std::time::Duration;

pub const AGGREGATOR_NATS_URL: &str = "AGGREGATOR_NATS_URL";
pub const AGGREGATOR_EVENT_SUBJECT_PREFIX: &str = "AGGREGATOR_EVENT_SUBJECT_PREFIX";
pub const AGGREGATOR_GAP_SUBJECT_PREFIX: &str = "AGGREGATOR_GAP_SUBJECT_PREFIX";
pub const AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX: &str = "AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX";
pub const AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX: &str = "AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX";
pub const AGGREGATOR_INTERVALS: &str = "AGGREGATOR_INTERVALS";
pub const AGGREGATOR_ALLOWED_LATENESS_MS: &str = "AGGREGATOR_ALLOWED_LATENESS_MS";
pub const AGGREGATOR_CHECKPOINT_INTERVAL_SECS: &str = "AGGREGATOR_CHECKPOINT_INTERVAL_SECS";
pub const AGGREGATOR_CHECKPOINT_BUCKET: &str = "AGGREGATOR_CHECKPOINT_BUCKET";
pub const AGGREGATOR_CHECKPOINT_PREFIX: &str = "AGGREGATOR_CHECKPOINT_PREFIX";
pub const AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS: &str = "AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS";
pub const AGGREGATOR_REPLAY_START_POLICY: &str = "AGGREGATOR_REPLAY_START_POLICY";
pub const AGGREGATOR_STATE_SCHEMA_VERSION: &str = "AGGREGATOR_STATE_SCHEMA_VERSION";
pub const AGGREGATOR_DURABLE_PREFIX: &str = "AGGREGATOR_DURABLE_PREFIX";
pub const AGGREGATOR_LEASE_TTL_SECS: &str = "AGGREGATOR_LEASE_TTL_SECS";
pub const AGGREGATOR_SHARD_SUBJECT_PREFIX: &str = "AGGREGATOR_SHARD_SUBJECT_PREFIX";
pub const AGGREGATOR_SHARD_COUNT: &str = "AGGREGATOR_SHARD_COUNT";
pub const AGGREGATOR_SHARD_INDEX: &str = "AGGREGATOR_SHARD_INDEX";

/// What a partition writer does with persisted state when it starts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReplayStartPolicy {
    /// Resume from a compatible checkpoint, rebuilding from the archive only
    /// when the checkpoint is missing, stale, or incompatible.
    #[default]
    Checkpoint,
    /// Ignore checkpoints and rebuild keyed state from archived raw events.
    Archive,
    /// Ignore persisted state and replay the live source from the beginning.
    Earliest,
    /// Ignore persisted state and process only messages published after start.
    Latest,
}

impl ReplayStartPolicy {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::Archive => "archive",
            Self::Earliest => "earliest",
            Self::Latest => "latest",
        }
    }

    /// Parses the operator-facing policy name, accepting a few common aliases.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "checkpoint" | "resume" | "checkpoint-first" | "checkpoint_first" => {
                Some(Self::Checkpoint)
            }
            "archive" | "rebuild" | "rebuild-archive" | "rebuild_archive" => Some(Self::Archive),
            "earliest" | "beginning" | "start" | "all" => Some(Self::Earliest),
            "latest" | "new" | "now" => Some(Self::Latest),
            _ => None,
        }
    }
}

impl fmt::Display for ReplayStartPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}

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
    /// Total number of key partitions.
    pub shard_count: usize,
    /// When set, this process only owns that single partition. Otherwise it runs
    /// every partition (useful for local development and tests).
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
    /// How long the input may be idle before the wall clock advances the
    /// event-time watermark so open candle windows can close.
    pub watermark_idle_timeout: Duration,
    /// What state a partition starts from.
    pub replay_start_policy: ReplayStartPolicy,
    /// State schema version written into new state and required in checkpoints.
    pub state_schema_version: u32,
    /// Prefix used for durable consumer names. The router uses
    /// `{prefix}-router` and each partition uses `{prefix}-shard-{n}`.
    pub consumer_prefix: String,
    /// Lifetime of the single-writer partition lease.
    pub lease_ttl: Duration,
}

impl AggregatorConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let log_filter = Config::log_filter(vars)?;
        let nats_url = Config::required_nats_url(vars, AGGREGATOR_NATS_URL)?;
        let event_subject_prefix =
            Config::required_nats_subject(vars, AGGREGATOR_EVENT_SUBJECT_PREFIX)?;
        let gap_subject_prefix =
            Config::required_nats_subject(vars, AGGREGATOR_GAP_SUBJECT_PREFIX)?;
        let open_candle_subject_prefix =
            Config::required_nats_subject(vars, AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX)?;
        let closed_candle_subject_prefix =
            Config::required_nats_subject(vars, AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX)?;

        let interval_labels = vars.required(AGGREGATOR_INTERVALS)?;
        let mut intervals = Vec::new();
        for label in interval_labels
            .split(',')
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            let interval = CandleInterval::from_label(label)
                .map_err(|error| BpError::config_invalid_source(AGGREGATOR_INTERVALS, error))?;
            if !intervals.contains(&interval) {
                intervals.push(interval);
            }
        }
        if intervals.is_empty() {
            return Err(BpError::config_invalid(
                AGGREGATOR_INTERVALS,
                "at least one interval is required",
            ));
        }

        let checkpoint_interval =
            Config::required_positive_duration_secs(vars, AGGREGATOR_CHECKPOINT_INTERVAL_SECS)?;
        let checkpoint_bucket = vars.required(AGGREGATOR_CHECKPOINT_BUCKET)?;
        validate_kv_bucket(AGGREGATOR_CHECKPOINT_BUCKET, &checkpoint_bucket)?;
        let checkpoint_key_prefix = vars.required(AGGREGATOR_CHECKPOINT_PREFIX)?;
        NatsSubject::validate_named(
            AGGREGATOR_CHECKPOINT_PREFIX,
            &format!("{checkpoint_key_prefix}-0"),
        )?;
        let watermark_idle_timeout =
            Config::required_positive_duration_secs(vars, AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS)?;
        let replay_value = vars.required(AGGREGATOR_REPLAY_START_POLICY)?;
        let replay_start_policy = ReplayStartPolicy::parse(&replay_value).ok_or_else(|| {
            BpError::config_invalid(
                AGGREGATOR_REPLAY_START_POLICY,
                "must be one of checkpoint, archive, earliest, or latest",
            )
        })?;
        let state_schema_version = vars.required_parsed(AGGREGATOR_STATE_SCHEMA_VERSION)?;
        let consumer_prefix = vars.required(AGGREGATOR_DURABLE_PREFIX)?;
        NatsSubject::validate_named(AGGREGATOR_DURABLE_PREFIX, &consumer_prefix)?;
        let lease_ttl = Config::required_positive_duration_secs(vars, AGGREGATOR_LEASE_TTL_SECS)?;
        if checkpoint_interval >= lease_ttl {
            return Err(BpError::config_invalid(
                AGGREGATOR_CHECKPOINT_INTERVAL_SECS,
                format!("must be lower than {AGGREGATOR_LEASE_TTL_SECS}"),
            ));
        }
        let allowed_lateness_ms: u64 = vars.required_parsed(AGGREGATOR_ALLOWED_LATENESS_MS)?;

        let shard_subject_prefix =
            Config::required_nats_subject(vars, AGGREGATOR_SHARD_SUBJECT_PREFIX)?;
        let shard_count = Config::required_positive_usize(vars, AGGREGATOR_SHARD_COUNT)?;
        if shard_count as u64 > RoutingBucket::COUNT {
            return Err(BpError::config_invalid(
                AGGREGATOR_SHARD_COUNT,
                format!(
                    "must not exceed the archive routing bucket count ({})",
                    RoutingBucket::COUNT
                ),
            ));
        }
        let shard_index = vars.optional_parsed(AGGREGATOR_SHARD_INDEX)?;
        if let Some(shard_index) = shard_index
            && shard_index >= shard_count
        {
            return Err(BpError::config_invalid(
                AGGREGATOR_SHARD_INDEX,
                format!("must be lower than {AGGREGATOR_SHARD_COUNT} ({shard_count})"),
            ));
        }

        Ok(Self {
            log_filter,
            nats_url,
            event_subject_prefix,
            gap_subject_prefix,
            open_candle_subject_prefix,
            closed_candle_subject_prefix,
            shard_subject_prefix,
            shard_count,
            shard_index,
            intervals,
            allowed_lateness: Duration::from_millis(allowed_lateness_ms),
            checkpoint_interval,
            checkpoint_bucket,
            checkpoint_key_prefix,
            watermark_idle_timeout,
            replay_start_policy,
            state_schema_version,
            consumer_prefix,
            lease_ttl,
        })
    }

    /// The KV bucket that stores source checkpoints for routed partitions.
    pub fn checkpoint_bucket(&self) -> String {
        self.checkpoint_bucket.clone()
    }

    /// Durable name used by the repartition router.
    pub fn router_durable_name(&self) -> String {
        format!("{}-router", self.consumer_prefix)
    }

    /// Durable name used by one partition writer.
    pub fn partition_durable_name(&self, partition: PartitionId) -> String {
        format!("{}-shard-{}", self.consumer_prefix, partition.get())
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

    /// The partitions this process is responsible for.
    pub fn partitions(&self) -> Vec<PartitionId> {
        match self.shard_index {
            Some(index) => vec![PartitionId::new(index as u32)],
            None => all_partitions(self.shard_count),
        }
    }
}

fn validate_kv_bucket(name: &str, bucket: &str) -> BpResult<()> {
    let valid = !bucket.is_empty()
        && bucket
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'));
    if valid {
        Ok(())
    } else {
        Err(BpError::config_invalid(
            name,
            "must only contain alphanumeric characters, dashes, and underscores",
        ))
    }
}
