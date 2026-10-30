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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::STATE_SCHEMA_VERSION;

    const REQUIRED: [(&str, &str); 18] = [
        ("RUST_LOG", "info"),
        (AGGREGATOR_NATS_URL, "nats://localhost:4222"),
        (AGGREGATOR_EVENT_SUBJECT_PREFIX, "market.events"),
        (AGGREGATOR_GAP_SUBJECT_PREFIX, "market.gaps"),
        (AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX, "market.candles.open"),
        (
            AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX,
            "market.candles.closed",
        ),
        (AGGREGATOR_INTERVALS, "1s,1m,5m"),
        (AGGREGATOR_ALLOWED_LATENESS_MS, "5000"),
        (AGGREGATOR_CHECKPOINT_INTERVAL_SECS, "5"),
        (
            AGGREGATOR_CHECKPOINT_BUCKET,
            "aggregator_shards_checkpoints",
        ),
        (AGGREGATOR_CHECKPOINT_PREFIX, "partition"),
        (AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS, "60"),
        (AGGREGATOR_REPLAY_START_POLICY, "checkpoint"),
        (AGGREGATOR_STATE_SCHEMA_VERSION, "4"),
        (AGGREGATOR_DURABLE_PREFIX, "aggregator"),
        (AGGREGATOR_LEASE_TTL_SECS, "30"),
        (AGGREGATOR_SHARD_SUBJECT_PREFIX, "aggregator.shards"),
        (AGGREGATOR_SHARD_COUNT, "16"),
    ];

    fn vars(values: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_map(values.iter().map(|(key, value)| (*key, *value)))
    }

    fn base_vars() -> Vec<(&'static str, &'static str)> {
        REQUIRED.to_vec()
    }

    fn with_value(
        mut values: Vec<(&'static str, &'static str)>,
        name: &'static str,
        value: &'static str,
    ) -> Vec<(&'static str, &'static str)> {
        if let Some(index) = values.iter().position(|(key, _)| *key == name) {
            values[index] = (name, value);
        } else {
            values.push((name, value));
        }
        values
    }

    #[test]
    fn parses_all_required_values() {
        let config = AggregatorConfig::from_vars(&vars(&base_vars())).unwrap();

        assert_eq!(config.shard_subject_prefix, "aggregator.shards");
        assert_eq!(config.gap_subject_prefix, "market.gaps");
        assert_eq!(config.shard_count, 16);
        assert_eq!(config.shard_index, None);
        assert_eq!(config.partitions().len(), 16);
        assert_eq!(config.checkpoint_bucket(), "aggregator_shards_checkpoints");
        assert_eq!(config.allowed_lateness, Duration::from_millis(5_000));
        assert_eq!(config.checkpoint_interval, Duration::from_secs(5));
        assert_eq!(config.checkpoint_key_prefix, "partition");
        assert_eq!(config.watermark_idle_timeout, Duration::from_secs(60));
        assert_eq!(config.replay_start_policy, ReplayStartPolicy::Checkpoint);
        assert_eq!(config.state_schema_version, STATE_SCHEMA_VERSION);
        assert_eq!(config.consumer_prefix, "aggregator");
        assert_eq!(config.router_durable_name(), "aggregator-router");
        assert_eq!(
            config.partition_durable_name(PartitionId::new(3)),
            "aggregator-shard-3"
        );
        assert_eq!(config.lease_ttl, Duration::from_secs(30));
    }

    #[test]
    fn every_value_is_required() {
        for (missing, _) in REQUIRED {
            let values = base_vars()
                .into_iter()
                .filter(|(name, _)| *name != missing)
                .collect::<Vec<_>>();

            assert!(
                AggregatorConfig::from_vars(&vars(&values)).is_err(),
                "{missing} should be required"
            );
        }
    }

    #[test]
    fn a_process_can_own_a_single_shard() {
        let values = with_value(base_vars(), AGGREGATOR_SHARD_COUNT, "8");
        let values = with_value(values, AGGREGATOR_SHARD_INDEX, "3");
        let values = with_value(values, AGGREGATOR_SHARD_SUBJECT_PREFIX, "aggregator.routed");
        let config = AggregatorConfig::from_vars(&vars(&values)).unwrap();

        assert_eq!(config.partitions(), vec![PartitionId::new(3)]);
        assert_eq!(config.shard_subject_prefix, "aggregator.routed");
    }

    #[test]
    fn gap_prefix_can_be_configured() {
        let values = with_value(base_vars(), AGGREGATOR_GAP_SUBJECT_PREFIX, "custom.gaps");
        let config = AggregatorConfig::from_vars(&vars(&values)).unwrap();

        assert_eq!(config.gap_subject_prefix, "custom.gaps");
    }

    #[test]
    fn shard_count_must_not_exceed_the_routing_bucket_count() {
        let values = with_value(base_vars(), AGGREGATOR_SHARD_COUNT, "65537");
        assert!(AggregatorConfig::from_vars(&vars(&values)).is_err());

        let values = with_value(base_vars(), AGGREGATOR_SHARD_COUNT, "65536");
        let config = AggregatorConfig::from_vars(&vars(&values)).unwrap();
        assert_eq!(config.shard_count, 65_536);
    }

    #[test]
    fn shard_index_must_be_in_range() {
        let values = with_value(base_vars(), AGGREGATOR_SHARD_COUNT, "8");
        let values = with_value(values, AGGREGATOR_SHARD_INDEX, "8");

        assert!(AggregatorConfig::from_vars(&vars(&values)).is_err());
    }

    #[test]
    fn checkpoint_interval_must_fit_inside_the_lease_ttl() {
        let values = with_value(base_vars(), AGGREGATOR_CHECKPOINT_INTERVAL_SECS, "30");
        let values = with_value(values, AGGREGATOR_LEASE_TTL_SECS, "30");

        assert!(AggregatorConfig::from_vars(&vars(&values)).is_err());
    }

    #[test]
    fn lateness_and_lease_settings_are_configurable() {
        let values = with_value(base_vars(), AGGREGATOR_ALLOWED_LATENESS_MS, "1500");
        let values = with_value(values, AGGREGATOR_CHECKPOINT_INTERVAL_SECS, "2");
        let values = with_value(values, AGGREGATOR_LEASE_TTL_SECS, "20");
        let config = AggregatorConfig::from_vars(&vars(&values)).unwrap();

        assert_eq!(config.allowed_lateness, Duration::from_millis(1_500));
        assert_eq!(config.processor_config().allowed_lateness.0, 1_500);
        assert_eq!(config.checkpoint_interval, Duration::from_secs(2));
        assert_eq!(config.lease_ttl, Duration::from_secs(20));
    }

    #[test]
    fn checkpoint_and_durable_prefixes_are_configurable() {
        let values = with_value(
            base_vars(),
            AGGREGATOR_CHECKPOINT_BUCKET,
            "shared_checkpoints",
        );
        let values = with_value(values, AGGREGATOR_CHECKPOINT_PREFIX, "production.partition");
        let values = with_value(values, AGGREGATOR_DURABLE_PREFIX, "market-aggregator");
        let config = AggregatorConfig::from_vars(&vars(&values)).unwrap();

        assert_eq!(config.checkpoint_bucket(), "shared_checkpoints");
        assert_eq!(config.checkpoint_key_prefix, "production.partition");
        assert_eq!(config.router_durable_name(), "market-aggregator-router");
        assert_eq!(
            config.partition_durable_name(PartitionId::new(7)),
            "market-aggregator-shard-7"
        );
    }

    #[test]
    fn replay_watermark_and_schema_settings_are_configurable() {
        let values = with_value(base_vars(), AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS, "15");
        let values = with_value(values, AGGREGATOR_REPLAY_START_POLICY, "latest");
        let values = with_value(values, AGGREGATOR_STATE_SCHEMA_VERSION, "9");
        let config = AggregatorConfig::from_vars(&vars(&values)).unwrap();

        assert_eq!(config.watermark_idle_timeout, Duration::from_secs(15));
        assert_eq!(config.replay_start_policy, ReplayStartPolicy::Latest);
        assert_eq!(config.state_schema_version, 9);
        assert_eq!(config.processor_config().state_schema_version, 9);
    }

    #[test]
    fn replay_start_policy_accepts_only_known_values() {
        assert_eq!(
            ReplayStartPolicy::parse("checkpoint"),
            Some(ReplayStartPolicy::Checkpoint)
        );
        assert_eq!(
            ReplayStartPolicy::parse("rebuild"),
            Some(ReplayStartPolicy::Archive)
        );
        assert_eq!(
            ReplayStartPolicy::parse("BEGINNING"),
            Some(ReplayStartPolicy::Earliest)
        );
        assert_eq!(
            ReplayStartPolicy::parse("new"),
            Some(ReplayStartPolicy::Latest)
        );
        assert_eq!(ReplayStartPolicy::parse("surprise"), None);

        let values = with_value(base_vars(), AGGREGATOR_REPLAY_START_POLICY, "surprise");
        assert!(AggregatorConfig::from_vars(&vars(&values)).is_err());
    }

    #[test]
    fn invalid_checkpoint_bucket_is_rejected() {
        let values = with_value(base_vars(), AGGREGATOR_CHECKPOINT_BUCKET, "not/a/bucket");

        assert!(AggregatorConfig::from_vars(&vars(&values)).is_err());
    }
}
