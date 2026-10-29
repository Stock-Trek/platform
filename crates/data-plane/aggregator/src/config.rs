use crate::partition::{PartitionId, all_partitions};
use crate::processor::ProcessorConfig;
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::CandleInterval;
use platform_nats::NatsSubject;
use platform_runtime::Config;
use platform_types::new_types::DurationMilliseconds;
use std::time::Duration;

pub const AGGREGATOR_NATS_URL: &str = "AGGREGATOR_NATS_URL";
pub const AGGREGATOR_EVENT_SUBJECT_PREFIX: &str = "AGGREGATOR_EVENT_SUBJECT_PREFIX";
pub const AGGREGATOR_GAP_SUBJECT_PREFIX: &str = "AGGREGATOR_GAP_SUBJECT_PREFIX";
pub const AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX: &str = "AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX";
pub const AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX: &str = "AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX";
pub const AGGREGATOR_INTERVALS: &str = "AGGREGATOR_INTERVALS";
pub const AGGREGATOR_ALLOWED_LATENESS_MS: &str = "AGGREGATOR_ALLOWED_LATENESS_MS";
pub const AGGREGATOR_CHECKPOINT_INTERVAL_SECS: &str = "AGGREGATOR_CHECKPOINT_INTERVAL_SECS";
pub const AGGREGATOR_LEASE_TTL_SECS: &str = "AGGREGATOR_LEASE_TTL_SECS";
pub const AGGREGATOR_SHARD_SUBJECT_PREFIX: &str = "AGGREGATOR_SHARD_SUBJECT_PREFIX";
pub const AGGREGATOR_SHARD_COUNT: &str = "AGGREGATOR_SHARD_COUNT";
pub const AGGREGATOR_SHARD_INDEX: &str = "AGGREGATOR_SHARD_INDEX";

const DEFAULT_INTERVALS: &str = "1s,1m,5m";
const DEFAULT_ALLOWED_LATENESS_MS: u64 = 5_000;
const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 5;
const DEFAULT_LEASE_TTL_SECS: u64 = 30;
const DEFAULT_SHARD_SUBJECT_PREFIX: &str = "aggregator.shards";
const DEFAULT_SHARD_COUNT: usize = 16;

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
    /// Lifetime of the single-writer partition lease.
    pub lease_ttl: Duration,
}

/// Derives the conventional gap prefix from the event prefix.
///
/// `market.events` and `market.gaps` are siblings; a root-style prefix such as
/// `market` becomes `market.gaps`.
pub fn default_gap_subject_prefix(event_subject_prefix: &str) -> String {
    match event_subject_prefix.strip_suffix(".events") {
        Some(root) => format!("{root}.gaps"),
        None => format!("{event_subject_prefix}.gaps"),
    }
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
        let gap_subject_prefix = match vars.optional(AGGREGATOR_GAP_SUBJECT_PREFIX)? {
            Some(value) => {
                NatsSubject::validate_named(AGGREGATOR_GAP_SUBJECT_PREFIX, &value).map_err(
                    |error| {
                        BpError::config_invalid(AGGREGATOR_GAP_SUBJECT_PREFIX, error.to_string())
                    },
                )?;
                value
            }
            None => default_gap_subject_prefix(&event_subject_prefix),
        };
        let open_candle_subject_prefix =
            Config::required_nats_subject(vars, AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX)?;
        let closed_candle_subject_prefix =
            Config::required_nats_subject(vars, AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX)?;

        let interval_labels = vars
            .optional(AGGREGATOR_INTERVALS)?
            .unwrap_or_else(|| DEFAULT_INTERVALS.to_string());
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

        let checkpoint_interval = Config::optional_positive_duration_secs(
            vars,
            AGGREGATOR_CHECKPOINT_INTERVAL_SECS,
            DEFAULT_CHECKPOINT_INTERVAL_SECS,
        )?;
        let lease_ttl = Config::optional_positive_duration_secs(
            vars,
            AGGREGATOR_LEASE_TTL_SECS,
            DEFAULT_LEASE_TTL_SECS,
        )?;
        if checkpoint_interval >= lease_ttl {
            return Err(BpError::config_invalid(
                AGGREGATOR_CHECKPOINT_INTERVAL_SECS,
                format!("must be lower than {AGGREGATOR_LEASE_TTL_SECS}"),
            ));
        }
        let allowed_lateness_ms: u64 =
            vars.optional_parsed_or(AGGREGATOR_ALLOWED_LATENESS_MS, DEFAULT_ALLOWED_LATENESS_MS)?;

        let shard_subject_prefix = match vars.optional(AGGREGATOR_SHARD_SUBJECT_PREFIX)? {
            Some(value) => {
                NatsSubject::validate_named(AGGREGATOR_SHARD_SUBJECT_PREFIX, &value).map_err(
                    |error| {
                        BpError::config_invalid(AGGREGATOR_SHARD_SUBJECT_PREFIX, error.to_string())
                    },
                )?;
                value
            }
            None => DEFAULT_SHARD_SUBJECT_PREFIX.to_string(),
        };
        let shard_count =
            Config::optional_positive_usize(vars, AGGREGATOR_SHARD_COUNT, DEFAULT_SHARD_COUNT)?;
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
            lease_ttl,
        })
    }

    /// The KV bucket that stores source checkpoints for routed partitions.
    pub fn checkpoint_bucket(&self) -> String {
        crate::checkpoint::checkpoint_bucket(&self.shard_subject_prefix)
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
    }

    /// The partitions this process is responsible for.
    pub fn partitions(&self) -> Vec<PartitionId> {
        match self.shard_index {
            Some(index) => vec![PartitionId::new(index as u32)],
            None => all_partitions(self.shard_count),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(values: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_map(values.iter().map(|(key, value)| (*key, *value)))
    }

    fn base_vars() -> Vec<(&'static str, &'static str)> {
        vec![
            ("RUST_LOG", "info"),
            (AGGREGATOR_NATS_URL, "nats://localhost:4222"),
            (AGGREGATOR_EVENT_SUBJECT_PREFIX, "market.events"),
            (AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX, "market.candles.open"),
            (
                AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX,
                "market.candles.closed",
            ),
        ]
    }

    #[test]
    fn shard_settings_have_safe_defaults() {
        let config = AggregatorConfig::from_vars(&vars(&base_vars())).unwrap();

        assert_eq!(config.shard_subject_prefix, DEFAULT_SHARD_SUBJECT_PREFIX);
        assert_eq!(config.gap_subject_prefix, "market.gaps");
        assert_eq!(config.shard_count, DEFAULT_SHARD_COUNT);
        assert_eq!(config.shard_index, None);
        assert_eq!(config.partitions().len(), DEFAULT_SHARD_COUNT);
        assert_eq!(config.checkpoint_bucket(), "aggregator_shards_checkpoints");
        assert_eq!(config.allowed_lateness, Duration::from_millis(5_000));
        assert_eq!(config.checkpoint_interval, Duration::from_secs(5));
        assert_eq!(config.lease_ttl, Duration::from_secs(30));
    }

    #[test]
    fn a_process_can_own_a_single_shard() {
        let mut values = base_vars();
        values.extend([
            (AGGREGATOR_SHARD_COUNT, "8"),
            (AGGREGATOR_SHARD_INDEX, "3"),
            (AGGREGATOR_SHARD_SUBJECT_PREFIX, "aggregator.routed"),
        ]);
        let config = AggregatorConfig::from_vars(&vars(&values)).unwrap();

        assert_eq!(config.partitions(), vec![PartitionId::new(3)]);
        assert_eq!(config.shard_subject_prefix, "aggregator.routed");
    }

    #[test]
    fn gap_prefix_can_be_configured() {
        let mut values = base_vars();
        values.push((AGGREGATOR_GAP_SUBJECT_PREFIX, "custom.gaps"));
        let config = AggregatorConfig::from_vars(&vars(&values)).unwrap();

        assert_eq!(config.gap_subject_prefix, "custom.gaps");
    }

    #[test]
    fn gap_prefix_is_derived_from_root_and_category_prefixes() {
        assert_eq!(default_gap_subject_prefix("market.events"), "market.gaps");
        assert_eq!(default_gap_subject_prefix("market"), "market.gaps");
    }

    #[test]
    fn shard_index_must_be_in_range() {
        let mut values = base_vars();
        values.extend([(AGGREGATOR_SHARD_COUNT, "8"), (AGGREGATOR_SHARD_INDEX, "8")]);

        assert!(AggregatorConfig::from_vars(&vars(&values)).is_err());
    }

    #[test]
    fn checkpoint_interval_must_fit_inside_the_lease_ttl() {
        let mut values = base_vars();
        values.extend([
            (AGGREGATOR_CHECKPOINT_INTERVAL_SECS, "30"),
            (AGGREGATOR_LEASE_TTL_SECS, "30"),
        ]);

        assert!(AggregatorConfig::from_vars(&vars(&values)).is_err());
    }

    #[test]
    fn lateness_and_lease_settings_are_configurable() {
        let mut values = base_vars();
        values.extend([
            (AGGREGATOR_ALLOWED_LATENESS_MS, "1500"),
            (AGGREGATOR_CHECKPOINT_INTERVAL_SECS, "2"),
            (AGGREGATOR_LEASE_TTL_SECS, "20"),
        ]);
        let config = AggregatorConfig::from_vars(&vars(&values)).unwrap();

        assert_eq!(config.allowed_lateness, Duration::from_millis(1_500));
        assert_eq!(config.processor_config().allowed_lateness.0, 1_500);
        assert_eq!(config.checkpoint_interval, Duration::from_secs(2));
        assert_eq!(config.lease_ttl, Duration::from_secs(20));
    }
}
