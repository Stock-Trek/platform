use super::aggregator::AggregatorConfig;
use super::env::{
    AGGREGATOR_ALLOWED_LATENESS_MS, AGGREGATOR_CHECKPOINT_BUCKET,
    AGGREGATOR_CHECKPOINT_INTERVAL_SECS, AGGREGATOR_CHECKPOINT_PREFIX,
    AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX, AGGREGATOR_DURABLE_PREFIX,
    AGGREGATOR_EVENT_SUBJECT_PREFIX, AGGREGATOR_GAP_SUBJECT_PREFIX, AGGREGATOR_INTERVALS,
    AGGREGATOR_LEASE_TTL_SECS, AGGREGATOR_NATS_URL, AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX,
    AGGREGATOR_REPLAY_START_POLICY, AGGREGATOR_SHARD_COUNT, AGGREGATOR_SHARD_INDEX,
    AGGREGATOR_SHARD_SUBJECT_PREFIX, AGGREGATOR_STATE_SCHEMA_VERSION,
    AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS,
};
use super::replay::ReplayStartPolicy;
use super::validation::validate_kv_bucket;
use platform_archive::RoutingBucket;
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::CandleInterval;
use platform_nats::NatsSubject;
use platform_runtime::Config;
use std::time::Duration;

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
}
