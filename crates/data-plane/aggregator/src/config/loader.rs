use super::aggregator::AggregatorConfig;
use super::env::{
    AGGREGATOR_ALLOWED_LATENESS_MS, AGGREGATOR_CHECKPOINT_BUCKET,
    AGGREGATOR_CHECKPOINT_INTERVAL_SECS, AGGREGATOR_CHECKPOINT_PREFIX,
    AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES, AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX,
    AGGREGATOR_CORRECTION_CUTOFF_MS, AGGREGATOR_DURABLE_PREFIX, AGGREGATOR_EVENT_SUBJECT_PREFIX,
    AGGREGATOR_GAP_SUBJECT_PREFIX, AGGREGATOR_LEASE_TTL_SECS,
    AGGREGATOR_MAX_CLOSED_CANDLES_PER_INTERVAL, AGGREGATOR_NATS_URL,
    AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX, AGGREGATOR_REPLAY_START_POLICY, AGGREGATOR_SHARD_COUNT,
    AGGREGATOR_SHARD_INDEX, AGGREGATOR_SHARD_SUBJECT_PREFIX, AGGREGATOR_STATE_SCHEMA_VERSION,
    AGGREGATOR_WATERMARK_IDLE_TIMEOUT_SECS,
};
use super::replay::ReplayStartPolicy;
use super::validation::{
    parse_correction_cutoff, parse_max_closed_candles_per_interval, validate_kv_bucket,
    validate_retention,
};
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

        let intervals = CandleInterval::all();

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
        let allowed_lateness = Duration::from_millis(allowed_lateness_ms);
        let max_closed_candles_per_interval = parse_max_closed_candles_per_interval(
            AGGREGATOR_MAX_CLOSED_CANDLES_PER_INTERVAL,
            &vars.required(AGGREGATOR_MAX_CLOSED_CANDLES_PER_INTERVAL)?,
            &intervals,
        )?;
        let correction_cutoff_ms: u64 = vars.required_parsed(AGGREGATOR_CORRECTION_CUTOFF_MS)?;
        let correction_cutoff = parse_correction_cutoff(
            AGGREGATOR_CORRECTION_CUTOFF_MS,
            correction_cutoff_ms,
            allowed_lateness,
        )?;
        let memory_budget_bytes: u64 =
            vars.required_parsed(AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES)?;
        let closed_candle_memory_budget_bytes =
            usize::try_from(memory_budget_bytes).map_err(|error| {
                BpError::config_invalid_source(AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES, error)
            })?;
        if closed_candle_memory_budget_bytes == 0 {
            return Err(BpError::config_invalid(
                AGGREGATOR_CLOSED_CANDLE_MEMORY_BUDGET_BYTES,
                "must be greater than zero",
            ));
        }

        let shard_subject_prefix =
            Config::required_nats_subject(vars, AGGREGATOR_SHARD_SUBJECT_PREFIX)?;
        let shard_count = Config::required_positive_usize(vars, AGGREGATOR_SHARD_COUNT)?;
        validate_retention(
            &intervals,
            &max_closed_candles_per_interval,
            shard_count,
            allowed_lateness,
            correction_cutoff_ms,
            closed_candle_memory_budget_bytes,
        )?;
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
            allowed_lateness,
            max_closed_candles_per_interval,
            correction_cutoff,
            closed_candle_memory_budget_bytes,
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
