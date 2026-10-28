use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::CandleInterval;
use platform_nats::NatsSubject;
use std::time::Duration;

pub const AGGREGATOR_NATS_URL: &str = "AGGREGATOR_NATS_URL";
pub const AGGREGATOR_EVENT_SUBJECT_PREFIX: &str = "AGGREGATOR_EVENT_SUBJECT_PREFIX";
pub const AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX: &str = "AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX";
pub const AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX: &str = "AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX";
pub const AGGREGATOR_INTERVALS: &str = "AGGREGATOR_INTERVALS";
pub const AGGREGATOR_FLUSH_INTERVAL_SECS: &str = "AGGREGATOR_FLUSH_INTERVAL_SECS";

const DEFAULT_INTERVALS: &str = "1s,1m,5m";
const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 1;

#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    pub log_filter: String,
    pub nats_url: String,
    pub event_subject_prefix: String,
    pub open_candle_subject_prefix: String,
    pub closed_candle_subject_prefix: String,
    pub intervals: Vec<CandleInterval>,
    pub flush_interval: Duration,
}

impl AggregatorConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let log_filter = vars.required_log_filter("RUST_LOG")?;
        let nats_url = vars.required_url(AGGREGATOR_NATS_URL, &["nats"])?;
        let event_subject_prefix = vars.required(AGGREGATOR_EVENT_SUBJECT_PREFIX)?;
        let open_candle_subject_prefix = vars.required(AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX)?;
        let closed_candle_subject_prefix =
            vars.required(AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX)?;
        Self::validate_subject_prefix(AGGREGATOR_EVENT_SUBJECT_PREFIX, &event_subject_prefix)?;
        Self::validate_subject_prefix(
            AGGREGATOR_OPEN_CANDLE_SUBJECT_PREFIX,
            &open_candle_subject_prefix,
        )?;
        Self::validate_subject_prefix(
            AGGREGATOR_CLOSED_CANDLE_SUBJECT_PREFIX,
            &closed_candle_subject_prefix,
        )?;

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

        let flush_interval_secs =
            vars.optional_parsed_or(AGGREGATOR_FLUSH_INTERVAL_SECS, DEFAULT_FLUSH_INTERVAL_SECS)?;
        if flush_interval_secs == 0 {
            return Err(BpError::config_invalid(
                AGGREGATOR_FLUSH_INTERVAL_SECS,
                "must be greater than zero",
            ));
        }

        Ok(Self {
            log_filter,
            nats_url,
            event_subject_prefix,
            open_candle_subject_prefix,
            closed_candle_subject_prefix,
            intervals,
            flush_interval: Duration::from_secs(flush_interval_secs),
        })
    }

    fn validate_subject_prefix(name: &str, value: &str) -> BpResult<()> {
        NatsSubject::validate(value)
            .map_err(|error| BpError::config_invalid(name, error.to_string()))
    }
}
