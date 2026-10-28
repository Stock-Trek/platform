use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_market_data::candle::CandleInterval;
use platform_runtime::Config;
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
        let log_filter = Config::log_filter(vars)?;
        let nats_url = Config::required_nats_url(vars, AGGREGATOR_NATS_URL)?;
        let event_subject_prefix =
            Config::required_nats_subject(vars, AGGREGATOR_EVENT_SUBJECT_PREFIX)?;
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

        let flush_interval = Config::optional_positive_duration_secs(
            vars,
            AGGREGATOR_FLUSH_INTERVAL_SECS,
            DEFAULT_FLUSH_INTERVAL_SECS,
        )?;

        Ok(Self {
            log_filter,
            nats_url,
            event_subject_prefix,
            open_candle_subject_prefix,
            closed_candle_subject_prefix,
            intervals,
            flush_interval,
        })
    }
}
