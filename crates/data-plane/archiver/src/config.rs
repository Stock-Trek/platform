use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_nats::NatsSubject;
use std::time::Duration;

const ARCHIVER_NATS_URL: &str = "ARCHIVER_NATS_URL";
const ARCHIVER_EVENT_SUBJECT_PREFIX: &str = "ARCHIVER_EVENT_SUBJECT_PREFIX";
const ARCHIVER_CLOSED_CANDLE_SUBJECT_PREFIX: &str = "ARCHIVER_CLOSED_CANDLE_SUBJECT_PREFIX";
const ARCHIVER_S3_BUCKET: &str = "ARCHIVER_S3_BUCKET";
const ARCHIVER_S3_PREFIX: &str = "ARCHIVER_S3_PREFIX";
const ARCHIVER_FLUSH_INTERVAL_SECS: &str = "ARCHIVER_FLUSH_INTERVAL_SECS";
const ARCHIVER_BUCKET_SECS: &str = "ARCHIVER_BUCKET_SECS";
const ARCHIVER_MAX_RECORDS: &str = "ARCHIVER_MAX_RECORDS";
const ARCHIVER_DEDUPE_CAPACITY: &str = "ARCHIVER_DEDUPE_CAPACITY";
const RUST_LOG: &str = "RUST_LOG";

const DEFAULT_S3_PREFIX: &str = "market-data/raw";
const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 60;
const DEFAULT_BUCKET_SECS: u64 = 3_600;
const DEFAULT_MAX_RECORDS: usize = 10_000;
const DEFAULT_DEDUPE_CAPACITY: usize = 100_000;

#[derive(Debug, Clone)]
pub struct ArchiverConfig {
    pub log_filter: String,
    pub nats_url: String,
    pub event_subject_prefix: String,
    pub closed_candle_subject_prefix: String,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub flush_interval: Duration,
    pub bucket_millis: i64,
    pub max_records: usize,
    pub dedupe_capacity: usize,
}

impl ArchiverConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let log_filter = vars.required_log_filter(RUST_LOG)?;
        let nats_url = vars.required_url(ARCHIVER_NATS_URL, &["nats"])?;
        let event_subject_prefix = vars.required(ARCHIVER_EVENT_SUBJECT_PREFIX)?;
        NatsSubject::validate(&event_subject_prefix)?;
        let closed_candle_subject_prefix = vars.required(ARCHIVER_CLOSED_CANDLE_SUBJECT_PREFIX)?;
        NatsSubject::validate(&closed_candle_subject_prefix)?;
        let s3_bucket = vars.required(ARCHIVER_S3_BUCKET)?;
        let s3_prefix = vars
            .optional(ARCHIVER_S3_PREFIX)?
            .unwrap_or_else(|| DEFAULT_S3_PREFIX.to_string());

        let flush_interval_secs =
            vars.optional_parsed_or(ARCHIVER_FLUSH_INTERVAL_SECS, DEFAULT_FLUSH_INTERVAL_SECS)?;
        if flush_interval_secs == 0 {
            return Err(BpError::config_invalid(
                ARCHIVER_FLUSH_INTERVAL_SECS,
                "must be greater than zero",
            ));
        }

        let bucket_secs = vars.optional_parsed_or(ARCHIVER_BUCKET_SECS, DEFAULT_BUCKET_SECS)?;
        if bucket_secs == 0 {
            return Err(BpError::config_invalid(
                ARCHIVER_BUCKET_SECS,
                "must be greater than zero",
            ));
        }
        let bucket_millis = bucket_secs
            .checked_mul(1_000)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| {
                BpError::config_invalid(ARCHIVER_BUCKET_SECS, "is too large to represent")
            })?;

        let max_records = vars.optional_parsed_or(ARCHIVER_MAX_RECORDS, DEFAULT_MAX_RECORDS)?;
        if max_records == 0 {
            return Err(BpError::config_invalid(
                ARCHIVER_MAX_RECORDS,
                "must be greater than zero",
            ));
        }

        let dedupe_capacity =
            vars.optional_parsed_or(ARCHIVER_DEDUPE_CAPACITY, DEFAULT_DEDUPE_CAPACITY)?;
        if dedupe_capacity == 0 {
            return Err(BpError::config_invalid(
                ARCHIVER_DEDUPE_CAPACITY,
                "must be greater than zero",
            ));
        }

        Ok(Self {
            log_filter,
            nats_url,
            event_subject_prefix,
            closed_candle_subject_prefix,
            s3_bucket,
            s3_prefix,
            flush_interval: Duration::from_secs(flush_interval_secs),
            bucket_millis,
            max_records,
            dedupe_capacity,
        })
    }
}
