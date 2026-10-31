use platform_archive::IcebergConfig;
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_nats::ACK_WAIT;
use platform_runtime::Config;
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
    pub iceberg: IcebergConfig,
}

impl ArchiverConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let log_filter = Config::log_filter(vars)?;
        let nats_url = Config::required_nats_url(vars, ARCHIVER_NATS_URL)?;
        let event_subject_prefix =
            Config::required_nats_subject(vars, ARCHIVER_EVENT_SUBJECT_PREFIX)?;
        let closed_candle_subject_prefix =
            Config::required_nats_subject(vars, ARCHIVER_CLOSED_CANDLE_SUBJECT_PREFIX)?;
        let s3_bucket = vars.required(ARCHIVER_S3_BUCKET)?;
        let s3_prefix = vars.required(ARCHIVER_S3_PREFIX)?;
        let flush_interval =
            Config::required_positive_duration_secs(vars, ARCHIVER_FLUSH_INTERVAL_SECS)?;
        let bucket = Config::required_positive_duration_secs(vars, ARCHIVER_BUCKET_SECS)?;
        let bucket_millis = i64::try_from(bucket.as_millis()).map_err(|_| {
            BpError::config_invalid(ARCHIVER_BUCKET_SECS, "is too large to represent")
        })?;

        let max_records = Config::required_positive_usize(vars, ARCHIVER_MAX_RECORDS)?;
        let dedupe_capacity = Config::required_positive_usize(vars, ARCHIVER_DEDUPE_CAPACITY)?;
        let iceberg = IcebergConfig::from_vars(vars)?;

        Ok(Self {
            log_filter,
            nats_url,
            event_subject_prefix,
            closed_candle_subject_prefix,
            s3_bucket,
            s3_prefix,
            flush_interval,
            bucket_millis,
            max_records,
            dedupe_capacity,
            iceberg,
        })
    }

    /// Acknowledgement window used by the archiver's durable consumers.
    ///
    /// Messages are only acknowledged after a successful durable flush, so the
    /// window must exceed the flush interval. The extra [`ACK_WAIT`] margin
    /// leaves time for the flush itself without triggering redelivery.
    pub fn ack_wait(&self) -> Duration {
        self.flush_interval.saturating_add(ACK_WAIT)
    }
}
