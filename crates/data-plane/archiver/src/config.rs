use platform_archive::IcebergConfig;
use platform_config::EnvVars;
use platform_errors::{PlatformError, PlatformResult};
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
const ARCHIVER_LEASE_BUCKET: &str = "ARCHIVER_LEASE_BUCKET";
const ARCHIVER_LEASE_TTL_SECS: &str = "ARCHIVER_LEASE_TTL_SECS";
const ARCHIVER_MAX_WORK_SECS: &str = "ARCHIVER_MAX_WORK_SECS";
const ARCHIVER_LEASE_INITIAL_TOKEN: &str = "ARCHIVER_LEASE_INITIAL_TOKEN";
const ARCHIVER_LEASE_CLOCK_SKEW_SECS: &str = "ARCHIVER_LEASE_CLOCK_SKEW_SECS";
const ARCHIVER_RECOVERY_PAGE_FILES: &str = "ARCHIVER_RECOVERY_PAGE_FILES";
const ARCHIVER_RECOVERY_PAGE_BYTES: &str = "ARCHIVER_RECOVERY_PAGE_BYTES";

#[derive(Debug, Clone)]
pub(crate) struct ArchiverConfig {
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
    pub lease_bucket: String,
    pub lease_ttl: Duration,
    pub max_work: Duration,
    pub lease_initial_token: u64,
    pub lease_clock_skew: Duration,
    pub recovery_page_files: usize,
    pub recovery_page_bytes: usize,
    pub iceberg: IcebergConfig,
}

impl ArchiverConfig {
    pub(crate) fn from_env() -> PlatformResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub(crate) fn from_vars(vars: &EnvVars) -> PlatformResult<Self> {
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
            PlatformError::config_invalid(ARCHIVER_BUCKET_SECS, "is too large to represent")
        })?;

        let max_records = Config::required_positive_usize(vars, ARCHIVER_MAX_RECORDS)?;
        let dedupe_capacity = Config::required_positive_usize(vars, ARCHIVER_DEDUPE_CAPACITY)?;
        let lease_bucket = vars.required(ARCHIVER_LEASE_BUCKET)?;
        let lease_ttl = Config::required_positive_duration_secs(vars, ARCHIVER_LEASE_TTL_SECS)?;
        let lease_clock_skew = Self::clock_skew(vars, lease_ttl)?;
        let effective_ttl = lease_ttl.saturating_sub(lease_clock_skew);
        let max_work = Config::required_positive_duration_secs(vars, ARCHIVER_MAX_WORK_SECS)?;
        if max_work >= effective_ttl {
            return Err(PlatformError::config_invalid(
                ARCHIVER_MAX_WORK_SECS,
                "must be shorter than the skew-adjusted ARCHIVER_LEASE_TTL_SECS",
            ));
        }
        platform_lease::LeaseRequest::renewal_interval(lease_ttl)?;
        let initial_token = vars
            .optional(ARCHIVER_LEASE_INITIAL_TOKEN)?
            .map(|value| {
                value.parse::<i64>().map_err(|error| {
                    PlatformError::config_invalid_source(ARCHIVER_LEASE_INITIAL_TOKEN, error)
                })
            })
            .transpose()?
            .unwrap_or(1);
        let lease_initial_token =
            platform_lease::LeaseRequest::validate_initial_token(initial_token)?;
        let recovery_page_files =
            Config::required_positive_usize(vars, ARCHIVER_RECOVERY_PAGE_FILES)?;
        let recovery_page_bytes =
            Config::required_positive_usize(vars, ARCHIVER_RECOVERY_PAGE_BYTES)?;
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
            lease_bucket,
            lease_ttl,
            max_work,
            lease_initial_token,
            lease_clock_skew,
            recovery_page_files,
            recovery_page_bytes,
            iceberg,
        })
    }

    fn clock_skew(vars: &EnvVars, lease_ttl: Duration) -> PlatformResult<Duration> {
        let Some(value) = vars.optional(ARCHIVER_LEASE_CLOCK_SKEW_SECS)? else {
            return Ok(Self::default_clock_skew(lease_ttl));
        };
        let seconds = value.parse::<u64>().map_err(|error| {
            PlatformError::config_invalid_source(ARCHIVER_LEASE_CLOCK_SKEW_SECS, error)
        })?;
        let skew = Duration::from_secs(seconds);
        if skew >= lease_ttl {
            return Err(PlatformError::config_invalid(
                ARCHIVER_LEASE_CLOCK_SKEW_SECS,
                "must be shorter than ARCHIVER_LEASE_TTL_SECS",
            ));
        }
        let renewal = platform_lease::LeaseRequest::renewal_interval(lease_ttl)?;
        if skew > lease_ttl.saturating_sub(renewal) {
            return Err(PlatformError::config_invalid(
                ARCHIVER_LEASE_CLOCK_SKEW_SECS,
                "must leave room for the renewal interval inside the lease TTL",
            ));
        }
        Ok(skew)
    }

    fn default_clock_skew(lease_ttl: Duration) -> Duration {
        Duration::from_secs(5).min(lease_ttl / 4)
    }

    /// Acknowledgement window used by the archiver's durable consumers.
    ///
    /// Messages are only acknowledged after a successful durable flush, so the
    /// window must exceed the flush interval. The extra [`ACK_WAIT`] margin
    /// leaves time for the flush itself without triggering redelivery.
    pub(crate) fn ack_wait(&self) -> Duration {
        self.flush_interval.saturating_add(ACK_WAIT)
    }
}
