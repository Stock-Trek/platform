use platform_archive::IcebergConfig;
use platform_config::EnvVars;
use platform_errors::PlatformResult;
use platform_runtime::Config;
use std::time::Duration;

const COMPACTOR_S3_BUCKET: &str = "COMPACTOR_S3_BUCKET";
const COMPACTOR_S3_PREFIX: &str = "COMPACTOR_S3_PREFIX";
const COMPACTOR_OUTPUT_PREFIX: &str = "COMPACTOR_OUTPUT_PREFIX";
const COMPACTOR_POLL_INTERVAL_SECS: &str = "COMPACTOR_POLL_INTERVAL_SECS";
const COMPACTOR_SETTLE_SECS: &str = "COMPACTOR_SETTLE_SECS";
const COMPACTOR_MIN_SOURCE_FILES: &str = "COMPACTOR_MIN_SOURCE_FILES";
const COMPACTOR_SPILL_RECORDS: &str = "COMPACTOR_SPILL_RECORDS";
const COMPACTOR_NATS_URL: &str = "COMPACTOR_NATS_URL";
const COMPACTOR_LEASE_BUCKET: &str = "COMPACTOR_LEASE_BUCKET";
const COMPACTOR_LEASE_TTL_SECS: &str = "COMPACTOR_LEASE_TTL_SECS";
const COMPACTOR_LEASE_TTL_SECS_EVENTS: &str = "COMPACTOR_LEASE_TTL_SECS_EVENTS";
const COMPACTOR_LEASE_TTL_SECS_CANDLES: &str = "COMPACTOR_LEASE_TTL_SECS_CANDLES";
const COMPACTOR_MAX_WORK_SECS: &str = "COMPACTOR_MAX_WORK_SECS";
const COMPACTOR_LEASE_INITIAL_TOKEN: &str = "COMPACTOR_LEASE_INITIAL_TOKEN";
const COMPACTOR_LEASE_CLOCK_SKEW_SECS: &str = "COMPACTOR_LEASE_CLOCK_SKEW_SECS";

#[derive(Debug, Clone)]
pub struct CompactorConfig {
    pub log_filter: String,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub output_prefix: String,
    pub poll_interval: Duration,
    pub settle: Duration,
    pub min_source_files: usize,
    pub spill_records: usize,
    pub nats_url: String,
    pub lease_bucket: String,
    pub lease_ttl_events: Duration,
    pub lease_ttl_candles: Duration,
    pub max_work: Duration,
    pub lease_initial_token: u64,
    pub lease_clock_skew: Duration,
    pub iceberg: IcebergConfig,
}

impl CompactorConfig {
    pub fn from_env() -> PlatformResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> PlatformResult<Self> {
        let log_filter = Config::log_filter(vars)?;
        let s3_bucket = vars.required(COMPACTOR_S3_BUCKET)?;
        let s3_prefix = vars.required(COMPACTOR_S3_PREFIX)?;
        let output_prefix = vars.required(COMPACTOR_OUTPUT_PREFIX)?;
        let poll_interval =
            Config::required_positive_duration_secs(vars, COMPACTOR_POLL_INTERVAL_SECS)?;
        let settle = Config::required_positive_duration_secs(vars, COMPACTOR_SETTLE_SECS)?;
        let min_source_files = Config::required_positive_usize(vars, COMPACTOR_MIN_SOURCE_FILES)?;
        let spill_records = Config::required_positive_usize(vars, COMPACTOR_SPILL_RECORDS)?;
        let nats_url = Config::required_nats_url(vars, COMPACTOR_NATS_URL)?;
        let lease_bucket = vars.required(COMPACTOR_LEASE_BUCKET)?;
        let lease_ttl = Config::required_positive_duration_secs(vars, COMPACTOR_LEASE_TTL_SECS)?;
        let lease_ttl_events =
            Self::optional_duration_secs(vars, COMPACTOR_LEASE_TTL_SECS_EVENTS, lease_ttl)?;
        let lease_ttl_candles =
            Self::optional_duration_secs(vars, COMPACTOR_LEASE_TTL_SECS_CANDLES, lease_ttl)?;
        let lease_clock_skew = Self::clock_skew(vars, lease_ttl_events, lease_ttl_candles)?;
        let max_work = Config::required_positive_duration_secs(vars, COMPACTOR_MAX_WORK_SECS)?;
        for (kind, ttl) in [
            (platform_archive::ArchiveKind::Events, lease_ttl_events),
            (platform_archive::ArchiveKind::Candles, lease_ttl_candles),
        ] {
            if max_work >= ttl.saturating_sub(lease_clock_skew) {
                return Err(platform_errors::PlatformError::config_invalid(
                    COMPACTOR_MAX_WORK_SECS,
                    format!(
                        "must be shorter than the skew-adjusted {} lease TTL",
                        kind.label()
                    ),
                ));
            }
            platform_lease::LeaseRequest::renewal_interval(ttl)?;
        }
        let initial_token = vars
            .optional(COMPACTOR_LEASE_INITIAL_TOKEN)?
            .map(|value| {
                value.parse::<i64>().map_err(|error| {
                    platform_errors::PlatformError::config_invalid_source(
                        COMPACTOR_LEASE_INITIAL_TOKEN,
                        error,
                    )
                })
            })
            .transpose()?
            .unwrap_or(1);
        let lease_initial_token =
            platform_lease::LeaseRequest::validate_initial_token(initial_token)?;
        let iceberg = IcebergConfig::from_vars(vars)?;

        Ok(Self {
            log_filter,
            s3_bucket,
            s3_prefix,
            output_prefix,
            poll_interval,
            settle,
            min_source_files,
            spill_records,
            nats_url,
            lease_bucket,
            lease_ttl_events,
            lease_ttl_candles,
            max_work,
            lease_initial_token,
            lease_clock_skew,
            iceberg,
        })
    }

    fn clock_skew(
        vars: &EnvVars,
        lease_ttl_events: Duration,
        lease_ttl_candles: Duration,
    ) -> PlatformResult<Duration> {
        let Some(value) = vars.optional(COMPACTOR_LEASE_CLOCK_SKEW_SECS)? else {
            return Ok(Self::default_clock_skew(
                lease_ttl_events,
                lease_ttl_candles,
            ));
        };
        let seconds = value.parse::<u64>().map_err(|error| {
            platform_errors::PlatformError::config_invalid_source(
                COMPACTOR_LEASE_CLOCK_SKEW_SECS,
                error,
            )
        })?;
        let skew = Duration::from_secs(seconds);
        if skew >= lease_ttl_events || skew >= lease_ttl_candles {
            return Err(platform_errors::PlatformError::config_invalid(
                COMPACTOR_LEASE_CLOCK_SKEW_SECS,
                "must be shorter than every lease TTL",
            ));
        }
        for ttl in [lease_ttl_events, lease_ttl_candles] {
            let renewal = platform_lease::LeaseRequest::renewal_interval(ttl)?;
            if skew > ttl.saturating_sub(renewal) {
                return Err(platform_errors::PlatformError::config_invalid(
                    COMPACTOR_LEASE_CLOCK_SKEW_SECS,
                    "must leave room for every renewal interval inside its lease TTL",
                ));
            }
        }
        Ok(skew)
    }

    fn default_clock_skew(lease_ttl_events: Duration, lease_ttl_candles: Duration) -> Duration {
        Duration::from_secs(5).min(lease_ttl_events.min(lease_ttl_candles) / 4)
    }

    fn optional_duration_secs(
        vars: &EnvVars,
        name: &'static str,
        default: Duration,
    ) -> PlatformResult<Duration> {
        match vars.optional(name)? {
            Some(value) => {
                let seconds = value.parse::<u64>().map_err(|error| {
                    platform_errors::PlatformError::config_invalid_source(name, error)
                })?;
                if seconds == 0 {
                    return Err(platform_errors::PlatformError::config_invalid(
                        name,
                        "must be greater than zero",
                    ));
                }
                Ok(Duration::from_secs(seconds))
            }
            None => Ok(default),
        }
    }

    pub fn lease_ttl_for(&self, kind: platform_archive::ArchiveKind) -> Duration {
        match kind {
            platform_archive::ArchiveKind::Events => self.lease_ttl_events,
            platform_archive::ArchiveKind::Candles => self.lease_ttl_candles,
        }
    }
}
