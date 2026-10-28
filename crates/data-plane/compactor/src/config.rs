use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_runtime::Config;
use std::time::Duration;

const COMPACTOR_S3_BUCKET: &str = "COMPACTOR_S3_BUCKET";
const COMPACTOR_S3_PREFIX: &str = "COMPACTOR_S3_PREFIX";
const COMPACTOR_OUTPUT_PREFIX: &str = "COMPACTOR_OUTPUT_PREFIX";
const COMPACTOR_PROCESSED_PREFIX: &str = "COMPACTOR_PROCESSED_PREFIX";
const COMPACTOR_POLL_INTERVAL_SECS: &str = "COMPACTOR_POLL_INTERVAL_SECS";
const COMPACTOR_SETTLE_SECS: &str = "COMPACTOR_SETTLE_SECS";
const COMPACTOR_MIN_SOURCE_FILES: &str = "COMPACTOR_MIN_SOURCE_FILES";
const COMPACTOR_SPILL_RECORDS: &str = "COMPACTOR_SPILL_RECORDS";

const DEFAULT_S3_PREFIX: &str = "market-data/raw";
const DEFAULT_OUTPUT_PREFIX: &str = "market-data/compacted";
const DEFAULT_PROCESSED_PREFIX: &str = "market-data/processed";
const DEFAULT_POLL_INTERVAL_SECS: u64 = 300;
const DEFAULT_SETTLE_SECS: u64 = 3_600;
const DEFAULT_MIN_SOURCE_FILES: usize = 1;
const DEFAULT_SPILL_RECORDS: usize = 50_000;

#[derive(Debug, Clone)]
pub struct CompactorConfig {
    pub log_filter: String,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub output_prefix: String,
    pub processed_prefix: String,
    pub poll_interval: Duration,
    pub settle: Duration,
    pub min_source_files: usize,
    pub spill_records: usize,
}

impl CompactorConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let log_filter = Config::log_filter(vars)?;
        let s3_bucket = vars.required(COMPACTOR_S3_BUCKET)?;
        let s3_prefix = vars
            .optional(COMPACTOR_S3_PREFIX)?
            .unwrap_or_else(|| DEFAULT_S3_PREFIX.to_string());
        let output_prefix = vars
            .optional(COMPACTOR_OUTPUT_PREFIX)?
            .unwrap_or_else(|| DEFAULT_OUTPUT_PREFIX.to_string());
        let processed_prefix = vars
            .optional(COMPACTOR_PROCESSED_PREFIX)?
            .unwrap_or_else(|| DEFAULT_PROCESSED_PREFIX.to_string());

        let poll_interval = Config::optional_positive_duration_secs(
            vars,
            COMPACTOR_POLL_INTERVAL_SECS,
            DEFAULT_POLL_INTERVAL_SECS,
        )?;

        let settle = Config::optional_positive_duration_secs(
            vars,
            COMPACTOR_SETTLE_SECS,
            DEFAULT_SETTLE_SECS,
        )?;
        let min_source_files = Config::optional_positive_usize(
            vars,
            COMPACTOR_MIN_SOURCE_FILES,
            DEFAULT_MIN_SOURCE_FILES,
        )?;

        let spill_records =
            Config::optional_positive_usize(vars, COMPACTOR_SPILL_RECORDS, DEFAULT_SPILL_RECORDS)?;

        Ok(Self {
            log_filter,
            s3_bucket,
            s3_prefix,
            output_prefix,
            processed_prefix,
            poll_interval,
            settle,
            min_source_files,
            spill_records,
        })
    }
}
