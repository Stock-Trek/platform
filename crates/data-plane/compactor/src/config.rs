use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use std::time::Duration;

const COMPACTOR_S3_BUCKET: &str = "COMPACTOR_S3_BUCKET";
const COMPACTOR_S3_PREFIX: &str = "COMPACTOR_S3_PREFIX";
const COMPACTOR_OUTPUT_PREFIX: &str = "COMPACTOR_OUTPUT_PREFIX";
const COMPACTOR_POLL_INTERVAL_SECS: &str = "COMPACTOR_POLL_INTERVAL_SECS";
const COMPACTOR_SETTLE_SECS: &str = "COMPACTOR_SETTLE_SECS";
const COMPACTOR_MIN_SOURCE_FILES: &str = "COMPACTOR_MIN_SOURCE_FILES";
const RUST_LOG: &str = "RUST_LOG";

const DEFAULT_S3_PREFIX: &str = "market-data/raw";
const DEFAULT_OUTPUT_PREFIX: &str = "market-data/compacted";
const DEFAULT_POLL_INTERVAL_SECS: u64 = 300;
const DEFAULT_SETTLE_SECS: u64 = 3_600;
const DEFAULT_MIN_SOURCE_FILES: usize = 1;

#[derive(Debug, Clone)]
pub struct CompactorConfig {
    pub log_filter: String,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub output_prefix: String,
    pub poll_interval: Duration,
    pub settle: Duration,
    pub min_source_files: usize,
}

impl CompactorConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let log_filter = vars.required_log_filter(RUST_LOG)?;
        let s3_bucket = vars.required(COMPACTOR_S3_BUCKET)?;
        let s3_prefix = vars
            .optional(COMPACTOR_S3_PREFIX)?
            .unwrap_or_else(|| DEFAULT_S3_PREFIX.to_string());
        let output_prefix = vars
            .optional(COMPACTOR_OUTPUT_PREFIX)?
            .unwrap_or_else(|| DEFAULT_OUTPUT_PREFIX.to_string());

        let poll_interval_secs =
            vars.optional_parsed_or(COMPACTOR_POLL_INTERVAL_SECS, DEFAULT_POLL_INTERVAL_SECS)?;
        if poll_interval_secs == 0 {
            return Err(BpError::config_invalid(
                COMPACTOR_POLL_INTERVAL_SECS,
                "must be greater than zero",
            ));
        }

        let settle_secs = vars.optional_parsed_or(COMPACTOR_SETTLE_SECS, DEFAULT_SETTLE_SECS)?;
        let min_source_files =
            vars.optional_parsed_or(COMPACTOR_MIN_SOURCE_FILES, DEFAULT_MIN_SOURCE_FILES)?;
        if min_source_files == 0 {
            return Err(BpError::config_invalid(
                COMPACTOR_MIN_SOURCE_FILES,
                "must be greater than zero",
            ));
        }

        Ok(Self {
            log_filter,
            s3_bucket,
            s3_prefix,
            output_prefix,
            poll_interval: Duration::from_secs(poll_interval_secs),
            settle: Duration::from_secs(settle_secs),
            min_source_files,
        })
    }
}
