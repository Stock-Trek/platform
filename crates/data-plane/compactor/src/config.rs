use platform_archive::IcebergConfig;
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_runtime::Config;
use std::time::Duration;

const COMPACTOR_S3_BUCKET: &str = "COMPACTOR_S3_BUCKET";
const COMPACTOR_S3_PREFIX: &str = "COMPACTOR_S3_PREFIX";
const COMPACTOR_OUTPUT_PREFIX: &str = "COMPACTOR_OUTPUT_PREFIX";
const COMPACTOR_POLL_INTERVAL_SECS: &str = "COMPACTOR_POLL_INTERVAL_SECS";
const COMPACTOR_SETTLE_SECS: &str = "COMPACTOR_SETTLE_SECS";
const COMPACTOR_MIN_SOURCE_FILES: &str = "COMPACTOR_MIN_SOURCE_FILES";
const COMPACTOR_SPILL_RECORDS: &str = "COMPACTOR_SPILL_RECORDS";

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
    pub iceberg: IcebergConfig,
}

impl CompactorConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let log_filter = Config::log_filter(vars)?;
        let s3_bucket = vars.required(COMPACTOR_S3_BUCKET)?;
        let s3_prefix = vars.required(COMPACTOR_S3_PREFIX)?;
        let output_prefix = vars.required(COMPACTOR_OUTPUT_PREFIX)?;
        let poll_interval =
            Config::required_positive_duration_secs(vars, COMPACTOR_POLL_INTERVAL_SECS)?;
        let settle = Config::required_positive_duration_secs(vars, COMPACTOR_SETTLE_SECS)?;
        let min_source_files = Config::required_positive_usize(vars, COMPACTOR_MIN_SOURCE_FILES)?;
        let spill_records = Config::required_positive_usize(vars, COMPACTOR_SPILL_RECORDS)?;
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
            iceberg,
        })
    }
}
