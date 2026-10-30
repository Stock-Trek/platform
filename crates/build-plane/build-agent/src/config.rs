use platform_config::env_names::{
    AWS_LAMBDA_FUNCTION_NAME, BUILD_AGENT_SOURCE_DOWNLOAD_TIMEOUT_SECS, BUILD_AGENT_TIMEOUT_SECS,
    RUST_LOG, S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES,
};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use std::{num::NonZeroU64, path::PathBuf, time::Duration};

const BUILD_AGENT_BUILD_CACHE_DIR: &str = "BUILD_AGENT_BUILD_CACHE_DIR";
const BUILD_AGENT_MAX_EXTRACTED_SOURCE_SIZE_BYTES: &str =
    "BUILD_AGENT_MAX_EXTRACTED_SOURCE_SIZE_BYTES";
const BUILD_AGENT_SOURCE_DIR: &str = "BUILD_AGENT_SOURCE_DIR";
const BUILD_AGENT_FAILURE_PUBLISH_BUDGET_SECS: &str = "BUILD_AGENT_FAILURE_PUBLISH_BUDGET_SECS";

/// Configuration required by the build agent process.
///
/// Every value is parsed and validated before the Lambda handler is installed,
/// so a missing or invalid environment variable fails startup instead of the
/// first build.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub log_filter: String,
    pub lambda_function_name: String,
    pub build_timeout: Duration,
    pub failure_publish_budget: Duration,
    pub source_dir: PathBuf,
    pub build_cache_dir: PathBuf,
    pub source_download_timeout: Duration,
    pub max_source_archive_size_bytes: NonZeroU64,
    pub max_extracted_source_size_bytes: NonZeroU64,
}

impl AgentConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let build_timeout = vars.required_positive_duration_secs(BUILD_AGENT_TIMEOUT_SECS)?;
        let source_download_timeout =
            vars.required_positive_duration_secs(BUILD_AGENT_SOURCE_DOWNLOAD_TIMEOUT_SECS)?;
        let failure_publish_budget =
            vars.required_positive_duration_secs(BUILD_AGENT_FAILURE_PUBLISH_BUDGET_SECS)?;

        Ok(Self {
            log_filter: vars.required_log_filter(RUST_LOG)?,
            lambda_function_name: vars.required(AWS_LAMBDA_FUNCTION_NAME)?,
            build_timeout,
            failure_publish_budget,
            source_dir: vars.required_path(BUILD_AGENT_SOURCE_DIR)?,
            build_cache_dir: vars.required_path(BUILD_AGENT_BUILD_CACHE_DIR)?,
            source_download_timeout,
            max_source_archive_size_bytes: vars
                .required_nonzero_u64(S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES)?,
            max_extracted_source_size_bytes: vars
                .required_nonzero_u64(BUILD_AGENT_MAX_EXTRACTED_SOURCE_SIZE_BYTES)?,
        })
    }
}
