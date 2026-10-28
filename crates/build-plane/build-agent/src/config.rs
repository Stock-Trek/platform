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
const FAILURE_PUBLISH_BUDGET_SECS: &str = "FAILURE_PUBLISH_BUDGET_SECS";

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
            vars.required_positive_duration_secs(FAILURE_PUBLISH_BUDGET_SECS)?;

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

#[cfg(test)]
mod tests {
    use super::*;

    const REQUIRED: [(&str, &str); 9] = [
        (RUST_LOG, "info"),
        (AWS_LAMBDA_FUNCTION_NAME, "platform-build-agent"),
        (BUILD_AGENT_TIMEOUT_SECS, "300"),
        (FAILURE_PUBLISH_BUDGET_SECS, "30"),
        (BUILD_AGENT_SOURCE_DIR, "/var/task"),
        (BUILD_AGENT_BUILD_CACHE_DIR, "/tmp/platform-build-cache"),
        (BUILD_AGENT_SOURCE_DOWNLOAD_TIMEOUT_SECS, "60"),
        (BUILD_AGENT_MAX_EXTRACTED_SOURCE_SIZE_BYTES, "67108864"),
        (S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES, "16777216"),
    ];

    fn vars(values: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_map(values.iter().copied())
    }

    #[test]
    fn parses_all_required_values() {
        let config = AgentConfig::from_vars(&vars(&REQUIRED)).unwrap();

        assert_eq!(config.lambda_function_name, "platform-build-agent");
        assert_eq!(config.build_timeout, Duration::from_secs(300));
        assert_eq!(config.failure_publish_budget, Duration::from_secs(30));
        assert_eq!(config.source_dir, PathBuf::from("/var/task"));
        assert_eq!(
            config.max_source_archive_size_bytes,
            NonZeroU64::new(16 * 1024 * 1024).unwrap()
        );
    }

    #[test]
    fn every_value_is_required() {
        for (missing, _) in REQUIRED {
            let values = REQUIRED
                .iter()
                .copied()
                .filter(|(name, _)| *name != missing)
                .collect::<Vec<_>>();

            assert!(
                AgentConfig::from_vars(&vars(&values)).is_err(),
                "{missing} should be required"
            );
        }
    }

    #[test]
    fn rejects_invalid_values() {
        let mut values = REQUIRED.to_vec();
        values[2] = (BUILD_AGENT_TIMEOUT_SECS, "not a number");
        assert!(AgentConfig::from_vars(&vars(&values)).is_err());

        let mut values = REQUIRED.to_vec();
        values[2] = (BUILD_AGENT_TIMEOUT_SECS, "0");
        assert!(AgentConfig::from_vars(&vars(&values)).is_err());

        let mut values = REQUIRED.to_vec();
        let budget_index = values
            .iter()
            .position(|(name, _)| *name == FAILURE_PUBLISH_BUDGET_SECS)
            .unwrap();
        values[budget_index] = (FAILURE_PUBLISH_BUDGET_SECS, "0");
        assert!(AgentConfig::from_vars(&vars(&values)).is_err());

        let mut values = REQUIRED.to_vec();
        let archive_index = values
            .iter()
            .position(|(name, _)| *name == S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES)
            .unwrap();
        values[archive_index] = (S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES, "0");
        assert!(AgentConfig::from_vars(&vars(&values)).is_err());

        let mut values = REQUIRED.to_vec();
        let extracted_index = values
            .iter()
            .position(|(name, _)| *name == BUILD_AGENT_MAX_EXTRACTED_SOURCE_SIZE_BYTES)
            .unwrap();
        values[extracted_index] = (BUILD_AGENT_MAX_EXTRACTED_SOURCE_SIZE_BYTES, "0");
        assert!(AgentConfig::from_vars(&vars(&values)).is_err());

        let mut values = REQUIRED.to_vec();
        values[0] = (RUST_LOG, "not a valid [directive");
        assert!(AgentConfig::from_vars(&vars(&values)).is_err());
    }
}
