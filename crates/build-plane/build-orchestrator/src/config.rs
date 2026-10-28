use crate::aws_ops::s3::S3Config;
use platform_config::env_names::{
    AWS_CONNECT_TIMEOUT_SECS, AWS_LAMBDA_FUNCTION_NAME, AWS_OPERATION_TIMEOUT_SECS,
    BUILD_AGENT_SOURCE_DOWNLOAD_TIMEOUT_SECS, BUILD_AGENT_TIMEOUT_SECS, DEADLINE_BUFFER_SECS,
    GITHUB_ENTERPRISE_ALLOWED_HOSTS, RUST_LOG, S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES,
    S3_SIGNED_URL_EXPIRY_SECS, SQS_QUEUE_URL,
};
use platform_config::{env_vars::EnvVars, git::DirectoryLimits};
use platform_errors::{BpError, BpResult};
use platform_github::{app_config::GitHubAppConfig, host_allow_list::HostAllowlist};
use platform_time::timeouts::Timeouts;
use std::{num::NonZeroUsize, time::Duration};

const AWS_ENDPOINT_URL_S3: &str = "AWS_ENDPOINT_URL_S3";
const BUILD_AGENT_FUNCTION_NAME: &str = "BUILD_AGENT_FUNCTION_NAME";
const GITHUB_APP_ID: &str = "GITHUB_APP_ID";
const GITHUB_APP_PRIVATE_KEY: &str = "GITHUB_APP_PRIVATE_KEY";
const GITHUB_CONNECT_TIMEOUT_SECS: &str = "GITHUB_CONNECT_TIMEOUT_SECS";
const GITHUB_MAX_CACHED_INSTALLATIONS: &str = "GITHUB_MAX_CACHED_INSTALLATIONS";
const GITHUB_OPERATION_TIMEOUT_SECS: &str = "GITHUB_OPERATION_TIMEOUT_SECS";
const GIT_MAX_DIRECTORY_BYTES: &str = "GIT_MAX_DIRECTORY_BYTES";
const GIT_MAX_DIRECTORY_DEPTH: &str = "GIT_MAX_DIRECTORY_DEPTH";
const GIT_MAX_DIRECTORY_FILES: &str = "GIT_MAX_DIRECTORY_FILES";
const S3_BUCKET_USER_ARTIFACTS: &str = "S3_BUCKET_USER_ARTIFACTS";
const S3_MAX_ARTIFACT_SIZE_BYTES: &str = "S3_MAX_ARTIFACT_SIZE_BYTES";
const SQS_DEDUP_MAX_MESSAGES: &str = "SQS_DEDUP_MAX_MESSAGES";
const SQS_DEDUP_TTL_SECS: &str = "SQS_DEDUP_TTL_SECS";
const SQS_PROCESS_CONCURRENCY: &str = "SQS_PROCESS_CONCURRENCY";

const MAX_SIGNED_URL_EXPIRY: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const SIGNED_POST_UPLOAD_MARGIN_SECS: u64 = 5 * 60;

#[derive(Debug, Clone)]
pub struct GitHubConfig {
    pub app_id: u64,
    pub private_key: String,
    pub timeouts: Timeouts,
    pub allowed_hosts: HostAllowlist,
    pub max_cached_installations: NonZeroUsize,
    pub directory_limits: DirectoryLimits,
}

/// Configuration required by the orchestrator process.
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    pub log_filter: String,
    pub lambda_function_name: String,
    pub deadline_buffer: Duration,
    pub aws_timeouts: Timeouts,
    pub github: GitHubConfig,
    pub sqs_queue_url: String,
    pub sqs_dedup_max_messages: NonZeroUsize,
    pub sqs_dedup_ttl: Duration,
    pub sqs_process_concurrency: NonZeroUsize,
    pub s3: S3Config,
    pub build_agent_function_name: String,
}

impl OrchestratorConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let aws_timeouts = Timeouts::from_durations(
            vars.required_duration_secs(AWS_CONNECT_TIMEOUT_SECS)?,
            vars.required_duration_secs(AWS_OPERATION_TIMEOUT_SECS)?,
        );
        let github_timeouts = Timeouts::from_durations(
            vars.required_duration_secs(GITHUB_CONNECT_TIMEOUT_SECS)?,
            vars.required_duration_secs(GITHUB_OPERATION_TIMEOUT_SECS)?,
        );
        let max_cached_installations =
            vars.required_nonzero_usize(GITHUB_MAX_CACHED_INSTALLATIONS)?;
        let directory_limits = DirectoryLimits {
            max_depth: vars.required_nonzero_usize(GIT_MAX_DIRECTORY_DEPTH)?.get(),
            max_files: vars.required_nonzero_usize(GIT_MAX_DIRECTORY_FILES)?.get(),
            max_total_bytes: vars.required_nonzero_u64(GIT_MAX_DIRECTORY_BYTES)?.get(),
        };
        let allowed_hosts =
            HostAllowlist::from_hosts(vars.required_csv(GITHUB_ENTERPRISE_ALLOWED_HOSTS)?)?;

        let endpoint_url = vars.required_url(AWS_ENDPOINT_URL_S3, &["http", "https"])?;
        let build_agent_timeout = vars.required_positive_duration_secs(BUILD_AGENT_TIMEOUT_SECS)?;
        let source_download_timeout =
            vars.required_positive_duration_secs(BUILD_AGENT_SOURCE_DOWNLOAD_TIMEOUT_SECS)?;
        let minimum_signed_post_expiry = build_agent_timeout
            .checked_add(source_download_timeout)
            .and_then(|expiry| {
                expiry.checked_add(Duration::from_secs(SIGNED_POST_UPLOAD_MARGIN_SECS))
            })
            .ok_or_else(|| {
                BpError::config_invalid(
                    S3_SIGNED_URL_EXPIRY_SECS,
                    "minimum signed POST lifetime overflows the duration",
                )
            })?;
        let signed_url_expiry = vars.required_duration_secs(S3_SIGNED_URL_EXPIRY_SECS)?;
        if signed_url_expiry.is_zero()
            || signed_url_expiry > MAX_SIGNED_URL_EXPIRY
            || signed_url_expiry < minimum_signed_post_expiry
        {
            return Err(BpError::config_invalid(
                S3_SIGNED_URL_EXPIRY_SECS,
                format!(
                    "must be at least {} seconds (build agent timeout plus source download timeout plus upload margin) and at most {} seconds (7 days)",
                    minimum_signed_post_expiry.as_secs(),
                    MAX_SIGNED_URL_EXPIRY.as_secs()
                ),
            ));
        }
        Ok(Self {
            log_filter: vars.required_log_filter(RUST_LOG)?,
            lambda_function_name: vars.required(AWS_LAMBDA_FUNCTION_NAME)?,
            deadline_buffer: vars.required_duration_secs(DEADLINE_BUFFER_SECS)?,
            aws_timeouts,
            github: GitHubConfig {
                app_id: vars.required_parsed(GITHUB_APP_ID)?,
                private_key: vars.required(GITHUB_APP_PRIVATE_KEY)?.replace("\\n", "\n"),
                timeouts: github_timeouts,
                allowed_hosts,
                max_cached_installations,
                directory_limits,
            },
            sqs_queue_url: vars.required(SQS_QUEUE_URL)?,
            sqs_dedup_max_messages: vars.required_nonzero_usize(SQS_DEDUP_MAX_MESSAGES)?,
            sqs_dedup_ttl: vars.required_duration_secs(SQS_DEDUP_TTL_SECS)?,
            sqs_process_concurrency: vars.required_nonzero_usize(SQS_PROCESS_CONCURRENCY)?,
            s3: S3Config {
                user_artifacts_bucket: vars.required(S3_BUCKET_USER_ARTIFACTS)?,
                max_source_archive_size_bytes: vars
                    .required_nonzero_u64(S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES)?,
                max_artifact_size_bytes: vars.required_nonzero_u64(S3_MAX_ARTIFACT_SIZE_BYTES)?,
                signed_url_expiry,
                endpoint_url: Some(endpoint_url),
            },
            build_agent_function_name: vars.required(BUILD_AGENT_FUNCTION_NAME)?,
        })
    }

    pub fn github_app_config(&self) -> GitHubAppConfig {
        GitHubAppConfig::new(self.github.app_id, self.github.private_key.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    const REQUIRED: [(&str, &str); 26] = [
        (RUST_LOG, "info"),
        (AWS_LAMBDA_FUNCTION_NAME, "platform-build-orchestrator"),
        (BUILD_AGENT_TIMEOUT_SECS, "300"),
        (BUILD_AGENT_SOURCE_DOWNLOAD_TIMEOUT_SECS, "60"),
        (DEADLINE_BUFFER_SECS, "5"),
        (AWS_CONNECT_TIMEOUT_SECS, "10"),
        (AWS_OPERATION_TIMEOUT_SECS, "120"),
        (GITHUB_APP_ID, "1"),
        (GITHUB_APP_PRIVATE_KEY, "private-key"),
        (GITHUB_CONNECT_TIMEOUT_SECS, "10"),
        (GITHUB_OPERATION_TIMEOUT_SECS, "120"),
        (GITHUB_ENTERPRISE_ALLOWED_HOSTS, "github.com"),
        (GITHUB_MAX_CACHED_INSTALLATIONS, "128"),
        (GIT_MAX_DIRECTORY_DEPTH, "8"),
        (GIT_MAX_DIRECTORY_FILES, "100"),
        (GIT_MAX_DIRECTORY_BYTES, "1048576"),
        (SQS_QUEUE_URL, "https://sqs.test/queue"),
        (SQS_DEDUP_MAX_MESSAGES, "4096"),
        (SQS_DEDUP_TTL_SECS, "3600"),
        (SQS_PROCESS_CONCURRENCY, "10"),
        (S3_BUCKET_USER_ARTIFACTS, "user-artifacts"),
        (AWS_ENDPOINT_URL_S3, "http://localhost:9000"),
        (S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES, "16777216"),
        (S3_MAX_ARTIFACT_SIZE_BYTES, "16777216"),
        (S3_SIGNED_URL_EXPIRY_SECS, "3600"),
        (BUILD_AGENT_FUNCTION_NAME, "platform-build-agent"),
    ];

    fn vars(values: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_map(values.iter().copied())
    }

    #[test]
    fn parses_all_required_values() {
        let config = OrchestratorConfig::from_vars(&vars(&REQUIRED)).unwrap();

        assert_eq!(config.lambda_function_name, "platform-build-orchestrator");
        assert_eq!(config.s3.user_artifacts_bucket, "user-artifacts");
        assert_eq!(
            config.s3.max_source_archive_size_bytes,
            NonZeroU64::new(16 * 1024 * 1024).unwrap()
        );
        assert_eq!(
            config.s3.max_artifact_size_bytes,
            NonZeroU64::new(16 * 1024 * 1024).unwrap()
        );
        assert_eq!(config.sqs_process_concurrency.get(), 10);
        assert_eq!(config.deadline_buffer, Duration::from_secs(5));
        assert_eq!(config.s3.signed_url_expiry, Duration::from_secs(3600));
    }

    #[test]
    fn signed_post_expiry_covers_the_build_and_upload_window() {
        let minimum = Duration::from_secs(300 + 60 + SIGNED_POST_UPLOAD_MARGIN_SECS);

        let mut values = REQUIRED.to_vec();
        let index = values
            .iter()
            .position(|(name, _)| *name == S3_SIGNED_URL_EXPIRY_SECS)
            .unwrap();
        values[index] = (S3_SIGNED_URL_EXPIRY_SECS, "659");
        assert!(OrchestratorConfig::from_vars(&vars(&values)).is_err());

        values[index] = (S3_SIGNED_URL_EXPIRY_SECS, "660");
        let config = OrchestratorConfig::from_vars(&vars(&values)).unwrap();
        assert_eq!(config.s3.signed_url_expiry, minimum);
    }

    #[test]
    fn build_agent_durations_must_be_positive() {
        for (name, value) in [
            (BUILD_AGENT_TIMEOUT_SECS, "0"),
            (BUILD_AGENT_SOURCE_DOWNLOAD_TIMEOUT_SECS, "0"),
        ] {
            let mut values = REQUIRED.to_vec();
            let index = values
                .iter()
                .position(|(required, _)| *required == name)
                .unwrap();
            values[index] = (name, value);

            assert!(OrchestratorConfig::from_vars(&vars(&values)).is_err());
        }
    }

    #[test]
    fn reads_s3_endpoint_url() {
        let config = OrchestratorConfig::from_vars(&vars(&REQUIRED)).unwrap();
        assert_eq!(
            config.s3.endpoint_url.as_deref(),
            Some("http://localhost:9000")
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
                OrchestratorConfig::from_vars(&vars(&values)).is_err(),
                "{missing} should be required"
            );
        }
    }

    #[test]
    fn rejects_invalid_values() {
        for (name, value) in [
            (GITHUB_APP_ID, "not a number"),
            (SQS_PROCESS_CONCURRENCY, "0"),
            (S3_SIGNED_URL_EXPIRY_SECS, "0"),
            (S3_MAX_SOURCE_ARCHIVE_SIZE_BYTES, "0"),
            (S3_MAX_ARTIFACT_SIZE_BYTES, "0"),
            (S3_SIGNED_URL_EXPIRY_SECS, "604801"),
            (AWS_ENDPOINT_URL_S3, "ftp://minio.example.test"),
            (GITHUB_ENTERPRISE_ALLOWED_HOSTS, "not a host!"),
            (RUST_LOG, "not a valid [directive"),
        ] {
            let mut values = REQUIRED.to_vec();
            let index = values
                .iter()
                .position(|(required, _)| *required == name)
                .unwrap();
            values[index] = (name, value);

            assert!(
                OrchestratorConfig::from_vars(&vars(&values)).is_err(),
                "{name}={value} should be rejected"
            );
        }
    }
}
