use crate::secrets::{SSM_PATH_ENV, SsmConfig};
use platform_config::env_names::{
    AWS_CONNECT_TIMEOUT_SECS, AWS_LAMBDA_FUNCTION_NAME, AWS_OPERATION_TIMEOUT_SECS,
    DEADLINE_BUFFER_SECS, GITHUB_ENTERPRISE_ALLOWED_HOSTS, RUST_LOG, SQS_QUEUE_URL,
};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_github::host_allow_list::HostAllowlist;
use platform_time::timeouts::Timeouts;
use std::{num::NonZeroUsize, time::Duration};

const SECRETS_SSM_DECRYPT: &str = "SECRETS_SSM_DECRYPT";
const WEBHOOK_COLD_START_REFRESH_TIMEOUT_SECS: &str = "WEBHOOK_COLD_START_REFRESH_TIMEOUT_SECS";
const WEBHOOK_DEDUP_MAX_DELIVERIES: &str = "WEBHOOK_DEDUP_MAX_DELIVERIES";
const WEBHOOK_DEDUP_TTL_SECS: &str = "WEBHOOK_DEDUP_TTL_SECS";
const WEBHOOK_REFRESH_COOLDOWN_SECS: &str = "WEBHOOK_REFRESH_COOLDOWN_SECS";
const WEBHOOK_SECRET_FAILURE_BACKOFF_SECS: &str = "WEBHOOK_SECRET_FAILURE_BACKOFF_SECS";

/// Configuration required by the webhook process.
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    pub log_filter: String,
    pub lambda_function_name: String,
    pub deadline_buffer: Duration,
    pub aws_timeouts: Timeouts,
    pub sqs_queue_url: String,
    pub ssm: SsmConfig,
    pub github_allowed_hosts: HostAllowlist,
    pub dedup_max_deliveries: NonZeroUsize,
    pub dedup_ttl: Duration,
    pub refresh_cooldown: Duration,
    pub secret_failure_backoff: Duration,
    pub cold_start_refresh_timeout: Duration,
}

impl WebhookConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        Ok(Self {
            log_filter: vars.required_log_filter(RUST_LOG)?,
            lambda_function_name: vars.required(AWS_LAMBDA_FUNCTION_NAME)?,
            deadline_buffer: vars.required_duration_secs(DEADLINE_BUFFER_SECS)?,
            aws_timeouts: Timeouts::from_durations(
                vars.required_duration_secs(AWS_CONNECT_TIMEOUT_SECS)?,
                vars.required_duration_secs(AWS_OPERATION_TIMEOUT_SECS)?,
            ),
            sqs_queue_url: vars.required(SQS_QUEUE_URL)?,
            ssm: SsmConfig::new(
                vars.required(SSM_PATH_ENV)?,
                vars.optional_parsed_or(SECRETS_SSM_DECRYPT, true)?,
            )?,
            github_allowed_hosts: HostAllowlist::from_hosts(
                vars.required_csv(GITHUB_ENTERPRISE_ALLOWED_HOSTS)?,
            )?,
            dedup_max_deliveries: vars.required_nonzero_usize(WEBHOOK_DEDUP_MAX_DELIVERIES)?,
            dedup_ttl: vars.required_duration_secs(WEBHOOK_DEDUP_TTL_SECS)?,
            refresh_cooldown: vars.required_duration_secs(WEBHOOK_REFRESH_COOLDOWN_SECS)?,
            secret_failure_backoff: vars
                .required_duration_secs(WEBHOOK_SECRET_FAILURE_BACKOFF_SECS)?,
            cold_start_refresh_timeout: vars
                .required_duration_secs(WEBHOOK_COLD_START_REFRESH_TIMEOUT_SECS)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REQUIRED: [(&str, &str); 13] = [
        (RUST_LOG, "info"),
        (AWS_LAMBDA_FUNCTION_NAME, "platform-git-webhook"),
        (DEADLINE_BUFFER_SECS, "5"),
        (AWS_CONNECT_TIMEOUT_SECS, "10"),
        (AWS_OPERATION_TIMEOUT_SECS, "120"),
        (SQS_QUEUE_URL, "https://sqs.test/queue"),
        (SSM_PATH_ENV, "/platform/git-webhook/production"),
        (GITHUB_ENTERPRISE_ALLOWED_HOSTS, "github.com"),
        (WEBHOOK_DEDUP_MAX_DELIVERIES, "4096"),
        (WEBHOOK_DEDUP_TTL_SECS, "3600"),
        (WEBHOOK_REFRESH_COOLDOWN_SECS, "60"),
        (WEBHOOK_SECRET_FAILURE_BACKOFF_SECS, "30"),
        (WEBHOOK_COLD_START_REFRESH_TIMEOUT_SECS, "10"),
    ];

    fn vars(values: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_map(values.iter().copied())
    }

    #[test]
    fn parses_all_required_values() {
        let config = WebhookConfig::from_vars(&vars(&REQUIRED)).unwrap();

        assert_eq!(config.lambda_function_name, "platform-git-webhook");
        assert_eq!(config.sqs_queue_url, "https://sqs.test/queue");
        assert_eq!(config.dedup_max_deliveries.get(), 4096);
        assert_eq!(config.ssm.path, "/platform/git-webhook/production");
        assert!(config.ssm.decrypt);
    }

    #[test]
    fn normalises_the_ssm_path_and_reads_the_decrypt_flag() {
        let mut values = REQUIRED.to_vec();
        let index = values
            .iter()
            .position(|(required, _)| *required == SSM_PATH_ENV)
            .unwrap();
        values[index] = (SSM_PATH_ENV, "platform/git-webhook/production");
        values.push((SECRETS_SSM_DECRYPT, "false"));

        let config = WebhookConfig::from_vars(&vars(&values)).unwrap();

        assert_eq!(config.ssm.path, "/platform/git-webhook/production");
        assert!(!config.ssm.decrypt);
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
                WebhookConfig::from_vars(&vars(&values)).is_err(),
                "{missing} should be required"
            );
        }
    }

    #[test]
    fn rejects_invalid_values() {
        for (name, value) in [
            (WEBHOOK_DEDUP_MAX_DELIVERIES, "0"),
            (SSM_PATH_ENV, "/platform/git-webhook/production/"),
            (SECRETS_SSM_DECRYPT, "not a bool"),
            (GITHUB_ENTERPRISE_ALLOWED_HOSTS, "not a host!"),
            (RUST_LOG, "not a valid [directive"),
        ] {
            let mut values = REQUIRED.to_vec();
            if let Some(index) = values.iter().position(|(required, _)| *required == name) {
                values[index] = (name, value);
            } else {
                values.push((name, value));
            }

            assert!(
                WebhookConfig::from_vars(&vars(&values)).is_err(),
                "{name}={value} should be rejected"
            );
        }
    }
}
