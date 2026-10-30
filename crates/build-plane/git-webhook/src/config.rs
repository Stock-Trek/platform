use platform_aws::secrets::{SSM_DECRYPT_ENV, SSM_PATH_ENV, SsmConfig};
use platform_config::env_names::{
    AWS_CONNECT_TIMEOUT_SECS, AWS_LAMBDA_FUNCTION_NAME, AWS_OPERATION_TIMEOUT_SECS,
    DEADLINE_BUFFER_SECS, GITHUB_ENTERPRISE_ALLOWED_HOSTS, RUST_LOG, SQS_QUEUE_URL,
};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_github::host_allow_list::HostAllowlist;
use platform_time::timeouts::Timeouts;
use std::{num::NonZeroUsize, time::Duration};

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
                vars.required_parsed(SSM_DECRYPT_ENV)?,
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
