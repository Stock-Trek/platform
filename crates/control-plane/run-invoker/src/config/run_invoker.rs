use platform_config::{
    AWS_CONNECT_TIMEOUT_SECS, AWS_OPERATION_TIMEOUT_SECS, EnvVars, RUST_LOG,
    S3_SIGNED_URL_EXPIRY_SECS,
};
use platform_errors::BuildResult;
use platform_time::timeouts::Timeouts;
use std::time::Duration;

const RUN_INVOKER_AUTH_SERVICE_URL: &str = "RUN_INVOKER_AUTH_SERVICE_URL";
const RUN_INVOKER_ARTIFACT_BUCKET: &str = "RUN_INVOKER_ARTIFACT_BUCKET";
const RUN_INVOKER_LOGS_BUCKET: &str = "RUN_INVOKER_LOGS_BUCKET";
const RUN_INVOKER_MAX_BATCH: &str = "RUN_INVOKER_MAX_BATCH";
const RUN_INVOKER_ORDER_REQUEST_URL: &str = "RUN_INVOKER_ORDER_REQUEST_URL";
const RUN_INVOKER_RUNNER_FUNCTION_NAME: &str = "RUN_INVOKER_RUNNER_FUNCTION_NAME";
const RUN_INVOKER_TABLE: &str = "RUN_INVOKER_TABLE";

/// Configuration required by the run invoker process.
#[derive(Debug, Clone)]
pub struct RunInvokerConfig {
    pub log_filter: String,
    pub table: String,
    pub artifact_bucket: String,
    pub logs_bucket: String,
    pub order_request_url: String,
    pub auth_service_url: String,
    pub runner_function_name: String,
    pub max_batch: usize,
    pub signed_url_expiry: Duration,
    pub aws_timeouts: Timeouts,
}

impl RunInvokerConfig {
    pub fn from_env() -> BuildResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BuildResult<Self> {
        Ok(Self {
            log_filter: vars.required_log_filter(RUST_LOG)?,
            table: vars.required(RUN_INVOKER_TABLE)?,
            artifact_bucket: vars.required(RUN_INVOKER_ARTIFACT_BUCKET)?,
            logs_bucket: vars.required(RUN_INVOKER_LOGS_BUCKET)?,
            order_request_url: vars.required(RUN_INVOKER_ORDER_REQUEST_URL)?,
            auth_service_url: vars.required(RUN_INVOKER_AUTH_SERVICE_URL)?,
            runner_function_name: vars.required(RUN_INVOKER_RUNNER_FUNCTION_NAME)?,
            max_batch: vars.required_nonzero_usize(RUN_INVOKER_MAX_BATCH)?.get(),
            signed_url_expiry: vars.required_duration_secs(S3_SIGNED_URL_EXPIRY_SECS)?,
            aws_timeouts: Timeouts::from_durations(
                vars.required_duration_secs(AWS_CONNECT_TIMEOUT_SECS)?,
                vars.required_duration_secs(AWS_OPERATION_TIMEOUT_SECS)?,
            ),
        })
    }
}
