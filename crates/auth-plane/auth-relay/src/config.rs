use platform_auth_service::{TokenUsageQueueConfig, VaultTokenStoreConfig};
use platform_config::EnvVars;
use platform_config::{AWS_CONNECT_TIMEOUT_SECS, AWS_OPERATION_TIMEOUT_SECS};
use platform_errors::BpResult;
use platform_runtime::Config;
use platform_time::timeouts::Timeouts;
use platform_types::url_scheme::UrlScheme;
use secrecy::SecretString;
use std::time::Duration;

const AUTH_RELAY_AUDIT_QUEUE_URL: &str = "AUTH_RELAY_AUDIT_QUEUE_URL";
const AUTH_RELAY_AUDIT_TIMEOUT_SECS: &str = "AUTH_RELAY_AUDIT_TIMEOUT_SECS";
const AUTH_RELAY_VAULT_ADDRESS: &str = "AUTH_RELAY_VAULT_ADDRESS";
const AUTH_RELAY_VAULT_MOUNT: &str = "AUTH_RELAY_VAULT_MOUNT";
const AUTH_RELAY_VAULT_TIMEOUT_SECS: &str = "AUTH_RELAY_VAULT_TIMEOUT_SECS";
const AUTH_RELAY_VAULT_TOKEN: &str = "AUTH_RELAY_VAULT_TOKEN";
const AUTH_RELAY_USAGE_QUEUE_URL: &str = "AUTH_RELAY_USAGE_QUEUE_URL";
const AUTH_RELAY_USAGE_QUEUE_PREFIX: &str = "AUTH_RELAY_USAGE_QUEUE_PREFIX";
const AUTH_RELAY_USAGE_QUEUE_TIMEOUT_SECS: &str = "AUTH_RELAY_USAGE_QUEUE_TIMEOUT_SECS";
const AUTH_RELAY_USAGE_QUEUE_CAPACITY: &str = "AUTH_RELAY_USAGE_QUEUE_CAPACITY";
const AUTH_RELAY_USAGE_QUEUE_BATCH_SIZE: &str = "AUTH_RELAY_USAGE_QUEUE_BATCH_SIZE";

#[derive(Debug, Clone)]
pub struct AuthRelayConfig {
    pub log_filter: String,
    pub vault: VaultTokenStoreConfig,
    pub audit_queue_url: String,
    pub audit_timeout: Duration,
    pub aws_timeouts: Timeouts,
    pub usage_queue: TokenUsageQueueConfig,
}

impl AuthRelayConfig {
    pub fn from_env() -> BpResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let vault = VaultTokenStoreConfig::new(
            vars.required_url(AUTH_RELAY_VAULT_ADDRESS, &UrlScheme::HTTP)?,
            SecretString::from(vars.required(AUTH_RELAY_VAULT_TOKEN)?),
            vars.required(AUTH_RELAY_VAULT_MOUNT)?,
            vars.required_positive_duration_secs(AUTH_RELAY_VAULT_TIMEOUT_SECS)?,
        )?;
        let usage_queue = TokenUsageQueueConfig::new(
            vars.required_url(
                AUTH_RELAY_USAGE_QUEUE_URL,
                &[
                    UrlScheme::Redis,
                    UrlScheme::Rediss,
                    UrlScheme::Valkey,
                    UrlScheme::Valkeys,
                ],
            )?,
            vars.required(AUTH_RELAY_USAGE_QUEUE_PREFIX)?,
            vars.required_positive_duration_secs(AUTH_RELAY_USAGE_QUEUE_TIMEOUT_SECS)?,
            vars.required_parsed(AUTH_RELAY_USAGE_QUEUE_CAPACITY)?,
            vars.required_parsed(AUTH_RELAY_USAGE_QUEUE_BATCH_SIZE)?,
        )?;

        Ok(Self {
            log_filter: Config::log_filter(vars)?,
            vault,
            audit_queue_url: vars.required_url(AUTH_RELAY_AUDIT_QUEUE_URL, &UrlScheme::HTTP)?,
            audit_timeout: vars.required_positive_duration_secs(AUTH_RELAY_AUDIT_TIMEOUT_SECS)?,
            aws_timeouts: Timeouts::from_durations(
                vars.required_positive_duration_secs(AWS_CONNECT_TIMEOUT_SECS)?,
                vars.required_positive_duration_secs(AWS_OPERATION_TIMEOUT_SECS)?,
            ),
            usage_queue,
        })
    }
}
