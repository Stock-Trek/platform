use platform_auth_service::VaultTokenStoreConfig;
use platform_config::env_names::{AWS_CONNECT_TIMEOUT_SECS, AWS_OPERATION_TIMEOUT_SECS};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_runtime::Config;
use platform_time::timeouts::Timeouts;
use platform_types::url_scheme::UrlScheme;
use secrecy::SecretString;
use std::time::Duration;

const AUTH_RELAY_AUDIT_QUEUE_URL: &str = "AUTH_RELAY_AUDIT_QUEUE_URL";
const AUTH_RELAY_AUDIT_TIMEOUT_SECS: &str = "AUTH_RELAY_AUDIT_TIMEOUT_SECS";
const AUTH_RELAY_POLL_INTERVAL_SECS: &str = "AUTH_RELAY_POLL_INTERVAL_SECS";
const AUTH_RELAY_VAULT_ADDRESS: &str = "AUTH_RELAY_VAULT_ADDRESS";
const AUTH_RELAY_VAULT_MOUNT: &str = "AUTH_RELAY_VAULT_MOUNT";
const AUTH_RELAY_VAULT_TIMEOUT_SECS: &str = "AUTH_RELAY_VAULT_TIMEOUT_SECS";
const AUTH_RELAY_VAULT_TOKEN: &str = "AUTH_RELAY_VAULT_TOKEN";

#[derive(Debug, Clone)]
pub struct AuthRelayConfig {
    pub log_filter: String,
    pub vault: VaultTokenStoreConfig,
    pub audit_queue_url: String,
    pub audit_timeout: Duration,
    pub poll_interval: Duration,
    pub aws_timeouts: Timeouts,
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

        Ok(Self {
            log_filter: Config::log_filter(vars)?,
            vault,
            audit_queue_url: vars.required_url(AUTH_RELAY_AUDIT_QUEUE_URL, &UrlScheme::HTTP)?,
            audit_timeout: vars.required_positive_duration_secs(AUTH_RELAY_AUDIT_TIMEOUT_SECS)?,
            poll_interval: vars.required_positive_duration_secs(AUTH_RELAY_POLL_INTERVAL_SECS)?,
            aws_timeouts: Timeouts::from_durations(
                vars.required_positive_duration_secs(AWS_CONNECT_TIMEOUT_SECS)?,
                vars.required_positive_duration_secs(AWS_OPERATION_TIMEOUT_SECS)?,
            ),
        })
    }
}
