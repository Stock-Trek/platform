use platform_auth_service::{
    TokenUsageQueueConfig, ValkeyReplayStoreConfig, VaultTokenStoreConfig,
};
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_types::url_scheme::UrlScheme;
use secrecy::SecretString;
use std::fmt;
use std::time::Duration;

const VALKEY_URL: &str = "PORTFOLIO_VALKEY_URL";
const VALKEY_KEY_PREFIX: &str = "PORTFOLIO_VALKEY_KEY_PREFIX";
const VALKEY_TIMEOUT_SECS: &str = "PORTFOLIO_VALKEY_TIMEOUT_SECS";
const VAULT_ADDRESS: &str = "PORTFOLIO_VAULT_ADDRESS";
const VAULT_TOKEN: &str = "PORTFOLIO_VAULT_TOKEN";
const VAULT_MOUNT: &str = "PORTFOLIO_VAULT_MOUNT";
const VAULT_TIMEOUT: Duration = Duration::from_secs(10);
const JWKS_URL: &str = "PORTFOLIO_JWKS_URL";
const USAGE_QUEUE_URL: &str = "PORTFOLIO_USAGE_QUEUE_URL";
const USAGE_QUEUE_PREFIX: &str = "PORTFOLIO_USAGE_QUEUE_PREFIX";
const USAGE_QUEUE_TIMEOUT_SECS: &str = "PORTFOLIO_USAGE_QUEUE_TIMEOUT_SECS";
const USAGE_QUEUE_CAPACITY: &str = "PORTFOLIO_USAGE_QUEUE_CAPACITY";
const USAGE_QUEUE_BATCH_SIZE: &str = "PORTFOLIO_USAGE_QUEUE_BATCH_SIZE";

#[derive(Clone)]
pub struct AuthConfig {
    vault: VaultTokenStoreConfig,
    valkey: ValkeyReplayStoreConfig,
    usage_queue: TokenUsageQueueConfig,
    jwks_url: String,
}

impl AuthConfig {
    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let vault = VaultTokenStoreConfig::new(
            vars.required_url(VAULT_ADDRESS, &UrlScheme::HTTP)?,
            SecretString::from(vars.required(VAULT_TOKEN)?),
            vars.required(VAULT_MOUNT)?,
            VAULT_TIMEOUT,
        )?;
        let valkey = ValkeyReplayStoreConfig::new(
            vars.required(VALKEY_URL)?,
            vars.required(VALKEY_KEY_PREFIX)?,
            vars.required_positive_duration_secs(VALKEY_TIMEOUT_SECS)?,
        )?;
        let usage_queue = TokenUsageQueueConfig::new(
            vars.required_url(
                USAGE_QUEUE_URL,
                &[
                    UrlScheme::Redis,
                    UrlScheme::Rediss,
                    UrlScheme::Valkey,
                    UrlScheme::Valkeys,
                ],
            )?,
            vars.required(USAGE_QUEUE_PREFIX)?,
            vars.required_positive_duration_secs(USAGE_QUEUE_TIMEOUT_SECS)?,
            vars.required_parsed(USAGE_QUEUE_CAPACITY)?,
            vars.required_parsed(USAGE_QUEUE_BATCH_SIZE)?,
        )?;
        let jwks_url = vars.required(JWKS_URL)?;
        let parsed = reqwest::Url::parse(&jwks_url)
            .map_err(|error| BpError::config_invalid_source(JWKS_URL, error))?;
        if !UrlScheme::from_url(&parsed).is_some_and(UrlScheme::is_http) {
            return Err(BpError::config_invalid(JWKS_URL, "must use http or https"));
        }
        Ok(Self {
            vault,
            valkey,
            usage_queue,
            jwks_url,
        })
    }

    pub fn vault(&self) -> &VaultTokenStoreConfig {
        &self.vault
    }

    pub fn valkey(&self) -> &ValkeyReplayStoreConfig {
        &self.valkey
    }

    pub fn usage_queue(&self) -> &TokenUsageQueueConfig {
        &self.usage_queue
    }

    pub fn jwks_url(&self) -> &str {
        &self.jwks_url
    }
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthConfig")
            .field("vault", &self.vault)
            .field("valkey_url", &self.valkey.url())
            .field("valkey_key_prefix", &self.valkey.key_prefix())
            .field("valkey_timeout", &self.valkey.timeout())
            .field("usage_queue", &self.usage_queue)
            .field("jwks_url", &self.jwks_url)
            .finish()
    }
}
