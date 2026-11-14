use platform_auth_service::{
    TokenCacheConfig, TokenUsageQueueConfig, ValkeyReplayStoreConfig, VaultTokenStoreConfig,
};
use platform_config::EnvVars;
use platform_errors::{PlatformError, PlatformResult};
use platform_types::UrlScheme;
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
const TOKEN_CACHE_MAX_ENTRIES: &str = "PORTFOLIO_TOKEN_CACHE_MAX_ENTRIES";
const TOKEN_CACHE_NEGATIVE_ENTRIES: &str = "PORTFOLIO_TOKEN_CACHE_NEGATIVE_ENTRIES";
const TOKEN_CACHE_FRESHNESS_SECS: &str = "PORTFOLIO_TOKEN_CACHE_FRESHNESS_SECS";
const TOKEN_CACHE_MAX_STALE_SECS: &str = "PORTFOLIO_TOKEN_CACHE_MAX_STALE_SECS";
const TOKEN_CACHE_FAILURE_THRESHOLD: &str = "PORTFOLIO_TOKEN_CACHE_FAILURE_THRESHOLD";
const TOKEN_CACHE_OPEN_SECS: &str = "PORTFOLIO_TOKEN_CACHE_OPEN_SECS";

#[derive(Clone)]
pub struct AuthConfig {
    vault: VaultTokenStoreConfig,
    valkey: ValkeyReplayStoreConfig,
    usage_queue: TokenUsageQueueConfig,
    jwks_url: String,
    token_cache: TokenCacheConfig,
}

impl AuthConfig {
    pub fn from_vars(vars: &EnvVars) -> PlatformResult<Self> {
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
        let token_cache = TokenCacheConfig::from_vars(
            vars,
            [
                TOKEN_CACHE_MAX_ENTRIES,
                TOKEN_CACHE_NEGATIVE_ENTRIES,
                TOKEN_CACHE_FRESHNESS_SECS,
                TOKEN_CACHE_MAX_STALE_SECS,
                TOKEN_CACHE_FAILURE_THRESHOLD,
                TOKEN_CACHE_OPEN_SECS,
            ],
        )?;
        let jwks_url = vars.required(JWKS_URL)?;
        let parsed = reqwest::Url::parse(&jwks_url)
            .map_err(|error| PlatformError::config_invalid_source(JWKS_URL, error))?;
        if !UrlScheme::from_url(&parsed).is_some_and(UrlScheme::is_http) {
            return Err(PlatformError::config_invalid(
                JWKS_URL,
                "must use http or https",
            ));
        }
        Ok(Self {
            vault,
            valkey,
            usage_queue,
            jwks_url,
            token_cache,
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

    pub fn token_cache(&self) -> &TokenCacheConfig {
        &self.token_cache
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
            .field("token_cache", &self.token_cache)
            .finish()
    }
}
