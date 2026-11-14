use crate::{
    TokenCacheConfig, TokenUsageQueueConfig, ValkeyReplayStoreConfig, VaultTokenStoreConfig,
    jwt_signing_key::JwtSigningKey,
};
use platform_config::{EnvVars, RUST_LOG};
use platform_errors::PlatformResult;
use platform_sigv4::SigV4Config;
use platform_types::UrlScheme;
use secrecy::SecretString;
use std::{fmt, net::SocketAddr, time::Duration};

const AUTH_SERVICE_BIND_ADDR: &str = "AUTH_SERVICE_BIND_ADDR";
const AUTH_SERVICE_VAULT_ADDRESS: &str = "AUTH_SERVICE_VAULT_ADDRESS";
const AUTH_SERVICE_VAULT_MOUNT: &str = "AUTH_SERVICE_VAULT_MOUNT";
const AUTH_SERVICE_VAULT_TOKEN: &str = "AUTH_SERVICE_VAULT_TOKEN";
const AUTH_SERVICE_JWT_KEY_ID: &str = "AUTH_SERVICE_JWT_KEY_ID";
const AUTH_SERVICE_JWT_PRIVATE_KEY_PEM: &str = "AUTH_SERVICE_JWT_PRIVATE_KEY_PEM";
const AUTH_SERVICE_USAGE_QUEUE_URL: &str = "AUTH_SERVICE_USAGE_QUEUE_URL";
const AUTH_SERVICE_USAGE_QUEUE_PREFIX: &str = "AUTH_SERVICE_USAGE_QUEUE_PREFIX";
const AUTH_SERVICE_USAGE_QUEUE_TIMEOUT_SECS: &str = "AUTH_SERVICE_USAGE_QUEUE_TIMEOUT_SECS";
const AUTH_SERVICE_USAGE_QUEUE_CAPACITY: &str = "AUTH_SERVICE_USAGE_QUEUE_CAPACITY";
const AUTH_SERVICE_USAGE_QUEUE_BATCH_SIZE: &str = "AUTH_SERVICE_USAGE_QUEUE_BATCH_SIZE";
const AUTH_SERVICE_VALKEY_KEY_PREFIX: &str = "AUTH_SERVICE_VALKEY_KEY_PREFIX";
const AUTH_SERVICE_VALKEY_TIMEOUT_SECS: &str = "AUTH_SERVICE_VALKEY_TIMEOUT_SECS";
const AUTH_SERVICE_VALKEY_URL: &str = "AUTH_SERVICE_VALKEY_URL";
const AUTH_SERVICE_TOKEN_CACHE_MAX_ENTRIES: &str = "AUTH_SERVICE_TOKEN_CACHE_MAX_ENTRIES";
const AUTH_SERVICE_TOKEN_CACHE_NEGATIVE_ENTRIES: &str = "AUTH_SERVICE_TOKEN_CACHE_NEGATIVE_ENTRIES";
const AUTH_SERVICE_TOKEN_CACHE_FRESHNESS_SECS: &str = "AUTH_SERVICE_TOKEN_CACHE_FRESHNESS_SECS";
const AUTH_SERVICE_TOKEN_CACHE_MAX_STALE_SECS: &str = "AUTH_SERVICE_TOKEN_CACHE_MAX_STALE_SECS";
const AUTH_SERVICE_TOKEN_CACHE_FAILURE_THRESHOLD: &str =
    "AUTH_SERVICE_TOKEN_CACHE_FAILURE_THRESHOLD";
const AUTH_SERVICE_TOKEN_CACHE_OPEN_SECS: &str = "AUTH_SERVICE_TOKEN_CACHE_OPEN_SECS";
const VAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct JwtServerConfig {
    log_filter: String,
    bind_address: SocketAddr,
    vault: VaultTokenStoreConfig,
    valkey: ValkeyReplayStoreConfig,
    sigv4: SigV4Config,
    jwt_key_id: String,
    jwt_private_key_pem: SecretString,
    usage_queue: TokenUsageQueueConfig,
    token_cache: TokenCacheConfig,
}

impl JwtServerConfig {
    pub fn from_env() -> PlatformResult<Self> {
        Self::from_vars(&EnvVars::from_env())
    }

    pub fn from_vars(vars: &EnvVars) -> PlatformResult<Self> {
        let vault = VaultTokenStoreConfig::new(
            vars.required_url(AUTH_SERVICE_VAULT_ADDRESS, &UrlScheme::HTTP)?,
            SecretString::from(vars.required(AUTH_SERVICE_VAULT_TOKEN)?),
            vars.required(AUTH_SERVICE_VAULT_MOUNT)?,
            VAULT_TIMEOUT,
        )?;
        let jwt_key_id = vars.required(AUTH_SERVICE_JWT_KEY_ID)?;
        let jwt_private_key_pem =
            SecretString::from(vars.required(AUTH_SERVICE_JWT_PRIVATE_KEY_PEM)?);
        let valkey = ValkeyReplayStoreConfig::new(
            vars.required(AUTH_SERVICE_VALKEY_URL)?,
            vars.required(AUTH_SERVICE_VALKEY_KEY_PREFIX)?,
            vars.required_positive_duration_secs(AUTH_SERVICE_VALKEY_TIMEOUT_SECS)?,
        )?;
        let usage_queue = TokenUsageQueueConfig::new(
            vars.required_url(
                AUTH_SERVICE_USAGE_QUEUE_URL,
                &[
                    UrlScheme::Redis,
                    UrlScheme::Rediss,
                    UrlScheme::Valkey,
                    UrlScheme::Valkeys,
                ],
            )?,
            vars.required(AUTH_SERVICE_USAGE_QUEUE_PREFIX)?,
            vars.required_positive_duration_secs(AUTH_SERVICE_USAGE_QUEUE_TIMEOUT_SECS)?,
            vars.required_parsed(AUTH_SERVICE_USAGE_QUEUE_CAPACITY)?,
            vars.required_parsed(AUTH_SERVICE_USAGE_QUEUE_BATCH_SIZE)?,
        )?;
        let token_cache = TokenCacheConfig::from_vars(
            vars,
            [
                AUTH_SERVICE_TOKEN_CACHE_MAX_ENTRIES,
                AUTH_SERVICE_TOKEN_CACHE_NEGATIVE_ENTRIES,
                AUTH_SERVICE_TOKEN_CACHE_FRESHNESS_SECS,
                AUTH_SERVICE_TOKEN_CACHE_MAX_STALE_SECS,
                AUTH_SERVICE_TOKEN_CACHE_FAILURE_THRESHOLD,
                AUTH_SERVICE_TOKEN_CACHE_OPEN_SECS,
            ],
        )?;
        Ok(Self {
            log_filter: vars.required_log_filter(RUST_LOG)?,
            bind_address: vars.required_parsed(AUTH_SERVICE_BIND_ADDR)?,
            vault,
            valkey,
            sigv4: SigV4Config::from_vars(vars)?,
            jwt_key_id,
            jwt_private_key_pem,
            usage_queue,
            token_cache,
        })
    }

    pub fn log_filter(&self) -> &str {
        &self.log_filter
    }

    pub fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }

    pub fn vault(&self) -> &VaultTokenStoreConfig {
        &self.vault
    }

    pub fn valkey(&self) -> &ValkeyReplayStoreConfig {
        &self.valkey
    }

    pub fn sigv4(&self) -> &SigV4Config {
        &self.sigv4
    }

    #[cfg(test)]
    pub fn jwt_key_id(&self) -> &str {
        &self.jwt_key_id
    }

    pub fn usage_queue(&self) -> &TokenUsageQueueConfig {
        &self.usage_queue
    }

    pub fn token_cache(&self) -> &TokenCacheConfig {
        &self.token_cache
    }

    pub fn jwt_signing_key(&self) -> PlatformResult<JwtSigningKey> {
        use secrecy::ExposeSecret;
        JwtSigningKey::from_pkcs8_pem(&self.jwt_key_id, self.jwt_private_key_pem.expose_secret())
    }
}

impl fmt::Debug for JwtServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwtServerConfig")
            .field("log_filter", &self.log_filter)
            .field("bind_address", &self.bind_address)
            .field("vault", &self.vault)
            .field("valkey", &self.valkey)
            .field("sigv4", &self.sigv4)
            .field("jwt_key_id", &self.jwt_key_id)
            .field("jwt_private_key_pem", &"<redacted>")
            .field("usage_queue", &self.usage_queue)
            .field("token_cache", &self.token_cache)
            .finish()
    }
}
