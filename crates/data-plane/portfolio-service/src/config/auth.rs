use platform_auth_service::{ValkeyReplayStoreConfig, VaultTokenStoreConfig};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
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

#[derive(Clone)]
pub struct AuthConfig {
    vault: VaultTokenStoreConfig,
    valkey: ValkeyReplayStoreConfig,
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
        Ok(Self { vault, valkey })
    }

    pub fn vault(&self) -> &VaultTokenStoreConfig {
        &self.vault
    }

    pub fn valkey(&self) -> &ValkeyReplayStoreConfig {
        &self.valkey
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
            .finish()
    }
}
