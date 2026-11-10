use platform_auth_service::{ValkeyReplayStoreConfig, VaultTokenStoreConfig};
use platform_config::env_vars::EnvVars;
use platform_errors::BpResult;
use platform_types::url_scheme::UrlScheme;
use secrecy::SecretString;
use std::fmt;
use std::time::Duration;

const AUDIT_QUEUE_URL: &str = "PORTFOLIO_AUDIT_QUEUE_URL";
const AUDIT_TIMEOUT_SECS: &str = "PORTFOLIO_AUDIT_TIMEOUT_SECS";
const VALKEY_URL: &str = "PORTFOLIO_VALKEY_URL";
const VALKEY_KEY_PREFIX: &str = "PORTFOLIO_VALKEY_KEY_PREFIX";
const VALKEY_TIMEOUT_SECS: &str = "PORTFOLIO_VALKEY_TIMEOUT_SECS";
const VAULT_ADDRESS: &str = "PORTFOLIO_VAULT_ADDRESS";
const VAULT_TOKEN: &str = "PORTFOLIO_VAULT_TOKEN";
const VAULT_MOUNT: &str = "PORTFOLIO_VAULT_MOUNT";
const VAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct AuditConfig {
    queue_url: String,
    timeout: Duration,
}

impl AuditConfig {
    fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        Ok(Self {
            queue_url: vars.required_url(AUDIT_QUEUE_URL, &UrlScheme::HTTP)?,
            timeout: vars.required_positive_duration_secs(AUDIT_TIMEOUT_SECS)?,
        })
    }

    pub fn queue_url(&self) -> &str {
        &self.queue_url
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[derive(Clone)]
pub struct AuthConfig {
    vault: VaultTokenStoreConfig,
    valkey: ValkeyReplayStoreConfig,
    audit: AuditConfig,
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
        let audit = AuditConfig::from_vars(vars)?;
        Ok(Self {
            vault,
            valkey,
            audit,
        })
    }

    pub fn vault(&self) -> &VaultTokenStoreConfig {
        &self.vault
    }

    pub fn valkey(&self) -> &ValkeyReplayStoreConfig {
        &self.valkey
    }

    pub fn audit(&self) -> &AuditConfig {
        &self.audit
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
            .field("audit", &self.audit)
            .finish()
    }
}
