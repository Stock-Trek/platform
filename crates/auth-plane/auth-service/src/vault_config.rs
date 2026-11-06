use platform_errors::{BpError, BpResult};
use secrecy::{ExposeSecret, SecretString};
use std::time::Duration;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct VaultTokenStoreConfig {
    pub(crate) address: String,
    pub(crate) token: SecretString,
    pub(crate) mount: String,
    timeout: Duration,
}

impl VaultTokenStoreConfig {
    pub fn new(address: impl Into<String>, token: SecretString, mount: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            token,
            mount: mount.into(),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn mount(&self) -> &str {
        &self.mount
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn validate(&self) -> BpResult<()> {
        if self.address.trim().is_empty() {
            return Err(BpError::config_empty("vault_address"));
        }
        if self.token.expose_secret().is_empty() {
            return Err(BpError::config_empty("vault_token"));
        }
        if self.mount.trim().is_empty() {
            return Err(BpError::config_empty("vault_mount"));
        }
        Ok(())
    }
}
