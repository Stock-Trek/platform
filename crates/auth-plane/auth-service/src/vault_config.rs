use platform_errors::{BpError, BpResult};
use secrecy::{ExposeSecret, SecretString};
use std::time::Duration;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
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
        let address = reqwest::Url::parse(self.address.trim())
            .map_err(|e| BpError::config_invalid_source("vault_address", e))?;
        if !matches!(address.scheme(), "http" | "https") || address.host_str().is_none() {
            return Err(BpError::config_invalid(
                "vault_address",
                "must be an absolute http or https URL",
            ));
        }
        if self.token.expose_secret().is_empty() {
            return Err(BpError::config_empty("vault_token"));
        }
        if self.mount.trim().is_empty() {
            return Err(BpError::config_empty("vault_mount"));
        }
        if !self.mount.split('/').all(Self::valid_segment) {
            return Err(BpError::config_invalid(
                "vault_mount",
                "must only contain path segments made of ASCII letters, digits, '-' and '_'",
            ));
        }
        Ok(())
    }

    fn valid_segment(segment: &str) -> bool {
        !segment.is_empty()
            && segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    }
}
