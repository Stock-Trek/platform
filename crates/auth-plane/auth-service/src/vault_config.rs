use platform_errors::{BpError, BpResult};
use secrecy::{ExposeSecret, SecretString};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct VaultTokenStoreConfig {
    pub(crate) address: String,
    pub(crate) token: SecretString,
    pub(crate) mount: String,
    timeout: Duration,
}

impl VaultTokenStoreConfig {
    pub fn new(
        address: impl Into<String>,
        token: SecretString,
        mount: impl Into<String>,
        timeout: Duration,
    ) -> Self {
        let address = address.into();
        let token = SecretString::from(token.expose_secret().trim());
        let mount = mount.into();
        Self {
            address: address.trim().to_string(),
            token,
            mount: mount.trim().to_string(),
            timeout,
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn token(&self) -> &SecretString {
        &self.token
    }

    pub fn mount(&self) -> &str {
        &self.mount
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
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
        if !address.username().is_empty() || address.password().is_some() {
            return Err(BpError::config_invalid(
                "vault_address",
                "must not contain embedded credentials",
            ));
        }
        if address.query().is_some() || address.fragment().is_some() {
            return Err(BpError::config_invalid(
                "vault_address",
                "must not contain a query or fragment",
            ));
        }
        if self.token.expose_secret().trim().is_empty() {
            return Err(BpError::config_empty("vault_token"));
        }
        if reqwest::header::HeaderValue::from_str(self.token.expose_secret()).is_err() {
            return Err(BpError::config_invalid(
                "vault_token",
                "must be a valid HTTP header value",
            ));
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
        if self.timeout.is_zero() {
            return Err(BpError::config_invalid(
                "vault_timeout",
                "must be greater than zero",
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
