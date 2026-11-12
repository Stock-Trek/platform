use platform_errors::{BuildError, BuildResult};
use platform_types::url_scheme::UrlScheme;
use reqwest::Url;
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
        token: impl Into<SecretString>,
        mount: impl Into<String>,
        timeout: Duration,
    ) -> BuildResult<Self> {
        let address = address.into();
        let token = token.into();
        let mount = mount.into();
        let config = Self {
            address: address.trim().to_string(),
            token: SecretString::from(token.expose_secret().trim()),
            mount: mount.trim().to_string(),
            timeout,
        };
        config.validate()?;
        Ok(config)
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

    pub fn validate(&self) -> BuildResult<()> {
        if self.address.trim().is_empty() {
            return Err(BuildError::config_empty("vault_address"));
        }
        let address = reqwest::Url::parse(self.address.trim())
            .map_err(|e| BuildError::config_invalid_source("vault_address", e))?;
        if !UrlScheme::from_url(&address).is_some_and(UrlScheme::is_http)
            || address.host_str().is_none()
        {
            return Err(BuildError::config_invalid(
                "vault_address",
                "must be an absolute http or https URL",
            ));
        }
        if !address.username().is_empty() || address.password().is_some() {
            return Err(BuildError::config_invalid(
                "vault_address",
                "must not contain embedded credentials",
            ));
        }
        if address.query().is_some() || address.fragment().is_some() {
            return Err(BuildError::config_invalid(
                "vault_address",
                "must not contain a query or fragment",
            ));
        }
        if self.token.expose_secret().trim().is_empty() {
            return Err(BuildError::config_empty("vault_token"));
        }
        if reqwest::header::HeaderValue::from_str(self.token.expose_secret()).is_err() {
            return Err(BuildError::config_invalid(
                "vault_token",
                "must be a valid HTTP header value",
            ));
        }
        if self.mount.trim().is_empty() {
            return Err(BuildError::config_empty("vault_mount"));
        }
        if !self.mount.split('/').all(Self::valid_segment) {
            return Err(BuildError::config_invalid(
                "vault_mount",
                "must only contain path segments made of ASCII letters, digits, '-' and '_'",
            ));
        }
        if self.timeout.is_zero() {
            return Err(BuildError::config_invalid(
                "vault_timeout",
                "must be greater than zero",
            ));
        }
        Ok(())
    }

    pub(crate) fn url(&self, suffix: &[&str]) -> BuildResult<Url> {
        let address = self.address.trim();
        let mut url = Url::parse(address.trim_end_matches('/'))
            .map_err(|e| BuildError::config_invalid_source("vault_address", e))?;
        url.set_query(None);
        url.set_fragment(None);
        {
            let mut segments = url.path_segments_mut().map_err(|()| {
                BuildError::config_invalid("vault_address", "must be a hierarchical URL")
            })?;
            segments.pop_if_empty();
            segments.push("v1");
            for segment in self.mount.split('/') {
                segments.push(segment);
            }
            for segment in suffix {
                segments.push(segment);
            }
        }
        Ok(url)
    }

    fn valid_segment(segment: &str) -> bool {
        !segment.is_empty()
            && segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    }
}
