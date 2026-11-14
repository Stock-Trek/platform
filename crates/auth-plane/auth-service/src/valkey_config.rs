use crate::ReplayStoreError;
use platform_errors::{PlatformError, PlatformResult};
use platform_types::UrlScheme;
use redis::{ErrorKind, RedisError};
use reqwest::Url;
use std::{fmt, time::Duration};

pub const DEFAULT_VALKEY_REPLAY_PREFIX: &str = "auth-replay";
const MAX_KEY_PREFIX_LENGTH: usize = 128;

#[derive(Clone)]
pub struct ValkeyReplayStoreConfig {
    url: String,
    key_prefix: String,
    timeout: Duration,
}

impl ValkeyReplayStoreConfig {
    pub fn new(
        url: impl Into<String>,
        key_prefix: impl Into<String>,
        timeout: Duration,
    ) -> PlatformResult<Self> {
        let config = Self {
            url: url.into().trim().to_string(),
            key_prefix: key_prefix.into().trim().to_string(),
            timeout,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn from_url(url: impl Into<String>, timeout: Duration) -> PlatformResult<Self> {
        Self::new(url, DEFAULT_VALKEY_REPLAY_PREFIX, timeout)
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn validate(&self) -> PlatformResult<()> {
        if self.url.is_empty() {
            return Err(PlatformError::config_empty("valkey_url"));
        }
        let url = Url::parse(&self.url)
            .map_err(|error| PlatformError::config_invalid_source("valkey_url", error))?;
        if !UrlScheme::from_url(&url).is_some_and(UrlScheme::is_valkey) || url.host_str().is_none()
        {
            return Err(PlatformError::config_invalid(
                "valkey_url",
                "must be an absolute redis, rediss, valkey, or valkeys URL",
            ));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(PlatformError::config_invalid(
                "valkey_url",
                "must not contain a query or fragment",
            ));
        }
        if self.key_prefix.is_empty() {
            return Err(PlatformError::config_empty("valkey_key_prefix"));
        }
        if self.key_prefix.len() > MAX_KEY_PREFIX_LENGTH {
            return Err(PlatformError::config_invalid(
                "valkey_key_prefix",
                format!("must be at most {MAX_KEY_PREFIX_LENGTH} characters"),
            ));
        }
        if !self.key_prefix.split(':').all(Self::valid_prefix_segment) {
            return Err(PlatformError::config_invalid(
                "valkey_key_prefix",
                "must contain only colon-separated segments of ASCII letters, digits, '-' and '_'",
            ));
        }
        if self.timeout.is_zero() {
            return Err(PlatformError::config_invalid(
                "valkey_timeout",
                "must be greater than zero",
            ));
        }
        Ok(())
    }

    fn valid_prefix_segment(segment: &str) -> bool {
        !segment.is_empty()
            && segment.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
    }
}

impl fmt::Debug for ValkeyReplayStoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValkeyReplayStoreConfig")
            .field("url", &Self::redacted_url(&self.url))
            .field("key_prefix", &self.key_prefix)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl ValkeyReplayStoreConfig {
    fn redacted_url(value: &str) -> String {
        let Ok(mut url) = Url::parse(value) else {
            return "<invalid>".to_string();
        };
        if !url.username().is_empty() {
            let _ = url.set_username("<redacted>");
        }
        if url.password().is_some() {
            let _ = url.set_password(Some("<redacted>"));
        }
        url.to_string()
    }
}
const TARGET: &str = "valkey-replay-store";

pub struct ValkeyError;

impl ValkeyError {
    pub fn store(operation: &'static str, error: RedisError) -> ReplayStoreError {
        ReplayStoreError::unavailable(operation, Self::retryable(&error), error)
    }

    pub fn connect(error: ReplayStoreError) -> PlatformError {
        PlatformError::auth_store("connect", TARGET, error.is_retryable(), error)
    }

    pub fn result<T>(result: Result<T, ReplayStoreError>) -> PlatformResult<T> {
        result.map_err(Self::connect)
    }

    fn retryable(error: &RedisError) -> bool {
        error.is_io_error()
            || error.is_timeout()
            || error.is_connection_dropped()
            || error.is_cluster_error()
            || matches!(
                error.kind(),
                ErrorKind::BusyLoadingError
                    | ErrorKind::TryAgain
                    | ErrorKind::ClusterDown
                    | ErrorKind::MasterDown
                    | ErrorKind::ReadOnly
            )
    }
}
