use platform_errors::{AuthError, BpError};
use reqwest::{
    Error, StatusCode,
    header::{HeaderMap, RETRY_AFTER},
};
use std::{
    error::Error as StdError,
    fmt,
    time::{Duration, SystemTime},
};

const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

pub(crate) struct VaultError;

impl VaultError {
    pub fn transport(operation: &'static str, target: &str, error: Error) -> BpError {
        let retryable = Self::retryable_transport(&error);
        BpError::auth_store(operation, target, retryable, error)
    }

    pub fn response(
        operation: &'static str,
        target: &str,
        status: StatusCode,
        headers: &HeaderMap,
        body: String,
    ) -> BpError {
        let retryable = Self::retryable_status(status);
        if retryable && let Some(retry_after) = Self::retry_after(headers) {
            return BpError::Auth(AuthError::Unavailable { retry_after });
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            return BpError::Auth(AuthError::Unavailable {
                retry_after: DEFAULT_RETRY_AFTER,
            });
        }
        BpError::auth_store(
            operation,
            target,
            retryable,
            VaultStatusError { status, body },
        )
    }

    pub fn duplicate_write(status: StatusCode, body: &str) -> bool {
        matches!(status, StatusCode::BAD_REQUEST | StatusCode::CONFLICT)
            && (body.contains("check-and-set") || body.contains("did not match"))
    }

    fn retryable_transport(error: &Error) -> bool {
        if Self::configuration_transport(error) {
            return false;
        }
        error.is_timeout() || error.is_connect() || error.is_body()
    }

    fn configuration_transport(error: &Error) -> bool {
        if error.is_builder() || error.is_redirect() {
            return true;
        }
        let mut source = error.source();
        while let Some(current) = source {
            let description = current.to_string().to_ascii_lowercase();
            let configuration = [
                "builder error",
                "invalid url",
                "certificate",
                "unknown issuer",
                "invalid dnsname",
                "tls handshake",
                "tls alert",
            ]
            .iter()
            .any(|marker| description.contains(marker));
            if configuration {
                return true;
            }
            source = current.source();
        }
        false
    }

    fn retryable_status(status: StatusCode) -> bool {
        status == StatusCode::REQUEST_TIMEOUT
            || status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::TOO_EARLY
            || status.is_server_error()
    }

    fn retry_after(headers: &HeaderMap) -> Option<Duration> {
        let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
        if let Ok(seconds) = value.parse::<u64>() {
            return (seconds > 0).then(|| Duration::from_secs(seconds));
        }
        let retry_at = httpdate::parse_http_date(value).ok()?;
        retry_at
            .duration_since(SystemTime::now())
            .ok()
            .filter(|duration| !duration.is_zero())
    }
}

struct VaultStatusError {
    status: StatusCode,
    body: String,
}

impl fmt::Display for VaultStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "HTTP {}: {}", self.status, self.body)
    }
}

impl fmt::Debug for VaultStatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VaultStatusError")
            .field("status", &self.status)
            .field("body", &self.body)
            .finish()
    }
}

impl std::error::Error for VaultStatusError {}
