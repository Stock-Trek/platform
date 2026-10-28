use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_lambda::{
    error::ProvideErrorMetadata, operation::invoke::InvokeError as SdkInvokeError,
};
use std::{borrow::Borrow, error::Error as StdError, io::ErrorKind, process::ExitStatus};

const HTTP_FORBIDDEN: u16 = 403;
const HTTP_REQUEST_TIMEOUT: u16 = 408;
const HTTP_TOO_MANY_REQUESTS: u16 = 429;
const HTTP_INTERNAL_SERVER_ERROR: u16 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retryability {
    Retryable,
    Permanent,
}

impl Retryability {
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Retryable)
    }

    pub fn is_permanent(self) -> bool {
        matches!(self, Self::Permanent)
    }

    pub fn sqs_batch_failure_retryability(code: &str) -> Retryability {
        if Self::is_permanent_sqs_error_code(code) {
            Retryability::Permanent
        } else {
            Retryability::Retryable
        }
    }

    pub fn from_http_status(status: u16) -> Retryability {
        match status {
            HTTP_REQUEST_TIMEOUT | HTTP_TOO_MANY_REQUESTS => Retryability::Retryable,
            status if status >= HTTP_INTERNAL_SERVER_ERROR => Retryability::Retryable,
            status if (400..500).contains(&status) => Retryability::Permanent,
            _ => Retryability::Retryable,
        }
    }

    pub fn from_s3_error_code(code: &str) -> Retryability {
        if code.starts_with("Invalid")
            || code.starts_with("NoSuch")
            || code.ends_with("NotFound")
            || matches!(
                code,
                "AccessDenied" | "NotImplemented" | "ObjectLockConfigurationNotFoundError"
            )
        {
            Retryability::Permanent
        } else {
            Retryability::Retryable
        }
    }

    pub fn from_io_error(error: impl Borrow<std::io::Error>) -> Retryability {
        let error = error.borrow();
        match error.kind() {
            ErrorKind::NotFound
            | ErrorKind::PermissionDenied
            | ErrorKind::InvalidInput
            | ErrorKind::InvalidData
            | ErrorKind::Unsupported
            | ErrorKind::AlreadyExists
            | ErrorKind::FileTooLarge
            | ErrorKind::InvalidFilename
            | ErrorKind::TooManyLinks
            | ErrorKind::ArgumentListTooLong
            | ErrorKind::CrossesDevices
            | ErrorKind::NotSeekable
            | ErrorKind::UnexpectedEof => Retryability::Permanent,
            _ if Self::is_permanent_os_error(error) => Retryability::Permanent,
            _ => Retryability::Retryable,
        }
    }

    pub fn from_io_source(source: &(dyn StdError + 'static)) -> Retryability {
        let mut source = source;
        loop {
            if let Some(error) = source.downcast_ref::<std::io::Error>() {
                return Self::from_io_error(error);
            }
            match source.source() {
                Some(next) => source = next,
                None => return Retryability::Retryable,
            }
        }
    }

    pub fn from_exit_status(status: impl Borrow<ExitStatus>) -> Retryability {
        let status = status.borrow();
        if status.success() {
            return Retryability::Permanent;
        }

        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;

            if let Some(signal) = status.signal() {
                return match signal {
                    2 | 3 => Retryability::Permanent,
                    _ => Retryability::Retryable,
                };
            }
        }

        match status.code() {
            Some(1 | 2 | 101 | 126 | 127 | 130 | 131) => Retryability::Permanent,
            _ => Retryability::Retryable,
        }
    }

    pub fn from_join_error(error: impl Borrow<tokio::task::JoinError>) -> Retryability {
        if error.borrow().is_panic() {
            Retryability::Permanent
        } else {
            Retryability::Retryable
        }
    }

    pub fn from_binance_code(code: i64) -> Retryability {
        match code {
            -1001 | -1003 | -1006 | -1007 | -1015 | -1016 | -1021 => Retryability::Retryable,
            _ => Retryability::Permanent,
        }
    }

    pub fn from_binance_code_and_status(code: i64, status: u16) -> Retryability {
        match code {
            -1 => Self::from_http_status(status),
            code if (100..=599).contains(&code) => Self::from_http_status(code as u16),
            code => Self::from_binance_code(code),
        }
    }

    pub fn from_credentials(error: &CredentialsError) -> Retryability {
        match error {
            CredentialsError::ProviderTimedOut(_) | CredentialsError::ProviderError(_) => {
                Retryability::Retryable
            }
            _ => Retryability::Permanent,
        }
    }

    pub fn from_reqwest(error: &reqwest::Error) -> Retryability {
        match error.status().map(|status| status.as_u16()) {
            Some(status) => Self::from_http_status(status),
            None => Retryability::Retryable,
        }
    }

    pub fn from_octocrab(error: &octocrab::Error) -> Retryability {
        match error {
            octocrab::Error::GitHub { source, .. } => Self::from_github_response(
                source.status_code.as_u16(),
                &source.message,
                source.documentation_url.as_deref(),
            ),
            octocrab::Error::Installation { .. }
            | octocrab::Error::InstallationTokenInvalidAuth { .. }
            | octocrab::Error::InvalidHeaderValue { .. }
            | octocrab::Error::InvalidUtf8 { .. }
            | octocrab::Error::Json { .. }
            | octocrab::Error::JWT { .. }
            | octocrab::Error::Serde { .. }
            | octocrab::Error::SerdeUrlEncoded { .. }
            | octocrab::Error::Uri { .. }
            | octocrab::Error::UriParse { .. } => Retryability::Permanent,
            _ => Retryability::Retryable,
        }
    }

    pub fn from_lambda_invoke(error: &SdkInvokeError) -> Retryability {
        match error.code() {
            Some(
                "InvalidParameterValueException"
                | "InvalidRequestContentException"
                | "RecursiveInvocationException"
                | "ResourceConflictException"
                | "ResourceNotFoundException"
                | "UnsupportedMediaTypeException",
            ) => Retryability::Permanent,
            _ => Retryability::Retryable,
        }
    }

    fn from_github_response(
        status: u16,
        message: &str,
        documentation_url: Option<&str>,
    ) -> Retryability {
        match status {
            HTTP_FORBIDDEN if Self::is_rate_limit_error(message, documentation_url) => {
                Retryability::Retryable
            }
            _ => Self::from_http_status(status),
        }
    }

    fn is_rate_limit_error(message: &str, documentation_url: Option<&str>) -> bool {
        Self::mentions_rate_limit(message)
            || documentation_url.is_some_and(Self::mentions_rate_limit)
    }

    fn mentions_rate_limit(value: &str) -> bool {
        let value = value.to_ascii_lowercase();
        value.contains("rate limit")
            || value.contains("rate-limit")
            || value.contains("rate_limit")
            || value.contains("ratelimit")
    }

    #[cfg(unix)]
    fn is_permanent_os_error(error: &std::io::Error) -> bool {
        matches!(
            error.raw_os_error(),
            Some(8 | 19 | 20 | 21 | 25 | 30 | 39 | 40 | 84)
        )
    }

    #[cfg(not(unix))]
    fn is_permanent_os_error(_error: &std::io::Error) -> bool {
        false
    }

    fn is_permanent_sqs_error_code(code: &str) -> bool {
        matches!(
            code,
            "AccessDenied"
                | "BatchEntryIdsNotDistinct"
                | "BatchRequestTooLong"
                | "BpError"
                | "EmptyBatchRequest"
                | "InvalidAddress"
                | "InvalidBatchEntryId"
                | "InvalidMessageContents"
                | "InvalidSecurity"
                | "KmsAccessDenied"
                | "KmsDisabled"
                | "KmsInvalidKeyUsage"
                | "KmsInvalidState"
                | "KmsNotFound"
                | "KmsOptInRequired"
                | "QueueDoesNotExist"
                | "TooManyEntriesInBatchRequest"
                | "UnsupportedOperation"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn predicates_match_the_retryability_variant() {
        assert!(Retryability::Retryable.is_retryable());
        assert!(!Retryability::Retryable.is_permanent());
        assert!(Retryability::Permanent.is_permanent());
        assert!(!Retryability::Permanent.is_retryable());
    }

    #[test]
    fn permanent_s3_error_codes_are_not_retried() {
        for code in [
            "AccessDenied",
            "InvalidArgument",
            "NoSuchBucket",
            "NoSuchKey",
            "NotImplemented",
            "ObjectLockConfigurationNotFoundError",
        ] {
            assert_eq!(
                Retryability::from_s3_error_code(code),
                Retryability::Permanent,
                "{code}"
            );
        }
    }

    #[test]
    fn transient_s3_error_codes_are_retried() {
        for code in [
            "InternalError",
            "RequestTimeout",
            "ServiceUnavailable",
            "SlowDown",
        ] {
            assert_eq!(
                Retryability::from_s3_error_code(code),
                Retryability::Retryable,
                "{code}"
            );
        }
    }
}
