use super::BpError;
use crate::ErrorType;
use crate::connector::SendFailure;
#[cfg(feature = "github")]
use crate::github::GitHubError;
use crate::retryability::Retryability;

impl BpError {
    pub fn any_sent(&self) -> bool {
        matches!(self, Self::Sqs(error) if error.any_sent())
    }

    pub fn is_client_error(&self) -> bool {
        #[cfg(feature = "github")]
        {
            matches!(
                self,
                Self::GitHub(GitHubError::HostInvalid { .. } | GitHubError::HostNotAllowed { .. })
            )
        }
        #[cfg(not(feature = "github"))]
        {
            false
        }
    }

    pub fn send_failure(&self) -> Option<SendFailure> {
        match self {
            Self::Connector(error) => error.send_failure(),
            _ => None,
        }
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::Binance(error) => error.retryability(),
            Self::Config(error) => error.retryability(),
            Self::Connector(error) => error.retryability(),
            Self::Data(error) => error.retryability(),
            Self::Exchange(error) => error.retryability(),
            #[cfg(feature = "github")]
            Self::GitHub(error) => error.retryability(),
            #[cfg(feature = "http")]
            Self::Http(error) => error.retryability(),
            Self::Ingester(error) => error.retryability(),
            Self::Io(error) => error.retryability(),
            #[cfg(feature = "lambda")]
            Self::Lambda(error) => error.retryability(),
            Self::Nats(error) => error.retryability(),
            Self::RateLimit(error) => error.retryability(),
            Self::Secrets(error) => error.retryability(),
            Self::Sqs(error) => error.retryability(),
            #[cfg(feature = "storage")]
            Self::Storage(error) => error.retryability(),
            Self::System(error) => error.retryability(),
            Self::Timeout(error) => error.retryability(),
            Self::Validation(error) => error.retryability(),
        }
    }
}

impl ErrorType for BpError {
    fn error_type(&self) -> &'static str {
        match self {
            Self::Binance(error) => error.error_type(),
            Self::Config(error) => error.error_type(),
            Self::Connector(error) => error.error_type(),
            Self::Data(error) => error.error_type(),
            Self::Exchange(error) => error.error_type(),
            #[cfg(feature = "github")]
            Self::GitHub(error) => error.error_type(),
            #[cfg(feature = "http")]
            Self::Http(error) => error.error_type(),
            Self::Ingester(error) => error.error_type(),
            Self::Io(error) => error.error_type(),
            #[cfg(feature = "lambda")]
            Self::Lambda(error) => error.error_type(),
            Self::Nats(error) => error.error_type(),
            Self::RateLimit(error) => error.error_type(),
            Self::Secrets(error) => error.error_type(),
            Self::Sqs(error) => error.error_type(),
            #[cfg(feature = "storage")]
            Self::Storage(error) => error.error_type(),
            Self::System(error) => error.error_type(),
            Self::Timeout(error) => error.error_type(),
            Self::Validation(error) => error.error_type(),
        }
    }
}
