#[cfg(feature = "http")]
use crate::http::HttpError;
use crate::{
    build::BuildError, retryability::Retryability, system::SystemError, timeout::TimeoutError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendFailure {
    Failed,
    NotSent,
    Unknown,
}

impl SendFailure {
    pub fn is_not_sent(self) -> bool {
        matches!(self, Self::NotSent)
    }

    pub fn is_unknown(self) -> bool {
        matches!(self, Self::Unknown)
    }
}

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ConnectorError {
    #[error("Auto resync clock task panicked")]
    AutoResyncClockPanicked,
    #[error("Auto resync clock task is no longer running")]
    AutoResyncClockStopped,
    #[error("A user callback panicked: {0}")]
    CallbackPanicked(String),
    #[error(
        "The server clock has not been synchronised; call sync_clock_http or sync_clock_websocket before sending signed requests"
    )]
    ClockNotSynced,
    #[error("HTTP request failed with status {status}")]
    GatewayHttpStatus { status: u16 },
    #[error("Failed to parse HTTP response: {source}")]
    HttpParseError {
        #[source]
        source: Box<BuildError>,
    },
    #[error("Clock sync frequency must be at least 1 minute")]
    InvalidSyncFrequency,
    #[error("Server time response did not contain a server time")]
    MissingServerTime,
    #[error("Connector is not connected")]
    NotConnected,
    #[error("{source}")]
    Send {
        failure: SendFailure,
        #[source]
        source: Box<BuildError>,
    },
    #[error("Connector was not initialised with a websocket listener")]
    WebsocketListenerMissing,
    #[error("Failed to parse websocket response: {source}")]
    WebsocketParseError {
        #[source]
        source: Box<BuildError>,
    },
}

impl ConnectorError {
    pub fn external(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::from(BuildError::external(source))
    }

    pub fn send_not_sent(source: BuildError) -> Self {
        Self::Send {
            failure: SendFailure::NotSent,
            source: Box::new(source),
        }
    }

    pub fn send_not_sent_external(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::send_not_sent(BuildError::external(source))
    }

    pub fn send_failed(source: BuildError) -> Self {
        Self::Send {
            failure: SendFailure::Failed,
            source: Box::new(source),
        }
    }

    pub fn send_unknown(source: BuildError) -> Self {
        Self::Send {
            failure: SendFailure::Unknown,
            source: Box::new(source),
        }
    }

    pub fn send_unknown_external(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::send_unknown(BuildError::external(source))
    }

    pub fn send_failure(&self) -> Option<SendFailure> {
        match self {
            Self::Send { failure, .. } => Some(*failure),
            _ => None,
        }
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::GatewayHttpStatus { status } => Retryability::from_http_status(*status),
            Self::Send { source, .. } => match source.as_ref() {
                BuildError::Timeout(TimeoutError::TimedOut)
                | BuildError::System(SystemError::External(_)) => Retryability::Retryable,
                BuildError::Connector(Self::GatewayHttpStatus { status }) => {
                    Retryability::from_http_status(*status)
                }
                #[cfg(feature = "http")]
                BuildError::Http(HttpError::Request { source, .. }) => {
                    Retryability::from_reqwest(source)
                }
                _ => Retryability::Permanent,
            },
            _ => Retryability::Permanent,
        }
    }
}

impl From<BuildError> for ConnectorError {
    fn from(source: BuildError) -> Self {
        match source {
            BuildError::Connector(error) => error,
            source => Self::Send {
                failure: SendFailure::Unknown,
                source: Box::new(source),
            },
        }
    }
}
