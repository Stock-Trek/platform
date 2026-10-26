use crate::{
    bp::BpError, http::HttpError, retryability::Retryability, system::SystemError,
    timeout::TimeoutError,
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
        source: Box<BpError>,
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
        source: Box<BpError>,
    },
    #[error("Connector was not initialised with a websocket listener")]
    WebsocketListenerMissing,
    #[error("Failed to parse websocket response: {source}")]
    WebsocketParseError {
        #[source]
        source: Box<BpError>,
    },
}

impl ConnectorError {
    pub fn external(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::from(BpError::external(source))
    }

    pub fn send_not_sent(source: BpError) -> Self {
        Self::Send {
            failure: SendFailure::NotSent,
            source: Box::new(source),
        }
    }

    pub fn send_not_sent_external(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::send_not_sent(BpError::external(source))
    }

    pub fn send_failed(source: BpError) -> Self {
        Self::Send {
            failure: SendFailure::Failed,
            source: Box::new(source),
        }
    }

    pub fn send_unknown(source: BpError) -> Self {
        Self::Send {
            failure: SendFailure::Unknown,
            source: Box::new(source),
        }
    }

    pub fn send_unknown_external(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::send_unknown(BpError::external(source))
    }

    pub fn send_failure(&self) -> Option<SendFailure> {
        match self {
            Self::Send { failure, .. } => Some(*failure),
            _ => None,
        }
    }

    pub fn error_type(&self) -> &'static str {
        self.into()
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::GatewayHttpStatus { status } => Retryability::from_http_status(*status),
            Self::Send { source, .. } => match source.as_ref() {
                BpError::Timeout(TimeoutError::TimedOut)
                | BpError::System(SystemError::External(_)) => Retryability::Retryable,
                BpError::Connector(Self::GatewayHttpStatus { status }) => {
                    Retryability::from_http_status(*status)
                }
                BpError::Http(HttpError::Request { source, .. }) => {
                    Retryability::from_reqwest(source)
                }
                _ => Retryability::Permanent,
            },
            _ => Retryability::Permanent,
        }
    }
}

impl From<BpError> for ConnectorError {
    fn from(source: BpError) -> Self {
        match source {
            BpError::Connector(error) => error,
            source => Self::Send {
                failure: SendFailure::Unknown,
                source: Box::new(source),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> BpError {
        BpError::external("test failure")
    }

    #[test]
    fn send_failure_reports_the_send_outcome() {
        assert_eq!(
            ConnectorError::send_not_sent(source()).send_failure(),
            Some(SendFailure::NotSent)
        );
        assert_eq!(
            ConnectorError::send_failed(source()).send_failure(),
            Some(SendFailure::Failed)
        );
        assert_eq!(
            ConnectorError::send_unknown(source()).send_failure(),
            Some(SendFailure::Unknown)
        );
    }

    #[test]
    fn non_send_errors_have_no_send_failure() {
        assert_eq!(ConnectorError::NotConnected.send_failure(), None);
    }

    #[test]
    fn converting_a_bp_error_does_not_claim_the_request_was_not_sent() {
        let converted = ConnectorError::from(source());

        assert_eq!(converted.send_failure(), Some(SendFailure::Unknown));
        assert!(
            !converted
                .send_failure()
                .is_some_and(SendFailure::is_not_sent)
        );
    }

    #[test]
    fn converting_an_existing_connector_error_preserves_it() {
        let converted = ConnectorError::from(BpError::Connector(ConnectorError::NotConnected));

        assert!(matches!(converted, ConnectorError::NotConnected));
        assert_eq!(converted.send_failure(), None);
    }
}
