use crate::retryability::Retryability;
use http::header::{InvalidHeaderName, InvalidHeaderValue, ToStrError};

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum HttpError {
    #[error("Http error: {0}")]
    Http(#[from] http::Error),
    #[error("Invalid header name: {0}")]
    InvalidHeaderName(#[from] InvalidHeaderName),
    #[error("Invalid header value: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("Invalid header value string: {0}")]
    InvalidHeaderValueStr(#[from] ToStrError),
    #[error("{operation} request for {target} was rejected with status {status}: {body}")]
    Rejected {
        operation: &'static str,
        target: String,
        status: u16,
        body: String,
    },
    #[cfg(feature = "http-client")]
    #[error("{operation} request failed for {target}: {source}")]
    Request {
        operation: &'static str,
        target: String,
        #[source]
        source: reqwest::Error,
    },
    #[cfg(feature = "http-client")]
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

impl HttpError {
    pub fn retryability(&self) -> Retryability {
        match self {
            #[cfg(feature = "http-client")]
            Self::Reqwest(error) => Retryability::from_reqwest(error),
            #[cfg(feature = "http-client")]
            Self::Request { source, .. } => Retryability::from_reqwest(source),
            Self::Rejected { status, .. } => Retryability::from_http_status(*status),
            Self::Http(_)
            | Self::InvalidHeaderName(_)
            | Self::InvalidHeaderValue(_)
            | Self::InvalidHeaderValueStr(_)
            | Self::Serde(_) => Retryability::Permanent,
        }
    }
}
