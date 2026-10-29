use crate::retryability::Retryability;
use aws_sigv4::{
    http_request::SigningError, sign::v4::signing_params::BuildError as SigningParamsError,
};
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
    #[error("{operation} request failed for {target}: {source}")]
    Request {
        operation: &'static str,
        target: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Signing error: {0}")]
    Signing(#[from] SigningError),
    #[error("Signing params error: {0}")]
    SigningParams(#[from] SigningParamsError),
}

impl HttpError {
    pub fn retryability(&self) -> Retryability {
        match self {
            Self::Reqwest(error) => Retryability::from_reqwest(error),
            Self::Request { source, .. } => Retryability::from_reqwest(source),
            Self::Rejected { status, .. } => Retryability::from_http_status(*status),
            Self::Http(_)
            | Self::InvalidHeaderName(_)
            | Self::InvalidHeaderValue(_)
            | Self::InvalidHeaderValueStr(_)
            | Self::Serde(_)
            | Self::SigningParams(_)
            | Self::Signing(_) => Retryability::Permanent,
        }
    }
}
