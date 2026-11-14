use http_body_util::Full;
use hyper::{Response, StatusCode, body::Bytes, header::RETRY_AFTER};
use platform_errors::{AuthError, PlatformError, SystemError};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct JwtServerError {
    status: StatusCode,
    message: String,
    retry_after: Option<Duration>,
}

impl JwtServerError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn issuance(error: PlatformError) -> Self {
        let error = match error {
            PlatformError::System(SystemError::External(source)) => match source.downcast::<Self>() {
                Ok(error) => return *error,
                Err(source) => PlatformError::System(SystemError::External(source)),
            },
            error => error,
        };
        let retry_after = match &error {
            PlatformError::Auth(AuthError::Unavailable { retry_after }) => Some(*retry_after),
            _ => None,
        };
        let status = if error.retryability().is_retryable() {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::BAD_REQUEST
        };
        Self {
            status,
            message: error.to_string(),
            retry_after,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn response(self) -> Response<Full<Bytes>> {
        let mut builder = Response::builder().status(self.status);
        if let Some(retry_after) = self.retry_after {
            builder = builder.header(RETRY_AFTER, retry_after.as_secs().max(1).to_string());
        }
        builder
            .body(Full::new(Bytes::from_static(b"JWT request failed")))
            .expect("static response is valid")
    }
}

impl From<JwtServerError> for PlatformError {
    fn from(error: JwtServerError) -> Self {
        Self::external(error)
    }
}
