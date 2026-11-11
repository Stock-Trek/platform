use http_body_util::Full;
use hyper::{Response, StatusCode, body::Bytes};
use platform_errors::{AuthError, BpError, SystemError};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderRequestError {
    InvalidRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    MethodNotAllowed,
    RequestTimeout,
    PayloadTooLarge,
    ServiceUnavailable,
    Internal,
}

impl OrderRequestError {
    pub fn status(self) -> StatusCode {
        match self {
            Self::InvalidRequest => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::RequestTimeout => StatusCode::REQUEST_TIMEOUT,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::NotFound => "not_found",
            Self::MethodNotAllowed => "method_not_allowed",
            Self::RequestTimeout => "request_timeout",
            Self::PayloadTooLarge => "payload_too_large",
            Self::ServiceUnavailable => "service_unavailable",
            Self::Internal => "internal_error",
        }
    }

    pub fn message(self) -> &'static str {
        match self {
            Self::InvalidRequest => "the request is invalid",
            Self::Unauthorized => "authentication is required",
            Self::Forbidden => "the request is not permitted",
            Self::NotFound => "the requested resource was not found",
            Self::MethodNotAllowed => "POST is required",
            Self::RequestTimeout => "the request timed out",
            Self::PayloadTooLarge => "the request body is too large",
            Self::ServiceUnavailable => "the service is temporarily unavailable",
            Self::Internal => "the request could not be completed",
        }
    }

    pub fn from_authentication_error(error: &BpError) -> Self {
        match error {
            BpError::Auth(_) if error.retryability().is_retryable() => Self::ServiceUnavailable,
            BpError::Auth(_) => Self::Unauthorized,
            error if error.retryability().is_retryable() => Self::ServiceUnavailable,
            _ => Self::Internal,
        }
    }

    pub fn from_signing_error(error: &BpError) -> Self {
        match error {
            BpError::Auth(AuthError::UnknownToken { .. }) => Self::NotFound,
            BpError::Auth(AuthError::InvalidField { field: "owner", .. }) => Self::NotFound,
            BpError::Auth(AuthError::InvalidField {
                field: "account" | "scope",
                ..
            }) => Self::Forbidden,
            BpError::Auth(AuthError::MissingScope { .. }) => Self::Forbidden,
            BpError::Auth(
                AuthError::InvalidSecret { .. }
                | AuthError::Revoked { .. }
                | AuthError::Expired { .. }
                | AuthError::RotationOverlapEnded { .. }
                | AuthError::InvalidJwt { .. },
            ) => Self::Unauthorized,
            BpError::Auth(AuthError::Store { .. } | AuthError::Unavailable { .. })
            | BpError::Auth(AuthError::Conflict { .. })
                if error.retryability().is_retryable() =>
            {
                Self::ServiceUnavailable
            }
            BpError::Auth(AuthError::InvalidField { .. } | AuthError::TokenExists { .. }) => {
                Self::InvalidRequest
            }
            BpError::System(SystemError::External(_)) => Self::InvalidRequest,
            error if error.retryability().is_retryable() => Self::ServiceUnavailable,
            _ => Self::Internal,
        }
    }

    pub fn response(self) -> Response<Full<Bytes>> {
        let body = PublicErrorBody {
            error: self.code(),
            message: self.message(),
        };
        let bytes = serde_json::to_vec(&body).expect("error body is serializable");
        Response::builder()
            .status(self.status())
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(bytes)))
            .expect("error response is valid")
    }
}

#[derive(Serialize)]
struct PublicErrorBody {
    error: &'static str,
    message: &'static str,
}
