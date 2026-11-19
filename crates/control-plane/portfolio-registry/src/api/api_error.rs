use http_body_util::Full;
use hyper::{
    Method, Response, StatusCode,
    body::Bytes,
    header::{ALLOW, CONTENT_TYPE},
};
use serde_json::json;

#[derive(Debug)]
pub(crate) struct ApiError {
    status: StatusCode,
    message: String,
    allow: Option<String>,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            allow: None,
        }
    }

    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, message)
    }

    pub(crate) fn method_not_allowed(allowed: &[Method]) -> Self {
        let allow = allowed
            .iter()
            .map(Method::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        Self {
            status: StatusCode::METHOD_NOT_ALLOWED,
            message: "method not allowed".to_string(),
            allow: Some(allow),
        }
    }

    pub(crate) fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "not found")
    }

    pub(crate) fn payload_too_large() -> Self {
        Self::new(StatusCode::PAYLOAD_TOO_LARGE, "request body is too large")
    }

    pub(crate) fn request_timeout(message: impl Into<String>) -> Self {
        Self::new(StatusCode::REQUEST_TIMEOUT, message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }

    pub(crate) fn response(self) -> Response<Full<Bytes>> {
        let body = json!({ "error": self.message }).to_string();
        let mut builder = Response::builder()
            .status(self.status)
            .header(CONTENT_TYPE, "application/json");
        if let Some(allow) = self.allow {
            builder = builder.header(ALLOW, allow);
        }
        builder
            .body(Full::new(Bytes::from(body)))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
    }
}
