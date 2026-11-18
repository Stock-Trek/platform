use http_body_util::Full;
use hyper::{
    Method, Response, StatusCode,
    body::Bytes,
    header::{ALLOW, CONTENT_TYPE, RETRY_AFTER},
};
use serde::Serialize;
use serde_json::json;

const ERROR_TYPE_BASE: &str = "https://api.example.com/errors/";

#[derive(Debug, Clone, Serialize)]
pub struct QuotaDetail {
    pub quota: &'static str,
    pub limit: u64,
    pub used: u64,
}

#[derive(Debug)]
pub struct Problem {
    status: StatusCode,
    code: &'static str,
    title: &'static str,
    detail: String,
    quota: Option<QuotaDetail>,
    retry_after: Option<u64>,
    allow: Option<String>,
}

impl Problem {
    fn new(
        status: StatusCode,
        code: &'static str,
        title: &'static str,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            status,
            code,
            title,
            detail: detail.into(),
            quota: None,
            retry_after: None,
            allow: None,
        }
    }

    pub fn validation_failed(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "validation_failed",
            "Validation failed",
            detail,
        )
    }

    pub fn unauthorized(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Unauthorized",
            detail,
        )
    }

    pub fn forbidden(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", "Forbidden", detail)
    }

    pub fn step_up_required() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "step_up_required",
            "Step-up required",
            "this operation requires a recent step-up with the admin scope",
        )
    }

    pub fn quota_exceeded(quota: QuotaDetail) -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "quota_exceeded",
            "Quota exceeded",
            format!("the {} quota would be exceeded", quota.quota),
        )
        .with_quota(quota)
    }

    pub fn not_found(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", "Not found", detail)
    }

    pub fn method_not_allowed(allowed: &[Method]) -> Self {
        let allow = allowed
            .iter()
            .map(Method::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        Self::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "Method not allowed",
            "the method is not supported by this endpoint",
        )
        .with_allow(allow)
    }

    pub fn already_exists(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "already_exists",
            "Already exists",
            detail,
        )
    }

    pub fn state_conflict(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "state_conflict",
            "State conflict",
            detail,
        )
    }

    pub fn idempotency_conflict() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "Idempotency conflict",
            "the idempotency key was reused with a different request body",
        )
    }

    pub fn precondition_failed() -> Self {
        Self::new(
            StatusCode::PRECONDITION_FAILED,
            "precondition_failed",
            "Precondition failed",
            "If-Match does not match the current version",
        )
    }

    pub fn payload_too_large() -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "Payload too large",
            "the request body exceeds the maximum size",
        )
    }

    pub fn precondition_required() -> Self {
        Self::new(
            StatusCode::PRECONDITION_REQUIRED,
            "precondition_required",
            "Precondition required",
            "this endpoint requires an If-Match header",
        )
    }

    pub fn rate_limited(retry_after: u64) -> Self {
        Self::new(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Rate limited",
            "the rate limit is exhausted, retry later",
        )
        .with_retry_after(retry_after)
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "Internal error",
            detail,
        )
    }

    pub fn unavailable() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "Unavailable",
            "a dependency is down, retry with backoff",
        )
    }

    fn with_quota(mut self, quota: QuotaDetail) -> Self {
        self.quota = Some(quota);
        self
    }

    fn with_retry_after(mut self, retry_after: u64) -> Self {
        self.retry_after = Some(retry_after);
        self
    }

    fn with_allow(mut self, allow: String) -> Self {
        self.allow = Some(allow);
        self
    }

    #[cfg(test)]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn response(self, request_id: &str) -> Response<Full<Bytes>> {
        let mut body = json!({
            "type": format!("{ERROR_TYPE_BASE}{}", self.code.replace('_', "-")),
            "title": self.title,
            "status": self.status.as_u16(),
            "detail": self.detail,
            "code": self.code,
            "requestId": request_id,
        });
        if let Some(quota) = &self.quota {
            body["quota"] = json!(quota.quota);
            body["limit"] = json!(quota.limit);
            body["used"] = json!(quota.used);
        }
        let mut builder = Response::builder()
            .status(self.status)
            .header(CONTENT_TYPE, "application/problem+json")
            .header("x-request-id", request_id);
        if let Some(retry_after) = self.retry_after {
            builder = builder.header(RETRY_AFTER, retry_after.to_string());
        }
        if let Some(allow) = self.allow {
            builder = builder.header(ALLOW, allow);
        }
        builder
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
    }
}
