use crate::api::{ApiRequest, Problem, Respond};
use http_body_util::Full;
use hyper::{
    Response, StatusCode,
    body::Bytes,
    header::{
        ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
        ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE, HeaderValue, VARY,
    },
};
use std::collections::HashSet;

const EXPOSED_HEADERS: [&str; 4] = ["X-Request-Id", "Retry-After", "ETag", "Location"];
const ALLOWED_HEADERS: [&str; 5] = [
    "Authorization",
    "Content-Type",
    "Idempotency-Key",
    "If-Match",
    "X-Request-Id",
];
const ALLOWED_METHODS: [&str; 5] = ["GET", "POST", "PUT", "PATCH", "DELETE"];
const PREFLIGHT_MAX_AGE_SECONDS: u32 = 600;

pub struct Cors {
    allowed_origins: HashSet<String>,
}

impl Cors {
    pub fn new(allowed_origins: Vec<String>) -> Self {
        Self {
            allowed_origins: allowed_origins.into_iter().collect(),
        }
    }

    pub fn preflight(&self, request: &ApiRequest) -> Response<Full<Bytes>> {
        match self.allowed(request.origin()) {
            Some(origin) => {
                let mut response = Respond::empty(StatusCode::NO_CONTENT);
                let headers = response.headers_mut();
                headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
                headers.insert(ACCESS_CONTROL_ALLOW_METHODS, Self::values(&ALLOWED_METHODS));
                headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, Self::values(&ALLOWED_HEADERS));
                headers.insert(
                    ACCESS_CONTROL_MAX_AGE,
                    HeaderValue::from(PREFLIGHT_MAX_AGE_SECONDS),
                );
                headers.insert(VARY, HeaderValue::from_static("Origin"));
                response
            }
            None => Problem::forbidden("the origin is not allowed").response(""),
        }
    }

    pub fn apply(
        &self,
        request: &ApiRequest,
        mut response: Response<Full<Bytes>>,
    ) -> Response<Full<Bytes>> {
        if let Some(origin) = self.allowed(request.origin()) {
            let headers = response.headers_mut();
            headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
            headers.insert(
                ACCESS_CONTROL_EXPOSE_HEADERS,
                Self::values(&EXPOSED_HEADERS),
            );
            if !headers.contains_key(VARY) {
                headers.insert(VARY, HeaderValue::from_static("Origin"));
            }
        }
        response
    }

    fn allowed(&self, origin: Option<&str>) -> Option<HeaderValue> {
        let origin = origin?;
        self.allowed_origins
            .contains(origin)
            .then(|| HeaderValue::from_str(origin).expect("origin header"))
    }

    fn values(values: &[&str]) -> HeaderValue {
        HeaderValue::from_str(&values.join(", ")).expect("header list")
    }
}
