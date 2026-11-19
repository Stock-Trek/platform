use crate::api::{ApiError, empty_response};
use http_body_util::Full;
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
};

const ALLOWED_METHODS: &[Method] = &[Method::GET, Method::HEAD];

pub(crate) struct HealthRoute;

impl HealthRoute {
    pub(crate) fn matches(request: &Request<Incoming>) -> bool {
        matches!(request.uri().path(), "/healthz" | "/readyz")
    }

    pub(crate) fn dispatch(request: &Request<Incoming>) -> Result<Response<Full<Bytes>>, ApiError> {
        match *request.method() {
            Method::GET | Method::HEAD => Ok(empty_response(StatusCode::NO_CONTENT)),
            _ => Err(ApiError::method_not_allowed(ALLOWED_METHODS)),
        }
    }
}
