use http::{Response, StatusCode};
use lambda_http::Body;
use platform_errors::{HttpError, PlatformError, PlatformResult};
use std::time::Duration;

pub(crate) struct Responses;

impl Responses {
    pub(crate) fn ok(body: &str) -> PlatformResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::Text(body.into()))
            .map_err(|error| PlatformError::Http(HttpError::Http(error)))
    }

    pub(crate) fn accepted() -> PlatformResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::Empty)
            .map_err(|error| PlatformError::Http(HttpError::Http(error)))
    }

    pub(crate) fn no_content() -> PlatformResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::Empty)
            .map_err(|error| PlatformError::Http(HttpError::Http(error)))
    }

    pub(crate) fn bad_request() -> PlatformResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::Empty)
            .map_err(|error| PlatformError::Http(HttpError::Http(error)))
    }

    pub(crate) fn unauthorized() -> PlatformResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Body::Empty)
            .map_err(|error| PlatformError::Http(HttpError::Http(error)))
    }

    pub(crate) fn internal_server_error() -> PlatformResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::Empty)
            .map_err(|error| PlatformError::Http(HttpError::Http(error)))
    }

    pub(crate) fn service_unavailable(retry_after: Duration) -> PlatformResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(
                http::header::RETRY_AFTER,
                retry_after.as_secs().max(1).to_string(),
            )
            .body(Body::Empty)
            .map_err(|error| PlatformError::Http(HttpError::Http(error)))
    }
}
