use http::{Response, StatusCode};
use lambda_http::Body;
use platform_errors::{BuildError, BuildResult, HttpError};
use std::time::Duration;

pub struct Responses;

impl Responses {
    pub fn ok(body: &str) -> BuildResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::Text(body.into()))
            .map_err(|error| BuildError::Http(HttpError::Http(error)))
    }

    pub fn accepted() -> BuildResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::Empty)
            .map_err(|error| BuildError::Http(HttpError::Http(error)))
    }

    pub fn no_content() -> BuildResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::Empty)
            .map_err(|error| BuildError::Http(HttpError::Http(error)))
    }

    pub fn bad_request() -> BuildResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::Empty)
            .map_err(|error| BuildError::Http(HttpError::Http(error)))
    }

    pub fn unauthorized() -> BuildResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Body::Empty)
            .map_err(|error| BuildError::Http(HttpError::Http(error)))
    }

    pub fn internal_server_error() -> BuildResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::Empty)
            .map_err(|error| BuildError::Http(HttpError::Http(error)))
    }

    pub fn service_unavailable(retry_after: Duration) -> BuildResult<Response<Body>> {
        Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(
                http::header::RETRY_AFTER,
                retry_after.as_secs().max(1).to_string(),
            )
            .body(Body::Empty)
            .map_err(|error| BuildError::Http(HttpError::Http(error)))
    }
}
