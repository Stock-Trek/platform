use super::ApiError;
use hyper::StatusCode;

struct Fixture;

impl Fixture {
    fn status(error: ApiError) -> StatusCode {
        error.response().status()
    }
}

#[test]
fn request_timeout_maps_to_request_timeout_status() {
    assert_eq!(
        Fixture::status(ApiError::request_timeout("request body timed out")),
        StatusCode::REQUEST_TIMEOUT
    );
}
