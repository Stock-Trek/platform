use crate::api::ApiError;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::{
    Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE},
};
use serde::{Serialize, de::DeserializeOwned};
use std::time::Duration;
use tokio::time::timeout;

const MAX_REQUEST_BYTES: usize = 64 * 1024;
const BODY_TIMEOUT: Duration = Duration::from_secs(10);

pub fn json_response<T: Serialize>(status: StatusCode, value: &T) -> Response<Full<Bytes>> {
    match serde_json::to_vec(value) {
        Ok(body) => Response::builder()
            .status(status)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::new()))),
        Err(error) => ApiError::internal(error.to_string()).response(),
    }
}

pub fn empty_response(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

pub async fn read_json<T: DeserializeOwned>(request: Request<Incoming>) -> Result<T, ApiError> {
    let too_large = request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_REQUEST_BYTES);
    if too_large {
        return Err(ApiError::payload_too_large());
    }
    let body = Limited::new(request.into_body(), MAX_REQUEST_BYTES);
    let collected = timeout(BODY_TIMEOUT, body.collect())
        .await
        .map_err(|_| ApiError::bad_request("request body timed out"))?
        .map_err(|error| {
            if error.downcast_ref::<LengthLimitError>().is_some() {
                ApiError::payload_too_large()
            } else {
                ApiError::bad_request(error.to_string())
            }
        })?;
    serde_json::from_slice(&collected.to_bytes())
        .map_err(|error| ApiError::bad_request(error.to_string()))
}

pub fn bearer(request: &Request<Incoming>) -> Result<&str, ApiError> {
    let value = request
        .headers()
        .get(AUTHORIZATION)
        .ok_or_else(|| ApiError::unauthorized("bearer token is required"))?
        .to_str()
        .map_err(|error| ApiError::unauthorized(error.to_string()))?;
    value
        .strip_prefix("Bearer ")
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| ApiError::unauthorized("authorization must use the Bearer scheme"))
}
