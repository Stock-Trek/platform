use crate::{
    ApiToken, AuthService,
    jwt_server_error::{JwtServerError, JwtServerResult},
};
use chrono::Utc;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header::{CONTENT_LENGTH, HeaderMap},
};
use platform_messages::{JwtRequest, JwtResponse};
use platform_sigv4::{SigV4Config, SigV4Request};
use serde::Serialize;
use std::{collections::BTreeMap, time::Duration, time::SystemTime};
use tokio::time::timeout;

const MAX_REQUEST_BYTES: usize = 64 * 1024;
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(10);
const JWT_PATH: &str = "/jwt";
const JWKS_PATH: &str = "/.well-known/jwks.json";
const JWKS_ALIAS_PATH: &str = "/jwks";

pub(super) struct JwtServerRequest;

impl JwtServerRequest {
    pub(super) async fn dispatch(
        service: &AuthService,
        sigv4: &SigV4Config,
        request: Request<Incoming>,
    ) -> JwtServerResult<Response<Full<Bytes>>> {
        if request.method() == Method::GET
            && matches!(request.uri().path(), JWKS_PATH | JWKS_ALIAS_PATH)
        {
            return Ok(Self::json(StatusCode::OK, service.jwks()));
        }
        if request.method() != Method::POST || request.uri().path() != JWT_PATH {
            return Err(JwtServerError::new(StatusCode::NOT_FOUND, "not found"));
        }
        Self::ensure_content_length(&request)?;
        let (parts, incoming) = request.into_parts();
        let body = Self::read_body(incoming).await?;
        let headers = Self::headers(&parts.headers)?;
        let caller = Self::verify(sigv4, &parts, &headers, &body)?;
        let request: JwtRequest = serde_json::from_slice(&body)
            .map_err(|error| JwtServerError::new(StatusCode::BAD_REQUEST, error.to_string()))?;
        let owner = ApiToken::validated_owner(request.owner())
            .map_err(|error| JwtServerError::new(StatusCode::BAD_REQUEST, error.to_string()))?;
        if owner != caller.owner() {
            return Err(JwtServerError::new(
                StatusCode::FORBIDDEN,
                "requested owner does not match the authenticated caller",
            ));
        }
        let issued = service
            .issue_jwt_for_owner(owner, Utc::now())
            .await
            .map_err(JwtServerError::issuance)?;
        Ok(Self::json(
            StatusCode::OK,
            &JwtResponse::new(issued.token(), issued.expires_at().timestamp()),
        ))
    }

    async fn read_body(body: Incoming) -> JwtServerResult<Bytes> {
        let body = Limited::new(body, MAX_REQUEST_BYTES);
        let collected = timeout(BODY_READ_TIMEOUT, body.collect())
            .await
            .map_err(|_| {
                (
                    StatusCode::REQUEST_TIMEOUT,
                    "JWT request body timed out".to_string(),
                )
            })?
            .map_err(|error| {
                if error.downcast_ref::<LengthLimitError>().is_some() {
                    (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "JWT request is too large".to_string(),
                    )
                } else {
                    (StatusCode::BAD_REQUEST, error.to_string())
                }
            })?;
        Ok(collected.to_bytes())
    }

    fn ensure_content_length(request: &Request<Incoming>) -> JwtServerResult<()> {
        let too_large = request
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|length| length > MAX_REQUEST_BYTES);
        if too_large {
            Err(JwtServerError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "JWT request is too large",
            ))
        } else {
            Ok(())
        }
    }

    fn verify(
        sigv4: &SigV4Config,
        parts: &hyper::http::request::Parts,
        headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> JwtServerResult<platform_sigv4::VerifiedCaller> {
        let host = headers
            .get("host")
            .ok_or_else(|| (StatusCode::UNAUTHORIZED, "missing host header".to_string()))?;
        let path = parts
            .uri
            .path_and_query()
            .map(|value| value.as_str())
            .unwrap_or("/");
        let url = format!("http://{host}{path}");
        let request = SigV4Request::new(parts.method.as_str(), &url, headers, body);
        sigv4
            .verify(&request, SystemTime::now())
            .map_err(|error| JwtServerError::new(StatusCode::UNAUTHORIZED, error.to_string()))
    }

    fn headers(values: &HeaderMap) -> JwtServerResult<BTreeMap<String, String>> {
        values
            .iter()
            .map(|(name, value)| {
                value
                    .to_str()
                    .map(|value| (name.as_str().to_ascii_lowercase(), value.to_string()))
                    .map_err(|error| {
                        JwtServerError::new(StatusCode::BAD_REQUEST, error.to_string())
                    })
            })
            .collect()
    }

    fn json<T: Serialize>(status: StatusCode, value: &T) -> Response<Full<Bytes>> {
        match serde_json::to_vec(value) {
            Ok(body) => Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(body)))
                .unwrap_or_else(|_| Self::error(StatusCode::INTERNAL_SERVER_ERROR)),
            Err(error) => {
                tracing::error!("failed to serialize JWT response: {error}");
                Self::error(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }

    pub(super) fn error(status: StatusCode) -> Response<Full<Bytes>> {
        Response::builder()
            .status(status)
            .body(Full::new(Bytes::from_static(b"JWT request failed")))
            .expect("static response is valid")
    }
}
