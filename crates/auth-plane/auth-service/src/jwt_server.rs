use crate::AuthService;
use chrono::Utc;
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header::HeaderMap,
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use platform_errors::{BpError, BpResult};
use platform_messages::jwt::{JwtRequest, JwtResponse};
use platform_sigv4::{SigV4Config, SigV4Request};
use std::{
    collections::BTreeMap, convert::Infallible, net::SocketAddr, sync::Arc, time::SystemTime,
};
use tokio::net::TcpListener;

const MAX_REQUEST_BYTES: usize = 64 * 1024;

pub struct JwtServer {
    listener: TcpListener,
    service: Arc<AuthService>,
    sigv4: Arc<SigV4Config>,
}

impl JwtServer {
    pub async fn bind(address: SocketAddr, service: AuthService) -> BpResult<Self> {
        Self::bind_with_sigv4(address, service, SigV4Config::from_env()?).await
    }

    pub async fn bind_with_sigv4(
        address: SocketAddr,
        service: AuthService,
        sigv4: SigV4Config,
    ) -> BpResult<Self> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(BpError::external)?;
        Ok(Self {
            listener,
            service: Arc::new(service),
            sigv4: Arc::new(sigv4),
        })
    }

    pub async fn serve(self) -> BpResult<()> {
        loop {
            let (stream, _) = self.listener.accept().await.map_err(BpError::external)?;
            let service = self.service.clone();
            let sigv4 = self.sigv4.clone();
            tokio::spawn(async move {
                let connection = http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |request| {
                            Self::respond(service.clone(), sigv4.clone(), request)
                        }),
                    )
                    .await;
                if let Err(error) = connection {
                    tracing::warn!("JWT connection failed: {error}");
                }
            });
        }
    }

    pub fn local_addr(&self) -> BpResult<SocketAddr> {
        self.listener.local_addr().map_err(BpError::external)
    }

    async fn respond(
        service: Arc<AuthService>,
        sigv4: Arc<SigV4Config>,
        request: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let response = match Self::dispatch(&service, &sigv4, request).await {
            Ok(response) => match serde_json::to_vec(&response) {
                Ok(body) => Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(body)))
                    .unwrap_or_else(|_| Self::error(StatusCode::INTERNAL_SERVER_ERROR)),
                Err(error) => {
                    tracing::error!("failed to serialize JWT response: {error}");
                    Self::error(StatusCode::INTERNAL_SERVER_ERROR)
                }
            },
            Err((status, message)) => {
                tracing::warn!("rejected JWT request: {message}");
                Self::error(status)
            }
        };
        Ok(response)
    }

    async fn dispatch(
        service: &AuthService,
        sigv4: &SigV4Config,
        request: Request<Incoming>,
    ) -> Result<JwtResponse, (StatusCode, String)> {
        if request.method() != Method::POST || request.uri().path() != "/jwt" {
            return Err((StatusCode::NOT_FOUND, "not found".to_string()));
        }
        let (parts, incoming) = request.into_parts();
        let body = incoming
            .collect()
            .await
            .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?
            .to_bytes();
        if body.len() > MAX_REQUEST_BYTES {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                "JWT request is too large".to_string(),
            ));
        }
        let headers = Self::headers(&parts.headers)?;
        let caller = Self::verify(sigv4, &parts, &headers, &body)?;
        let request: JwtRequest = serde_json::from_slice(&body)
            .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
        if request.owner() != caller.owner() {
            return Err((
                StatusCode::FORBIDDEN,
                "requested owner does not match the authenticated caller".to_string(),
            ));
        }
        let issued = service
            .issue_jwt_for_owner(request.owner(), Utc::now())
            .await
            .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
        Ok(JwtResponse::new(
            issued.token(),
            issued.expires_at().timestamp(),
        ))
    }

    fn verify(
        sigv4: &SigV4Config,
        parts: &hyper::http::request::Parts,
        headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Result<platform_sigv4::VerifiedCaller, (StatusCode, String)> {
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
            .map_err(|error| (StatusCode::UNAUTHORIZED, error.to_string()))
    }

    fn headers(values: &HeaderMap) -> Result<BTreeMap<String, String>, (StatusCode, String)> {
        values
            .iter()
            .map(|(name, value)| {
                value
                    .to_str()
                    .map(|value| (name.as_str().to_ascii_lowercase(), value.to_string()))
                    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))
            })
            .collect()
    }

    fn error(status: StatusCode) -> Response<Full<Bytes>> {
        Response::builder()
            .status(status)
            .body(Full::new(Bytes::from_static(b"JWT request failed")))
            .expect("static response is valid")
    }
}
