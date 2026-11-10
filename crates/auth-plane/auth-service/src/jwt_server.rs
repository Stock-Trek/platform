use crate::AuthService;
use chrono::Utc;
use http_body_util::{BodyExt, Full};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use platform_errors::{BpError, BpResult};
use platform_messages::jwt::{JwtRequest, JwtResponse};
use std::{convert::Infallible, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

const MAX_REQUEST_BYTES: usize = 64 * 1024;

pub struct JwtServer {
    listener: TcpListener,
    service: Arc<AuthService>,
}

impl JwtServer {
    pub async fn bind(address: SocketAddr, service: AuthService) -> BpResult<Self> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(BpError::external)?;
        Ok(Self {
            listener,
            service: Arc::new(service),
        })
    }

    pub async fn serve(self) -> BpResult<()> {
        loop {
            let (stream, _) = self.listener.accept().await.map_err(BpError::external)?;
            let service = self.service.clone();
            tokio::spawn(async move {
                let connection = http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |request| Self::respond(service.clone(), request)),
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
        request: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let response = match Self::dispatch(&service, request).await {
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
        request: Request<Incoming>,
    ) -> Result<JwtResponse, (StatusCode, String)> {
        if request.method() != Method::POST || request.uri().path() != "/jwt" {
            return Err((StatusCode::NOT_FOUND, "not found".to_string()));
        }
        let body = request
            .into_body()
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
        let request: JwtRequest = serde_json::from_slice(&body)
            .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
        let issued = service
            .issue_jwt_for_owner(request.owner(), Utc::now())
            .await
            .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
        Ok(JwtResponse::new(
            issued.token(),
            issued.expires_at().timestamp(),
        ))
    }

    fn error(status: StatusCode) -> Response<Full<Bytes>> {
        Response::builder()
            .status(status)
            .body(Full::new(Bytes::from_static(b"JWT request failed")))
            .expect("static response is valid")
    }
}
