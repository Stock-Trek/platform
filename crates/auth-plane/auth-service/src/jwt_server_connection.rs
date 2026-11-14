use crate::{AuthService, jwt_server_request::JwtServerRequest};
use http_body_util::Full;
use hyper::{
    Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header::RETRY_AFTER,
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use platform_errors::{AuthError, PlatformError};
use platform_sigv4::SigV4Config;
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::watch};

const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

pub struct JwtServerConnection;

impl JwtServerConnection {
    pub async fn serve(
        stream: TcpStream,
        service: Arc<AuthService>,
        sigv4: Arc<SigV4Config>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut connection = Box::pin(
            http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_READ_TIMEOUT)
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |request| {
                        Self::respond(service.clone(), sigv4.clone(), request)
                    }),
                ),
        );
        tokio::select! {
            result = connection.as_mut() => Self::log_connection(result),
            _ = Self::wait_for_shutdown(&mut shutdown) => {
                connection.as_mut().graceful_shutdown();
                Self::log_connection(connection.as_mut().await);
            }
        }
    }

    async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
        let _ = shutdown.wait_for(|shutdown| *shutdown).await;
    }

    fn log_connection(result: Result<(), hyper::Error>) {
        if let Err(error) = result {
            tracing::warn!("JWT connection failed: {error}");
        }
    }

    async fn respond(
        service: Arc<AuthService>,
        sigv4: Arc<SigV4Config>,
        request: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let response = match JwtServerRequest::dispatch(&service, &sigv4, request).await {
            Ok(response) => response,
            Err(error) => {
                tracing::error!("JWT request failed: {error}");
                JwtServerRequest::error(StatusCode::INTERNAL_SERVER_ERROR)
            }
        };
        Ok(response)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct JwtServerError {
    status: StatusCode,
    message: String,
    retry_after: Option<Duration>,
}

impl JwtServerError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn issuance(error: PlatformError) -> Self {
        let retry_after = match &error {
            PlatformError::Auth(AuthError::Unavailable { retry_after }) => Some(*retry_after),
            _ => None,
        };
        let status = if error.retryability().is_retryable() {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::BAD_REQUEST
        };
        Self {
            status,
            message: error.to_string(),
            retry_after,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn response(self) -> Response<Full<Bytes>> {
        let mut builder = Response::builder().status(self.status);
        if let Some(retry_after) = self.retry_after {
            builder = builder.header(RETRY_AFTER, retry_after.as_secs().max(1).to_string());
        }
        builder
            .body(Full::new(Bytes::from_static(b"JWT request failed")))
            .expect("static response is valid")
    }
}

impl From<(StatusCode, String)> for JwtServerError {
    fn from((status, message): (StatusCode, String)) -> Self {
        Self::new(status, message)
    }
}

impl From<JwtServerError> for PlatformError {
    fn from(error: JwtServerError) -> Self {
        Self::external(error)
    }
}
