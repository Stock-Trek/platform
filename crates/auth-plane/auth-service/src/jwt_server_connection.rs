use crate::{AuthService, jwt_server_error::JwtServerError, jwt_server_request::JwtServerRequest};
use http_body_util::Full;
use hyper::{
    Request, Response,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
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
                let error = JwtServerError::from(error);
                tracing::warn!("rejected JWT request: {}", error.message());
                error.response()
            }
        };
        Ok(response)
    }
}
