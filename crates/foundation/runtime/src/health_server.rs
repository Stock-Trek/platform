use crate::{HealthConfig, HealthState, Runtime};
use http_body_util::Full;
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use platform_errors::{PlatformError, PlatformResult};
use std::{convert::Infallible, future::Future, net::SocketAddr, time::Duration};
use tokio::{net::TcpListener, sync::watch, task::JoinSet, time::timeout};
use tracing::warn;

const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const ALLOWED_METHODS: &str = "GET, HEAD";

pub struct HealthServer {
    listener: TcpListener,
    state: HealthState,
}

impl HealthServer {
    pub async fn bind(address: SocketAddr, state: HealthState) -> PlatformResult<Self> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(PlatformError::external)?;
        Ok(Self { listener, state })
    }

    pub async fn from_config(
        config: Option<HealthConfig>,
        state: HealthState,
    ) -> PlatformResult<Option<Self>> {
        match config {
            Some(config) => Self::bind(config.address(), state).await.map(Some),
            None => Ok(None),
        }
    }

    pub async fn from_env(state: HealthState) -> PlatformResult<Option<Self>> {
        Self::from_config(HealthConfig::from_env()?, state).await
    }

    pub async fn from_service_env(
        service: &str,
        state: HealthState,
    ) -> PlatformResult<Option<Self>> {
        Self::from_config(HealthConfig::from_service_env(service)?, state).await
    }

    pub fn local_addr(&self) -> PlatformResult<SocketAddr> {
        self.listener.local_addr().map_err(PlatformError::external)
    }

    pub async fn serve(self) -> PlatformResult<()> {
        self.serve_with_shutdown(Runtime::shutdown_signal()).await
    }

    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> PlatformResult<()>
    where
        F: Future<Output = PlatformResult<()>> + Send,
    {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut connections = JoinSet::new();
        tokio::pin!(shutdown);
        let result = loop {
            tokio::select! {
                signal = &mut shutdown => break signal,
                accepted = self.listener.accept() => {
                    let (stream, peer) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => break Err(PlatformError::external(error)),
                    };
                    let state = self.state.clone();
                    let mut shutdown = shutdown_rx.clone();
                    connections.spawn(async move {
                        let service = service_fn(move |request| {
                            Self::handle(state.clone(), request)
                        });
                        let connection = http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service);
                        tokio::pin!(connection);
                        tokio::select! {
                            result = &mut connection => {
                                if let Err(error) = result {
                                    warn!(%peer, %error, "health connection failed");
                                }
                            }
                            _ = shutdown.changed() => {
                                connection.as_mut().graceful_shutdown();
                                if let Err(error) = connection.await {
                                    warn!(%peer, %error, "health connection failed during shutdown");
                                }
                            }
                        }
                    });
                }
                joined = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = joined {
                        warn!(%error, "health connection task failed");
                    }
                }
            }
        };

        drop(self.listener);
        let _ = shutdown_tx.send(true);
        Self::drain(&mut connections).await;
        result
    }

    async fn handle(
        state: HealthState,
        request: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let path = request.uri().path();
        let method = request.method();
        let response = if path == "/healthz" || path == "/readyz" {
            if method == Method::GET || method == Method::HEAD {
                if path == "/readyz" && !state.is_ready() {
                    Self::response(StatusCode::SERVICE_UNAVAILABLE, None)
                } else {
                    Self::response(StatusCode::NO_CONTENT, None)
                }
            } else {
                Self::response(StatusCode::METHOD_NOT_ALLOWED, Some(ALLOWED_METHODS))
            }
        } else {
            Self::response(StatusCode::NOT_FOUND, None)
        };
        Ok(response)
    }

    fn response(status: StatusCode, allow: Option<&str>) -> Response<Full<Bytes>> {
        let mut response = Response::new(Full::default());
        *response.status_mut() = status;
        if let Some(allow) = allow {
            response.headers_mut().insert(
                hyper::header::ALLOW,
                allow.parse().expect("valid allow header"),
            );
        }
        response
    }

    async fn drain(connections: &mut JoinSet<()>) {
        let draining = async {
            while let Some(joined) = connections.join_next().await {
                if let Err(error) = joined {
                    warn!(%error, "health connection task failed");
                }
            }
        };
        if timeout(DRAIN_TIMEOUT, draining).await.is_err() {
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
    }
}
