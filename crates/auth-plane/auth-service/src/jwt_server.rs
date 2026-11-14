use crate::{AuthService, jwt_server_connection::JwtServerConnection};
use http_body_util::Full;
use hyper::{Response, StatusCode, body::Bytes, header::RETRY_AFTER};
use platform_errors::{AuthError, PlatformError, PlatformResult};
use platform_runtime::Runtime;
use platform_sigv4::SigV4Config;
use platform_time::RetryBackoff;
use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
    time::{sleep, timeout},
};

const MAX_CONNECTIONS: usize = 256;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const ACCEPT_BACKOFF_INITIAL: Duration = Duration::from_millis(100);
const ACCEPT_BACKOFF_MAXIMUM: Duration = Duration::from_secs(5);

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

pub struct JwtServer {
    listener: TcpListener,
    service: Arc<AuthService>,
    sigv4: Arc<SigV4Config>,
    slots: Arc<Semaphore>,
}

impl JwtServer {
    pub async fn bind_with_sigv4(
        address: SocketAddr,
        service: AuthService,
        sigv4: SigV4Config,
    ) -> PlatformResult<Self> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(PlatformError::external)?;
        Ok(Self {
            listener,
            service: Arc::new(service),
            sigv4: Arc::new(sigv4),
            slots: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        })
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
        let mut backoff = RetryBackoff::exponential(ACCEPT_BACKOFF_INITIAL, ACCEPT_BACKOFF_MAXIMUM)
            .without_jitter();
        tokio::pin!(shutdown);

        let result = loop {
            tokio::select! {
                signal = &mut shutdown => break signal,
                accepted = self.listener.accept() => {
                    let (stream, peer) = match accepted {
                        Ok(accepted) => {
                            backoff.reset();
                            accepted
                        }
                        Err(error) => {
                            let delay = backoff.next_delay();
                            tracing::warn!(
                                %error,
                                backoff_ms = delay.as_millis(),
                                "JWT accept failed; backing off"
                            );
                            tokio::select! {
                                signal = &mut shutdown => break signal,
                                _ = sleep(delay) => continue,
                            }
                        }
                    };
                    let permit = match self.slots.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            tracing::warn!(%peer, "JWT connection limit reached");
                            drop(stream);
                            continue;
                        }
                    };
                    let service = self.service.clone();
                    let sigv4 = self.sigv4.clone();
                    let shutdown = shutdown_rx.clone();
                    connections.spawn(async move {
                        JwtServerConnection::serve(stream, service, sigv4, shutdown).await;
                        drop(permit);
                    });
                }
                joined = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = joined {
                        tracing::warn!("JWT connection task failed: {error}");
                    }
                }
            }
        };

        drop(self.listener);
        let _ = shutdown_tx.send(true);
        Self::drain(&mut connections).await;
        result
    }

    #[cfg(test)]
    pub fn local_addr(&self) -> PlatformResult<SocketAddr> {
        self.listener.local_addr().map_err(PlatformError::external)
    }

    async fn drain(connections: &mut JoinSet<()>) {
        let draining = async {
            while let Some(joined) = connections.join_next().await {
                if let Err(error) = joined {
                    tracing::warn!("JWT connection task failed: {error}");
                }
            }
        };
        if timeout(DRAIN_TIMEOUT, draining).await.is_err() {
            tracing::warn!("JWT connection drain timed out; aborting remaining connections");
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
    }
}
