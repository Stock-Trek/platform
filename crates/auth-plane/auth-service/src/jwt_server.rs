use crate::{AuthService, jwt_server_connection::JwtServerConnection};
use platform_errors::{PlatformError, PlatformResult};
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

pub struct JwtServer {
    listener: TcpListener,
    service: Arc<AuthService>,
    sigv4: Arc<SigV4Config>,
    slots: Arc<Semaphore>,
}

impl JwtServer {
    pub async fn bind(address: SocketAddr, service: AuthService) -> PlatformResult<Self> {
        Self::bind_with_sigv4(address, service, SigV4Config::from_env()?).await
    }

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
