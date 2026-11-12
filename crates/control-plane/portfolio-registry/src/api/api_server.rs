use crate::api::{ApiConnection, RequestHandler};
use platform_errors::{BuildError, BuildResult};
use platform_runtime::Runtime;
use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
    task::JoinSet,
    time::timeout,
};

const MAX_CONNECTIONS: usize = 128;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

pub struct ApiServer {
    listener: TcpListener,
    handler: Arc<dyn RequestHandler>,
    slots: Arc<Semaphore>,
}

impl ApiServer {
    pub async fn bind(address: SocketAddr, handler: Arc<dyn RequestHandler>) -> BuildResult<Self> {
        let listener = TcpListener::bind(address)
            .await
            .map_err(BuildError::external)?;
        Ok(Self {
            listener,
            handler,
            slots: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        })
    }

    pub async fn serve(self) -> BuildResult<()> {
        self.serve_with_shutdown(Runtime::shutdown_signal()).await
    }

    pub async fn serve_with_shutdown<F>(self, shutdown: F) -> BuildResult<()>
    where
        F: Future<Output = BuildResult<()>> + Send,
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
                        Err(error) => break Err(BuildError::external(error)),
                    };
                    let permit = match self.slots.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            tracing::warn!(%peer, "portfolio registry connection limit reached");
                            drop(stream);
                            continue;
                        }
                    };
                    let handler = self.handler.clone();
                    let shutdown = shutdown_rx.clone();
                    connections.spawn(async move {
                        ApiConnection::serve(stream, handler, shutdown).await;
                        drop(permit);
                    });
                }
                joined = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = joined {
                        tracing::warn!("portfolio registry connection task failed: {error}");
                    }
                }
            }
        };
        drop(self.listener);
        let _ = shutdown_tx.send(true);
        Self::drain(&mut connections).await;
        result
    }

    async fn drain(connections: &mut JoinSet<()>) {
        let draining = async {
            while let Some(joined) = connections.join_next().await {
                if let Err(error) = joined {
                    tracing::warn!("portfolio registry connection task failed: {error}");
                }
            }
        };
        if timeout(DRAIN_TIMEOUT, draining).await.is_err() {
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
    }
}
