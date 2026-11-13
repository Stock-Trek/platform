use async_nats::jetstream::kv::{Operation, Store, Watch};
use futures::StreamExt;
use platform_errors::BuildResult;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

const WATCH_RETRY_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValueChange {
    pub revision: u64,
    pub value: Option<Vec<u8>>,
}

pub struct KeyValueWatch {
    store: Store,
    key: String,
    watch: Option<Pin<Box<Watch>>>,
    last_revision: Option<u64>,
}

impl KeyValueWatch {
    pub(super) fn new(store: Store, key: &str) -> Self {
        Self {
            store,
            key: key.to_string(),
            watch: None,
            last_revision: None,
        }
    }

    pub async fn next(&mut self) -> BuildResult<KeyValueChange> {
        loop {
            if self.watch.is_none() {
                self.connect().await;
            }
            let Some(watch) = self.watch.as_mut() else {
                continue;
            };
            match watch.next().await {
                Some(Ok(entry)) => {
                    self.last_revision = Some(entry.revision);
                    return Ok(KeyValueChange {
                        revision: entry.revision,
                        value: (entry.operation == Operation::Put).then(|| entry.value.to_vec()),
                    });
                }
                Some(Err(error)) => {
                    warn!(%error, key = %self.key, "KV watch failed; reconnecting");
                    self.watch = None;
                    sleep(WATCH_RETRY_BACKOFF).await;
                }
                None => {
                    warn!(key = %self.key, "KV watch ended; reconnecting");
                    self.watch = None;
                    sleep(WATCH_RETRY_BACKOFF).await;
                }
            }
        }
    }

    async fn connect(&mut self) {
        loop {
            let opened = match self.last_revision {
                Some(revision) => {
                    self.store
                        .watch_from_revision(&self.key, revision.saturating_add(1))
                        .await
                }
                None => self.store.watch_with_history(&self.key).await,
            };
            match opened {
                Ok(watch) => {
                    self.watch = Some(Box::pin(watch));
                    return;
                }
                Err(error) => {
                    warn!(%error, key = %self.key, "failed to open KV watch; retrying");
                    self.last_revision = None;
                    sleep(WATCH_RETRY_BACKOFF).await;
                }
            }
        }
    }
}
