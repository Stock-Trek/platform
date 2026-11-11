use crate::{OwnershipApply, OwnershipEventSource, PortfolioService};
use platform_errors::BpResult;
use std::{future::Future, sync::Arc, time::Duration};

pub struct OwnershipConsumer {
    service: Arc<PortfolioService>,
    source: Arc<dyn OwnershipEventSource>,
    retry_backoff: Duration,
}

impl OwnershipConsumer {
    pub fn new(
        service: Arc<PortfolioService>,
        source: Arc<dyn OwnershipEventSource>,
        retry_backoff: Duration,
    ) -> Self {
        Self {
            service,
            source,
            retry_backoff,
        }
    }

    pub async fn run_once(&self) -> BpResult<usize> {
        let mut processed = 0;
        while let Some(message) = self.source.next().await? {
            match self.service.apply_ownership_event(message.event()).await? {
                OwnershipApply::Applied { .. } => {
                    tracing::debug!(
                        account = %message.event().account().storage_key(),
                        version = message.event().version(),
                        "applied ownership event"
                    );
                }
                OwnershipApply::IgnoredStale => {
                    tracing::debug!(
                        account = %message.event().account().storage_key(),
                        version = message.event().version(),
                        "ignored stale ownership event"
                    );
                }
                OwnershipApply::IgnoredConflict => {
                    tracing::warn!(
                        account = %message.event().account().storage_key(),
                        version = message.event().version(),
                        "ignored conflicting ownership event"
                    );
                }
            }
            if let Some(sequence) = message.stream_sequence() {
                self.source.checkpoint(sequence).await?;
            }
            self.source.acknowledge(&message).await?;
            processed += 1;
        }
        Ok(processed)
    }

    pub async fn run<F>(&self, shutdown: F) -> BpResult<()>
    where
        F: Future<Output = BpResult<()>> + Send,
    {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                signal = &mut shutdown => return signal,
                result = self.run_once() => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "ownership consumer failed; retrying");
                        tokio::select! {
                            signal = &mut shutdown => return signal,
                            _ = tokio::time::sleep(self.retry_backoff) => {}
                        }
                    }
                }
            }
        }
    }
}
