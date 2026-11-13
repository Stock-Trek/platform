use crate::{
    AccountKey, OwnershipApply, OwnershipEventSource, PortfolioService,
    read_model::PortfolioReadModelStore, retry_backoff::RetryBackoff,
};
use platform_errors::BuildResult;
use platform_metrics::PortfolioMetrics;
use std::{future::Future, sync::Arc, time::Duration};

pub struct OwnershipConsumer {
    service: Arc<PortfolioService>,
    source: Arc<dyn OwnershipEventSource>,
    retry_backoff: Duration,
    store: Option<Arc<PortfolioReadModelStore>>,
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
            store: None,
        }
    }

    pub fn with_read_model_store(mut self, store: Arc<PortfolioReadModelStore>) -> Self {
        self.store = Some(store);
        self
    }

    pub async fn run_once(&self) -> BuildResult<usize> {
        let mut processed = 0;
        while let Some(message) = self.next().await? {
            let lag = chrono::Utc::now().signed_duration_since(message.event().occurred_at());
            PortfolioMetrics::record_ownership_lag(lag.to_std().unwrap_or_default());
            let outcome = match self.service.apply_ownership_event(message.event()).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    PortfolioMetrics::record_apply_failure("ownership", "apply");
                    return Err(error);
                }
            };
            match outcome {
                OwnershipApply::Applied { .. } => {
                    PortfolioMetrics::record_ownership_outcome("applied");
                    tracing::debug!(
                        account = %message.event().account().storage_key(),
                        version = message.event().version(),
                        "applied ownership event"
                    );
                }
                OwnershipApply::IgnoredStale => {
                    PortfolioMetrics::record_ownership_outcome("stale");
                    tracing::debug!(
                        account = %message.event().account().storage_key(),
                        version = message.event().version(),
                        "ignored stale ownership event"
                    );
                }
                OwnershipApply::IgnoredConflict => {
                    PortfolioMetrics::record_ownership_outcome("conflict");
                    tracing::warn!(
                        account = %message.event().account().storage_key(),
                        version = message.event().version(),
                        "ignored conflicting ownership event"
                    );
                }
            }
            if let Err(error) = self.persist(message.event().account()).await {
                PortfolioMetrics::record_apply_failure("ownership", "persist");
                return Err(error);
            }
            if let Some(sequence) = message.stream_sequence()
                && let Err(error) = self.source.checkpoint(sequence).await
            {
                PortfolioMetrics::record_apply_failure("ownership", "checkpoint");
                return Err(error);
            }
            if let Err(error) = self.source.acknowledge(&message).await {
                PortfolioMetrics::record_apply_failure("ownership", "ack");
                return Err(error);
            }
            processed += 1;
        }
        Ok(processed)
    }

    async fn next(&self) -> BuildResult<Option<crate::OwnershipMessage>> {
        self.source.next().await.inspect_err(|_| {
            PortfolioMetrics::record_apply_failure("ownership", "fetch");
        })
    }

    async fn persist(&self, account: &AccountKey) -> BuildResult<()> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        match self.service.ownership_record(account) {
            Some(record) => store.save_ownership(&record).await,
            None => {
                store.remove_portfolio(account).await?;
                store.remove_ownership(account).await
            }
        }
    }

    pub async fn run<F>(&self, shutdown: F) -> BuildResult<()>
    where
        F: Future<Output = BuildResult<()>> + Send,
    {
        tokio::pin!(shutdown);
        let mut backoff = RetryBackoff::new(self.retry_backoff);
        loop {
            tokio::select! {
                signal = &mut shutdown => return signal,
                result = self.run_once() => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "ownership consumer failed; retrying");
                        tokio::select! {
                            signal = &mut shutdown => return signal,
                            _ = tokio::time::sleep(backoff.delay()) => {}
                        }
                    } else {
                        backoff.reset();
                    }
                }
            }
        }
    }
}
