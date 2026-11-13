use crate::{
    AccountKey, PortfolioService, PortfolioUpdateOutcome, PortfolioUpdateSource,
    read_model::PortfolioReadModelStore,
};
use platform_errors::BuildResult;
use platform_metrics::PortfolioMetrics;
use platform_time::RetryBackoff;
use std::{future::Future, sync::Arc, time::Duration};

pub struct PortfolioUpdateConsumer {
    service: Arc<PortfolioService>,
    source: Arc<dyn PortfolioUpdateSource>,
    retry_backoff: Duration,
    store: Option<Arc<PortfolioReadModelStore>>,
}

impl PortfolioUpdateConsumer {
    pub fn new(
        service: Arc<PortfolioService>,
        source: Arc<dyn PortfolioUpdateSource>,
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
            let update = message.update().clone();
            let exchange = update.exchange_label();
            let lag = chrono::Utc::now().signed_duration_since(update.occurred_at());
            PortfolioMetrics::record_consumer_lag(lag.to_std().unwrap_or_default());
            match self.service.apply_portfolio_update(update.clone()) {
                PortfolioUpdateOutcome::Applied => {
                    PortfolioMetrics::record_update_outcome(exchange.clone(), "applied");
                    tracing::debug!(
                        account = %update.account().storage_key(),
                        version = update.version(),
                        "applied portfolio update"
                    );
                }
                PortfolioUpdateOutcome::IgnoredUntracked => {
                    PortfolioMetrics::record_update_outcome(exchange.clone(), "untracked");
                    tracing::debug!(
                        account = %update.account().storage_key(),
                        version = update.version(),
                        "ignored portfolio update for untracked account"
                    );
                }
                PortfolioUpdateOutcome::IgnoredStale => {
                    PortfolioMetrics::record_update_outcome(exchange, "stale");
                    tracing::debug!(
                        account = %update.account().storage_key(),
                        version = update.version(),
                        "ignored stale portfolio update"
                    );
                }
            }
            if let Err(error) = self.persist_portfolio(update.account()).await {
                PortfolioMetrics::record_apply_failure("update", "persist");
                return Err(error);
            }
            if let Some(sequence) = message.stream_sequence()
                && let Err(error) = self.source.checkpoint(sequence).await
            {
                PortfolioMetrics::record_apply_failure("update", "checkpoint");
                return Err(error);
            }
            if let Err(error) = self.source.acknowledge(&message).await {
                PortfolioMetrics::record_apply_failure("update", "ack");
                return Err(error);
            }
            processed += 1;
        }
        Ok(processed)
    }

    async fn next(&self) -> BuildResult<Option<crate::PortfolioUpdateMessage>> {
        self.source.next().await.inspect_err(|_| {
            PortfolioMetrics::record_apply_failure("update", "fetch");
        })
    }

    async fn persist_portfolio(&self, account: &AccountKey) -> BuildResult<()> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        match self.service.portfolio_record(account) {
            Some(record) => store.save_portfolio(&record).await,
            None => store.remove_portfolio(account).await,
        }
    }

    pub async fn run<F>(&self, shutdown: F) -> BuildResult<()>
    where
        F: Future<Output = BuildResult<()>> + Send,
    {
        tokio::pin!(shutdown);
        let mut backoff =
            RetryBackoff::exponential(self.retry_backoff, self.retry_backoff.saturating_mul(16));
        loop {
            tokio::select! {
                signal = &mut shutdown => return signal,
                result = self.run_once() => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "portfolio update consumer failed; retrying");
                        tokio::select! {
                            signal = &mut shutdown => return signal,
                            _ = tokio::time::sleep(backoff.next_delay()) => {}
                        }
                    } else {
                        backoff.reset();
                    }
                }
            }
        }
    }
}
