use crate::{PortfolioService, PortfolioUpdateOutcome, PortfolioUpdateSource};
use platform_errors::BpResult;
use platform_metrics::PortfolioMetrics;
use std::{future::Future, sync::Arc, time::Duration};

pub struct PortfolioUpdateConsumer {
    service: Arc<PortfolioService>,
    source: Arc<dyn PortfolioUpdateSource>,
    retry_backoff: Duration,
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
        }
    }

    pub async fn run_once(&self) -> BpResult<usize> {
        let mut processed = 0;
        while let Some(message) = self.source.next().await? {
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
                        tracing::warn!(%error, "portfolio update consumer failed; retrying");
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
