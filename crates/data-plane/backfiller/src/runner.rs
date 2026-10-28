use crate::{
    backfiller::{BackfillOptions, BackfillService},
    config::BackfillerConfig,
    fetcher::MissingEventFetcher,
    gap_marker_source::GapMarkerSource,
    retry::GapRetryStore,
};
use platform_errors::BpResult;
use platform_nats::EventPublisher;
use std::sync::Arc;
use tracing::info;

pub struct BackfillerRunner<S, F, P> {
    config: BackfillerConfig,
    source: S,
    fetcher: F,
    publisher: P,
}

impl<S, F, P> BackfillerRunner<S, F, P>
where
    S: GapMarkerSource,
    F: MissingEventFetcher,
    P: EventPublisher,
{
    pub fn new(config: BackfillerConfig, source: S, fetcher: F, publisher: P) -> Self {
        Self {
            config,
            source,
            fetcher,
            publisher,
        }
    }

    #[allow(dead_code)]
    pub async fn run(self) -> BpResult<()> {
        self.run_inner(None).await
    }

    pub async fn run_with_retry_store<R>(self, retries: R) -> BpResult<()>
    where
        R: GapRetryStore + 'static,
    {
        self.run_inner(Some(Arc::new(retries))).await
    }

    async fn run_inner(self, retries: Option<Arc<dyn GapRetryStore>>) -> BpResult<()> {
        let Self {
            config,
            source,
            fetcher,
            publisher,
        } = self;
        let options = BackfillOptions {
            exchange: config.exchange,
            event_subject_prefix: config.event_subject_prefix.clone(),
        };

        info!(
            exchange = %config.exchange,
            gap_subject_prefix = %config.gap_subject_prefix,
            event_subject_prefix = %config.event_subject_prefix,
            "backfiller started"
        );
        let service = BackfillService::new(source, fetcher, publisher, options);
        match retries {
            Some(retries) => service.with_retry_store_arc(retries).run().await,
            None => service.run().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{fetcher::BackfilledEvent, retry::InMemoryGapRetryStore};
    use async_trait::async_trait;
    use platform_errors::BpResult;
    use platform_market_data::event::GapMarker;
    use stock_trek_types::cex::cex_id::CexId;

    struct FakeSource;

    #[async_trait]
    impl GapMarkerSource for FakeSource {
        async fn next_gap(&mut self) -> BpResult<Option<GapMarker>> {
            Ok(None)
        }
    }

    struct FakeFetcher;

    #[async_trait]
    impl MissingEventFetcher for FakeFetcher {
        async fn fetch(&self, _gap: &GapMarker) -> BpResult<Vec<BackfilledEvent>> {
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct FakePublisher;

    #[async_trait]
    impl EventPublisher for FakePublisher {
        async fn publish(&self, _subject: &str, _payload: &[u8]) -> BpResult<()> {
            Ok(())
        }
    }

    fn config() -> BackfillerConfig {
        BackfillerConfig {
            log_filter: "info".to_string(),
            exchange: CexId::Binance,
            nats_url: "nats://localhost:4222".to_string(),
            gap_subject_prefix: "market.gaps".to_string(),
            event_subject_prefix: "market.events".to_string(),
            exchange_http_url: "https://example.invalid".to_string(),
            exchange_api_key: None,
        }
    }

    #[tokio::test]
    async fn runs_with_injected_source_fetcher_and_publisher() {
        let runner = BackfillerRunner::new(config(), FakeSource, FakeFetcher, FakePublisher);

        runner.run().await.unwrap();
    }

    #[tokio::test]
    async fn accepts_a_durable_retry_store() {
        let runner = BackfillerRunner::new(config(), FakeSource, FakeFetcher, FakePublisher);

        runner
            .run_with_retry_store(InMemoryGapRetryStore::new())
            .await
            .unwrap();
    }
}
