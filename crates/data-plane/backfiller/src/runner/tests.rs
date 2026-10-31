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
        trading_mode: platform_exchange_types::urls::TradingMode::Real,
        signing_algorithm: platform_exchange_types::encrypt::EncryptionAlgorithm::Ed25519,
        nats_url: "nats://localhost:4222".to_string(),
        gap_subject_prefix: "market.gaps".to_string(),
        event_subject_prefix: "market.events".to_string(),
        exchange_http_url: "https://example.invalid".to_string(),
        ssm: platform_aws::secrets::SsmConfig::new("/platform/backfiller/test".to_string(), true)
            .unwrap(),
    }
}

#[tokio::test]
async fn source_completion_is_an_error() {
    let runner = BackfillerRunner::new(config(), FakeSource, FakeFetcher, FakePublisher);

    let error = runner
        .run_with_retry_store(InMemoryGapRetryStore::new())
        .await
        .unwrap_err();

    assert!(error.to_string().contains("before shutdown"));
}
