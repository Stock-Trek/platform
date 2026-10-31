use super::*;
use crate::{
    config::IngesterConfig,
    sequence::tests::InMemorySequenceStore,
    source::{MarketMessageSource, StreamFormat},
};
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_market_data::event::ParsedMessage;
use platform_nats::EventPublisher;
use std::sync::Arc;
use stock_trek_types::cex::cex_id::CexId;
use tokio::sync::mpsc;

struct FakeSource {
    fail: bool,
}

#[async_trait]
impl MarketMessageSource for FakeSource {
    async fn run(&self, _sender: mpsc::Sender<ParsedMessage>) -> BpResult<()> {
        if self.fail {
            Err(platform_errors::BpError::external("source failed"))
        } else {
            Ok(())
        }
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

fn config() -> IngesterConfig {
    IngesterConfig {
        log_filter: "info".to_string(),
        exchange: CexId::Binance,
        websocket_url: "wss://example.invalid".to_string(),
        websocket_api_key: None,
        stream_format: StreamFormat::for_exchange("json", CexId::Binance).unwrap(),
        nats_url: "nats://localhost:4222".to_string(),
        nats_subject_prefix: "market".to_string(),
    }
}

#[tokio::test]
async fn source_completion_is_an_error() {
    let runner = IngesterRunner::new(
        config(),
        FakeSource { fail: false },
        Arc::new(FakePublisher),
        Arc::new(InMemorySequenceStore::new()),
    );

    let error = runner.run().await.unwrap_err();

    assert!(error.to_string().contains("before shutdown"));
}

#[tokio::test]
async fn propagates_source_error() {
    let runner = IngesterRunner::new(
        config(),
        FakeSource { fail: true },
        Arc::new(FakePublisher),
        Arc::new(InMemorySequenceStore::new()),
    );

    let error = runner.run().await.unwrap_err();

    assert_eq!(error.to_string(), "source failed");
}
