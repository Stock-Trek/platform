use super::test_feed_tests::{Fixture, RecordingFeed};
use crate::{
    PortfolioService, PortfolioUpdate, PortfolioUpdateConsumer, PortfolioUpdateMessage,
    PortfolioUpdateSource,
};
use async_trait::async_trait;
use platform_errors::BuildResult;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct FakeUpdateSource {
    messages: Mutex<VecDeque<PortfolioUpdateMessage>>,
    checkpoints: Mutex<Vec<u64>>,
    acknowledged: Mutex<usize>,
}

impl FakeUpdateSource {
    fn with_messages(messages: Vec<PortfolioUpdateMessage>) -> Self {
        Self {
            messages: Mutex::new(messages.into()),
            ..Self::default()
        }
    }

    fn checkpoints(&self) -> Vec<u64> {
        self.checkpoints.lock().expect("checkpoints").clone()
    }

    fn acknowledged(&self) -> usize {
        *self.acknowledged.lock().expect("acknowledged")
    }
}

#[async_trait]
impl PortfolioUpdateSource for FakeUpdateSource {
    async fn next(&self) -> BuildResult<Option<PortfolioUpdateMessage>> {
        Ok(self.messages.lock().expect("messages").pop_front())
    }

    async fn checkpoint(&self, sequence: u64) -> BuildResult<()> {
        self.checkpoints.lock().expect("checkpoints").push(sequence);
        Ok(())
    }

    async fn acknowledge(&self, _message: &PortfolioUpdateMessage) -> BuildResult<()> {
        *self.acknowledged.lock().expect("acknowledged") += 1;
        Ok(())
    }
}

fn message(account: &str, total: f64, version: u64, sequence: u64) -> PortfolioUpdateMessage {
    let update = PortfolioUpdate::versioned(
        Fixture::account(account),
        None,
        Fixture::portfolio(stock_trek_types::cex::asset_id::AssetId::Bitcoin, total),
        version,
        chrono::Utc::now(),
    );
    PortfolioUpdateMessage::new(update, Some(sequence), None)
}

fn service() -> (Arc<RecordingFeed>, Arc<PortfolioService>) {
    let feed = Arc::new(RecordingFeed::default());
    let service = Arc::new(PortfolioService::new(feed.clone()));
    (feed, service)
}

#[tokio::test]
async fn consumer_applies_checkpoints_and_acknowledges_updates() {
    let (_feed, service) = service();
    service
        .track_account(Fixture::account("primary"))
        .await
        .expect("track");
    let source = Arc::new(FakeUpdateSource::with_messages(vec![
        message("primary", 2.0, 1, 10),
        message("primary", 2.0, 1, 11),
        message("primary", 3.0, 2, 12),
    ]));
    let consumer =
        PortfolioUpdateConsumer::new(service.clone(), source.clone(), Duration::from_millis(10));

    assert_eq!(consumer.run_once().await.expect("consume"), 3);
    assert_eq!(source.checkpoints(), vec![10, 11, 12]);
    assert_eq!(source.acknowledged(), 3);
    let portfolio = service
        .account_portfolio(&Fixture::account("primary"))
        .expect("portfolio");
    assert_eq!(
        portfolio.asset_total(stock_trek_types::cex::asset_id::AssetId::Bitcoin),
        3.0
    );
    assert_eq!(
        service
            .update_metadata(&Fixture::account("primary"))
            .expect("metadata")
            .version(),
        2
    );
}

#[tokio::test]
async fn a_restarted_service_replays_the_update_stream() {
    let first_source = Arc::new(FakeUpdateSource::with_messages(vec![message(
        "primary", 1.0, 1, 1,
    )]));
    let (_first_feed, first_service) = service();
    first_service
        .track_account(Fixture::account("primary"))
        .await
        .expect("track");
    PortfolioUpdateConsumer::new(
        first_service,
        first_source.clone(),
        Duration::from_millis(10),
    )
    .run_once()
    .await
    .expect("first run");
    assert_eq!(first_source.checkpoints(), vec![1]);

    let (_second_feed, second_service) = service();
    second_service
        .track_account(Fixture::account("primary"))
        .await
        .expect("track");
    let replay = Arc::new(FakeUpdateSource::with_messages(vec![
        message("primary", 1.0, 1, 1),
        message("primary", 4.0, 2, 2),
    ]));
    PortfolioUpdateConsumer::new(
        second_service.clone(),
        replay.clone(),
        Duration::from_millis(10),
    )
    .run_once()
    .await
    .expect("replay");

    assert_eq!(replay.checkpoints(), vec![1, 2]);
    let portfolio = second_service
        .account_portfolio(&Fixture::account("primary"))
        .expect("portfolio");
    assert_eq!(
        portfolio.asset_total(stock_trek_types::cex::asset_id::AssetId::Bitcoin),
        4.0
    );
}

#[tokio::test]
async fn state_ignores_unversioned_duplicates_but_accepts_latest_unversioned_value() {
    let (_feed, service) = service();
    service
        .track_account(Fixture::account("primary"))
        .await
        .expect("track");
    assert!(service.apply_update(Fixture::update("primary", 1.0)));
    assert!(service.apply_update(Fixture::update("primary", 2.0)));
    let portfolio = service
        .account_portfolio(&Fixture::account("primary"))
        .expect("portfolio");
    assert_eq!(
        portfolio.asset_total(stock_trek_types::cex::asset_id::AssetId::Bitcoin),
        2.0
    );
}
