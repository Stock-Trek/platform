use super::test_feed_tests::Fixture;
use crate::{
    PortfolioService, PortfolioUpdate, PortfolioUpdateConsumer, PortfolioUpdateMessage,
    PortfolioUpdateSource,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::BpResult;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use stock_trek_types::cex::asset_id::AssetId;

#[derive(Default)]
struct RecordingUpdateSource {
    messages: Mutex<VecDeque<PortfolioUpdateMessage>>,
    checkpoints: Mutex<Vec<u64>>,
    acknowledged: AtomicUsize,
}

impl RecordingUpdateSource {
    fn push(&self, message: PortfolioUpdateMessage) {
        self.messages.lock().push_back(message);
    }

    fn checkpoints(&self) -> Vec<u64> {
        self.checkpoints.lock().clone()
    }

    fn acknowledged(&self) -> usize {
        self.acknowledged.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl PortfolioUpdateSource for RecordingUpdateSource {
    async fn next(&self) -> BpResult<Option<PortfolioUpdateMessage>> {
        Ok(self.messages.lock().pop_front())
    }

    async fn checkpoint(&self, sequence: u64) -> BpResult<()> {
        self.checkpoints.lock().push(sequence);
        Ok(())
    }

    async fn acknowledge(&self, _message: &PortfolioUpdateMessage) -> BpResult<()> {
        self.acknowledged.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct Harness;

impl Harness {
    fn consumer(
        source: Arc<RecordingUpdateSource>,
        service: Arc<PortfolioService>,
    ) -> PortfolioUpdateConsumer {
        PortfolioUpdateConsumer::new(service, source, Duration::from_millis(1))
    }

    async fn service() -> (Arc<PortfolioService>, crate::AccountKey) {
        let (_, service) = Fixture::service();
        let service = Arc::new(service);
        let account = Fixture::account("flood");
        service.track_account(account.clone()).await.unwrap();
        (service, account)
    }

    fn update(account: &str, total: f64, version: u64) -> PortfolioUpdate {
        PortfolioUpdate::versioned(
            Fixture::account(account),
            None,
            Fixture::portfolio(AssetId::Bitcoin, total),
            version,
            chrono::Utc::now(),
        )
    }
}

#[tokio::test]
async fn event_flood_is_applied_and_checkpointed_in_order() {
    const UPDATES: u64 = 10_000;
    let (service, account) = Harness::service().await;
    let source = Arc::new(RecordingUpdateSource::default());
    for version in 1..=UPDATES {
        source.push(PortfolioUpdateMessage::new(
            Harness::update("flood", version as f64, version),
            Some(version),
            None,
        ));
    }

    let processed = Harness::consumer(Arc::clone(&source), Arc::clone(&service))
        .run_once()
        .await
        .unwrap();

    assert_eq!(processed as u64, UPDATES);
    assert_eq!(source.acknowledged() as u64, UPDATES);
    assert_eq!(source.checkpoints(), (1..=UPDATES).collect::<Vec<_>>());
    assert_eq!(
        service
            .account_portfolio(&account)
            .unwrap()
            .asset_total(AssetId::Bitcoin),
        UPDATES as f64
    );
}

#[tokio::test]
async fn duplicate_versions_are_stale_and_do_not_overwrite_state() {
    let (service, account) = Harness::service().await;
    let source = Arc::new(RecordingUpdateSource::default());
    source.push(PortfolioUpdateMessage::new(
        Harness::update("flood", 1.0, 7),
        Some(1),
        None,
    ));
    source.push(PortfolioUpdateMessage::new(
        Harness::update("flood", 99.0, 7),
        Some(2),
        None,
    ));

    let processed = Harness::consumer(source, Arc::clone(&service))
        .run_once()
        .await
        .unwrap();

    assert_eq!(processed, 2);
    assert_eq!(
        service
            .account_portfolio(&account)
            .unwrap()
            .asset_total(AssetId::Bitcoin),
        1.0
    );
}
