use crate::tests::test_feed_tests::Fixture;
use crate::{
    AccountKey, OwnershipApply, OwnershipConsumer, OwnershipEventSource, OwnershipMessage,
    PortfolioService,
};
use async_trait::async_trait;
use chrono::Utc;
use parking_lot::Mutex;
use platform_errors::BuildResult;
use platform_exchange_gateway::PortfolioFeed;
use platform_portfolio_ownership::{OwnershipAction, OwnershipEvent};
use std::{collections::VecDeque, time::Duration};
use stock_trek::types::CexId;

#[derive(Default)]
struct FakeSource {
    messages: Mutex<VecDeque<OwnershipMessage>>,
    checkpoints: Mutex<Vec<u64>>,
    acknowledged: Mutex<usize>,
}

impl FakeSource {
    fn with_messages(messages: Vec<OwnershipMessage>) -> Self {
        Self {
            messages: Mutex::new(messages.into()),
            ..Self::default()
        }
    }

    fn checkpoints(&self) -> Vec<u64> {
        self.checkpoints.lock().clone()
    }

    fn acknowledged(&self) -> usize {
        *self.acknowledged.lock()
    }
}

#[async_trait]
impl OwnershipEventSource for FakeSource {
    async fn next(&self) -> BuildResult<Option<OwnershipMessage>> {
        Ok(self.messages.lock().pop_front())
    }

    async fn checkpoint(&self, sequence: u64) -> BuildResult<()> {
        self.checkpoints.lock().push(sequence);
        Ok(())
    }

    async fn acknowledge(&self, _message: &OwnershipMessage) -> BuildResult<()> {
        *self.acknowledged.lock() += 1;
        Ok(())
    }
}

fn event(owner: &str, account: &str, action: OwnershipAction, version: u64) -> OwnershipEvent {
    OwnershipEvent::new(
        format!("audit-{version}"),
        owner,
        Fixture::account(account),
        action,
        version,
        Utc::now(),
    )
}

fn message(event: OwnershipEvent, sequence: u64) -> OwnershipMessage {
    OwnershipMessage::new(event, Some(sequence), None)
}

#[tokio::test]
async fn add_tracks_the_account_for_its_owner_and_feed() {
    let (feed, service) = Fixture::service();
    let account = Fixture::account("first");
    let outcome = service
        .apply_ownership_event(&event("alice", "first", OwnershipAction::Add, 1))
        .await
        .expect("apply");

    assert_eq!(
        outcome,
        OwnershipApply::Applied {
            previous_owner: None,
            was_active: false,
        }
    );
    assert!(service.is_tracked_for_owner("alice", &account));
    assert!(!service.is_tracked_for_owner("bob", &account));
    assert_eq!(feed.calls().len(), 1);
    assert_eq!(service.ownership_version(&account), Some(1));
}

#[tokio::test]
async fn removal_clears_owner_state_feed_and_portfolio() {
    let (feed, service) = Fixture::service();
    let account = Fixture::account("first");
    service
        .apply_ownership_event(&event("alice", "first", OwnershipAction::Add, 1))
        .await
        .expect("add");
    assert_eq!(
        service.apply_portfolio_update(crate::PortfolioUpdate::new(
            account.clone(),
            Fixture::portfolio(stock_trek_types::cex::asset_id::AssetId::Bitcoin, 10.0),
        )),
        crate::PortfolioUpdateOutcome::Applied
    );

    let outcome = service
        .apply_ownership_event(&event("alice", "first", OwnershipAction::Remove, 2))
        .await
        .expect("remove");

    assert_eq!(
        outcome,
        OwnershipApply::Applied {
            previous_owner: Some("alice".to_string()),
            was_active: true,
        }
    );
    assert!(!service.is_tracked_for_owner("alice", &account));
    assert!(service.account_portfolio(&account).is_none());
    assert_eq!(feed.calls().len(), 2);
    assert!(feed.calls()[1].accounts.is_empty());
}

#[tokio::test]
async fn feed_failures_do_not_leave_pre_tracked_ownership_state() {
    let (feed, service) = Fixture::service();
    let account = Fixture::account("first");
    feed.fail_next();

    assert!(
        service
            .apply_ownership_event(&event("alice", "first", OwnershipAction::Add, 1))
            .await
            .is_err()
    );
    assert!(!service.is_tracked_for_owner("alice", &account));
    assert_eq!(service.ownership_version(&account), None);
}

#[tokio::test]
async fn an_account_can_transfer_after_the_previous_owner_removes_it() {
    let (feed, service) = Fixture::service();
    let account = Fixture::account("first");
    service
        .apply_ownership_event(&event("alice", "first", OwnershipAction::Add, 1))
        .await
        .expect("add");
    service
        .apply_ownership_event(&event("alice", "first", OwnershipAction::Remove, 2))
        .await
        .expect("remove");
    let outcome = service
        .apply_ownership_event(&event("bob", "first", OwnershipAction::Add, 3))
        .await
        .expect("transfer");

    assert_eq!(
        outcome,
        OwnershipApply::Applied {
            previous_owner: Some("alice".to_string()),
            was_active: false,
        }
    );
    assert!(!service.is_tracked_for_owner("alice", &account));
    assert!(service.is_tracked_for_owner("bob", &account));
    assert_eq!(feed.calls().len(), 3);
}

#[tokio::test]
async fn duplicate_events_are_ignored_and_owner_isolation_is_enforced() {
    let (feed, service) = Fixture::service();
    let first = event("alice", "first", OwnershipAction::Add, 1);
    service.apply_ownership_event(&first).await.expect("add");
    assert_eq!(
        service
            .apply_ownership_event(&first)
            .await
            .expect("duplicate"),
        OwnershipApply::IgnoredStale
    );

    let other_owner = event("bob", "first", OwnershipAction::Add, 2);
    assert_eq!(
        service
            .apply_ownership_event(&other_owner)
            .await
            .expect("conflict"),
        OwnershipApply::IgnoredConflict
    );
    assert!(service.is_tracked_for_owner("alice", &Fixture::account("first")));
    assert!(!service.is_tracked_for_owner("bob", &Fixture::account("first")));
    assert_eq!(feed.calls().len(), 1);
}

#[tokio::test]
async fn consumer_checkpoints_replays_and_acknowledges_events() {
    let source = std::sync::Arc::new(FakeSource::with_messages(vec![
        message(event("alice", "first", OwnershipAction::Add, 1), 10),
        message(event("alice", "first", OwnershipAction::Add, 1), 11),
        message(event("alice", "first", OwnershipAction::Remove, 2), 12),
    ]));
    let service = std::sync::Arc::new(PortfolioService::new(std::sync::Arc::new(
        crate::tests::test_feed_tests::RecordingFeed::default(),
    )));
    let consumer =
        OwnershipConsumer::new(service.clone(), source.clone(), Duration::from_millis(10));

    assert_eq!(consumer.run_once().await.expect("consume"), 3);
    assert_eq!(source.checkpoints(), vec![10, 11, 12]);
    assert_eq!(source.acknowledged(), 3);
    assert!(!service.is_tracked_for_owner("alice", &Fixture::account("first")));
}

#[tokio::test]
async fn restart_replays_from_the_persisted_events() {
    let first_source = std::sync::Arc::new(FakeSource::with_messages(vec![message(
        event("alice", "first", OwnershipAction::Add, 1),
        1,
    )]));
    let first_service = std::sync::Arc::new(PortfolioService::new(std::sync::Arc::new(
        crate::tests::test_feed_tests::RecordingFeed::default(),
    )));
    OwnershipConsumer::new(
        first_service.clone(),
        first_source.clone(),
        Duration::from_millis(10),
    )
    .run_once()
    .await
    .expect("first run");
    assert_eq!(first_source.checkpoints(), vec![1]);

    let second_service = std::sync::Arc::new(PortfolioService::new(std::sync::Arc::new(
        crate::tests::test_feed_tests::RecordingFeed::default(),
    )));
    let replay = std::sync::Arc::new(FakeSource::with_messages(vec![
        message(event("alice", "first", OwnershipAction::Add, 1), 1),
        message(event("alice", "first", OwnershipAction::Remove, 2), 2),
    ]));
    let second_source = replay.clone();
    OwnershipConsumer::new(
        second_service.clone(),
        second_source,
        Duration::from_millis(10),
    )
    .run_once()
    .await
    .expect("replay");

    assert_eq!(replay.checkpoints(), vec![1, 2]);
    assert!(!second_service.is_tracked_for_owner("alice", &Fixture::account("first")));
    assert_eq!(
        second_service.ownership_version(&Fixture::account("first")),
        Some(2)
    );
}

#[derive(Default)]
struct SnapshotFeed {
    service: Mutex<Option<std::sync::Arc<PortfolioService>>>,
    applied: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl PortfolioFeed for SnapshotFeed {
    async fn set_accounts(
        &self,
        _cex_id: stock_trek_types::cex::cex_id::CexId,
        accounts: std::collections::BTreeSet<stock_trek_types::cex::account_id::AccountId>,
    ) -> BuildResult<()> {
        if accounts.is_empty() {
            return Ok(());
        }
        let service = self.service.lock().as_ref().cloned().expect("service");
        let account = accounts.iter().next().expect("account");
        let applied = service.apply_portfolio_update(crate::PortfolioUpdate::versioned(
            Fixture::account(&account.0),
            None,
            Fixture::portfolio(stock_trek_types::cex::asset_id::AssetId::Bitcoin, 5.0),
            1,
            Utc::now(),
        )) == crate::PortfolioUpdateOutcome::Applied;
        self.applied
            .store(applied, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn connector_snapshot_during_sync_is_applied_for_the_new_owner() {
    let feed = std::sync::Arc::new(SnapshotFeed::default());
    let service = std::sync::Arc::new(PortfolioService::new(feed.clone()));
    *feed.service.lock() = Some(service.clone());

    service
        .apply_ownership_event(&event("alice", "first", OwnershipAction::Add, 1))
        .await
        .expect("add");

    assert!(feed.applied.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        service
            .user_account_portfolio("alice", &Fixture::account("first"))
            .is_some()
    );
}

#[test]
fn account_key_parses_validated_exchange_and_account() {
    let key = AccountKey::parse(CexId::Binance.to_string().as_str(), "primary").expect("parse");
    assert_eq!(key.storage_key(), format!("{}:primary", CexId::Binance));
    assert!(AccountKey::parse("unknown", "primary").is_err());
    assert!(AccountKey::parse(CexId::Binance.to_string().as_str(), " ").is_err());
}
