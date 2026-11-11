use super::test_feed::Fixture;
use crate::{
    AccountKey, OwnershipApply, OwnershipConsumer, OwnershipEventSource, OwnershipMessage,
    PortfolioService,
};
use async_trait::async_trait;
use chrono::Utc;
use platform_errors::BpResult;
use platform_portfolio_ownership::{OwnershipAction, OwnershipEvent};
use std::{collections::VecDeque, sync::Mutex, time::Duration};

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
        self.checkpoints.lock().expect("checkpoints").clone()
    }

    fn acknowledged(&self) -> usize {
        *self.acknowledged.lock().expect("acknowledged")
    }
}

#[async_trait]
impl OwnershipEventSource for FakeSource {
    async fn next(&self) -> BpResult<Option<OwnershipMessage>> {
        Ok(self.messages.lock().expect("messages").pop_front())
    }

    async fn checkpoint(&self, sequence: u64) -> BpResult<()> {
        self.checkpoints.lock().expect("checkpoints").push(sequence);
        Ok(())
    }

    async fn acknowledge(&self, _message: &OwnershipMessage) -> BpResult<()> {
        *self.acknowledged.lock().expect("acknowledged") += 1;
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
    service.apply_portfolio(
        account.clone(),
        Fixture::portfolio(stock_trek_types::cex::asset_id::AssetId::Bitcoin, 10.0),
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
        super::test_feed::RecordingFeed::default(),
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
        super::test_feed::RecordingFeed::default(),
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
        super::test_feed::RecordingFeed::default(),
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

#[test]
fn account_key_parses_validated_exchange_and_account() {
    let key = AccountKey::parse("binance", "primary").expect("parse");
    assert_eq!(key.storage_key(), "binance:primary");
    assert!(AccountKey::parse("unknown", "primary").is_err());
    assert!(AccountKey::parse("binance", " ").is_err());
}
