use crate::{MemoryOwnershipStore, OwnershipOutboxRelay, OwnershipStore, RegistryService};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use platform_errors::{BuildError, BuildResult};
use platform_nats::EventPublisher;
use platform_portfolio_ownership::{OwnershipAction, OwnershipEvent};
use std::{
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

#[derive(Default)]
struct RecordingPublisher {
    published: Mutex<Vec<Vec<u8>>>,
    fail_next: Mutex<bool>,
}

impl RecordingPublisher {
    fn published(&self) -> Vec<Vec<u8>> {
        self.published.lock().expect("published").clone()
    }

    fn fail_next(&self) {
        *self.fail_next.lock().expect("fail_next") = true;
    }
}

#[async_trait]
impl EventPublisher for RecordingPublisher {
    async fn publish(&self, _subject: &str, payload: &[u8]) -> BuildResult<()> {
        let mut fail_next = self.fail_next.lock().expect("fail_next");
        if *fail_next {
            *fail_next = false;
            return Err(BuildError::external(std::io::Error::other("publish failed")));
        }
        self.published
            .lock()
            .expect("published")
            .push(payload.to_vec());
        Ok(())
    }
}

fn service() -> (Arc<MemoryOwnershipStore>, RegistryService) {
    let store = Arc::new(MemoryOwnershipStore::new());
    let service = RegistryService::new(store.clone());
    (store, service)
}

#[tokio::test]
async fn add_is_audited_and_published_once() {
    let (store, service) = service();
    let at = Utc::now();
    let mapping = service
        .add("alice", "binance", "first", at)
        .await
        .expect("add");
    assert!(mapping.is_active());
    assert_eq!(mapping.owner(), "alice");
    assert_eq!(mapping.version(), 1);

    let record = store
        .get(mapping.account())
        .await
        .expect("load")
        .expect("record");
    assert_eq!(record.audit.len(), 1);
    assert_eq!(record.pending_events.len(), 1);

    let publisher = Arc::new(RecordingPublisher::default());
    let relay = OwnershipOutboxRelay::new(
        store.clone(),
        publisher.clone(),
        StdDuration::from_millis(10),
    )
    .expect("relay");
    assert_eq!(relay.flush_once().await.expect("flush"), 1);
    assert_eq!(relay.flush_once().await.expect("flush"), 0);
    let published = publisher.published();
    assert_eq!(published.len(), 1);
    let event: OwnershipEvent = serde_json::from_slice(&published[0]).expect("event");
    assert_eq!(event.owner(), "alice");
    assert_eq!(event.action(), OwnershipAction::Add);
    assert_eq!(event.version(), 1);
    assert!(store.pending_events(10).await.expect("pending").is_empty());
}

#[tokio::test]
async fn rapid_add_and_remove_keep_both_outbox_events() {
    let (store, service) = service();
    let at = Utc::now();
    service
        .add("alice", "binance", "first", at)
        .await
        .expect("add");
    let removed = service
        .remove("alice", "binance", "first", at + Duration::seconds(1))
        .await
        .expect("remove");
    assert!(!removed.is_active());
    assert_eq!(removed.version(), 2);
    assert_eq!(store.pending_events(10).await.expect("pending").len(), 2);

    let publisher = Arc::new(RecordingPublisher::default());
    let relay = OwnershipOutboxRelay::new(
        store.clone(),
        publisher.clone(),
        StdDuration::from_millis(10),
    )
    .expect("relay");
    assert_eq!(relay.flush_once().await.expect("flush"), 2);
    let events: Vec<OwnershipEvent> = publisher
        .published()
        .iter()
        .map(|payload| serde_json::from_slice(payload).expect("event"))
        .collect();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].action(), OwnershipAction::Add);
    assert_eq!(events[1].action(), OwnershipAction::Remove);
}

#[tokio::test]
async fn duplicate_add_is_idempotent_and_owner_isolation_is_enforced() {
    let (store, service) = service();
    let at = Utc::now();
    let first = service
        .add("alice", "binance", "first", at)
        .await
        .expect("add");
    let duplicate = service
        .add("alice", "binance", "first", at + Duration::seconds(1))
        .await
        .expect("duplicate");
    assert_eq!(duplicate.version(), first.version());
    assert_eq!(store.pending_events(10).await.expect("pending").len(), 1);

    let conflict = service.add("bob", "binance", "first", at).await;
    assert!(conflict.is_err());
    assert!(service.accounts("bob").await.expect("bob").is_empty());
    assert_eq!(service.accounts("alice").await.expect("alice").len(), 1);
}

#[tokio::test]
async fn failed_publish_leaves_the_event_pending() {
    let (store, service) = service();
    service
        .add("alice", "binance", "first", Utc::now())
        .await
        .expect("add");
    let publisher = Arc::new(RecordingPublisher::default());
    publisher.fail_next();
    let relay = OwnershipOutboxRelay::new(
        store.clone(),
        publisher.clone(),
        StdDuration::from_millis(10),
    )
    .expect("relay");
    assert!(relay.flush_once().await.is_err());
    assert_eq!(store.pending_events(10).await.expect("pending").len(), 1);
    assert!(publisher.published().is_empty());
    assert_eq!(relay.flush_once().await.expect("retry"), 1);
    assert!(store.pending_events(10).await.expect("pending").is_empty());
}

#[tokio::test]
async fn removed_accounts_are_not_returned_by_owner_reads() {
    let (_, service) = service();
    let at = Utc::now();
    service
        .add("alice", "binance", "first", at)
        .await
        .expect("add");
    service
        .remove("alice", "binance", "first", at + Duration::seconds(1))
        .await
        .expect("remove");
    assert!(
        service
            .accounts("alice")
            .await
            .expect("accounts")
            .is_empty()
    );
}
