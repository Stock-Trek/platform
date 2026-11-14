use crate::tests::support_tests::{ORDER_REQUEST, TestFixture};
use crate::tests::token_usage_queue_fixture_tests::ToggleTokenUsageQueue;
use crate::{
    AuthService, MemoryReplayStore, MemoryTokenStore, consume_result::ConsumeResult,
    replay_key::ReplayKey, replay_reservation::ReplayReservation, replay_store::ReplayStore,
    replay_store_error::ReplayStoreError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct FailingReleaseReplayStore {
    inner: MemoryReplayStore,
    release_attempts: AtomicUsize,
}

impl FailingReleaseReplayStore {
    fn new() -> Self {
        Self {
            inner: MemoryReplayStore::default(),
            release_attempts: AtomicUsize::new(0),
        }
    }

    fn release_attempts(&self) -> usize {
        self.release_attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ReplayStore for FailingReleaseReplayStore {
    async fn consume(
        &self,
        key: ReplayKey,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<ConsumeResult, ReplayStoreError> {
        self.inner.consume(key, expires_at, now).await
    }

    async fn release(&self, _reservation: &ReplayReservation) -> Result<(), ReplayStoreError> {
        self.release_attempts.fetch_add(1, Ordering::SeqCst);
        Err(ReplayStoreError::unavailable(
            "release",
            true,
            std::io::Error::other("release failed"),
        ))
    }
}

#[tokio::test]
async fn audit_delivery_failure_does_not_fail_signature_verification() {
    let replay = Arc::new(FailingReleaseReplayStore::new());
    let store = Arc::new(MemoryTokenStore::default());
    let service = AuthService::new(
        store.clone(),
        TestFixture::config(),
        replay.clone(),
        TestFixture::immediate_usage(store),
    )
    .expect("valid service");
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");

    let verified = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("signature verification succeeds despite audit delivery failure");

    assert!(verified.is_valid());
    assert_eq!(replay.release_attempts(), 0);
}

#[tokio::test]
async fn usage_queue_failure_is_reported_before_replay_release_failure() {
    let replay = Arc::new(FailingReleaseReplayStore::new());
    let store = Arc::new(MemoryTokenStore::default());
    let queue = Arc::new(ToggleTokenUsageQueue::new());
    let service = AuthService::new(store, TestFixture::config(), replay.clone(), queue.clone())
        .expect("valid service");
    let token = TestFixture::add_token(&service, "token-a", "owner-a").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");
    queue.fail();

    let verification = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("queue failure is classified");

    assert!(verification.is_retryable());
    assert_eq!(replay.release_attempts(), 1);
}
