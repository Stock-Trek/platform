use crate::tests::support_tests::{AuthServiceBuilder, ORDER_REQUEST, TestFixture};
use crate::{
    AuthService, MemoryReplayStore, MemoryTokenStore, consume_result::ConsumeResult,
    replay_reservation::ReplayReservation, replay_store::ReplayStore,
    replay_store_error::ReplayStoreError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::{num::NonZeroUsize, sync::Arc};

struct AuthReplayFixture;

impl AuthReplayFixture {
    fn services() -> (
        AuthService,
        AuthService,
        Arc<MemoryTokenStore>,
        Arc<MemoryReplayStore>,
    ) {
        let token_store = Arc::new(MemoryTokenStore::default());
        let replay = Arc::new(MemoryReplayStore::new(
            NonZeroUsize::new(8).expect("non-zero capacity"),
        ));
        let first = AuthServiceBuilder::new()
            .store(token_store.clone())
            .replay(replay.clone())
            .build();
        let second = AuthServiceBuilder::new()
            .store(token_store.clone())
            .replay(replay.clone())
            .build();
        (first, second, token_store, replay)
    }
}

struct UnavailableReplayStore;

#[async_trait]
impl ReplayStore for UnavailableReplayStore {
    async fn consume(
        &self,
        _key: crate::replay_key::ReplayKey,
        _expires_at: DateTime<Utc>,
        _now: DateTime<Utc>,
    ) -> Result<ConsumeResult, ReplayStoreError> {
        Err(ReplayStoreError::unavailable(
            "consume",
            true,
            std::io::Error::other("replay store unavailable"),
        ))
    }

    async fn release(&self, _reservation: &ReplayReservation) -> Result<(), ReplayStoreError> {
        Ok(())
    }
}

#[tokio::test]
async fn verifiers_sharing_a_replay_store_reject_replays_across_instances() {
    let (first, second, _, _) = AuthReplayFixture::services();
    let token = TestFixture::add_token(&first, "token-a", "demo-owner").await;
    let now = Utc::now();
    let signed = first
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");

    assert!(
        second
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("first verification")
            .is_valid()
    );
    assert!(
        first
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("replayed verification")
            .is_invalid()
    );
}

#[tokio::test]
async fn a_restarted_verifier_rejects_replays_when_the_store_is_shared() {
    let (first, _, token_store, replay) = AuthReplayFixture::services();
    let token = TestFixture::add_token(&first, "token-a", "demo-owner").await;
    let now = Utc::now();
    let signed = first
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");
    assert!(
        first
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("first verification")
            .is_valid()
    );

    let restarted = AuthServiceBuilder::new()
        .store(token_store)
        .replay(replay)
        .build();

    assert!(
        restarted
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("replayed verification")
            .is_invalid()
    );
}

#[tokio::test]
async fn replay_store_failures_are_reported_as_retryable() {
    let service = AuthServiceBuilder::new()
        .replay(Arc::new(UnavailableReplayStore))
        .build();
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");

    let verification = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect("replay store failure is classified");

    assert!(verification.is_retryable());
}
