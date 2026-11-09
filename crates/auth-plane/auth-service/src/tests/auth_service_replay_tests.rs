use super::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{
    AuthService, ConsumeResult, MemoryReplayStore, MemoryTokenStore, ReplayReservation,
    ReplayStore, ReplayStoreError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use platform_errors::{AuthError, BpError};
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
        let first = AuthService::new_with_replay_store(
            token_store.clone(),
            TestFixture::config(),
            replay.clone(),
        )
        .expect("valid service");
        let second = AuthService::new_with_replay_store(
            token_store.clone(),
            TestFixture::config(),
            replay.clone(),
        )
        .expect("valid service");
        (first, second, token_store, replay)
    }
}

struct UnavailableReplayStore;

#[async_trait]
impl ReplayStore for UnavailableReplayStore {
    async fn consume(
        &self,
        _key: crate::ReplayKey,
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
    );
    assert!(
        !first
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("replayed verification")
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
    );

    let restarted = AuthService::new_with_replay_store(token_store, TestFixture::config(), replay)
        .expect("valid service");

    assert!(
        !restarted
            .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
            .await
            .expect("replayed verification")
    );
}

#[tokio::test]
async fn replay_store_failures_are_returned_and_fail_closed() {
    let service = AuthService::new_with_replay_store(
        Arc::new(MemoryTokenStore::default()),
        TestFixture::config(),
        Arc::new(UnavailableReplayStore),
    )
    .expect("valid service");
    let token = TestFixture::add_token(&service, "token-a", "demo-owner").await;
    let now = Utc::now();
    let signed = service
        .sign_order_request(token.id(), token.secret(), ORDER_REQUEST, now, now)
        .await
        .expect("signed request");

    let error = service
        .verify_order_request(token.id(), ORDER_REQUEST, signed.envelope(), now)
        .await
        .expect_err("replay store failure");

    assert!(matches!(
        error,
        BpError::Auth(AuthError::Store {
            operation: "consume",
            retryable: true,
            ..
        })
    ));
}
