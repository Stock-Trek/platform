use crate::{
    ValkeyReplayStore, ValkeyReplayStoreConfig, consume_result::ConsumeResult,
    replay_key::ReplayKey, replay_store::ReplayStore,
};
use chrono::{DateTime, Duration, Utc};
use tokio::net::TcpListener;

use crate::tests::valkey_test_server_tests::FakeValkey;

struct ValkeyReplayFixture;

impl ValkeyReplayFixture {
    fn config(url: &str) -> ValkeyReplayStoreConfig {
        ValkeyReplayStoreConfig::new(url, "test-replay", std::time::Duration::from_secs(5))
            .expect("valid configuration")
    }

    async fn store(url: &str) -> ValkeyReplayStore {
        ValkeyReplayStore::connect(Self::config(url))
            .await
            .expect("connected store")
    }

    fn key(token_id: &str, nonce: &str) -> ReplayKey {
        ReplayKey::new(token_id, nonce)
    }

    fn expiry(now: DateTime<Utc>) -> DateTime<Utc> {
        now + Duration::minutes(5)
    }

    fn reservation(outcome: ConsumeResult) -> crate::replay_reservation::ReplayReservation {
        match outcome {
            ConsumeResult::Consumed(reservation) => reservation,
            outcome => panic!("unexpected outcome: {outcome:?}"),
        }
    }

    fn assert_consumed(outcome: ConsumeResult) {
        assert!(
            matches!(outcome, ConsumeResult::Consumed(_)),
            "expected consumed outcome, got {outcome:?}"
        );
    }
}

#[tokio::test]
async fn nonces_are_scoped_to_a_token_and_replays_are_rejected() {
    let url = FakeValkey::start().await;
    let store = ValkeyReplayFixture::store(&url).await;
    let now = Utc::now();
    let expires_at = ValkeyReplayFixture::expiry(now);
    let key = ValkeyReplayFixture::key("token-a", "nonce-a");

    ValkeyReplayFixture::assert_consumed(
        store.consume(key.clone(), expires_at, now).await.unwrap(),
    );
    assert_eq!(
        store.consume(key, expires_at, now).await.unwrap(),
        ConsumeResult::Replayed
    );
    ValkeyReplayFixture::assert_consumed(
        store
            .consume(
                ValkeyReplayFixture::key("token-b", "nonce-a"),
                expires_at,
                now,
            )
            .await
            .unwrap(),
    );
}

#[tokio::test]
async fn already_expired_nonces_are_rejected_without_a_store_lookup() {
    let config = ValkeyReplayFixture::config("redis://127.0.0.1:1");
    let store = ValkeyReplayStore::new(config).expect("valid store");
    let now = Utc::now();

    assert_eq!(
        store
            .consume(ValkeyReplayFixture::key("token-a", "nonce-a"), now, now)
            .await
            .unwrap(),
        ConsumeResult::Expired
    );
}

#[tokio::test]
async fn released_nonces_can_be_used_again() {
    let url = FakeValkey::start().await;
    let store = ValkeyReplayFixture::store(&url).await;
    let now = Utc::now();
    let expires_at = ValkeyReplayFixture::expiry(now);
    let key = ValkeyReplayFixture::key("token-a", "nonce-a");
    let reservation = ValkeyReplayFixture::reservation(
        store.consume(key.clone(), expires_at, now).await.unwrap(),
    );

    store.release(&reservation).await.unwrap();

    ValkeyReplayFixture::assert_consumed(store.consume(key, expires_at, now).await.unwrap());
}

#[tokio::test]
async fn delayed_release_does_not_remove_a_newer_reservation() {
    let url = FakeValkey::start().await;
    let store = ValkeyReplayFixture::store(&url).await;
    let now = Utc::now();
    let expires_at = ValkeyReplayFixture::expiry(now);
    let key = ValkeyReplayFixture::key("token-a", "nonce-a");
    let first = ValkeyReplayFixture::reservation(
        store.consume(key.clone(), expires_at, now).await.unwrap(),
    );
    store.release(&first).await.unwrap();
    let second = ValkeyReplayFixture::reservation(
        store.consume(key.clone(), expires_at, now).await.unwrap(),
    );

    store.release(&first).await.unwrap();

    assert_eq!(
        store.consume(key, expires_at, now).await.unwrap(),
        ConsumeResult::Replayed
    );
    store.release(&second).await.unwrap();
}

#[tokio::test]
async fn verifiers_sharing_valkey_reject_replays_across_instances_and_restarts() {
    let url = FakeValkey::start().await;
    let first = ValkeyReplayFixture::store(&url).await;
    let second = ValkeyReplayFixture::store(&url).await;
    let restarted = ValkeyReplayFixture::store(&url).await;
    let now = Utc::now();
    let expires_at = ValkeyReplayFixture::expiry(now);
    let key = ValkeyReplayFixture::key("token-a", "nonce-a");

    ValkeyReplayFixture::assert_consumed(
        first.consume(key.clone(), expires_at, now).await.unwrap(),
    );
    assert_eq!(
        second.consume(key.clone(), expires_at, now).await.unwrap(),
        ConsumeResult::Replayed
    );
    assert_eq!(
        restarted.consume(key, expires_at, now).await.unwrap(),
        ConsumeResult::Replayed
    );
}

#[tokio::test]
async fn keys_include_the_configured_prefix_and_nonce_identity() {
    let url = FakeValkey::start().await;
    let store = ValkeyReplayFixture::store(&url).await;
    let key = ValkeyReplayFixture::key("token-a", "nonce-a");

    assert_eq!(store.key_for_test(&key), "test-replay:token-a:nonce-a");
}

#[tokio::test]
async fn unavailable_valkey_returns_a_retryable_store_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let store = ValkeyReplayStore::new(ValkeyReplayFixture::config(&format!("redis://{address}")))
        .expect("valid store");

    let error = store
        .consume(
            ValkeyReplayFixture::key("token-a", "nonce-a"),
            Utc::now() + Duration::minutes(5),
            Utc::now(),
        )
        .await
        .unwrap_err();

    assert!(error.is_retryable());
}
