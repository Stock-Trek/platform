use crate::{ConsumeResult, MemoryReplayStore, ReplayKey, ReplayStore, ReplayStoreError};
use chrono::{DateTime, Duration, Utc};
use std::num::NonZeroUsize;

struct ReplayStoreFixture;

impl ReplayStoreFixture {
    fn store(capacity: usize) -> MemoryReplayStore {
        MemoryReplayStore::new(NonZeroUsize::new(capacity).expect("non-zero capacity"))
    }

    fn key(token_id: &str, nonce: &str) -> ReplayKey {
        ReplayKey::new(token_id, nonce)
    }

    fn expiry(now: DateTime<Utc>) -> DateTime<Utc> {
        now + Duration::minutes(5)
    }

    fn assert_consumed(outcome: ConsumeResult) {
        assert!(
            matches!(outcome, ConsumeResult::Consumed(_)),
            "expected consumed outcome, got {outcome:?}"
        );
    }
}

#[tokio::test]
async fn nonces_are_scoped_to_a_token() {
    let store = ReplayStoreFixture::store(8);
    let now = Utc::now();
    let expires_at = ReplayStoreFixture::expiry(now);
    let key = ReplayStoreFixture::key("token-a", "nonce-a");

    ReplayStoreFixture::assert_consumed(store.consume(key.clone(), expires_at, now).await.unwrap());
    assert_eq!(
        store.consume(key, expires_at, now).await.unwrap(),
        ConsumeResult::Replayed
    );
    ReplayStoreFixture::assert_consumed(
        store
            .consume(
                ReplayStoreFixture::key("token-b", "nonce-a"),
                expires_at,
                now,
            )
            .await
            .unwrap(),
    );
}

#[tokio::test]
async fn nonces_that_have_already_expired_are_rejected() {
    let store = ReplayStoreFixture::store(8);
    let now = Utc::now();
    let key = ReplayStoreFixture::key("token-a", "nonce-a");

    assert_eq!(
        store.consume(key.clone(), now, now).await.unwrap(),
        ConsumeResult::Expired
    );
    assert_eq!(
        store
            .consume(key, now - Duration::seconds(1), now)
            .await
            .unwrap(),
        ConsumeResult::Expired
    );
}

#[tokio::test]
async fn released_nonces_can_be_used_again() {
    let store = ReplayStoreFixture::store(8);
    let now = Utc::now();
    let expires_at = ReplayStoreFixture::expiry(now);
    let key = ReplayStoreFixture::key("token-a", "nonce-a");
    let reservation = match store.consume(key.clone(), expires_at, now).await.unwrap() {
        ConsumeResult::Consumed(reservation) => reservation,
        outcome => panic!("unexpected outcome: {outcome:?}"),
    };

    store.release(&reservation).await.unwrap();

    ReplayStoreFixture::assert_consumed(store.consume(key, expires_at, now).await.unwrap());
}

#[tokio::test]
async fn delayed_release_does_not_remove_a_newer_reservation() {
    let store = ReplayStoreFixture::store(8);
    let now = Utc::now();
    let expires_at = ReplayStoreFixture::expiry(now);
    let key = ReplayStoreFixture::key("token-a", "nonce-a");
    let first = match store.consume(key.clone(), expires_at, now).await.unwrap() {
        ConsumeResult::Consumed(reservation) => reservation,
        outcome => panic!("unexpected outcome: {outcome:?}"),
    };
    store.release(&first).await.unwrap();
    let second = match store.consume(key.clone(), expires_at, now).await.unwrap() {
        ConsumeResult::Consumed(reservation) => reservation,
        outcome => panic!("unexpected outcome: {outcome:?}"),
    };

    store.release(&first).await.unwrap();

    assert_eq!(
        store.consume(key, expires_at, now).await.unwrap(),
        ConsumeResult::Replayed
    );
    store.release(&second).await.unwrap();
}

#[tokio::test]
async fn expired_nonces_are_evicted() {
    let store = ReplayStoreFixture::store(8);
    let now = Utc::now();
    let expires_at = ReplayStoreFixture::expiry(now);
    let key = ReplayStoreFixture::key("token-a", "nonce-a");

    ReplayStoreFixture::assert_consumed(store.consume(key.clone(), expires_at, now).await.unwrap());
    let later = now + Duration::minutes(6);
    let later_expires_at = later + Duration::minutes(5);
    ReplayStoreFixture::assert_consumed(
        store
            .consume(key.clone(), later_expires_at, later)
            .await
            .unwrap(),
    );
    assert_eq!(
        store.consume(key, later_expires_at, later).await.unwrap(),
        ConsumeResult::Replayed
    );
}

#[tokio::test]
async fn capacity_rejects_new_unexpired_nonces() {
    let store = ReplayStoreFixture::store(1);
    let now = Utc::now();
    let expires_at = ReplayStoreFixture::expiry(now);

    ReplayStoreFixture::assert_consumed(
        store
            .consume(
                ReplayStoreFixture::key("token-a", "nonce-a"),
                expires_at,
                now,
            )
            .await
            .unwrap(),
    );
    let error = store
        .consume(
            ReplayStoreFixture::key("token-a", "nonce-b"),
            expires_at,
            now,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ReplayStoreError::CapacityExceeded { capacity: 1 }
    ));

    let later = now + Duration::minutes(6);
    ReplayStoreFixture::assert_consumed(
        store
            .consume(
                ReplayStoreFixture::key("token-a", "nonce-b"),
                later + Duration::minutes(5),
                later,
            )
            .await
            .unwrap(),
    );
}
