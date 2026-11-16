use crate::{
    InMemoryLeaseStore, Lease, LeaseGuard, LeaseRequest, LeaseScope, LeaseStore, NatsLeaseStore,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn scope(name: &str) -> LeaseScope {
    LeaseScope::new(format!("test:{name}"))
}

#[test]
fn scope_keys_are_stable_and_distinct() {
    let first = scope("first");
    let other = scope("other");

    assert_eq!(first.key(), scope("first").key());
    assert_ne!(first.key(), other.key());
    assert!(first.key().starts_with("lease-"));
    assert_eq!(first.key().len(), "lease-".len() + 64);
}

#[test]
fn initial_tokens_must_be_positive() {
    assert!(LeaseRequest::validate_initial_token(1).is_ok());
    assert!(LeaseRequest::validate_initial_token(0).is_err());
    assert!(LeaseRequest::validate_initial_token(-1).is_err());
}

#[test]
fn renewal_intervals_must_be_positive() {
    assert!(LeaseRequest::renewal_interval(Duration::from_millis(3)).is_ok());
    assert!(LeaseRequest::renewal_interval(Duration::from_millis(2)).is_err());
}

#[tokio::test]
async fn a_scope_has_one_live_owner() {
    let store = InMemoryLeaseStore::new();
    let request = LeaseRequest::new(scope("owned"), "owner-a", Duration::from_secs(30));

    let first = store.acquire(&request).await.unwrap().unwrap();
    let second = store
        .acquire(&LeaseRequest {
            owner_id: "owner-b".to_string(),
            ..request
        })
        .await
        .unwrap();

    assert_eq!(first.fencing_token, 1);
    assert!(second.is_none());
    assert!(store.is_current(&first).await.unwrap());
}

#[tokio::test]
async fn release_deletes_the_key_and_a_new_generation_starts_at_the_initial_token() {
    let store = InMemoryLeaseStore::new();
    let scope = scope("takeover");
    let first = store
        .acquire(&LeaseRequest::new(
            scope.clone(),
            "owner-a",
            Duration::from_secs(30),
        ))
        .await
        .unwrap()
        .unwrap();
    assert!(store.release(&first).await.unwrap());
    assert!(store.current(&scope).await.unwrap().is_none());

    let second = store
        .acquire(&LeaseRequest::new(
            scope,
            "owner-b",
            Duration::from_secs(30),
        ))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(second.fencing_token, 1);
    assert!(
        store
            .renew(&first, Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    assert!(!store.release(&first).await.unwrap());
}

#[tokio::test]
async fn an_expired_lease_can_be_taken_over() {
    let store = InMemoryLeaseStore::new();
    let scope = scope("expiry");
    let first = store
        .acquire(&LeaseRequest::new(
            scope.clone(),
            "owner-a",
            Duration::from_millis(20),
        ))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let second = store
        .acquire(&LeaseRequest::new(
            scope,
            "owner-b",
            Duration::from_secs(30),
        ))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(second.fencing_token, first.fencing_token + 1);
    assert!(
        !store
            .renew(&first, Duration::from_secs(30))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn waiting_owner_wakes_when_the_lease_is_released() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let scope = scope("waiting");
    let request = LeaseRequest::new(scope.clone(), "owner-a", Duration::from_secs(30));
    let held = store.acquire(&request).await.unwrap().unwrap();

    let waiting_store = store.clone();
    let waiter = tokio::spawn(async move {
        Lease::wait_for_lease(
            waiting_store.as_ref(),
            &LeaseRequest::new(scope, "owner-b", Duration::from_secs(30)),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    store.release(&held).await.unwrap();

    let lease = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("waiter should wake")
        .unwrap()
        .unwrap();
    assert_eq!(lease.owner_id, "owner-b");
}

#[tokio::test]
async fn guard_renews_and_releases_its_lease() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let scope = scope("guard");
    let request = LeaseRequest::new(scope.clone(), "owner-a", Duration::from_secs(1));
    let mut guard = LeaseGuard::acquire(store.clone(), request, Duration::from_millis(800))
        .await
        .unwrap()
        .unwrap();

    tokio::time::sleep(Duration::from_millis(450)).await;
    guard.ensure_writable().await.unwrap();
    assert!(!guard.lost());

    let lease = guard.lease().clone();
    drop(guard);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!store.is_current(&lease).await.unwrap());
}

#[tokio::test]
async fn a_recovered_lease_is_adopted_without_acquiring_it_again() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let request = LeaseRequest::new(scope("adopt"), "owner-a", Duration::from_secs(1))
        .with_context("raw_events", "digest");
    let lease = store.acquire(&request).await.unwrap().unwrap();

    let guard = LeaseGuard::adopt(
        store.clone(),
        lease.clone(),
        request,
        Duration::from_millis(800),
    )
    .await
    .unwrap();

    assert_eq!(guard.lease().fencing_token, lease.fencing_token);
    assert!(store.is_current(&lease).await.unwrap());
    assert!(guard.release().await.unwrap());
    assert!(store.current(&lease.scope).await.unwrap().is_none());
}

#[tokio::test]
async fn guard_rejects_work_beyond_its_deadline() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let request = LeaseRequest::new(scope("deadline"), "owner-a", Duration::from_millis(300));
    let guard = LeaseGuard::acquire(store, request, Duration::from_millis(30))
        .await
        .unwrap()
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(guard.lost());
}

#[tokio::test]
async fn a_failed_renewal_marks_the_guard_lost_and_blocks_writes() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let request = LeaseRequest::new(
        scope("renewal-failure"),
        "owner-a",
        Duration::from_millis(300),
    );
    let mut guard = LeaseGuard::acquire(store.clone(), request, Duration::from_millis(250))
        .await
        .unwrap()
        .unwrap();
    let lease = guard.lease().clone();

    store.release(&lease).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(guard.lost());
    assert!(guard.ensure_writable().await.is_err());
}

#[tokio::test]
async fn a_stalled_owner_cannot_write_after_a_successor_takes_over() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let scope = scope("stalled");
    let request = LeaseRequest::new(scope.clone(), "owner-a", Duration::from_millis(40));
    let mut guard = LeaseGuard::acquire(store.clone(), request, Duration::from_millis(20))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let successor = store
        .acquire(&LeaseRequest::new(
            scope,
            "owner-b",
            Duration::from_secs(30),
        ))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(successor.fencing_token, guard.lease().fencing_token + 1);
    assert!(guard.ensure_writable().await.is_err());
}

#[test]
fn lease_buckets_must_retain_history() {
    use crate::NatsLeaseStore;

    assert!(NatsLeaseStore::validate_history(1).is_ok());
    assert!(NatsLeaseStore::validate_history(10).is_ok());
    assert!(NatsLeaseStore::validate_history(0).is_err());
    assert!(NatsLeaseStore::validate_history(-1).is_err());
}

#[tokio::test]
async fn a_nats_store_serializes_owners_and_advances_fencing_tokens() {
    let Ok(url) = std::env::var("PLATFORM_TEST_NATS_URL") else {
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let bucket = format!("lease-test-{}-{nonce}", std::process::id());
    let kv = platform_nats::KeyValue::connect(&url, "platform-lease-tests", &bucket)
        .await
        .unwrap();
    assert_eq!(kv.history().await.unwrap(), 1);
    let store = NatsLeaseStore::connect(&url, "platform-lease-tests", &bucket)
        .await
        .unwrap();
    let scope = scope("nats-owned");
    let first = store
        .acquire(&LeaseRequest::new(
            scope.clone(),
            "owner-a",
            Duration::from_millis(50),
        ))
        .await
        .unwrap()
        .unwrap();
    let blocked = store
        .acquire(&LeaseRequest::new(
            scope.clone(),
            "owner-b",
            Duration::from_secs(30),
        ))
        .await
        .unwrap();

    assert_eq!(first.fencing_token, 1);
    assert!(blocked.is_none());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second = store
        .acquire(&LeaseRequest::new(
            scope.clone(),
            "owner-b",
            Duration::from_secs(30),
        ))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(second.fencing_token, first.fencing_token + 1);
    assert!(!store.release(&first).await.unwrap());
    assert!(store.release(&second).await.unwrap());
    assert!(store.current(&scope).await.unwrap().is_none());
}
