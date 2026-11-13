use crate::{
    shard_id::ShardId,
    writer_lease::{LeaseStore, WriterLease},
    writer_lease_tests::InMemoryLeaseStore,
};
use std::sync::Arc;
use std::time::Duration;

fn shard() -> ShardId {
    ShardId::new(3)
}

#[tokio::test]
async fn standby_wakes_on_release_without_polling() {
    let ttl = Duration::from_secs(30);
    let store = Arc::new(InMemoryLeaseStore::new());
    let held = store
        .acquire(shard(), "writer-a", ttl)
        .await
        .unwrap()
        .unwrap();
    let attempts_before_waiting = store.acquire_attempt_count();
    let waiting_store = store.clone();
    let waiter = tokio::spawn(async move {
        WriterLease::wait_for_lease(waiting_store.as_ref(), shard(), "writer-b", ttl).await
    });
    store.wait_for_acquire_attempt().await;

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(store.acquire_attempt_count(), attempts_before_waiting + 1);

    store.release(&held).await.unwrap();
    let lease = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("standby should wake when the lease is released")
        .expect("standby task should not panic")
        .unwrap();

    assert_eq!(lease.owner_id, "writer-b");
    assert_eq!(lease.fencing_token, held.fencing_token + 1);
}

#[tokio::test]
async fn standby_takes_over_when_the_lease_expires_without_a_write() {
    let ttl = Duration::from_millis(50);
    let store = InMemoryLeaseStore::new();
    let held = store
        .acquire(shard(), "writer-a", ttl)
        .await
        .unwrap()
        .unwrap();

    let lease = tokio::time::timeout(
        Duration::from_secs(2),
        WriterLease::wait_for_lease(&store, shard(), "writer-b", ttl),
    )
    .await
    .expect("standby should use the expiry timer")
    .unwrap();

    assert_eq!(lease.owner_id, "writer-b");
    assert_eq!(lease.fencing_token, held.fencing_token + 1);
}
