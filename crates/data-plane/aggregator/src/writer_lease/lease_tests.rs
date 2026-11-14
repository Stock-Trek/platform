use crate::{
    shard_id::ShardId,
    writer_lease::{LeaseStore, WriterLease, in_memory_store::InMemoryLeaseStore},
};
use std::time::Duration;

fn shard() -> ShardId {
    ShardId::new(3)
}

#[tokio::test]
async fn only_one_owner_can_hold_a_shard_lease() {
    let store = InMemoryLeaseStore::new();
    let ttl = Duration::from_secs(30);

    let first = store
        .acquire(shard(), "writer-a", ttl)
        .await
        .unwrap()
        .unwrap();
    let second = store.acquire(shard(), "writer-b", ttl).await.unwrap();

    assert_eq!(first.fencing_token, 1);
    assert_eq!(second, None);
    assert_eq!(
        store.current(shard()).await.unwrap().unwrap().owner_id,
        "writer-a"
    );
}

#[tokio::test]
async fn release_allows_a_new_owner_with_a_higher_fencing_token() {
    let store = InMemoryLeaseStore::new();
    let ttl = Duration::from_secs(30);
    let first = store
        .acquire(shard(), "writer-a", ttl)
        .await
        .unwrap()
        .unwrap();
    store.release(&first).await.unwrap();

    let second = store
        .acquire(shard(), "writer-b", ttl)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(second.fencing_token, 2);
}

#[tokio::test]
async fn a_stale_lease_cannot_be_renewed_or_released() {
    let store = InMemoryLeaseStore::new();
    let ttl = Duration::from_secs(30);
    let stale = store
        .acquire(shard(), "writer-a", ttl)
        .await
        .unwrap()
        .unwrap();
    store.release(&stale).await.unwrap();
    let current = store
        .acquire(shard(), "writer-b", ttl)
        .await
        .unwrap()
        .unwrap();

    assert!(store.renew(&stale, ttl).await.unwrap().is_none());
    store.release(&stale).await.unwrap();

    assert_eq!(store.current(shard()).await.unwrap().unwrap(), current);
}

#[test]
fn writer_ids_are_unique_per_call() {
    assert_ne!(WriterLease::new_writer_id(), WriterLease::new_writer_id());
}
