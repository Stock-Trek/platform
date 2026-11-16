use crate::runner::ProcessorRunner;
use crate::runner::tests::fixtures_tests::{FakeSink, FakeSource, Fixtures};
use crate::shard_id::ShardId;
use platform_lease::{InMemoryLeaseStore, LeaseStore};
use std::sync::Arc;

#[tokio::test]
async fn waits_for_an_active_writer_then_takes_over_and_releases_the_shard() {
    let leases = Arc::new(InMemoryLeaseStore::new());
    let shard = ShardId::new(0);
    let ttl = std::time::Duration::from_secs(30);
    let held = leases
        .acquire(&shard.lease_request("other-writer", ttl))
        .await
        .unwrap()
        .unwrap();
    let runner = ProcessorRunner::new(
        Fixtures::config(),
        FakeSource::default(),
        FakeSink::default(),
    )
    .with_lease_store(leases.clone())
    .with_writer_id("this-writer");

    let handle = tokio::spawn(async move { runner.run().await });
    leases
        .wait_for_acquire_attempt(&shard.lease_scope(), 2)
        .await;
    assert!(!handle.is_finished());
    leases.release(&held).await.unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), handle)
        .await
        .expect("runner should acquire the released lease")
        .expect("runner task should not panic")
        .unwrap();

    assert!(leases.current(&held.scope).await.unwrap().is_none());
}
