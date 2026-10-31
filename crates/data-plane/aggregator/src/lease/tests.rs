use async_trait::async_trait;
use crate::{
    lease::{LeaseStore, WriterLease},
    shard::ShardId,
};
use platform_errors::BpResult;
use platform_types::new_types::TimestampMilliseconds;
use std::time::Duration;
use parking_lot::Mutex;
use std::collections::HashMap;

/// In-memory lease store used by tests.
#[derive(Debug, Default)]
pub struct InMemoryLeaseStore {
    leases: Mutex<HashMap<ShardId, WriterLease>>,
}

impl InMemoryLeaseStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl LeaseStore for InMemoryLeaseStore {
    async fn acquire(
        &self,
        shard: ShardId,
        owner_id: &str,
        ttl: Duration,
    ) -> BpResult<Option<WriterLease>> {
        let now = TimestampMilliseconds::now();
        let mut leases = self.leases.lock();
        let lease = match leases.get(&shard) {
            Some(existing) if existing.owner_id == owner_id => {
                let mut renewed = existing.clone();
                renewed.expires_at = WriterLease::expires_at(now, ttl);
                renewed
            }
            Some(existing) if !existing.is_expired(now) => return Ok(None),
            Some(existing) => WriterLease {
                shard,
                owner_id: owner_id.to_string(),
                fencing_token: existing.fencing_token.saturating_add(1),
                expires_at: WriterLease::expires_at(now, ttl),
            },
            None => WriterLease {
                shard,
                owner_id: owner_id.to_string(),
                fencing_token: 1,
                expires_at: WriterLease::expires_at(now, ttl),
            },
        };
        leases.insert(shard, lease.clone());
        Ok(Some(lease))
    }

    async fn renew(&self, lease: &WriterLease, ttl: Duration) -> BpResult<Option<WriterLease>> {
        let now = TimestampMilliseconds::now();
        let mut leases = self.leases.lock();
        let Some(current) = leases.get_mut(&lease.shard) else {
            return Ok(None);
        };
        if !current.is_held_by(&lease.owner_id, lease.fencing_token) || current.is_expired(now) {
            return Ok(None);
        }
        current.expires_at = WriterLease::expires_at(now, ttl);
        Ok(Some(current.clone()))
    }

    async fn release(&self, lease: &WriterLease) -> BpResult<()> {
        let mut leases = self.leases.lock();
        if let Some(current) = leases.get_mut(&lease.shard)
            && current.is_held_by(&lease.owner_id, lease.fencing_token)
        {
            // Keep the fencing token as a tombstone. Removing the key would
            // let the next owner start again at token 1.
            current.expires_at = TimestampMilliseconds::now();
        }
        Ok(())
    }

    async fn current(&self, shard: ShardId) -> BpResult<Option<WriterLease>> {
        Ok(self.leases.lock().get(&shard).cloned())
    }
}


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
