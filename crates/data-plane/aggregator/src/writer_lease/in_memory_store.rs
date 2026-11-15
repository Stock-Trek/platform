use crate::{
    shard_id::ShardId,
    writer_lease::{LeaseStore, WriterLease},
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::PlatformResult;
use platform_lease::{
    InMemoryLeaseStore as PlatformMemory, LeaseScope, LeaseStore as PlatformLeaseStore,
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

pub struct InMemoryLeaseStore {
    inner: PlatformMemory,
    acquire_attempt_count: AtomicUsize,
    last_scope: Mutex<Option<LeaseScope>>,
}

impl Default for InMemoryLeaseStore {
    fn default() -> Self {
        Self {
            inner: PlatformMemory::new(),
            acquire_attempt_count: AtomicUsize::new(0),
            last_scope: Mutex::new(None),
        }
    }
}

impl InMemoryLeaseStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn wait_for_acquire_attempt(&self) {
        let scope = self.last_scope.lock().clone();
        let Some(scope) = scope else {
            return;
        };
        let count = u64::try_from(self.acquire_attempt_count()).unwrap_or(u64::MAX);
        self.inner.wait_for_acquire_attempt(&scope, count).await;
    }

    pub fn acquire_attempt_count(&self) -> usize {
        self.acquire_attempt_count.load(Ordering::Relaxed)
    }

    fn remember(&self, shard: ShardId) {
        *self.last_scope.lock() = Some(WriterLease::scope(shard));
    }
}

#[async_trait]
impl LeaseStore for InMemoryLeaseStore {
    async fn acquire(
        &self,
        shard: ShardId,
        owner_id: &str,
        ttl: Duration,
    ) -> PlatformResult<Option<WriterLease>> {
        self.acquire_attempt_count.fetch_add(1, Ordering::Relaxed);
        self.remember(shard);
        let lease = self
            .inner
            .acquire(&WriterLease::request(shard, owner_id, ttl))
            .await?;
        Ok(lease.map(|lease| WriterLease::from_lease(shard, lease)))
    }

    async fn renew(
        &self,
        lease: &WriterLease,
        ttl: Duration,
    ) -> PlatformResult<Option<WriterLease>> {
        let renewed = self.inner.renew(&lease.to_lease(), ttl).await?;
        Ok(renewed.map(|renewed| WriterLease::from_lease(lease.shard, renewed)))
    }

    async fn release(&self, lease: &WriterLease) -> PlatformResult<()> {
        self.inner.release(&lease.to_lease()).await?;
        Ok(())
    }

    async fn current(&self, shard: ShardId) -> PlatformResult<Option<WriterLease>> {
        let lease = self.inner.current(&WriterLease::scope(shard)).await?;
        Ok(lease.map(|lease| WriterLease::from_lease(shard, lease)))
    }

    fn platform_store(&self) -> &dyn platform_lease::LeaseStore {
        &self.inner
    }
}
