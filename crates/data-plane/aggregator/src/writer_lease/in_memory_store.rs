use crate::{
    LeaseStore, LeaseWatch,
    shard_id::ShardId,
    writer_lease::{WriterLease, watch::LeaseWatchStream},
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::PlatformResult;
use platform_types::TimestampMilliseconds;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::sync::{
    Notify,
    watch::{Receiver, Sender, channel},
};

#[derive(Debug)]
pub struct InMemoryLeaseStore {
    leases: Mutex<HashMap<ShardId, WriterLease>>,
    acquire_attempts: Notify,
    acquire_attempt_count: AtomicUsize,
    changes: Sender<Option<WriterLease>>,
}

impl Default for InMemoryLeaseStore {
    fn default() -> Self {
        let (changes, _receiver) = channel(None);
        Self {
            leases: Mutex::new(HashMap::new()),
            acquire_attempts: Notify::new(),
            acquire_attempt_count: AtomicUsize::new(0),
            changes,
        }
    }
}

struct MemoryLeaseWatch {
    changes: Receiver<Option<WriterLease>>,
}

#[async_trait]
impl LeaseWatchStream for MemoryLeaseWatch {
    async fn next(&mut self) -> PlatformResult<Option<WriterLease>> {
        if self.changes.changed().await.is_err() {
            return Ok(None);
        }
        Ok(self.changes.borrow_and_update().clone())
    }
}

impl InMemoryLeaseStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn wait_for_acquire_attempt(&self) {
        self.acquire_attempts.notified().await;
    }

    pub fn acquire_attempt_count(&self) -> usize {
        self.acquire_attempt_count.load(Ordering::Relaxed)
    }

    fn publish(&self, lease: Option<WriterLease>) {
        self.changes.send_replace(lease);
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
        self.acquire_attempts.notify_one();
        self.acquire_attempt_count.fetch_add(1, Ordering::Relaxed);
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
        drop(leases);
        self.publish(Some(lease.clone()));
        Ok(Some(lease))
    }

    async fn renew(
        &self,
        lease: &WriterLease,
        ttl: Duration,
    ) -> PlatformResult<Option<WriterLease>> {
        let now = TimestampMilliseconds::now();
        let mut leases = self.leases.lock();
        let Some(current) = leases.get_mut(&lease.shard) else {
            return Ok(None);
        };
        if !current.is_held_by(&lease.owner_id, lease.fencing_token) || current.is_expired(now) {
            return Ok(None);
        }
        current.expires_at = WriterLease::expires_at(now, ttl);
        let renewed = current.clone();
        drop(leases);
        self.publish(Some(renewed.clone()));
        Ok(Some(renewed))
    }

    async fn release(&self, lease: &WriterLease) -> PlatformResult<()> {
        let mut leases = self.leases.lock();
        if let Some(current) = leases.get_mut(&lease.shard)
            && current.is_held_by(&lease.owner_id, lease.fencing_token)
        {
            current.expires_at = TimestampMilliseconds::now();
        }
        let current = leases.get(&lease.shard).cloned();
        drop(leases);
        if current.is_some() {
            self.publish(current);
        }
        Ok(())
    }

    async fn current(&self, shard: ShardId) -> PlatformResult<Option<WriterLease>> {
        Ok(self.leases.lock().get(&shard).cloned())
    }

    fn watch(&self, _shard: ShardId) -> PlatformResult<LeaseWatch> {
        Ok(LeaseWatch::new(MemoryLeaseWatch {
            changes: self.changes.subscribe(),
        }))
    }
}
