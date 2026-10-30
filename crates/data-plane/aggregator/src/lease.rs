//! Single-writer leases and fencing tokens for shard state.

use crate::config::AggregatorConfig;
use crate::shard::ShardId;
use async_trait::async_trait;
#[cfg(test)]
use parking_lot::Mutex;
use platform_errors::{BpError, BpResult};
use platform_metrics::AggregatorMetrics;
use platform_nats::KeyValue;
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::info;

const LEASE_KEY_PREFIX: &str = "lease";
const LEASE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// A lease held by the only writer allowed to commit a shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterLease {
    pub shard: ShardId,
    pub owner_id: String,
    pub fencing_token: u64,
    pub expires_at: TimestampMilliseconds,
}

impl WriterLease {
    pub fn is_expired(&self, now: TimestampMilliseconds) -> bool {
        self.expires_at <= now
    }

    pub fn is_held_by(&self, owner_id: &str, fencing_token: u64) -> bool {
        self.owner_id == owner_id && self.fencing_token == fencing_token
    }
}

/// Durable storage for shard writer leases.
#[async_trait]
pub trait LeaseStore: Send + Sync {
    async fn acquire(
        &self,
        shard: ShardId,
        owner_id: &str,
        ttl: Duration,
    ) -> BpResult<Option<WriterLease>>;

    async fn renew(&self, lease: &WriterLease, ttl: Duration) -> BpResult<Option<WriterLease>>;

    async fn release(&self, lease: &WriterLease) -> BpResult<()>;

    async fn current(&self, shard: ShardId) -> BpResult<Option<WriterLease>>;
}

pub fn lease_key(shard: ShardId) -> String {
    format!("{LEASE_KEY_PREFIX}-{}", shard.get())
}

/// Waits until this owner can acquire `shard`'s lease.
///
/// A replica that starts while another writer owns the shard becomes a
/// standby instead of failing. It keeps polling until the active writer
/// releases the lease or lets it expire, at which point the next fencing token
/// is issued.
pub async fn wait_for_lease(
    leases: &dyn LeaseStore,
    shard: ShardId,
    owner_id: &str,
    ttl: Duration,
) -> BpResult<WriterLease> {
    let mut waiting = false;
    loop {
        if let Some(lease) = leases.acquire(shard, owner_id, ttl).await? {
            return Ok(lease);
        }
        if !waiting {
            waiting = true;
            AggregatorMetrics::record_rebalance(shard.get(), "lease_waiting");
            info!(
                shard = %shard,
                writer = %owner_id,
                "shard lease is held by another writer; standing by"
            );
        }
        tokio::time::sleep(LEASE_RETRY_INTERVAL).await;
    }
}

/// Generates an owner identity that is unique to one process incarnation.
pub fn new_writer_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}-{}-{}",
        std::process::id(),
        TimestampMilliseconds::now().0,
        counter
    )
}

fn expires_at(now: TimestampMilliseconds, ttl: Duration) -> TimestampMilliseconds {
    let millis = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
    now + DurationMilliseconds(millis)
}

/// In-memory lease store used by tests.
#[cfg(test)]
#[derive(Debug, Default)]
pub struct InMemoryLeaseStore {
    leases: Mutex<HashMap<ShardId, WriterLease>>,
}

#[cfg(test)]
impl InMemoryLeaseStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
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
                renewed.expires_at = expires_at(now, ttl);
                renewed
            }
            Some(existing) if !existing.is_expired(now) => return Ok(None),
            Some(existing) => WriterLease {
                shard,
                owner_id: owner_id.to_string(),
                fencing_token: existing.fencing_token.saturating_add(1),
                expires_at: expires_at(now, ttl),
            },
            None => WriterLease {
                shard,
                owner_id: owner_id.to_string(),
                fencing_token: 1,
                expires_at: expires_at(now, ttl),
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
        current.expires_at = expires_at(now, ttl);
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

/// Leases stored in the aggregator checkpoint bucket.
pub struct NatsLeaseStore {
    values: KeyValue,
}

impl NatsLeaseStore {
    pub async fn connect(config: &AggregatorConfig) -> BpResult<Self> {
        let values = KeyValue::connect(
            &config.nats_url,
            "platform-aggregator-leases",
            &config.checkpoint_bucket(),
        )
        .await?;
        Ok(Self { values })
    }

    async fn load(&self, shard: ShardId) -> BpResult<Option<(WriterLease, u64)>> {
        let Some((payload, revision)) = self.values.get_with_revision(&lease_key(shard)).await?
        else {
            return Ok(None);
        };
        let lease = serde_json::from_slice(&payload)?;
        Ok(Some((lease, revision)))
    }
}

#[async_trait]
impl LeaseStore for NatsLeaseStore {
    async fn acquire(
        &self,
        shard: ShardId,
        owner_id: &str,
        ttl: Duration,
    ) -> BpResult<Option<WriterLease>> {
        let now = TimestampMilliseconds::now();
        let key = lease_key(shard);
        let Some((existing, revision)) = self.load(shard).await? else {
            let lease = WriterLease {
                shard,
                owner_id: owner_id.to_string(),
                fencing_token: 1,
                expires_at: expires_at(now, ttl),
            };
            let payload = serde_json::to_vec(&lease)?;
            return match self.values.create(&key, &payload).await {
                Ok(_) => Ok(Some(lease)),
                Err(_) => {
                    let Some((current, _)) = self.load(shard).await? else {
                        return Err(BpError::bad_value(
                            "aggregator lease",
                            shard.to_string(),
                            "lease creation conflicted without a visible holder",
                        ));
                    };
                    if current.owner_id != owner_id && !current.is_expired(now) {
                        Ok(None)
                    } else {
                        Err(BpError::bad_value(
                            "aggregator lease",
                            shard.to_string(),
                            "lease creation conflicted; retry acquisition",
                        ))
                    }
                }
            };
        };

        if existing.owner_id != owner_id && !existing.is_expired(now) {
            return Ok(None);
        }
        let fencing_token = if existing.owner_id == owner_id {
            existing.fencing_token
        } else {
            existing.fencing_token.saturating_add(1)
        };
        let lease = WriterLease {
            shard,
            owner_id: owner_id.to_string(),
            fencing_token,
            expires_at: expires_at(now, ttl),
        };
        let payload = serde_json::to_vec(&lease)?;
        match self.values.update(&key, &payload, revision).await {
            Ok(_) => Ok(Some(lease)),
            Err(_) => Ok(None),
        }
    }

    async fn renew(&self, lease: &WriterLease, ttl: Duration) -> BpResult<Option<WriterLease>> {
        let now = TimestampMilliseconds::now();
        let Some((current, revision)) = self.load(lease.shard).await? else {
            return Ok(None);
        };
        if !current.is_held_by(&lease.owner_id, lease.fencing_token) || current.is_expired(now) {
            return Ok(None);
        }
        let mut renewed = current;
        renewed.expires_at = expires_at(now, ttl);
        let payload = serde_json::to_vec(&renewed)?;
        match self
            .values
            .update(&lease_key(lease.shard), &payload, revision)
            .await
        {
            Ok(_) => Ok(Some(renewed)),
            Err(_) => Ok(None),
        }
    }

    async fn release(&self, lease: &WriterLease) -> BpResult<()> {
        let Some((mut current, revision)) = self.load(lease.shard).await? else {
            return Ok(());
        };
        if current.is_held_by(&lease.owner_id, lease.fencing_token) {
            // Keep an expired tombstone so the next owner receives a strictly
            // higher fencing token even after a clean release.
            current.expires_at = TimestampMilliseconds::now();
            let payload = serde_json::to_vec(&current)?;
            self.values
                .update(&lease_key(lease.shard), &payload, revision)
                .await?;
        }
        Ok(())
    }

    async fn current(&self, shard: ShardId) -> BpResult<Option<WriterLease>> {
        Ok(self.load(shard).await?.map(|(lease, _)| lease))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_ne!(new_writer_id(), new_writer_id());
    }
}
