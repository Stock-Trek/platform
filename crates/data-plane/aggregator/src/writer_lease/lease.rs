use crate::{
    shard_id::ShardId,
    writer_lease::{LeaseStore, LeaseWatch},
};
use platform_errors::BuildResult;
use platform_metrics::AggregatorMetrics;
use platform_types::{DurationMilliseconds, TimestampMilliseconds};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{info, warn};

const LEASE_KEY_PREFIX: &str = "lease";
const LEASE_WATCH_ERROR_BACKOFF: Duration = Duration::from_secs(1);

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

    pub fn key(shard: ShardId) -> String {
        format!("{LEASE_KEY_PREFIX}-{}", shard.get())
    }

    pub async fn wait_for_lease(
        leases: &dyn LeaseStore,
        shard: ShardId,
        owner_id: &str,
        ttl: Duration,
    ) -> BuildResult<WriterLease> {
        let mut waiting = false;
        let mut watch: Option<LeaseWatch> = None;
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
            if watch.is_none() {
                watch = Some(leases.watch(shard)?);
            }
            let current = leases.current(shard).await?;
            let now = TimestampMilliseconds::now();
            let Some(current) = current else {
                continue;
            };
            if current.is_expired(now) {
                continue;
            }
            let delay = Self::remaining(&current, now);
            let watch = watch.as_mut().expect("lease watch is connected");
            tokio::select! {
                change = watch.next() => {
                    if let Err(error) = change {
                        warn!(%error, shard = %shard, "lease watch failed; retrying");
                        tokio::time::sleep(LEASE_WATCH_ERROR_BACKOFF).await;
                    }
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

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

    pub(crate) fn expires_at(now: TimestampMilliseconds, ttl: Duration) -> TimestampMilliseconds {
        let millis = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
        now + DurationMilliseconds(millis)
    }

    fn remaining(&self, now: TimestampMilliseconds) -> Duration {
        let remaining = self.expires_at - now;
        let millis = u64::try_from(remaining.0.max(1)).unwrap_or(u64::MAX);
        Duration::from_millis(millis)
    }
}
