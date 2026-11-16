use crate::{shard_id::ShardId, writer_lease::LeaseStore};
use platform_errors::{PlatformError, PlatformResult};
use platform_lease::{Lease, LeaseRequest, LeaseScope};
use platform_types::TimestampMilliseconds;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub fn scope(shard: ShardId) -> LeaseScope {
        LeaseScope::new(format!("aggregator-shard-{}", shard.get()))
    }

    pub fn key(shard: ShardId) -> String {
        Self::scope(shard).key()
    }

    pub fn request(shard: ShardId, owner_id: &str, ttl: Duration) -> LeaseRequest {
        LeaseRequest::new(Self::scope(shard), owner_id, ttl)
    }

    pub fn from_lease(shard: ShardId, lease: Lease) -> Self {
        Self {
            shard,
            owner_id: lease.owner_id,
            fencing_token: lease.fencing_token,
            expires_at: lease.expires_at,
        }
    }

    pub fn to_lease(&self) -> Lease {
        Lease {
            scope: Self::scope(self.shard),
            owner_id: self.owner_id.clone(),
            fencing_token: self.fencing_token,
            expires_at: self.expires_at,
        }
    }

    pub async fn wait_for_lease(
        leases: &dyn LeaseStore,
        shard: ShardId,
        owner_id: &str,
        ttl: Duration,
    ) -> PlatformResult<WriterLease> {
        let request = Self::request(shard, owner_id, ttl);
        let lease = platform_lease::Lease::wait_for_lease(leases.platform_store(), &request)
            .await
            .map_err(|error| {
                PlatformError::bad_value("aggregator lease", shard.to_string(), error.to_string())
            })?;
        Ok(Self::from_lease(shard, lease))
    }

    pub fn new_writer_id() -> String {
        Lease::new_owner_id()
    }
}
