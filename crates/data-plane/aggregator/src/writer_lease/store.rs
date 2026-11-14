use crate::{shard_id::ShardId, writer_lease::WriterLease};
use async_trait::async_trait;
use platform_errors::PlatformResult;
use std::time::Duration;

#[async_trait]
pub trait LeaseStore: Send + Sync {
    async fn acquire(
        &self,
        shard: ShardId,
        owner_id: &str,
        ttl: Duration,
    ) -> PlatformResult<Option<WriterLease>>;

    async fn renew(
        &self,
        lease: &WriterLease,
        ttl: Duration,
    ) -> PlatformResult<Option<WriterLease>>;

    async fn release(&self, lease: &WriterLease) -> PlatformResult<()>;

    async fn current(&self, shard: ShardId) -> PlatformResult<Option<WriterLease>>;

    fn watch(&self, shard: ShardId) -> PlatformResult<super::LeaseWatch>;
}
