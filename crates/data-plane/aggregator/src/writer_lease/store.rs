use crate::{shard_id::ShardId, writer_lease::WriterLease};
use async_trait::async_trait;
use platform_errors::BuildResult;
use std::time::Duration;

#[async_trait]
pub trait LeaseStore: Send + Sync {
    async fn acquire(
        &self,
        shard: ShardId,
        owner_id: &str,
        ttl: Duration,
    ) -> BuildResult<Option<WriterLease>>;

    async fn renew(&self, lease: &WriterLease, ttl: Duration) -> BuildResult<Option<WriterLease>>;

    async fn release(&self, lease: &WriterLease) -> BuildResult<()>;

    async fn current(&self, shard: ShardId) -> BuildResult<Option<WriterLease>>;

    fn watch(&self, shard: ShardId) -> BuildResult<super::LeaseWatch>;
}
