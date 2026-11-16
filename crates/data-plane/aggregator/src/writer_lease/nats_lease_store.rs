use crate::{
    config::AggregatorConfig,
    shard_id::ShardId,
    writer_lease::{LeaseStore, WriterLease},
};
use async_trait::async_trait;
use platform_errors::PlatformResult;
use platform_lease::{
    LeaseScope, LeaseStore as PlatformLeaseStore, NatsLeaseStore as PlatformNats,
};
use std::time::Duration;

pub struct NatsLeaseStore {
    inner: PlatformNats,
}

impl NatsLeaseStore {
    pub async fn connect(config: &AggregatorConfig) -> PlatformResult<Self> {
        let inner = PlatformNats::connect(
            &config.nats_url,
            "platform-aggregator",
            &config.checkpoint_bucket(),
        )
        .await?;
        Ok(Self { inner })
    }

    fn scope(shard: ShardId) -> LeaseScope {
        WriterLease::scope(shard)
    }
}

#[async_trait]
impl LeaseStore for NatsLeaseStore {
    async fn acquire(
        &self,
        shard: ShardId,
        owner_id: &str,
        ttl: Duration,
    ) -> PlatformResult<Option<WriterLease>> {
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
        let lease = self.inner.current(&Self::scope(shard)).await?;
        Ok(lease.map(|lease| WriterLease::from_lease(shard, lease)))
    }

    fn platform_store(&self) -> &dyn platform_lease::LeaseStore {
        &self.inner
    }
}
