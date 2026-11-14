use crate::{
    config::AggregatorConfig,
    shard_id::ShardId,
    writer_lease::{LeaseStore, LeaseWatch, WriterLease, watch::LeaseWatchStream},
};
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};
use platform_nats::{KeyValue, KeyValueWatch};
use platform_types::TimestampMilliseconds;
use std::time::Duration;

pub struct NatsLeaseStore {
    values: KeyValue,
}

impl NatsLeaseStore {
    pub async fn connect(config: &AggregatorConfig) -> PlatformResult<Self> {
        let values = KeyValue::connect(
            &config.nats_url,
            "platform-aggregator-leases",
            &config.checkpoint_bucket(),
        )
        .await?;
        Ok(Self { values })
    }

    async fn load(&self, shard: ShardId) -> PlatformResult<Option<(WriterLease, u64)>> {
        let Some((payload, revision)) = self
            .values
            .get_with_revision(&WriterLease::key(shard))
            .await?
        else {
            return Ok(None);
        };
        let lease = serde_json::from_slice(&payload)?;
        Ok(Some((lease, revision)))
    }
}

struct NatsLeaseWatch {
    values: KeyValueWatch,
}

#[async_trait]
impl LeaseWatchStream for NatsLeaseWatch {
    async fn next(&mut self) -> PlatformResult<Option<WriterLease>> {
        let change = self.values.next().await?;
        Ok(change
            .value
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()?)
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
        let now = TimestampMilliseconds::now();
        let key = WriterLease::key(shard);
        let Some((existing, revision)) = self.load(shard).await? else {
            let lease = WriterLease {
                shard,
                owner_id: owner_id.to_string(),
                fencing_token: 1,
                expires_at: WriterLease::expires_at(now, ttl),
            };
            let payload = serde_json::to_vec(&lease)?;
            return match self.values.create(&key, &payload).await {
                Ok(_) => Ok(Some(lease)),
                Err(_) => {
                    let Some((current, _)) = self.load(shard).await? else {
                        return Err(PlatformError::bad_value(
                            "aggregator lease",
                            shard.to_string(),
                            "lease creation conflicted without a visible holder",
                        ));
                    };
                    if current.owner_id != owner_id && !current.is_expired(now) {
                        Ok(None)
                    } else {
                        Err(PlatformError::bad_value(
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
            expires_at: WriterLease::expires_at(now, ttl),
        };
        let payload = serde_json::to_vec(&lease)?;
        match self.values.update(&key, &payload, revision).await {
            Ok(_) => Ok(Some(lease)),
            Err(_) => Ok(None),
        }
    }

    async fn renew(
        &self,
        lease: &WriterLease,
        ttl: Duration,
    ) -> PlatformResult<Option<WriterLease>> {
        let now = TimestampMilliseconds::now();
        let Some((current, revision)) = self.load(lease.shard).await? else {
            return Ok(None);
        };
        if !current.is_held_by(&lease.owner_id, lease.fencing_token) || current.is_expired(now) {
            return Ok(None);
        }
        let mut renewed = current;
        renewed.expires_at = WriterLease::expires_at(now, ttl);
        let payload = serde_json::to_vec(&renewed)?;
        match self
            .values
            .update(&WriterLease::key(lease.shard), &payload, revision)
            .await
        {
            Ok(_) => Ok(Some(renewed)),
            Err(_) => Ok(None),
        }
    }

    async fn release(&self, lease: &WriterLease) -> PlatformResult<()> {
        let Some((mut current, revision)) = self.load(lease.shard).await? else {
            return Ok(());
        };
        if current.is_held_by(&lease.owner_id, lease.fencing_token) {
            current.expires_at = TimestampMilliseconds::now();
            let payload = serde_json::to_vec(&current)?;
            self.values
                .update(&WriterLease::key(lease.shard), &payload, revision)
                .await?;
        }
        Ok(())
    }

    async fn current(&self, shard: ShardId) -> PlatformResult<Option<WriterLease>> {
        Ok(self.load(shard).await?.map(|(lease, _)| lease))
    }

    fn watch(&self, shard: ShardId) -> PlatformResult<LeaseWatch> {
        let values = self.values.watch(&WriterLease::key(shard))?;
        Ok(LeaseWatch::new(NatsLeaseWatch { values }))
    }
}
