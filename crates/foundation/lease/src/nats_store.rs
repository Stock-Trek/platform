use crate::{Lease, LeaseRequest, LeaseScope, LeaseStore, LeaseWatch};
use async_trait::async_trait;
use platform_errors::PlatformResult;
use platform_nats::KeyValue;
use platform_types::TimestampMilliseconds;
use std::time::Duration;

pub struct NatsLeaseStore {
    kv: KeyValue,
}

impl NatsLeaseStore {
    pub async fn connect(url: &str, name: &str, bucket: &str) -> PlatformResult<Self> {
        let kv = KeyValue::connect_existing(url, name, bucket).await?;
        if kv.history().await? < 1 {
            return Err(platform_errors::PlatformError::config_invalid(
                "lease KV bucket",
                "must retain key history",
            ));
        }
        Ok(Self { kv })
    }

    fn decode(&self, value: &[u8]) -> PlatformResult<Lease> {
        serde_json::from_slice(value).map_err(platform_errors::PlatformError::external)
    }

    async fn load(&self, scope: &LeaseScope) -> PlatformResult<Option<(Lease, u64)>> {
        let Some((payload, revision)) = self.kv.get_with_revision(&scope.key()).await? else {
            return Ok(None);
        };
        Ok(Some((self.decode(&payload)?, revision)))
    }

    fn encode(lease: &Lease) -> PlatformResult<Vec<u8>> {
        serde_json::to_vec(lease).map_err(platform_errors::PlatformError::external)
    }
}

#[async_trait]
impl LeaseStore for NatsLeaseStore {
    async fn acquire(&self, request: &LeaseRequest) -> PlatformResult<Option<Lease>> {
        let key = request.scope.key();
        for _ in 0..4 {
            let now = TimestampMilliseconds::now();
            let Some((current, revision)) = self.load(&request.scope).await? else {
                let lease = Lease {
                    scope: request.scope.clone(),
                    owner_id: request.owner_id.clone(),
                    fencing_token: request.initial_token,
                    expires_at: Lease::expires_at(now, request.ttl),
                };
                match self.kv.create(&key, &Self::encode(&lease)?).await {
                    Ok(_) => return Ok(Some(lease)),
                    Err(_) => continue,
                }
            };
            if !current.is_expired(now) && current.owner_id != request.owner_id {
                return Ok(None);
            }
            let lease = Lease {
                scope: request.scope.clone(),
                owner_id: request.owner_id.clone(),
                fencing_token: if current.is_expired(now) {
                    current.fencing_token.saturating_add(1)
                } else {
                    current.fencing_token
                },
                expires_at: Lease::expires_at(now, request.ttl),
            };
            match self.kv.update(&key, &Self::encode(&lease)?, revision).await {
                Ok(_) => return Ok(Some(lease)),
                Err(_) => continue,
            }
        }
        Ok(None)
    }

    async fn renew(&self, lease: &Lease, ttl: Duration) -> PlatformResult<Option<Lease>> {
        let key = lease.scope.key();
        let Some((current, revision)) = self.load(&lease.scope).await? else {
            return Ok(None);
        };
        let now = TimestampMilliseconds::now();
        if !current.is_held_by(&lease.owner_id, lease.fencing_token) || current.is_expired(now) {
            return Ok(None);
        }
        let renewed = Lease {
            expires_at: Lease::expires_at(now, ttl),
            ..current
        };
        match self
            .kv
            .update(&key, &Self::encode(&renewed)?, revision)
            .await
        {
            Ok(_) => Ok(Some(renewed)),
            Err(_) => Ok(None),
        }
    }

    async fn release(&self, lease: &Lease) -> PlatformResult<bool> {
        let key = lease.scope.key();
        let Some((current, revision)) = self.load(&lease.scope).await? else {
            return Ok(false);
        };
        let now = TimestampMilliseconds::now();
        if !current.is_held_by(&lease.owner_id, lease.fencing_token) || current.is_expired(now) {
            return Ok(false);
        }
        let released = Lease {
            expires_at: now,
            ..current
        };
        match self
            .kv
            .update(&key, &Self::encode(&released)?, revision)
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn current(&self, scope: &LeaseScope) -> PlatformResult<Option<Lease>> {
        Ok(self.load(scope).await?.map(|(lease, _)| lease))
    }

    fn watch(&self, scope: &LeaseScope) -> PlatformResult<LeaseWatch> {
        Ok(LeaseWatch::Nats(Box::new(self.kv.watch(&scope.key())?)))
    }
}
