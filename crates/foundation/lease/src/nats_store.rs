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
        Self::validate_history(kv.history().await?)?;
        Self::validate_max_age(kv.max_age().await?)?;
        Self::validate_max_messages(kv.max_messages().await?)?;
        Self::validate_max_bytes(kv.max_bytes().await?)?;
        Ok(Self { kv })
    }

    pub fn validate_history(history: i64) -> PlatformResult<()> {
        if history < 1 {
            return Err(platform_errors::PlatformError::config_invalid(
                "lease KV bucket",
                "must retain key history",
            ));
        }
        Ok(())
    }

    pub fn validate_max_age(max_age: Duration) -> PlatformResult<()> {
        if !max_age.is_zero() {
            return Err(platform_errors::PlatformError::config_invalid(
                "lease KV bucket",
                "must not expire keys by age",
            ));
        }
        Ok(())
    }

    pub fn validate_max_messages(max_messages: i64) -> PlatformResult<()> {
        if max_messages > 0 {
            return Err(platform_errors::PlatformError::config_invalid(
                "lease KV bucket",
                "must not evict keys by message count",
            ));
        }
        Ok(())
    }

    pub fn validate_max_bytes(max_bytes: i64) -> PlatformResult<()> {
        if max_bytes > 0 {
            return Err(platform_errors::PlatformError::config_invalid(
                "lease KV bucket",
                "must not evict keys by byte count",
            ));
        }
        Ok(())
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
            let clock_skew_ms = request.clock_skew_millis();
            let Some((current, revision)) = self.load(&request.scope).await? else {
                let lease = Lease {
                    scope: request.scope.clone(),
                    owner_id: request.owner_id.clone(),
                    fencing_token: request.initial_token,
                    expires_at: Lease::expires_at(now, request.ttl),
                    clock_skew_ms,
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
                clock_skew_ms,
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
            clock_skew_ms: lease.clock_skew_ms,
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
        let released = Lease::released(
            lease.scope.clone(),
            lease.fencing_token,
            lease.clock_skew_ms,
        );
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
        Ok(self
            .load(scope)
            .await?
            .map(|(lease, _)| lease)
            .filter(|lease| !lease.is_released()))
    }

    fn watch(&self, scope: &LeaseScope) -> PlatformResult<LeaseWatch> {
        Ok(LeaseWatch::Nats(Box::new(self.kv.watch(&scope.key())?)))
    }
}
