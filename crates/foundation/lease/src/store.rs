use crate::{Lease, LeaseRequest, LeaseScope, LeaseWatch};
use async_trait::async_trait;
use platform_errors::PlatformResult;
use platform_types::TimestampMilliseconds;
use std::time::Duration;

#[async_trait]
pub trait LeaseStore: Send + Sync {
    async fn acquire(&self, request: &LeaseRequest) -> PlatformResult<Option<Lease>>;

    async fn renew(&self, lease: &Lease, ttl: Duration) -> PlatformResult<Option<Lease>>;

    async fn release(&self, lease: &Lease) -> PlatformResult<bool>;

    async fn current(&self, scope: &LeaseScope) -> PlatformResult<Option<Lease>>;

    fn watch(&self, scope: &LeaseScope) -> PlatformResult<LeaseWatch>;

    async fn is_current(&self, lease: &Lease) -> PlatformResult<bool> {
        let now = TimestampMilliseconds::now();
        Ok(self.current(&lease.scope).await?.is_some_and(|current| {
            !current.is_expired(now) && current.is_held_by(&lease.owner_id, lease.fencing_token)
        }))
    }
}
