use crate::{Lease, LeaseContext, LeaseStore};
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};
use platform_metrics::MarketDataMetrics;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::warn;

#[async_trait]
pub trait CommitAuthorizer: Send + Sync {
    async fn authorize(&self) -> PlatformResult<()>;

    fn remaining(&self) -> Option<Duration> {
        None
    }
}

pub struct LeaseCommitAuthorizer {
    store: Arc<dyn LeaseStore>,
    lease: Mutex<Lease>,
    ttl: Duration,
    deadline: Instant,
    context: LeaseContext,
    lost: Arc<AtomicBool>,
}

impl LeaseCommitAuthorizer {
    pub(crate) fn new(
        store: Arc<dyn LeaseStore>,
        lease: Lease,
        ttl: Duration,
        deadline: Instant,
        context: LeaseContext,
        lost: Arc<AtomicBool>,
    ) -> Self {
        Self {
            store,
            lease: Mutex::new(lease),
            ttl,
            deadline,
            context,
            lost,
        }
    }

    fn not_writable(lease: &Lease) -> PlatformError {
        PlatformError::external(std::io::Error::other(format!(
            "lease {} for {} with fencing token {} is no longer writable",
            lease.scope.as_str(),
            lease.owner_id,
            lease.fencing_token
        )))
    }
}

#[async_trait]
impl CommitAuthorizer for LeaseCommitAuthorizer {
    fn remaining(&self) -> Option<Duration> {
        Some(self.deadline.saturating_duration_since(Instant::now()))
    }

    async fn authorize(&self) -> PlatformResult<()> {
        if self.lost.load(Ordering::SeqCst) || Instant::now() >= self.deadline {
            let lease = self.lease.lock().expect("lease authorizer lock poisoned");
            MarketDataMetrics::record_lease_lost(lease.scope.as_str(), "deadline");
            return Err(Self::not_writable(&lease));
        }
        let lease = self
            .lease
            .lock()
            .expect("lease authorizer lock poisoned")
            .clone();
        let Some(renewed) = self.store.renew(&lease, self.ttl).await? else {
            self.lost.store(true, Ordering::SeqCst);
            MarketDataMetrics::record_lease_lost(lease.scope.as_str(), "expired");
            return Err(Self::not_writable(&lease));
        };
        if !renewed.is_held_by(&lease.owner_id, lease.fencing_token) {
            self.lost.store(true, Ordering::SeqCst);
            MarketDataMetrics::record_lease_lost(lease.scope.as_str(), "token_changed");
            return Err(Self::not_writable(&lease));
        }
        *self.lease.lock().expect("lease authorizer lock poisoned") = renewed;
        Ok(())
    }
}

impl Drop for LeaseCommitAuthorizer {
    fn drop(&mut self) {
        if self.lost.load(Ordering::SeqCst) {
            let lease = self.lease.lock().expect("lease authorizer lock poisoned");
            warn!(
                table = self.context.table(),
                scope = %lease.scope.as_str(),
                owner = %lease.owner_id,
                token = lease.fencing_token,
                "lease commit authorization ended"
            );
        }
    }
}
