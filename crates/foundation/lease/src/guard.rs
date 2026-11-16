use crate::commit_authorizer::LeaseCommitAuthorizer;
use crate::{Lease, LeaseContext, LeaseRequest, LeaseStore};
use platform_errors::{PlatformError, PlatformResult};
use platform_metrics::MarketDataMetrics;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

pub struct LeaseGuard {
    store: Arc<dyn LeaseStore>,
    lease: Lease,
    ttl: Duration,
    deadline: Instant,
    context: LeaseContext,
    lost: Arc<AtomicBool>,
    renewal: JoinHandle<()>,
    renewal_lock: Arc<Mutex<()>>,
    released: bool,
}

impl LeaseGuard {
    pub async fn acquire(
        store: Arc<dyn LeaseStore>,
        request: LeaseRequest,
        max_work: Duration,
    ) -> PlatformResult<Option<Self>> {
        Self::validate(request.ttl, max_work)?;
        let Some(lease) = store.acquire(&request).await? else {
            MarketDataMetrics::record_lease_contention(request.scope.as_str());
            info!(
                table = request.context.table(),
                scope = %request.scope.as_str(),
                owner = %request.owner_id,
                source_batch_digest = request.context.source_batch_digest(),
                "lease contention"
            );
            return Ok(None);
        };
        Ok(Some(Self::adopt(store, lease, request, max_work).await?))
    }

    pub async fn adopt(
        store: Arc<dyn LeaseStore>,
        lease: Lease,
        request: LeaseRequest,
        max_work: Duration,
    ) -> PlatformResult<Self> {
        Self::validate(request.ttl, max_work)?;
        let interval = LeaseRequest::renewal_interval(request.ttl)?;
        let context = request.context;
        MarketDataMetrics::record_lease_acquisition(lease.scope.as_str());
        info!(
            table = context.table(),
            scope = %lease.scope.as_str(),
            owner = %lease.owner_id,
            token = lease.fencing_token,
            expires_at = lease.expires_at.0,
            source_batch_digest = context.source_batch_digest(),
            "lease acquired"
        );
        let lost = Arc::new(AtomicBool::new(false));
        let renewal_lock = Arc::new(Mutex::new(()));
        let deadline = Instant::now() + max_work;
        let renewal = Renewal {
            store: store.clone(),
            lease: lease.clone(),
            ttl: request.ttl,
            interval,
            deadline,
            context: context.clone(),
            lost: lost.clone(),
            renewal_lock: renewal_lock.clone(),
        }
        .start();
        Ok(Self {
            store,
            lease,
            ttl: request.ttl,
            deadline,
            context,
            lost,
            renewal,
            renewal_lock,
            released: false,
        })
    }

    fn validate(ttl: Duration, max_work: Duration) -> PlatformResult<()> {
        if max_work.is_zero() || max_work >= ttl {
            return Err(PlatformError::config_invalid(
                "lease max work time",
                "must be positive and shorter than the lease TTL",
            ));
        }
        Ok(())
    }

    fn release_on_drop(
        store: Arc<dyn LeaseStore>,
        lease: Lease,
        context: LeaseContext,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            match store.release(&lease).await {
                Ok(true) => {
                    debug!(
                        table = context.table(),
                        scope = %lease.scope.as_str(),
                        owner = %lease.owner_id,
                        token = lease.fencing_token,
                        source_batch_digest = context.source_batch_digest(),
                        "released lease on drop"
                    );
                }
                Ok(false) => {}
                Err(error) => warn!(
                    %error,
                    table = context.table(),
                    scope = %lease.scope.as_str(),
                    owner = %lease.owner_id,
                    token = lease.fencing_token,
                    source_batch_digest = context.source_batch_digest(),
                    "failed to release lease on drop"
                ),
            }
        })
    }

    pub fn lease(&self) -> &Lease {
        &self.lease
    }

    pub fn authorizer(&self) -> Arc<LeaseCommitAuthorizer> {
        Arc::new(LeaseCommitAuthorizer::new(
            self.store.clone(),
            self.lease.clone(),
            self.ttl,
            self.deadline,
            self.context.clone(),
            self.lost.clone(),
        ))
    }

    pub fn lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst) || Instant::now() >= self.deadline
    }

    pub async fn ensure_writable(&mut self) -> PlatformResult<()> {
        let _guard = self.renewal_lock.lock().await;
        if self.lost() {
            MarketDataMetrics::record_lease_lost(self.lease.scope.as_str(), "deadline");
            warn!(
                table = self.context.table(),
                scope = %self.lease.scope.as_str(),
                owner = %self.lease.owner_id,
                token = self.lease.fencing_token,
                source_batch_digest = self.context.source_batch_digest(),
                "lease is no longer writable"
            );
            return Err(Self::not_writable(&self.lease));
        }
        let Some(renewed) = self.store.renew(&self.lease, self.ttl).await? else {
            self.lost.store(true, Ordering::SeqCst);
            MarketDataMetrics::record_lease_lost(self.lease.scope.as_str(), "expired");
            warn!(
                table = self.context.table(),
                scope = %self.lease.scope.as_str(),
                owner = %self.lease.owner_id,
                token = self.lease.fencing_token,
                source_batch_digest = self.context.source_batch_digest(),
                "lease lost before commit"
            );
            return Err(Self::not_writable(&self.lease));
        };
        if !renewed.is_held_by(&self.lease.owner_id, self.lease.fencing_token) {
            self.lost.store(true, Ordering::SeqCst);
            MarketDataMetrics::record_lease_lost(self.lease.scope.as_str(), "token_changed");
            warn!(
                table = self.context.table(),
                scope = %self.lease.scope.as_str(),
                owner = %self.lease.owner_id,
                token = self.lease.fencing_token,
                source_batch_digest = self.context.source_batch_digest(),
                "lease token changed before commit"
            );
            return Err(Self::not_writable(&self.lease));
        }
        self.lease = renewed;
        Ok(())
    }

    pub async fn release(mut self) -> PlatformResult<bool> {
        self.renewal.abort();
        let _ = (&mut self.renewal).await;
        self.released = true;
        self.store.release(&self.lease).await
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

struct Renewal {
    store: Arc<dyn LeaseStore>,
    lease: Lease,
    ttl: Duration,
    interval: Duration,
    deadline: Instant,
    context: LeaseContext,
    lost: Arc<AtomicBool>,
    renewal_lock: Arc<Mutex<()>>,
}

impl Renewal {
    fn start(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if Instant::now() >= self.deadline {
                    return;
                }
                let _guard = self.renewal_lock.lock().await;
                match self.store.renew(&self.lease, self.ttl).await {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        self.lost.store(true, Ordering::SeqCst);
                        MarketDataMetrics::record_lease_lost(self.lease.scope.as_str(), "renewal");
                        warn!(
                            table = self.context.table(),
                            scope = %self.lease.scope.as_str(),
                            owner = %self.lease.owner_id,
                            token = self.lease.fencing_token,
                            source_batch_digest = self.context.source_batch_digest(),
                            "lease lost"
                        );
                        return;
                    }
                    Err(error) => {
                        self.lost.store(true, Ordering::SeqCst);
                        MarketDataMetrics::record_lease_lost(
                            self.lease.scope.as_str(),
                            "renewal_error",
                        );
                        warn!(
                            %error,
                            table = self.context.table(),
                            scope = %self.lease.scope.as_str(),
                            owner = %self.lease.owner_id,
                            token = self.lease.fencing_token,
                            source_batch_digest = self.context.source_batch_digest(),
                            "lease renewal failed"
                        );
                        return;
                    }
                }
            }
        })
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.renewal.abort();
        let store = self.store.clone();
        let lease = self.lease.clone();
        let context = self.context.clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            Self::release_on_drop(store, lease, context);
        }
    }
}
