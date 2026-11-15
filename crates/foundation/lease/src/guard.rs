use crate::{Lease, LeaseRequest, LeaseStore};
use platform_errors::{PlatformError, PlatformResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

pub struct LeaseGuard {
    store: Arc<dyn LeaseStore>,
    lease: Lease,
    ttl: Duration,
    deadline: Instant,
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
        if max_work.is_zero() || max_work >= request.ttl {
            return Err(PlatformError::config_invalid(
                "lease max work time",
                "must be positive and shorter than the lease TTL",
            ));
        }
        let interval = LeaseRequest::renewal_interval(request.ttl)?;
        let Some(lease) = store.acquire(&request).await? else {
            return Ok(None);
        };
        let lost = Arc::new(AtomicBool::new(false));
        let renewal_lock = Arc::new(Mutex::new(()));
        let renewal = Self::spawn_renewal(
            store.clone(),
            lease.clone(),
            request.ttl,
            interval,
            lost.clone(),
            renewal_lock.clone(),
        );
        Ok(Some(Self {
            store,
            lease,
            ttl: request.ttl,
            deadline: Instant::now() + max_work,
            lost,
            renewal,
            renewal_lock,
            released: false,
        }))
    }

    fn spawn_renewal(
        store: Arc<dyn LeaseStore>,
        lease: Lease,
        ttl: Duration,
        interval: Duration,
        lost: Arc<AtomicBool>,
        renewal_lock: Arc<Mutex<()>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let _guard = renewal_lock.lock().await;
                match store.renew(&lease, ttl).await {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        lost.store(true, Ordering::SeqCst);
                        warn!(scope = %lease.scope.as_str(), owner = %lease.owner_id, token = lease.fencing_token, "lease lost");
                        return;
                    }
                    Err(error) => {
                        lost.store(true, Ordering::SeqCst);
                        warn!(%error, scope = %lease.scope.as_str(), owner = %lease.owner_id, "lease renewal failed");
                        return;
                    }
                }
            }
        })
    }

    pub fn lease(&self) -> &Lease {
        &self.lease
    }

    pub fn lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst) || Instant::now() >= self.deadline
    }

    pub async fn ensure_writable(&mut self) -> PlatformResult<()> {
        let _guard = self.renewal_lock.lock().await;
        if self.lost() {
            return Err(Self::not_writable(&self.lease));
        }
        let Some(renewed) = self.store.renew(&self.lease, self.ttl).await? else {
            self.lost.store(true, Ordering::SeqCst);
            return Err(Self::not_writable(&self.lease));
        };
        if !renewed.is_held_by(&self.lease.owner_id, self.lease.fencing_token) {
            self.lost.store(true, Ordering::SeqCst);
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

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.renewal.abort();
        let store = self.store.clone();
        let lease = self.lease.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                match store.release(&lease).await {
                    Ok(true) => debug!(scope = %lease.scope.as_str(), "released lease on drop"),
                    Ok(false) => {}
                    Err(error) => warn!(%error, scope = %lease.scope.as_str(), "failed to release lease on drop"),
                }
            });
        }
    }
}
