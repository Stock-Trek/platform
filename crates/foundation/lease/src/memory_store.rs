use crate::{Lease, LeaseRequest, LeaseScope, LeaseStore, LeaseWatch};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::PlatformResult;
use platform_types::TimestampMilliseconds;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, watch};

#[derive(Default)]
pub struct InMemoryLeaseStore {
    leases: Mutex<HashMap<LeaseScope, Lease>>,
    watchers: Mutex<HashMap<LeaseScope, watch::Sender<Option<Lease>>>>,
    attempts: Mutex<HashMap<LeaseScope, u64>>,
    attempt_notify: Notify,
}

impl InMemoryLeaseStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn notify(&self, scope: &LeaseScope) -> PlatformResult<()> {
        let lease = self.leases.lock().get(scope).cloned();
        let mut watchers = self.watchers.lock();
        let sender = watchers
            .entry(scope.clone())
            .or_insert_with(|| watch::channel(None).0);
        sender.send_replace(lease);
        Ok(())
    }

    fn sender(&self, scope: &LeaseScope) -> watch::Sender<Option<Lease>> {
        self.watchers
            .lock()
            .entry(scope.clone())
            .or_insert_with(|| watch::channel(None).0)
            .clone()
    }

    pub fn acquire_attempt_count(&self, scope: &LeaseScope) -> u64 {
        self.attempts.lock().get(scope).copied().unwrap_or(0)
    }

    pub async fn wait_for_acquire_attempt(&self, scope: &LeaseScope, count: u64) {
        loop {
            let notified = self.attempt_notify.notified();
            if self.acquire_attempt_count(scope) >= count {
                return;
            }
            notified.await;
        }
    }
}

#[async_trait]
impl LeaseStore for InMemoryLeaseStore {
    async fn acquire(&self, request: &LeaseRequest) -> PlatformResult<Option<Lease>> {
        let now = TimestampMilliseconds::now();
        let mut leases = self.leases.lock();
        *self
            .attempts
            .lock()
            .entry(request.scope.clone())
            .or_default() += 1;
        let lease = match leases.get(&request.scope) {
            Some(current) if !current.is_expired(now) => {
                if current.owner_id != request.owner_id {
                    drop(leases);
                    self.attempt_notify.notify_waiters();
                    return Ok(None);
                }
                Lease {
                    expires_at: Lease::expires_at(now, request.ttl),
                    ..current.clone()
                }
            }
            Some(current) => Lease {
                scope: request.scope.clone(),
                owner_id: request.owner_id.clone(),
                fencing_token: current.fencing_token.saturating_add(1),
                expires_at: Lease::expires_at(now, request.ttl),
            },
            None => Lease {
                scope: request.scope.clone(),
                owner_id: request.owner_id.clone(),
                fencing_token: request.initial_token,
                expires_at: Lease::expires_at(now, request.ttl),
            },
        };
        leases.insert(request.scope.clone(), lease.clone());
        drop(leases);
        self.notify(&request.scope)?;
        self.attempt_notify.notify_waiters();
        Ok(Some(lease))
    }

    async fn renew(&self, lease: &Lease, ttl: Duration) -> PlatformResult<Option<Lease>> {
        let now = TimestampMilliseconds::now();
        let mut leases = self.leases.lock();
        let Some(current) = leases.get(&lease.scope) else {
            return Ok(None);
        };
        if !current.is_held_by(&lease.owner_id, lease.fencing_token) || current.is_expired(now) {
            return Ok(None);
        }
        let renewed = Lease {
            expires_at: Lease::expires_at(now, ttl),
            ..current.clone()
        };
        leases.insert(lease.scope.clone(), renewed.clone());
        drop(leases);
        self.notify(&lease.scope)?;
        Ok(Some(renewed))
    }

    async fn release(&self, lease: &Lease) -> PlatformResult<bool> {
        let now = TimestampMilliseconds::now();
        let mut leases = self.leases.lock();
        let Some(current) = leases.get(&lease.scope) else {
            return Ok(false);
        };
        if !current.is_held_by(&lease.owner_id, lease.fencing_token) || current.is_expired(now) {
            return Ok(false);
        }
        let released = Lease {
            expires_at: now,
            ..current.clone()
        };
        leases.insert(lease.scope.clone(), released);
        drop(leases);
        self.notify(&lease.scope)?;
        Ok(true)
    }

    async fn current(&self, scope: &LeaseScope) -> PlatformResult<Option<Lease>> {
        Ok(self.leases.lock().get(scope).cloned())
    }

    fn watch(&self, scope: &LeaseScope) -> PlatformResult<LeaseWatch> {
        let mut receiver = self.sender(scope).subscribe();
        receiver.borrow_and_update();
        Ok(LeaseWatch::Memory(receiver))
    }
}

impl Clone for InMemoryLeaseStore {
    fn clone(&self) -> Self {
        Self {
            leases: Mutex::new(self.leases.lock().clone()),
            watchers: Mutex::new(self.watchers.lock().clone()),
            attempts: Mutex::new(self.attempts.lock().clone()),
            attempt_notify: Notify::new(),
        }
    }
}

#[allow(dead_code)]
fn assert_send_sync(_: Arc<dyn LeaseStore>) {}
