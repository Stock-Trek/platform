use lru::LruCache;
use parking_lot::Mutex;
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

/// Best-effort, in-process record of recently seen delivery IDs.
///
/// Lambda reuses a warm execution environment between invocations, so a replayed
/// delivery is dropped here before it is forwarded to SQS. The cache is bounded
/// and entries expire, so a long-lived environment cannot grow without limit.
/// Redeliveries handled by another environment are still deduped downstream by
/// the ECS client token.
#[derive(Clone)]
pub struct DeliveryDedup {
    seen: Arc<Mutex<LruCache<String, Instant>>>,
    ttl: Duration,
}

impl DeliveryDedup {
    pub fn new(capacity: NonZeroUsize, ttl: Duration) -> Self {
        Self {
            seen: Arc::new(Mutex::new(LruCache::new(capacity))),
            ttl,
        }
    }

    /// Marks `delivery_id` as seen and reports whether it was already seen
    /// within the TTL window, in which case the delivery is a duplicate.
    pub fn is_duplicate(&self, delivery_id: &str) -> bool {
        let mut seen = self.seen.lock();
        if seen
            .get(delivery_id)
            .is_some_and(|inserted| inserted.elapsed() < self.ttl)
        {
            return true;
        }
        seen.put(delivery_id.to_owned(), Instant::now());
        false
    }

    /// Removes `delivery_id` so a later retry can be processed after a failed
    /// attempt.
    pub fn forget(&self, delivery_id: &str) {
        self.seen.lock().pop(delivery_id);
    }
}
