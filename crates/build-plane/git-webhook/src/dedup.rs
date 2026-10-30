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
/// Redeliveries handled by another environment are still deduped downstream
/// when the build orchestrator consumes them from SQS.
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

    pub fn claim(&self, delivery_id: &str) -> Option<DeliveryDedupGuard> {
        let mut seen = self.seen.lock();
        if seen
            .get(delivery_id)
            .is_some_and(|inserted| inserted.elapsed() < self.ttl)
        {
            return None;
        }
        seen.put(delivery_id.to_owned(), Instant::now());
        Some(DeliveryDedupGuard {
            dedup: self.clone(),
            delivery_id: delivery_id.to_owned(),
            completed: false,
        })
    }

    /// Removes `delivery_id` so a later retry can be processed after a failed
    /// attempt.
    pub fn forget(&self, delivery_id: &str) {
        self.seen.lock().pop(delivery_id);
    }
}

pub struct DeliveryDedupGuard {
    dedup: DeliveryDedup,
    delivery_id: String,
    completed: bool,
}

impl DeliveryDedupGuard {
    pub fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for DeliveryDedupGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.dedup.forget(&self.delivery_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl DeliveryDedup {
        pub fn is_duplicate(&self, delivery_id: &str) -> bool {
            match self.claim(delivery_id) {
                Some(guard) => {
                    guard.complete();
                    false
                }
                None => true,
            }
        }
    }

    fn capacity(capacity: usize) -> NonZeroUsize {
        NonZeroUsize::new(capacity).unwrap()
    }

    #[test]
    fn completed_guard_keeps_delivery_deduplicated() {
        let dedup = DeliveryDedup::new(capacity(2), Duration::from_secs(60));

        let guard = dedup.claim("delivery").expect("delivery should be claimed");
        assert!(dedup.claim("delivery").is_none());
        guard.complete();

        assert!(dedup.claim("delivery").is_none());
    }

    #[test]
    fn is_duplicate_marks_delivery_as_seen() {
        let dedup = DeliveryDedup::new(capacity(2), Duration::from_secs(60));

        assert!(!dedup.is_duplicate("delivery"));
        assert!(dedup.is_duplicate("delivery"));

        dedup.forget("delivery");
        assert!(!dedup.is_duplicate("delivery"));
    }

    #[test]
    fn dropping_uncompleted_guard_forgets_delivery() {
        let dedup = DeliveryDedup::new(capacity(2), Duration::from_secs(60));

        let guard = dedup.claim("delivery").expect("delivery should be claimed");
        assert!(dedup.claim("delivery").is_none());
        drop(guard);

        assert!(dedup.claim("delivery").is_some());
    }

    #[test]
    fn guard_forgets_delivery_when_processing_panics() {
        let dedup = DeliveryDedup::new(capacity(2), Duration::from_secs(60));

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = dedup.claim("delivery").expect("delivery should be claimed");
            panic!("processing failed");
        }));

        assert!(result.is_err());
        assert!(dedup.claim("delivery").is_some());
    }
}
