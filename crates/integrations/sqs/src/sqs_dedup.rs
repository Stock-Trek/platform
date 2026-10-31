use lru::LruCache;
use parking_lot::Mutex;
use platform_messages::sqs_message::SqsMessage;
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

/// Best-effort, in-process record of recently processed SQS messages.
///
/// Lambda reuses a warm execution environment between invocations, so a
/// redelivered message is dropped here before it can re-run the pipeline. The
/// cache is bounded and entries expire, so a long-lived environment cannot grow
/// without limit. Redeliveries handled by another environment are still
/// possible; this cache only removes duplicate work within one environment.
#[derive(Clone)]
pub struct SqsDedup {
    seen: Arc<Mutex<LruCache<SqsMessage, Instant>>>,
    ttl: Duration,
}

impl SqsDedup {
    pub fn new(capacity: NonZeroUsize, ttl: Duration) -> Self {
        Self {
            seen: Arc::new(Mutex::new(LruCache::new(capacity))),
            ttl,
        }
    }

    /// Marks `message` as in progress and reports whether an identical message
    /// was already seen within the TTL window, in which case it is a duplicate.
    ///
    /// Call [`Self::forget`] if processing fails so the message can be retried.
    pub fn is_duplicate(&self, message: &SqsMessage) -> bool {
        match self.claim(message) {
            Some(guard) => {
                guard.complete();
                false
            }
            None => true,
        }
    }

    /// Marks `message` as in progress and returns a guard that forgets it again
    /// unless processing completes successfully.
    ///
    /// Returns `None` when an identical message was already seen within the TTL
    /// window, in which case it is a duplicate. Call
    /// [`SqsDedupGuard::complete`] once processing succeeds so the message stays
    /// deduplicated.
    pub fn claim(&self, message: &SqsMessage) -> Option<SqsDedupGuard> {
        let mut seen = self.seen.lock();
        if seen
            .get(message)
            .is_some_and(|inserted| inserted.elapsed() < self.ttl)
        {
            return None;
        }
        seen.put(message.clone(), Instant::now());
        Some(SqsDedupGuard {
            dedup: self.clone(),
            message: message.clone(),
            completed: false,
        })
    }

    /// Removes `message` so a later retry can be processed after a failed
    /// attempt.
    pub fn forget(&self, message: &SqsMessage) {
        self.seen.lock().pop(message);
    }
}

/// Keeps a claimed message marked as seen until processing completes.
///
/// Dropping the guard without calling [`Self::complete`] forgets the message,
/// including when processing panics, so a redelivery can be retried.
pub struct SqsDedupGuard {
    dedup: SqsDedup,
    message: SqsMessage,
    completed: bool,
}

impl SqsDedupGuard {
    /// Marks processing as successful so the message stays deduplicated.
    pub fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for SqsDedupGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.dedup.forget(&self.message);
        }
    }
}

#[cfg(test)]
mod tests;
