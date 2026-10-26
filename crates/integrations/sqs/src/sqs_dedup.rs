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
mod tests {
    use super::*;
    use platform_messages::sqs_message::{GitSource, SqsDetail, SqsRefType};

    fn message(commit_hash: &str) -> SqsMessage {
        SqsMessage {
            source: GitSource::GitHub {
                host: "github.com".to_owned(),
                delivery_id: Some("delivery".to_owned()),
                installation_id: 1,
            },
            detail: SqsDetail::Commit {
                repo_id: "1".to_owned(),
                ref_name: "main".to_owned(),
                ref_type: SqsRefType::Branch,
                commit_hash: commit_hash.to_owned(),
            },
        }
    }

    fn capacity(capacity: usize) -> NonZeroUsize {
        NonZeroUsize::new(capacity).unwrap()
    }

    #[test]
    fn detects_duplicates_and_forgets_failures() {
        let dedup = SqsDedup::new(capacity(2), Duration::from_secs(60));
        let message = message("abc");

        assert!(!dedup.is_duplicate(&message));
        assert!(dedup.is_duplicate(&message));

        dedup.forget(&message);
        assert!(!dedup.is_duplicate(&message));
    }

    #[test]
    fn guard_forgets_message_when_processing_panics() {
        let dedup = SqsDedup::new(capacity(2), Duration::from_secs(60));
        let message = message("abc");

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = dedup.claim(&message).expect("message should be claimed");
            panic!("processing failed");
        }));

        assert!(result.is_err());
        assert!(dedup.claim(&message).is_some());
    }

    #[test]
    fn completed_guard_keeps_message_deduplicated() {
        let dedup = SqsDedup::new(capacity(2), Duration::from_secs(60));
        let message = message("abc");

        let guard = dedup.claim(&message).expect("message should be claimed");
        guard.complete();

        assert!(dedup.claim(&message).is_none());
    }

    #[test]
    fn treats_different_messages_as_distinct() {
        let dedup = SqsDedup::new(capacity(2), Duration::from_secs(60));

        assert!(!dedup.is_duplicate(&message("abc")));
        assert!(!dedup.is_duplicate(&message("def")));
    }

    #[test]
    fn expires_entries_after_the_ttl() {
        let dedup = SqsDedup::new(capacity(2), Duration::from_millis(10));
        let message = message("abc");

        assert!(!dedup.is_duplicate(&message));
        std::thread::sleep(Duration::from_millis(20));
        assert!(!dedup.is_duplicate(&message));
    }

    #[test]
    fn evicts_the_least_recently_used_message() {
        let dedup = SqsDedup::new(capacity(2), Duration::from_secs(60));
        let first = message("first");
        let second = message("second");
        let third = message("third");

        assert!(!dedup.is_duplicate(&first));
        assert!(!dedup.is_duplicate(&second));
        assert!(dedup.is_duplicate(&first));
        assert!(!dedup.is_duplicate(&third));

        assert!(dedup.is_duplicate(&first));
        assert!(!dedup.is_duplicate(&second));
    }
}
