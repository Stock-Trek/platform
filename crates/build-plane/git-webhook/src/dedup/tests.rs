use super::*;
use std::num::NonZeroUsize;
use std::time::Duration;

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
