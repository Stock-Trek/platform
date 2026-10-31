use super::*;
use platform_messages::sqs_message::SqsMessage;
use std::num::NonZeroUsize;
use std::time::Duration;
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
