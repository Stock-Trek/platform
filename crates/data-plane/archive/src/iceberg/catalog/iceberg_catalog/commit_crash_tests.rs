use super::commit_algorithm::{CommitRetry, ConditionalCommit};
use super::commit_fake_tests::{FakeBackend, FakeTarget};
use crate::iceberg::{CommitContext, RegisterOutcome};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

fn single_attempt() -> CommitRetry {
    CommitRetry {
        max_attempts: 1,
        backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
    }
}

#[tokio::test]
async fn a_crash_before_the_commit_leaves_no_claim_for_a_later_run() {
    let backend = Arc::new(FakeBackend::default());
    backend.fail_before_apply.store(true, Ordering::SeqCst);
    let registration = FakeTarget::registration(Some(&["s3://raw/a"]), &["s3://out/a"]);

    let crashed = ConditionalCommit::new(FakeTarget::new(backend.clone()), single_attempt())
        .run(&registration, &CommitContext::default())
        .await;
    assert!(crashed.is_err());

    let recovered = ConditionalCommit::new(FakeTarget::new(backend.clone()), FakeTarget::retry())
        .run(&registration, &CommitContext::default())
        .await
        .unwrap();

    assert_eq!(recovered, RegisterOutcome::Applied);
    assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_commit_that_applied_before_a_crash_is_idempotent_on_the_next_run() {
    let backend = Arc::new(FakeBackend::default());
    let registration = FakeTarget::registration(Some(&["s3://raw/a"]), &["s3://out/a"]);

    let first = ConditionalCommit::new(FakeTarget::new(backend.clone()), FakeTarget::retry())
        .run(&registration, &CommitContext::default())
        .await
        .unwrap();
    let restarted = ConditionalCommit::new(FakeTarget::new(backend.clone()), FakeTarget::retry())
        .run(&registration, &CommitContext::default())
        .await
        .unwrap();

    assert_eq!(first, RegisterOutcome::Applied);
    assert_eq!(restarted, RegisterOutcome::AlreadyApplied);
    assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_raw_batch_with_every_output_live_is_already_applied() {
    let backend = Arc::new(FakeBackend::default());
    backend.claim(&[], &["s3://out/a", "s3://out/b"]);
    let registration = FakeTarget::registration(None, &["s3://out/a", "s3://out/b"]);

    let outcome = ConditionalCommit::new(FakeTarget::new(backend.clone()), FakeTarget::retry())
        .run(&registration, &CommitContext::default())
        .await
        .unwrap();

    assert_eq!(outcome, RegisterOutcome::AlreadyApplied);
    assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn compacted_outputs_without_source_claims_are_not_reappended() {
    let backend = Arc::new(FakeBackend::default());
    backend.claim(&[], &["s3://out/a"]);
    let registration = FakeTarget::registration(Some(&["s3://raw/a"]), &["s3://out/a"]);

    let outcome = ConditionalCommit::new(FakeTarget::new(backend.clone()), FakeTarget::retry())
        .run(&registration, &CommitContext::default())
        .await
        .unwrap();

    assert_eq!(
        outcome,
        RegisterOutcome::Conflict(crate::iceberg::ConflictDetails {
            claimed_sources: Vec::new(),
            live_outputs: vec!["s3://out/a".to_string()],
        })
    );
    assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn commit_context_is_available_to_every_outcome() {
    let context = CommitContext::default().with_lease("compact:events:prefix", "owner-a", 7);

    assert_eq!(context.scope(), "compact:events:prefix");
    assert_eq!(context.owner(), "owner-a");
    assert_eq!(context.fencing_token(), 7);
    assert_eq!(CommitContext::default().scope(), "");
    assert_eq!(CommitContext::default().fencing_token(), 0);
}
