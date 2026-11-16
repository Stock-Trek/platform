use super::commit_algorithm::ConditionalCommit;
use super::commit_fake_tests::{FakeBackend, FakeTarget};
use crate::iceberg::{CommitContext, RegisterOutcome};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::Barrier;

#[tokio::test]
async fn exact_match_is_already_applied_without_another_commit() {
    let backend = Arc::new(FakeBackend::default());
    backend.claim(&["s3://raw/a"], &["s3://out/a"]);
    let registration = FakeTarget::registration(Some(&["s3://raw/a"]), &["s3://out/a"]);
    let target = FakeTarget::new(backend.clone());

    let outcome = ConditionalCommit::new(target, FakeTarget::retry())
        .run(&registration, &CommitContext::default())
        .await
        .unwrap();

    assert_eq!(outcome, RegisterOutcome::AlreadyApplied);
    assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn partial_source_overlap_returns_the_overlapping_sources() {
    let backend = Arc::new(FakeBackend::default());
    backend.claim(&["s3://raw/a"], &[]);
    let registration =
        FakeTarget::registration(Some(&["s3://raw/a", "s3://raw/b"]), &["s3://out/a"]);
    let target = FakeTarget::new(backend);

    let outcome = ConditionalCommit::new(target, FakeTarget::retry())
        .run(&registration, &CommitContext::default())
        .await
        .unwrap();

    assert_eq!(
        outcome,
        RegisterOutcome::Conflict(crate::iceberg::ConflictDetails {
            claimed_sources: vec!["s3://raw/a".to_string()],
            live_outputs: Vec::new(),
        })
    );
}

#[tokio::test]
async fn partial_output_overlap_returns_the_live_outputs() {
    let backend = Arc::new(FakeBackend::default());
    backend.claim(&[], &["s3://out/a"]);
    let registration = FakeTarget::registration(None, &["s3://out/a", "s3://out/b"]);
    let target = FakeTarget::new(backend);

    let outcome = ConditionalCommit::new(target, FakeTarget::retry())
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
}

#[tokio::test]
async fn an_ambiguous_lost_response_is_resolved_as_already_applied() {
    let backend = Arc::new(FakeBackend::default());
    backend.after_apply_error.store(true, Ordering::SeqCst);
    let registration = FakeTarget::registration(Some(&["s3://raw/a"]), &["s3://out/a"]);
    let target = FakeTarget::new(backend.clone());

    let outcome = ConditionalCommit::new(target, FakeTarget::retry())
        .run(&registration, &CommitContext::default())
        .await
        .unwrap();

    assert_eq!(outcome, RegisterOutcome::AlreadyApplied);
    assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_transport_failure_before_apply_retries_the_new_state() {
    let backend = Arc::new(FakeBackend::default());
    backend.fail_before_apply.store(true, Ordering::SeqCst);
    let registration = FakeTarget::registration(Some(&["s3://raw/a"]), &["s3://out/a"]);
    let target = FakeTarget::new(backend.clone());

    let outcome = ConditionalCommit::new(target, FakeTarget::retry())
        .run(&registration, &CommitContext::default())
        .await
        .unwrap();

    assert_eq!(outcome, RegisterOutcome::Applied);
    assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_writer_that_checks_after_a_commit_conflicts() {
    let backend = Arc::new(FakeBackend::default());
    let first_registration = FakeTarget::registration(Some(&["s3://raw/a"]), &["s3://out/a"]);
    let second_registration =
        FakeTarget::registration(Some(&["s3://raw/a", "s3://raw/b"]), &["s3://out/b"]);
    let first = ConditionalCommit::new(FakeTarget::new(backend.clone()), FakeTarget::retry())
        .run(&first_registration, &CommitContext::default())
        .await
        .unwrap();
    let second = ConditionalCommit::new(FakeTarget::new(backend.clone()), FakeTarget::retry())
        .run(&second_registration, &CommitContext::default())
        .await
        .unwrap();

    assert_eq!(first, RegisterOutcome::Applied);
    assert_eq!(
        second,
        RegisterOutcome::Conflict(crate::iceberg::ConflictDetails {
            claimed_sources: vec!["s3://raw/a".to_string()],
            live_outputs: Vec::new(),
        })
    );
    assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn writers_that_check_before_either_commits_produce_one_winner() {
    let backend = Arc::new(FakeBackend::default());
    let barrier = Arc::new(Barrier::new(2));
    let first_registration = FakeTarget::registration(Some(&["s3://raw/a"]), &["s3://out/a"]);
    let second_registration =
        FakeTarget::registration(Some(&["s3://raw/a", "s3://raw/b"]), &["s3://out/b"]);
    let first = ConditionalCommit::new(
        FakeTarget::with_barrier(backend.clone(), barrier.clone()),
        FakeTarget::retry(),
    );
    let second = ConditionalCommit::new(
        FakeTarget::waiting_for_first_commit(backend.clone(), barrier),
        FakeTarget::retry(),
    );

    let first = tokio::spawn(async move {
        first
            .run(&first_registration, &CommitContext::default())
            .await
    });
    let second = tokio::spawn(async move {
        second
            .run(&second_registration, &CommitContext::default())
            .await
    });
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    let outcomes = [first, second];

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, RegisterOutcome::Applied))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, RegisterOutcome::Conflict(_)))
            .count(),
        1
    );
    assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
}
