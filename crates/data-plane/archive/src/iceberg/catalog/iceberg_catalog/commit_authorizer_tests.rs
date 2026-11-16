use super::commit_algorithm::{CommitRetry, ConditionalCommit};
use super::commit_fake_tests::{FakeBackend, FakeTarget};
use crate::iceberg::CommitContext;
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};
use platform_lease::CommitAuthorizer;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingAuthorizer {
    calls: AtomicUsize,
    fail_after: Option<usize>,
    deadline: Option<std::time::Duration>,
}

impl CountingAuthorizer {
    fn new(fail_after: Option<usize>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            fail_after,
            deadline: Some(std::time::Duration::from_secs(30)),
        }
    }
}

#[async_trait]
impl CommitAuthorizer for CountingAuthorizer {
    async fn authorize(&self) -> PlatformResult<()> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_after.is_some_and(|limit| call > limit) {
            return Err(PlatformError::external("authorization failed"));
        }
        Ok(())
    }

    fn remaining(&self) -> Option<std::time::Duration> {
        self.deadline
    }
}

#[tokio::test]
async fn authorization_runs_before_each_submit_and_after_backoff() {
    let backend = Arc::new(FakeBackend::default());
    backend.fail_before_apply.store(true, Ordering::SeqCst);
    let target = FakeTarget::new(backend.clone());
    let authorizer = Arc::new(CountingAuthorizer::new(None));
    let context = CommitContext::default().with_authorizer(authorizer.clone());
    let registration = FakeTarget::registration(None, &["s3://archive/out/a.parquet"]);
    let commit = ConditionalCommit::new(target, FakeTarget::retry());

    let outcome = commit.run(&registration, &context).await.unwrap();

    assert!(outcome.is_success());
    assert_eq!(authorizer.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn a_failed_authorization_prevents_every_submit() {
    let backend = Arc::new(FakeBackend::default());
    let target = FakeTarget::new(backend.clone());
    let authorizer = Arc::new(CountingAuthorizer::new(Some(0)));
    let context = CommitContext::default().with_authorizer(authorizer);
    let registration = FakeTarget::registration(None, &["s3://archive/out/a.parquet"]);
    let commit = ConditionalCommit::new(
        target,
        CommitRetry {
            max_attempts: 3,
            backoff: std::time::Duration::ZERO,
            max_backoff: std::time::Duration::ZERO,
        },
    );

    assert!(commit.run(&registration, &context).await.is_err());
    assert_eq!(backend.commits.load(Ordering::SeqCst), 0);
}
