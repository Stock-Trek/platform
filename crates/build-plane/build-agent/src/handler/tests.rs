use super::*;
use crate::source::SourceLimits;
use platform_errors::{BpError, ConfigError, ErrorType, TimeoutError};
use platform_messages::build_request::BuildRequest;
use platform_messages::{build_request::BuildRequestSignedPost, sqs_message::SqsRefType};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn request(signed_post: BuildRequestSignedPost) -> BuildRequest {
    BuildRequest {
        git_host: "github.com".to_string(),
        git_provider: "github".to_string(),
        repo_id: "42".to_string(),
        ref_name: "main".to_string(),
        ref_type: SqsRefType::Branch,
        commit_hash: None,
        signed_get_url: "https://example.com/source.zip".to_string(),
        signed_post,
    }
}

fn handler(timeout: Duration, failure_publish_budget: Duration) -> Handler {
    Handler::with_source_limits(
        timeout,
        failure_publish_budget,
        PathBuf::from("/unused-source"),
        PathBuf::from("/unused-cache"),
        SourceLimits::default(),
    )
}

#[test]
fn permanent_build_errors_are_returned_as_structured_outcomes() {
    let error = BpError::Config(ConfigError::Build("compile failed".to_string()));

    let outcome = BuildAgent::handle_error(error, Instant::now() - Duration::from_secs(1)).unwrap();

    assert!(outcome.is_failure());
    let error = outcome.error().unwrap();
    assert_eq!(error.error_type, "build");
    assert!(error.message.contains("compile failed"));
    assert!(!error.retryable);
}

#[test]
fn retryable_build_errors_are_returned_so_lambda_retries_the_invocation() {
    let error = BpError::Timeout(TimeoutError::BuildTimeout(Duration::from_secs(300)));

    let error =
        BuildAgent::handle_error(error, Instant::now() - Duration::from_secs(1)).unwrap_err();

    assert_eq!(error.error_type(), "build_timeout");
    assert!(error.retryability().is_retryable());
}

#[tokio::test]
async fn handle_rejects_invalid_signed_post_before_building() {
    let signed_post = BuildRequestSignedPost {
        url: "https://example.com/upload".to_string(),
        fields: BTreeMap::new(),
        max_artifact_size_bytes: None,
    };

    let timeout = Duration::from_secs(1);
    let failure_publish_budget = Duration::from_secs(30);
    let deadline = Instant::now() + timeout + failure_publish_budget;
    let error = handler(timeout, failure_publish_budget)
        .handle(request(signed_post), deadline)
        .await
        .unwrap_err();

    assert!(matches!(error, BpError::Config(_)));
}

#[test]
fn failure_publication_budget_is_reserved_from_the_build_budget() {
    let remaining = Duration::from_secs(40);
    let failure_publish_budget = Duration::from_secs(30);
    let failure_budget = failure_publish_budget.min(remaining / 4);
    let build_budget = Duration::from_secs(300).min(remaining.saturating_sub(failure_budget));

    assert_eq!(failure_budget, Duration::from_secs(10));
    assert_eq!(build_budget, Duration::from_secs(30));
}
