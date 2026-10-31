use super::*;
use platform_messages::{build_request::BuildRequestSignedPost, sqs_message::SqsRefType};
use std::collections::BTreeMap;

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
