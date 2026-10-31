use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use platform_errors::{BpError, StorageError};
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::time::Duration;
use platform_errors::ErrorType;
use serde_json::json;

fn signed_post(policy: Option<&str>, max_artifact_size_bytes: Option<NonZeroU64>) -> SignedPost {
    let mut fields = BTreeMap::from([("key".to_string(), "artifacts/${filename}".to_string())]);
    if let Some(policy) = policy {
        fields.insert(POLICY_FIELD.to_string(), policy.to_string());
    }
    let signed_post = SignedPost::new(
        "https://example.com/upload".to_string(),
        fields,
        Duration::from_secs(1),
    )
    .unwrap();
    match max_artifact_size_bytes {
        Some(max_bytes) => signed_post.with_max_artifact_size(max_bytes),
        None => signed_post,
    }
}

fn encoded_policy(max_bytes: u64) -> String {
    let policy = json!({
        "conditions": [
            { "bucket": "artifacts" },
            ["content-length-range", 0, max_bytes],
        ],
    });
    BASE64.encode(serde_json::to_vec(&policy).unwrap())
}

#[test]
fn reads_artifact_limit_from_signed_post_policy() {
    let signed_post = signed_post(Some(&encoded_policy(1024)), None);

    assert_eq!(signed_post.max_artifact_size_bytes, NonZeroU64::new(1024));
}

#[test]
fn explicit_artifact_limit_overrides_policy() {
    let signed_post = signed_post(
        Some(&encoded_policy(1024)),
        Some(NonZeroU64::new(2048).unwrap()),
    );

    assert_eq!(signed_post.max_artifact_size_bytes, NonZeroU64::new(2048));
}

#[test]
fn ignores_zero_limits() {
    let signed_post = signed_post(Some(&encoded_policy(0)), None);

    assert_eq!(signed_post.max_artifact_size_bytes, None);
}

#[tokio::test]
async fn rejects_oversized_artifacts_before_uploading() {
    let directory = tempfile::tempdir().unwrap();
    let artifact = directory.path().join("runner.cwasm");
    tokio::fs::write(&artifact, b"too large").await.unwrap();

    let error = signed_post(None, Some(NonZeroU64::new(4).unwrap()))
        .upload(&artifact)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BpError::Storage(StorageError::ArtifactTooLarge { .. })
    ));
    assert_eq!(error.error_type(), "artifact_too_large");
    assert!(error.to_string().contains("maximum artifact size"));
}
