use super::*;
use crate::aws_ops::presigned_post::PresignedPost;
use platform_messages::build_request::BuildRequest as BuildRequestPayload;
use platform_messages::sqs_message::{GitSource, SqsRefType};
use std::{collections::BTreeMap, num::NonZeroU64};

fn request(commit_hash: Option<&str>, max_artifact_size_bytes: NonZeroU64) -> BuildRequest {
    BuildRequest {
        source: GitSource::GitHub {
            host: "github.com".to_string(),
            delivery_id: None,
            installation_id: 7,
        },
        repo_id: "42".to_string(),
        ref_name: "main".to_string(),
        ref_type: SqsRefType::Branch,
        commit_hash: commit_hash.map(str::to_string),
        signed_get_url: "https://example.com/source.zip".to_string(),
        signed_post: PresignedPost::for_test(
            "https://example.com/upload".to_string(),
            BTreeMap::from([
                ("key".to_string(), "artifacts/${filename}".to_string()),
                ("policy".to_string(), "policy".to_string()),
            ]),
            max_artifact_size_bytes,
        ),
    }
}

#[test]
fn payload_contains_build_fields() {
    let payload: BuildRequestPayload = serde_json::from_slice(
        &request(Some("abc123"), NonZeroU64::new(1024).unwrap())
            .payload()
            .unwrap(),
    )
    .unwrap();

    assert_eq!(payload.git_host, "github.com");
    assert_eq!(payload.git_provider, "github");
    assert_eq!(payload.repo_id, "42");
    assert_eq!(payload.ref_name, "main");
    assert_eq!(payload.ref_type, SqsRefType::Branch);
    assert_eq!(payload.commit_hash.as_deref(), Some("abc123"));
    assert_eq!(payload.signed_get_url, "https://example.com/source.zip");
    assert_eq!(payload.signed_post.url, "https://example.com/upload");
    assert_eq!(payload.signed_post.fields["key"], "artifacts/${filename}");
    assert_eq!(
        payload.signed_post.max_artifact_size_bytes,
        NonZeroU64::new(1024)
    );
}

#[test]
fn payload_omits_absent_commit_hash_and_always_includes_the_artifact_limit() {
    let payload: serde_json::Value = serde_json::from_slice(
        &request(None, NonZeroU64::new(1024).unwrap())
            .payload()
            .unwrap(),
    )
    .unwrap();

    assert!(payload.get("commit_hash").is_none());
    assert_eq!(payload["signed_post"]["max_artifact_size_bytes"], 1024);
}
