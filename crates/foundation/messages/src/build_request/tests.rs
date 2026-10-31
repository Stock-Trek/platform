use super::*;
use serde_json::json;

fn request() -> BuildRequest {
    BuildRequest {
        git_host: "github.com".to_string(),
        git_provider: "github".to_string(),
        repo_id: "42".to_string(),
        ref_name: "main".to_string(),
        ref_type: SqsRefType::Branch,
        commit_hash: Some("0123456789abcdef".to_string()),
        signed_get_url: "https://example.com/source.zip".to_string(),
        signed_post: BuildRequestSignedPost {
            url: "https://example.com/upload".to_string(),
            fields: BTreeMap::from([
                (KEY_FIELD.to_string(), "artifacts/${filename}".to_string()),
                ("policy".to_string(), "policy".to_string()),
            ]),
            max_artifact_size_bytes: Some(NonZeroU64::new(1024).unwrap()),
        },
    }
}

#[test]
fn round_trips_through_the_lambda_payload() {
    let request = request();
    let payload = serde_json::to_vec(&request).unwrap();

    assert_eq!(
        serde_json::from_slice::<BuildRequest>(&payload).unwrap(),
        request
    );
}

#[test]
fn uses_plain_json_field_names_and_a_nested_signed_post() {
    let payload = serde_json::to_value(request()).unwrap();

    assert_eq!(payload["ref_type"], "branch");
    assert_eq!(payload["signed_get_url"], "https://example.com/source.zip");
    assert_eq!(payload["signed_post"]["url"], "https://example.com/upload");
    assert_eq!(
        payload["signed_post"]["fields"]["key"],
        "artifacts/${filename}"
    );
    assert_eq!(payload["signed_post"]["max_artifact_size_bytes"], 1024);
    assert!(payload.get("SIGNED_GET_URL").is_none());
}

#[test]
fn shares_the_signed_post_filename_contract() {
    let signed_post = request().signed_post;

    assert_eq!(signed_post.key_prefix(), Some("artifacts/"));
    assert_eq!(
        BuildRequestSignedPost::key_template("artifacts/"),
        signed_post.fields[KEY_FIELD]
    );
    assert_eq!(
        BuildRequestSignedPost::field_value(&signed_post.fields[KEY_FIELD], "runner.cwasm"),
        "artifacts/runner.cwasm"
    );

    let without_filename = BuildRequestSignedPost {
        fields: BTreeMap::from([(KEY_FIELD.to_string(), "artifacts/runner.cwasm".to_string())]),
        ..signed_post
    };
    assert_eq!(without_filename.key_prefix(), None);
}

#[test]
fn rejects_a_zero_artifact_limit() {
    let payload = json!({
        "url": "https://example.com/upload",
        "fields": { "key": "artifacts/${filename}" },
        "max_artifact_size_bytes": 0,
    });

    assert!(serde_json::from_value::<BuildRequestSignedPost>(payload).is_err());
}

#[test]
fn optional_fields_may_be_absent() {
    let payload = json!({
        "git_host": "github.com",
        "git_provider": "github",
        "repo_id": "42",
        "ref_name": "main",
        "ref_type": "tag",
        "signed_get_url": "https://example.com/source.zip",
        "signed_post": {
            "url": "https://example.com/upload",
            "fields": { "key": "artifacts/${filename}" },
        },
    });
    let request: BuildRequest = serde_json::from_value(payload).unwrap();

    assert_eq!(request.commit_hash, None);
    assert_eq!(request.signed_post.max_artifact_size_bytes, None);
}

#[test]
fn omits_absent_optional_fields_when_serialising() {
    let mut request = request();
    request.commit_hash = None;
    request.signed_post.max_artifact_size_bytes = None;
    let payload = serde_json::to_value(request).unwrap();

    assert!(payload.get("commit_hash").is_none());
    assert!(
        payload["signed_post"]
            .get("max_artifact_size_bytes")
            .is_none()
    );
}

#[test]
fn success_outcome_keeps_the_existing_status_payload() {
    let payload = serde_json::to_value(BuildOutcome::success()).unwrap();

    assert_eq!(payload, json!({ "status": "ok" }));
}

#[test]
fn failure_outcome_contains_structured_error_details() {
    let outcome = BuildOutcome::failure("command_failed", "compile failed", false);
    let payload = serde_json::to_value(&outcome).unwrap();

    assert_eq!(payload["status"], "failed");
    assert_eq!(payload["error"]["error_type"], "command_failed");
    assert_eq!(payload["error"]["message"], "compile failed");
    assert_eq!(payload["error"]["retryable"], false);
    assert!(outcome.is_failure());
    assert!(!outcome.is_success());
    assert_eq!(outcome.error().unwrap().error_type, "command_failed");
    assert_eq!(
        serde_json::from_value::<BuildOutcome>(payload).unwrap(),
        outcome
    );
}
