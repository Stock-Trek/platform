//! The payload sent from the orchestrator to the build agent Lambda.
//!
//! The payload is the Lambda invocation event, so it contains everything that
//! changes per build. Process-wide settings remain in the agent's environment.

use crate::sqs_message::SqsRefType;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, num::NonZeroU64};

/// The form field in an S3 pre-signed POST that selects the object key.
pub const KEY_FIELD: &str = "key";

/// The S3 variable that a pre-signed POST replaces with the uploaded file name.
pub const FILENAME_VARIABLE: &str = "${filename}";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildRequest {
    pub git_host: String,
    pub git_provider: String,
    pub repo_id: String,
    pub ref_name: String,
    pub ref_type: SqsRefType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_hash: Option<String>,
    pub signed_get_url: String,
    pub signed_post: BuildRequestSignedPost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildRequestSignedPost {
    pub url: String,
    pub fields: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_artifact_size_bytes: Option<NonZeroU64>,
}

impl BuildRequestSignedPost {
    /// Builds the object key template for `key_prefix`.
    ///
    /// S3 replaces the trailing [`FILENAME_VARIABLE`] with the uploaded file's
    /// name, allowing one signed POST to upload several artifacts without them
    /// overwriting each other.
    pub fn key_template(key_prefix: &str) -> String {
        format!("{key_prefix}{FILENAME_VARIABLE}")
    }

    /// Returns the key prefix from the signed POST's key field.
    ///
    /// Returns `None` when the fields do not include a key or the key does not
    /// contain [`FILENAME_VARIABLE`].
    pub fn key_prefix(&self) -> Option<&str> {
        let key = self.fields.get(KEY_FIELD)?;
        key.find(FILENAME_VARIABLE).map(|position| &key[..position])
    }

    /// Replaces [`FILENAME_VARIABLE`] in a form field value with `filename`.
    pub fn field_value(value: &str, filename: &str) -> String {
        value.replace(FILENAME_VARIABLE, filename)
    }
}

/// The outcome of a build agent invocation.
///
/// This is the value returned by the agent's Lambda handler. The orchestrator
/// invokes the agent asynchronously, so it receives only the invocation
/// acknowledgement and never this payload. Consequently, an `Ok` result (even
/// for a permanent failure such as a user compile error) marks the asynchronous
/// invocation as successful and prevents Lambda from retrying it. Transient
/// failures are returned as `Err`, which marks the invocation as failed so the
/// Lambda retry policy can retry the build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildOutcome {
    pub status: BuildStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<BuildFailure>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildStatus {
    #[serde(rename = "ok")]
    Ok,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildFailure {
    pub error_type: String,
    pub message: String,
    pub retryable: bool,
}

impl BuildOutcome {
    pub fn success() -> Self {
        Self {
            status: BuildStatus::Ok,
            error: None,
        }
    }

    pub fn failure(
        error_type: impl Into<String>,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            status: BuildStatus::Failed,
            error: Some(BuildFailure {
                error_type: error_type.into(),
                message: message.into(),
                retryable,
            }),
        }
    }

    pub fn is_success(&self) -> bool {
        self.status == BuildStatus::Ok
    }

    pub fn is_failure(&self) -> bool {
        self.status == BuildStatus::Failed
    }

    pub fn error(&self) -> Option<&BuildFailure> {
        self.error.as_ref()
    }
}

#[cfg(test)]
mod tests {
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
}
