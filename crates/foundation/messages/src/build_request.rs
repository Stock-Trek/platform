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
mod tests;
