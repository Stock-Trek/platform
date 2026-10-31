//! Upload build artifacts to S3 using a pre-signed POST policy.
//!
//! The orchestrator sends a short-lived pre-signed POST in the build request
//! so the agent can write its artifacts without any long-lived AWS credentials.
//! The url and form fields are the `signed_post.url` and `signed_post.fields`
//! properties of the Lambda invocation payload. The fields hold the policy
//! values that must be sent alongside the file (for example `key`, `policy`,
//! `x-amz-signature`, ...).
//!
//! A single pre-signed POST can upload more than one artifact when the `key`
//! field contains the `${filename}` variable, which S3 replaces with the name
//! of the uploaded file.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use platform_errors::{BpError, BpResult, ConfigError, StorageError};
use platform_messages::build_request::{BuildRequestSignedPost, FILENAME_VARIABLE, KEY_FIELD};
use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    path::{Component, Path},
    time::Duration,
};
use tracing::info;

/// The form field that holds the object body in an S3 pre-signed POST.
pub const FILE_FIELD: &str = "file";

/// The form field holding the base64-encoded POST policy.
const POLICY_FIELD: &str = "policy";

/// A pre-signed POST target for uploading build artifacts.
#[derive(Debug, Clone)]
pub struct SignedPost {
    url: String,
    fields: BTreeMap<String, String>,
    max_artifact_size_bytes: Option<NonZeroU64>,
    client: reqwest::Client,
}

impl SignedPost {
    pub fn from_build_request(
        payload: &BuildRequestSignedPost,
        timeout: Duration,
    ) -> BpResult<Self> {
        let signed_post = Self::new(payload.url.clone(), payload.fields.clone(), timeout)?;
        Ok(match payload.max_artifact_size_bytes {
            Some(max_bytes) => signed_post.with_max_artifact_size(max_bytes),
            None => signed_post,
        })
    }

    pub fn new(url: String, fields: BTreeMap<String, String>, timeout: Duration) -> BpResult<Self> {
        let key = fields.get(KEY_FIELD).ok_or_else(|| {
            BpError::Config(ConfigError::Build(format!(
                "signed_post.fields must include a {KEY_FIELD} field containing {FILENAME_VARIABLE}"
            )))
        })?;
        if !key.contains(FILENAME_VARIABLE) {
            return Err(BpError::Config(ConfigError::Build(format!(
                "signed_post.fields.{KEY_FIELD} must contain {FILENAME_VARIABLE} so artifacts do not overwrite each other"
            ))));
        }

        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|source| {
                BpError::Config(ConfigError::Build(format!(
                    "failed to build HTTP client: {source}"
                )))
            })?;

        let max_artifact_size_bytes = fields
            .get(POLICY_FIELD)
            .and_then(|policy| Self::max_artifact_size_from_policy(policy));

        Ok(Self {
            url,
            fields,
            max_artifact_size_bytes,
            client,
        })
    }

    /// Override the artifact size limit advertised by the signed POST policy.
    pub fn with_max_artifact_size(mut self, max_bytes: NonZeroU64) -> Self {
        self.max_artifact_size_bytes = Some(max_bytes);
        self
    }

    fn max_artifact_size_from_policy(policy: &str) -> Option<NonZeroU64> {
        let policy = BASE64.decode(policy).ok()?;
        let policy: serde_json::Value = serde_json::from_slice(&policy).ok()?;
        policy
            .get("conditions")?
            .as_array()?
            .iter()
            .find_map(|condition| {
                let condition = condition.as_array()?;
                if condition.first()?.as_str()? != "content-length-range" {
                    return None;
                }
                condition.get(2)?.as_u64().and_then(NonZeroU64::new)
            })
    }

    /// Upload `path` using the configured pre-signed POST.
    pub async fn upload(&self, path: &Path) -> BpResult<()> {
        let file_name = Self::file_name(path)?;
        self.upload_named(path, Path::new(&file_name)).await
    }

    /// Upload `path` using `name` as the S3 object name.
    pub async fn upload_named(&self, path: &Path, name: &Path) -> BpResult<()> {
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|source| BpError::io("read artifact", path.display().to_string(), source))?;
        let size = metadata.len();
        self.check_artifact_size(path, size)?;
        let file_name = Self::upload_name(name)?;

        // Stream the file from disk instead of buffering the whole artifact in
        // memory. `Part::file` uses the file's length for the multipart
        // content length, and the size is checked above before any bytes are
        // sent.
        let file = reqwest::multipart::Part::file(path)
            .await
            .map_err(|source| BpError::io("read artifact", path.display().to_string(), source))?
            .file_name(file_name.clone())
            .mime_str("application/octet-stream")
            .map_err(|source| {
                BpError::http_request("build upload request", path.display().to_string(), source)
            })?;

        // S3 requires the file to be the last field in the multipart body.
        let mut form = reqwest::multipart::Form::new();
        for (name, value) in &self.fields {
            form = form.text(
                name.clone(),
                BuildRequestSignedPost::field_value(value, &file_name),
            );
        }
        form = form.part(FILE_FIELD, file);

        let response = self
            .client
            .post(&self.url)
            .multipart(form)
            .send()
            .await
            .map_err(|source| {
                BpError::http_request("upload artifact", path.display().to_string(), source)
            })?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.map_err(|source| {
                BpError::http_request(
                    "read upload failure response",
                    path.display().to_string(),
                    source,
                )
            })?;
            if let Some(max_bytes) = self.max_artifact_size_bytes
                && Self::is_entity_too_large(&body)
            {
                return Err(BpError::Storage(StorageError::ArtifactTooLarge {
                    artifact: path.display().to_string(),
                    size: usize::try_from(size).unwrap_or(usize::MAX),
                    max_bytes: max_bytes.get(),
                }));
            }
            return Err(BpError::http_rejected(
                "upload artifact",
                path.display().to_string(),
                status.as_u16(),
                body,
            ));
        }

        info!(artifact = %path.display(), object = %file_name, "uploaded build artifact");
        Ok(())
    }

    fn check_artifact_size(&self, path: &Path, size: u64) -> BpResult<()> {
        if let Some(max_bytes) = self.max_artifact_size_bytes
            && size > max_bytes.get()
        {
            return Err(BpError::Storage(StorageError::ArtifactTooLarge {
                artifact: path.display().to_string(),
                size: usize::try_from(size).unwrap_or(usize::MAX),
                max_bytes: max_bytes.get(),
            }));
        }
        Ok(())
    }

    fn is_entity_too_large(body: &str) -> bool {
        body.contains("EntityTooLarge") || body.contains("content-length-range")
    }

    fn file_name(path: &Path) -> BpResult<String> {
        path.file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
            .ok_or_else(|| {
                BpError::Config(ConfigError::Build(format!(
                    "artifact path {path:?} has no file name"
                )))
            })
    }

    fn upload_name(path: &Path) -> BpResult<String> {
        let mut parts = Vec::new();
        for component in path.components() {
            match component {
                Component::Normal(part) => {
                    let part = part.to_str().ok_or_else(|| {
                        BpError::Config(ConfigError::Build(format!(
                            "artifact name {path:?} is not valid UTF-8"
                        )))
                    })?;
                    parts.push(part);
                }
                _ => {
                    return Err(BpError::Config(ConfigError::Build(format!(
                        "artifact name {path:?} must be relative"
                    ))));
                }
            }
        }

        if parts.is_empty() {
            return Err(BpError::Config(ConfigError::Build(format!(
                "artifact name {path:?} is empty"
            ))));
        }
        Ok(parts.join("/"))
    }
}

#[cfg(test)]
mod tests;
