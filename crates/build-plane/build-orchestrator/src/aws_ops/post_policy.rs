//! Presigned POST signing for build artifacts.
//!
//! The SigV4 POST-policy signing itself lives in `platform-aws`; this module
//! wraps it with the build plane's artifact key template and size limit.

use aws_credential_types::Credentials;
use platform_aws::{PresignedPost as BasePost, PresignedPostSigner as SharedSigner};
use platform_errors::BuildResult;
use platform_messages::BuildRequestSignedPost;
use serde_json::json;
use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    time::{Duration, SystemTime},
};

const MIN_OBJECT_SIZE_BYTES: u64 = 0;

pub struct PresignedPost {
    pub url: String,
    pub fields: BTreeMap<String, String>,
    pub(crate) max_artifact_size_bytes: NonZeroU64,
}

impl PresignedPost {
    pub fn max_artifact_size_bytes(&self) -> NonZeroU64 {
        self.max_artifact_size_bytes
    }

    fn from_base_post(post: BasePost, max_artifact_size_bytes: NonZeroU64) -> Self {
        Self {
            url: post.url,
            fields: post.fields,
            max_artifact_size_bytes,
        }
    }
}

pub struct PresignedPostSigner {
    signer: SharedSigner,
    max_artifact_size_bytes: NonZeroU64,
}

impl PresignedPostSigner {
    pub fn new(
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        max_artifact_size_bytes: NonZeroU64,
    ) -> Self {
        Self {
            signer: SharedSigner::new(bucket, region, endpoint_url),
            max_artifact_size_bytes,
        }
    }

    pub fn sign(
        &self,
        credentials: &Credentials,
        key_prefix: &str,
        now: SystemTime,
        expires_in: Duration,
    ) -> BuildResult<PresignedPost> {
        let max_artifact_size_bytes = self.max_artifact_size_bytes.get();
        let post = self.signer.sign(
            credentials,
            &BuildRequestSignedPost::key_template(key_prefix),
            json!(["starts-with", "$key", key_prefix]),
            &[json!([
                "content-length-range",
                MIN_OBJECT_SIZE_BYTES,
                max_artifact_size_bytes
            ])],
            now,
            expires_in,
        )?;
        Ok(PresignedPost::from_base_post(
            post,
            self.max_artifact_size_bytes,
        ))
    }
}
