use crate::aws_ops::presigned_post::PresignedPostSigner;
use aws_credential_types::{
    Credentials,
    provider::{ProvideCredentials, SharedCredentialsProvider},
};
use aws_sdk_s3::{Client as S3Client, presigning::PresigningConfig, primitives::ByteStream};
use platform_aws::SdkConfig;
use platform_config::env_names::S3_SIGNED_URL_EXPIRY_SECS;
use platform_errors::{BpError, BpResult, StorageError, TimeoutError};
use platform_time::{deadline::Deadline, timeouts::Timeouts};
use std::{num::NonZeroU64, time::Duration};

pub use crate::aws_ops::presigned_post::PresignedPost;

#[derive(Debug, Clone)]
pub struct S3Config {
    pub user_artifacts_bucket: String,
    pub max_source_archive_size_bytes: NonZeroU64,
    pub max_artifact_size_bytes: NonZeroU64,
    pub signed_url_expiry: Duration,
    pub endpoint_url: Option<String>,
}

pub struct UploadedObject {
    pub signed_get_url: String,
}

pub struct S3 {
    client: S3Client,
    signer: PresignedPostSigner,
    credentials_provider: SharedCredentialsProvider,
    timeouts: Timeouts,
    config: S3Config,
}

impl S3 {
    fn new(
        client: S3Client,
        signer: PresignedPostSigner,
        credentials_provider: SharedCredentialsProvider,
        timeouts: Timeouts,
        config: S3Config,
    ) -> Self {
        Self {
            client,
            signer,
            credentials_provider,
            timeouts,
            config,
        }
    }

    pub fn from_sdk_config(
        sdk_config: &SdkConfig,
        timeouts: Timeouts,
        config: S3Config,
    ) -> BpResult<Self> {
        let credentials_provider = sdk_config
            .credentials_provider()
            .ok_or(BpError::Storage(StorageError::MissingCredentials))?;
        let region = sdk_config
            .region()
            .map(|region| region.as_ref().to_string())
            .unwrap_or_else(|| "us-east-1".to_string());
        let endpoint_url = config
            .endpoint_url
            .clone()
            .or_else(|| sdk_config.endpoint_url().map(str::to_string));
        let client = match endpoint_url.as_deref() {
            Some(endpoint_url) => {
                platform_aws::Aws::s3_client_with_path_style_endpoint(sdk_config, endpoint_url)
            }
            None => platform_aws::Aws::s3_client(sdk_config),
        };
        let signer = PresignedPostSigner::new(
            config.user_artifacts_bucket.clone(),
            region,
            endpoint_url,
            config.max_artifact_size_bytes,
        );
        Ok(Self::new(
            client,
            signer,
            credentials_provider,
            timeouts,
            config,
        ))
    }

    pub async fn signed_post(
        &self,
        key_prefix: &str,
        deadline: Deadline,
    ) -> BpResult<PresignedPost> {
        let credentials = self.credentials(deadline).await?;
        self.signer.sign(
            &credentials,
            key_prefix,
            std::time::SystemTime::now(),
            self.config.signed_url_expiry,
        )
    }

    pub async fn put_object(
        &self,
        key: &str,
        contents: Vec<u8>,
        deadline: Deadline,
    ) -> BpResult<UploadedObject> {
        let max_source_archive_size_bytes = self.config.max_source_archive_size_bytes.get();
        if contents.len() as u64 > max_source_archive_size_bytes {
            return Err(BpError::Storage(StorageError::ObjectTooLarge {
                key: key.to_owned(),
                size: contents.len(),
                max_bytes: max_source_archive_size_bytes,
            }));
        }
        let target = format!("s3://{}/{key}", self.config.user_artifacts_bucket);
        let timeout = self.timeouts.operation_timeout(deadline)?;
        tokio::time::timeout(
            timeout,
            self.client
                .put_object()
                .bucket(&self.config.user_artifacts_bucket)
                .key(key)
                .content_type("application/zip")
                .body(ByteStream::from(contents))
                .send(),
        )
        .await
        .map_err(|_| {
            BpError::Timeout(TimeoutError::Operation {
                operation: "S3 upload object",
            })
        })?
        .map_err(|error| BpError::s3("upload object", target, error))?;
        let signed_get_url = self.signed_get_url(key, deadline).await?;
        Ok(UploadedObject { signed_get_url })
    }

    async fn signed_get_url(&self, key: &str, deadline: Deadline) -> BpResult<String> {
        let target = format!("s3://{}/{key}", self.config.user_artifacts_bucket);
        let presigning_config = PresigningConfig::expires_in(self.config.signed_url_expiry)
            .map_err(|error| BpError::config_invalid_source(S3_SIGNED_URL_EXPIRY_SECS, error))?;
        let timeout = self.timeouts.operation_timeout(deadline)?;
        let request = tokio::time::timeout(
            timeout,
            self.client
                .get_object()
                .bucket(&self.config.user_artifacts_bucket)
                .key(key)
                .presigned(presigning_config),
        )
        .await
        .map_err(|_| {
            BpError::Timeout(TimeoutError::Operation {
                operation: "S3 presign object download",
            })
        })?
        .map_err(|error| BpError::s3("presign object download", target, error))?;
        Ok(request.uri().to_string())
    }

    async fn credentials(&self, deadline: Deadline) -> BpResult<Credentials> {
        let timeout = self.timeouts.operation_timeout(deadline)?;
        tokio::time::timeout(timeout, self.credentials_provider.provide_credentials())
            .await
            .map_err(|_| {
                BpError::Timeout(TimeoutError::Operation {
                    operation: "S3 provide_credentials",
                })
            })?
            .map_err(|error| BpError::Storage(StorageError::Credentials(error)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_credential_types::Credentials;
    use platform_aws::Region;
    use platform_time::timeouts::Timeout;

    fn s3_client() -> (S3, Credentials) {
        let credentials = Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("token/with+chars=".to_string()),
            None,
            "test",
        );
        let sdk_config = SdkConfig::builder()
            .region(Region::new("us-east-1"))
            .credentials_provider(SharedCredentialsProvider::new(credentials.clone()))
            .build();
        let s3 = S3::from_sdk_config(
            &sdk_config,
            Timeouts {
                connect: Timeout::Disabled,
                operation: Timeout::Disabled,
            },
            S3Config {
                user_artifacts_bucket: "stock-trek-user-artifacts".to_string(),
                max_source_archive_size_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
                max_artifact_size_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
                signed_url_expiry: Duration::from_secs(900),
                endpoint_url: None,
            },
        )
        .unwrap();
        (s3, credentials)
    }

    #[tokio::test]
    async fn presigns_get_url_with_sdk() {
        let (s3, _) = s3_client();
        let url = s3
            .signed_get_url(
                "github/github.com/42/7/heads/main/abc123/source/algorithms.zip",
                Deadline::new(
                    std::time::SystemTime::now() + Duration::from_secs(30),
                    Duration::ZERO,
                ),
            )
            .await
            .unwrap();

        assert!(url.starts_with(concat!(
            "https://stock-trek-user-artifacts.s3.us-east-1.amazonaws.com/",
            "github/github.com/42/7/heads/main/abc123/source/algorithms.zip?"
        )));
        assert!(url.contains("X-Amz-Expires=900"));
        assert!(url.contains("X-Amz-Signature="));
    }
}
