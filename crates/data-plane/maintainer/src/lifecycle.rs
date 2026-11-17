use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus, LifecycleRule,
    LifecycleRuleFilter, Transition, TransitionStorageClass,
};
use platform_errors::{PlatformError, PlatformResult};

const RAW_TIERING_RULE: &str = "archive-raw-intelligent-tiering";
const ABORT_UPLOAD_RULE: &str = "archive-abort-incomplete-uploads";
const TIERING_DAYS: i32 = 30;
const ABORT_DAYS: i32 = 7;

/// Installs the baseline S3 lifecycle policy for the archive bucket.
///
/// The policy is a pure backstop: it transitions eligible raw objects to
/// Intelligent-Tiering and aborts incomplete multipart uploads. It never
/// expires any archive prefix, so the maintenance runner stays the only
/// deletion path for raw objects.
pub struct LifecycleBackstop {
    client: S3Client,
    bucket: String,
}

impl LifecycleBackstop {
    pub fn new(client: S3Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
    }

    pub async fn apply(&self) -> PlatformResult<()> {
        self.client
            .put_bucket_lifecycle_configuration()
            .bucket(&self.bucket)
            .lifecycle_configuration(self.configuration()?)
            .send()
            .await
            .map_err(|error| {
                PlatformError::s3(
                    "install the archive S3 lifecycle backstop",
                    &self.bucket,
                    error,
                )
            })?;
        Ok(())
    }

    fn configuration(&self) -> PlatformResult<BucketLifecycleConfiguration> {
        let tiering = LifecycleRule::builder()
            .id(RAW_TIERING_RULE)
            .filter(LifecycleRuleFilter::builder().prefix("raw/").build())
            .status(ExpirationStatus::Enabled)
            .transitions(
                Transition::builder()
                    .days(TIERING_DAYS)
                    .storage_class(TransitionStorageClass::IntelligentTiering)
                    .build(),
            )
            .build()
            .map_err(|error| {
                PlatformError::config_invalid_source("S3 lifecycle tiering rule", error)
            })?;
        let abort = LifecycleRule::builder()
            .id(ABORT_UPLOAD_RULE)
            .filter(LifecycleRuleFilter::builder().build())
            .status(ExpirationStatus::Enabled)
            .abort_incomplete_multipart_upload(
                AbortIncompleteMultipartUpload::builder()
                    .days_after_initiation(ABORT_DAYS)
                    .build(),
            )
            .build()
            .map_err(|error| {
                PlatformError::config_invalid_source("S3 lifecycle abort rule", error)
            })?;
        BucketLifecycleConfiguration::builder()
            .rules(tiering)
            .rules(abort)
            .build()
            .map_err(|error| {
                PlatformError::config_invalid_source("S3 lifecycle configuration", error)
            })
    }
}
