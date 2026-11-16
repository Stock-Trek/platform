use async_trait::async_trait;
use platform_archive::{Bucket, ObjectKey, S3, S3ObjectPage};
use platform_errors::PlatformResult;

#[async_trait]
pub(crate) trait ObjectLister: Send + Sync {
    async fn list_page(
        &self,
        bucket: &Bucket,
        prefix: &ObjectKey,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
        max_keys: i32,
    ) -> PlatformResult<S3ObjectPage>;
}

pub(crate) struct S3ObjectLister {
    client: aws_sdk_s3::Client,
}

impl S3ObjectLister {
    pub(crate) fn new(client: aws_sdk_s3::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ObjectLister for S3ObjectLister {
    async fn list_page(
        &self,
        bucket: &Bucket,
        prefix: &ObjectKey,
        continuation_token: Option<&str>,
        start_after: Option<&str>,
        max_keys: i32,
    ) -> PlatformResult<S3ObjectPage> {
        S3::list_objects_page(
            &self.client,
            bucket,
            prefix,
            continuation_token,
            start_after,
            max_keys,
        )
        .await
    }
}
