use crate::s3_object::{Bucket, ObjectKey, S3, S3Object, S3ObjectPage};
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use platform_errors::PlatformResult;

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn list_page(
        &self,
        bucket: &Bucket,
        prefix: &ObjectKey,
        continuation_token: Option<&str>,
        max_keys: i32,
    ) -> PlatformResult<S3ObjectPage>;

    async fn list_all(&self, bucket: &Bucket, prefix: &ObjectKey) -> PlatformResult<Vec<S3Object>> {
        let mut objects = Vec::new();
        let mut continuation = None;
        loop {
            let page = self
                .list_page(bucket, prefix, continuation.as_deref(), 1_000)
                .await?;
            objects.extend(page.objects);
            match page.next_continuation_token {
                Some(token) => continuation = Some(token),
                None => return Ok(objects),
            }
        }
    }

    async fn delete(&self, bucket: &Bucket, key: &ObjectKey) -> PlatformResult<()>;
}

pub struct S3ObjectStore {
    client: S3Client,
}

impl S3ObjectStore {
    pub fn new(client: S3Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn list_page(
        &self,
        bucket: &Bucket,
        prefix: &ObjectKey,
        continuation_token: Option<&str>,
        max_keys: i32,
    ) -> PlatformResult<S3ObjectPage> {
        S3::list_objects_page(
            &self.client,
            bucket,
            prefix,
            continuation_token,
            None,
            max_keys,
        )
        .await
    }

    async fn list_all(&self, bucket: &Bucket, prefix: &ObjectKey) -> PlatformResult<Vec<S3Object>> {
        S3::list_objects(&self.client, bucket, prefix).await
    }

    async fn delete(&self, bucket: &Bucket, key: &ObjectKey) -> PlatformResult<()> {
        S3::delete_object(&self.client, bucket, key).await
    }
}
