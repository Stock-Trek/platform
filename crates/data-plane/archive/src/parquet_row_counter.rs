use crate::parquet_reader::ParquetReader;
use crate::s3_object::{Bucket, ObjectKey};
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use platform_errors::PlatformResult;

#[async_trait]
pub trait ParquetRowCounter: Send + Sync {
    async fn row_count(
        &self,
        bucket: &Bucket,
        key: &ObjectKey,
        file_size: u64,
    ) -> PlatformResult<i64>;
}

pub struct S3ParquetRowCounter {
    client: S3Client,
}

impl S3ParquetRowCounter {
    pub fn new(client: S3Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ParquetRowCounter for S3ParquetRowCounter {
    async fn row_count(
        &self,
        bucket: &Bucket,
        key: &ObjectKey,
        file_size: u64,
    ) -> PlatformResult<i64> {
        ParquetReader::new(&self.client, bucket, key, file_size)
            .row_count()
            .await
    }
}
