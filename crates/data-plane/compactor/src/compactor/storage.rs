use super::Compactor;
use aws_sdk_s3::primitives::ByteStream;
use platform_archive::{ArchiveHour, ArchivePath, ContentHash, SourceBatch};
use platform_errors::{DataError, PlatformError, PlatformResult};
use std::path::Path;
use tokio::io::{AsyncWriteExt, copy};

impl Compactor {
    pub(super) async fn download_to_file(&self, key: &str, path: &Path) -> PlatformResult<()> {
        let target = format!("s3://{}/{key}", self.bucket);
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| PlatformError::s3("get raw archive object", target.clone(), error))?;

        let mut body = Box::pin(output.body.into_async_read());
        let mut file = tokio::fs::File::create(path).await.map_err(|error| {
            PlatformError::io(
                "create temporary source file",
                path.display().to_string(),
                error,
            )
        })?;
        copy(&mut body, &mut file)
            .await
            .map_err(|error| PlatformError::io("stream raw archive object", target, error))?;
        file.flush().await.map_err(|error| {
            PlatformError::io(
                "flush temporary source file",
                path.display().to_string(),
                error,
            )
        })?;
        Ok(())
    }

    pub(super) async fn write_object_from_path(
        &self,
        key: &str,
        path: &Path,
    ) -> PlatformResult<()> {
        let body = ByteStream::from_path(path).await.map_err(|error| {
            PlatformError::io(
                "read compacted parquet file",
                path.display().to_string(),
                error,
            )
        })?;
        let target = format!("s3://{}/{key}", self.bucket);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type("application/vnd.apache.parquet")
            .body(body)
            .send()
            .await
            .map_err(|error| PlatformError::s3("put compacted parquet object", target, error))?;
        Ok(())
    }

    pub(super) fn output_key(
        &self,
        batch: &SourceBatch,
        partition: &str,
        source_digest: &str,
        content_hash: ContentHash,
        length: u64,
    ) -> PlatformResult<String> {
        let hour = ArchiveHour::from_timestamp(batch.bucket_start).ok_or(PlatformError::Data(
            DataError::DateTimeOutOfRange {
                operation: "build compacted object key",
            },
        ))?;
        Ok(ArchivePath::join(
            &self.output_prefix,
            &format!(
                "{partition}/{}",
                hour.file_path(format!("{source_digest}-{content_hash}-{length:x}.parquet"))
            ),
        ))
    }
}
