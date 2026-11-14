use crate::s3_object::{Bucket, ObjectKey};
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use platform_errors::{PlatformError, PlatformResult};
use std::{ops::Range, sync::Arc};

#[derive(Clone)]
pub struct ParquetReader {
    s3_client: S3Client,
    bucket: Bucket,
    key: ObjectKey,
    file_size: u64,
}

impl ParquetReader {
    pub fn new(s3_client: &S3Client, bucket: &Bucket, key: &ObjectKey, file_size: u64) -> Self {
        Self {
            s3_client: s3_client.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            file_size,
        }
    }

    pub async fn row_count(mut self) -> PlatformResult<i64> {
        let metadata = self.get_metadata(None).await?;
        Ok(metadata.file_metadata().num_rows())
    }

    async fn fetch_range(&self, range: Range<u64>) -> ParquetResult<Bytes> {
        if range.start >= range.end {
            return Ok(Bytes::new());
        }
        let range_header = format!("bytes={}-{}", range.start, range.end - 1);
        let target = format!("s3://{}/{}", self.bucket, self.key);
        let output = self
            .s3_client
            .get_object()
            .bucket(self.bucket.as_str())
            .key(self.key.as_str())
            .range(range_header)
            .send()
            .await
            .map_err(|error| {
                ParquetError::External(Box::new(PlatformError::s3(
                    "get object range",
                    target.clone(),
                    error,
                )))
            })?;
        output
            .body
            .collect()
            .await
            .map(|data| data.into_bytes())
            .map_err(|error| {
                ParquetError::External(Box::new(PlatformError::s3(
                    "read object range",
                    target,
                    error,
                )))
            })
    }
}

impl AsyncFileReader for ParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move { self.fetch_range(range).await }.boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        let reader = self.clone();
        async move {
            let fetches = ranges
                .into_iter()
                .map(|range| {
                    let reader = reader.clone();
                    async move { reader.fetch_range(range).await }
                })
                .collect::<Vec<_>>();
            futures::future::try_join_all(fetches).await
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let file_size = self.file_size;
            let metadata = ParquetMetaDataReader::new()
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}
