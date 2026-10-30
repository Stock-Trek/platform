use async_trait::async_trait;
use bytes::Bytes;
use platform_archive::{
    ArchiveBatch, ArchiveBucket, ArchiveCatalog, ArchiveIcebergTable, ArchiveKind, ArchiveParquet,
    ArchivePartition, ArchiveRecord, ArchiveTableLayer, Bucket, IcebergDataFile, ObjectKey,
    RoutingBucket, S3,
};
use platform_errors::BpResult;
use platform_market_data::candle::ClosedCandle;
use std::sync::Arc;
use tracing::info;

/// Counts the archive objects produced by a rebuild.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CandleWriteStats {
    pub objects: usize,
    pub records: usize,
}

/// Writes regenerated candles to the archive.
#[async_trait]
pub trait CandleSink: Send + Sync {
    async fn write(&self, candles: &[ClosedCandle]) -> BpResult<CandleWriteStats>;
}

/// Writes regenerated candles to the selected archive layer.
///
/// Raw output is grouped into hourly objects and registered in the raw candle
/// Iceberg table. Compacted output is additionally partitioned by candle key
/// and registered in the compacted candle table.
pub struct S3CandleSink {
    transfer_client: aws_sdk_s3_transfer_manager::Client,
    catalog: Arc<dyn ArchiveCatalog>,
    bucket: Bucket,
    prefix: String,
    bucket_millis: i64,
    layer: ArchiveTableLayer,
}

impl S3CandleSink {
    pub async fn connect(
        bucket: &str,
        prefix: &str,
        bucket_millis: i64,
        catalog: Arc<dyn ArchiveCatalog>,
        layer: ArchiveTableLayer,
    ) -> BpResult<Self> {
        Ok(Self {
            transfer_client: S3::transfer_client().await,
            catalog,
            bucket: Bucket::new(bucket),
            prefix: prefix.to_string(),
            bucket_millis,
            layer,
        })
    }

    fn object_key(
        &self,
        bucket: ArchiveBucket,
        partition: &ArchivePartition,
        routing_bucket: RoutingBucket,
        bytes: &[u8],
    ) -> BpResult<ObjectKey> {
        let file_name = format!("{}-{:x}.parquet", Self::content_id(bytes), bytes.len());
        let key = match self.layer {
            ArchiveTableLayer::Raw => bucket.raw_key(&self.prefix, routing_bucket, file_name),
            ArchiveTableLayer::Compacted => {
                bucket.compacted_key(&self.prefix, partition, routing_bucket, file_name)
            }
        }?;
        Ok(ObjectKey::new(key))
    }

    fn data_file(&self, key: &ObjectKey, bytes: &[u8], records: usize) -> IcebergDataFile {
        IcebergDataFile::new(
            format!("s3://{}/{key}", self.bucket),
            i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            i64::try_from(records).unwrap_or(i64::MAX),
        )
    }

    fn content_id(bytes: &[u8]) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }
}

#[async_trait]
impl CandleSink for S3CandleSink {
    async fn write(&self, candles: &[ClosedCandle]) -> BpResult<CandleWriteStats> {
        let mut records = candles
            .iter()
            .cloned()
            .map(ArchiveRecord::ClosedCandle)
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            ArchiveBucket::new(left, self.bucket_millis)
                .cmp(&ArchiveBucket::new(right, self.bucket_millis))
                .then_with(|| left.routing_bucket().cmp(&right.routing_bucket()))
                .then_with(|| match self.layer {
                    ArchiveTableLayer::Raw => std::cmp::Ordering::Equal,
                    ArchiveTableLayer::Compacted => {
                        left.partition().path().cmp(&right.partition().path())
                    }
                })
                .then_with(|| left.sort_key().cmp(&right.sort_key()))
                .then_with(|| left.key().cmp(&right.key()))
        });

        let mut stats = CandleWriteStats::default();
        let mut data_files = Vec::new();
        let mut start = 0;
        while start < records.len() {
            let bucket = ArchiveBucket::new(&records[start], self.bucket_millis);
            let partition = records[start].partition();
            let routing_bucket = records[start].routing_bucket();
            let mut end = start + 1;
            while end < records.len()
                && ArchiveBucket::new(&records[end], self.bucket_millis) == bucket
                && records[end].routing_bucket() == routing_bucket
                && (self.layer == ArchiveTableLayer::Raw || records[end].partition() == partition)
            {
                end += 1;
            }

            let batch = ArchiveBatch::new(ArchiveKind::Candles, records[start..end].to_vec())?;
            let bytes = ArchiveParquet::encode(&batch)?;
            let key = self.object_key(bucket, &partition, routing_bucket, &bytes)?;
            S3::upload_bytes(
                &self.transfer_client,
                &self.bucket,
                &key,
                Bytes::from(bytes.clone()),
            )
            .await?;
            data_files.push(self.data_file(&key, &bytes, end - start));
            stats.objects += 1;
            stats.records += end - start;
            start = end;
        }

        if !data_files.is_empty() {
            self.catalog
                .register_files(
                    ArchiveIcebergTable::new(self.layer, ArchiveKind::Candles),
                    &data_files,
                    None,
                )
                .await?;
        }
        info!(
            layer = self.layer.label(),
            objects = stats.objects,
            records = stats.records,
            "wrote rebuilt candles to the archive"
        );
        Ok(stats)
    }
}
