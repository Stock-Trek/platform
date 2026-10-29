use async_trait::async_trait;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use platform_archive::{
    ArchiveCatalog, ArchiveIcebergTable, ArchiveKind, ArchiveParquet, ArchiveRecord,
    ArchiveTableLayer, Bucket, IcebergDataFile, ObjectKey, ParquetReader, S3,
};
use platform_errors::{BpError, BpResult};
use platform_market_data::event::MarketEvent;
use tracing::info;

use crate::config::RebuildSourceLayer;

/// A source of archived events for a rebuild.
#[async_trait]
pub trait EventSource: Send {
    /// Returns every event that should be replayed, in a deterministic order.
    async fn read_events(&mut self) -> BpResult<Vec<MarketEvent>>;
}

/// Reads events from the live data files registered in an Iceberg table.
pub struct IcebergEventReader {
    transfer_client: TransferClient,
    layer: ArchiveTableLayer,
    files: Vec<IcebergDataFile>,
}

impl IcebergEventReader {
    /// Discovers the preferred event table and all of its live data files.
    ///
    /// `Auto` prefers compacted event partitions because they are already
    /// de-duplicated and sorted by stream, timestamp, and sequence. Raw
    /// partitions are used only when the compacted table has no data.
    pub async fn connect(
        catalog: &dyn ArchiveCatalog,
        source_layer: RebuildSourceLayer,
    ) -> BpResult<Self> {
        let (layer, files) = discover_event_files(catalog, source_layer).await?;
        let transfer_client = S3::transfer_client().await;
        info!(
            layer = layer.label(),
            files = files.len(),
            "discovered rebuild event partitions"
        );
        Ok(Self {
            transfer_client,
            layer,
            files,
        })
    }

    pub fn layer(&self) -> ArchiveTableLayer {
        self.layer
    }

    pub fn files(&self) -> &[IcebergDataFile] {
        &self.files
    }

    /// Reads all events from the selected files.
    ///
    /// TODO(#159): stream and externally sort raw events. The first cut keeps
    /// events in memory because raw hourly files are not globally ordered by
    /// stream, timestamp, and sequence.
    pub async fn read_all(&self) -> BpResult<Vec<MarketEvent>> {
        let mut events = Vec::new();
        for file in &self.files {
            let (bucket, key) = parse_s3_uri(&file.file_path)?;
            let file_size = u64::try_from(file.file_size_in_bytes).map_err(|error| {
                BpError::external(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "event file {} has invalid size {}: {error}",
                        file.file_path, file.file_size_in_bytes
                    ),
                ))
            })?;
            let reader = ParquetReader::new(&self.transfer_client, &bucket, &key, file_size);
            let batch = ArchiveParquet::decode_reader(ArchiveKind::Events, reader).await?;
            let count = batch.len();
            events.extend(
                batch
                    .into_records()
                    .into_iter()
                    .filter_map(|record| match record {
                        ArchiveRecord::MarketEvent(event) => Some(event),
                        ArchiveRecord::ClosedCandle(_) => None,
                    }),
            );
            info!(
                file = file.file_path,
                records = count,
                total_events = events.len(),
                "read rebuild event partition"
            );
        }
        Ok(events)
    }
}

#[async_trait]
impl EventSource for IcebergEventReader {
    async fn read_events(&mut self) -> BpResult<Vec<MarketEvent>> {
        self.read_all().await
    }
}

/// Selects the source table and returns its data files in stable path order.
pub async fn discover_event_files(
    catalog: &dyn ArchiveCatalog,
    source_layer: RebuildSourceLayer,
) -> BpResult<(ArchiveTableLayer, Vec<IcebergDataFile>)> {
    match source_layer {
        RebuildSourceLayer::Raw => {
            let files = catalog
                .data_files(ArchiveIcebergTable::raw(ArchiveKind::Events))
                .await?;
            Ok((ArchiveTableLayer::Raw, files))
        }
        RebuildSourceLayer::Compacted => {
            let files = catalog
                .data_files(ArchiveIcebergTable::compacted(ArchiveKind::Events))
                .await?;
            Ok((ArchiveTableLayer::Compacted, files))
        }
        RebuildSourceLayer::Auto => {
            let compacted = catalog
                .data_files(ArchiveIcebergTable::compacted(ArchiveKind::Events))
                .await?;
            if !compacted.is_empty() {
                return Ok((ArchiveTableLayer::Compacted, compacted));
            }
            let raw = catalog
                .data_files(ArchiveIcebergTable::raw(ArchiveKind::Events))
                .await?;
            Ok((ArchiveTableLayer::Raw, raw))
        }
    }
}

fn parse_s3_uri(uri: &str) -> BpResult<(Bucket, ObjectKey)> {
    let path = uri.strip_prefix("s3://").ok_or_else(|| {
        BpError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("rebuild input {uri} is not an s3:// URI"),
        ))
    })?;
    let (bucket, key) = path.split_once('/').ok_or_else(|| {
        BpError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("rebuild input {uri} does not name an S3 object"),
        ))
    })?;
    if bucket.is_empty() || key.is_empty() {
        return Err(BpError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("rebuild input {uri} does not name an S3 object"),
        )));
    }
    Ok((Bucket::new(bucket), ObjectKey::new(key)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_uris_are_split_without_losing_the_key_prefix() {
        let (bucket, key) = parse_s3_uri("s3://archive/market-data/events/a.parquet").unwrap();
        assert_eq!(bucket.as_str(), "archive");
        assert_eq!(key.as_str(), "market-data/events/a.parquet");
    }

    #[test]
    fn non_s3_and_incomplete_uris_are_rejected() {
        assert!(parse_s3_uri("https://archive/a.parquet").is_err());
        assert!(parse_s3_uri("s3://archive").is_err());
        assert!(parse_s3_uri("s3:///a.parquet").is_err());
        assert!(parse_s3_uri("s3://archive/").is_err());
    }
}
