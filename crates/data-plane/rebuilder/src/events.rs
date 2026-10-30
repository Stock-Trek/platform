use async_trait::async_trait;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use platform_archive::{
    ArchiveCatalog, ArchiveHandoff, ArchiveIcebergTable, ArchiveKind, ArchiveParquet,
    ArchiveRecord, IcebergDataFile, ParquetReader, S3,
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

/// Reads events from the compacted archive plus the un-compacted raw tail.
pub struct IcebergEventReader {
    transfer_client: TransferClient,
    source_layer: RebuildSourceLayer,
    handoff: ArchiveHandoff,
}

impl IcebergEventReader {
    /// Discovers which archive files must be replayed.
    ///
    /// `Auto` uses the explicit compacted/raw handoff contract: all compacted
    /// event files plus only the raw objects that are not yet represented by
    /// the compacted snapshot. It never falls back to a full second scan of the
    /// raw history when compacted data exists.
    pub async fn connect(
        catalog: &dyn ArchiveCatalog,
        source_layer: RebuildSourceLayer,
    ) -> BpResult<Self> {
        let (source_layer, handoff) = discover_event_files(catalog, source_layer).await?;
        let transfer_client = S3::transfer_client().await;
        info!(
            source_layer = source_layer.label(),
            compacted_files = handoff.compacted.len(),
            raw_tail_files = handoff.raw_tail.len(),
            files = handoff.len(),
            "discovered rebuild event partitions"
        );
        Ok(Self {
            transfer_client,
            source_layer,
            handoff,
        })
    }

    pub fn source_layer(&self) -> RebuildSourceLayer {
        self.source_layer
    }

    pub fn compacted_files(&self) -> &[IcebergDataFile] {
        &self.handoff.compacted
    }

    pub fn raw_tail_files(&self) -> &[IcebergDataFile] {
        &self.handoff.raw_tail
    }

    pub fn file_count(&self) -> usize {
        self.handoff.len()
    }

    /// Reads all events from the selected files.
    ///
    /// TODO(#159): stream and externally sort raw events. The first cut keeps
    /// events in memory because raw hourly files are not globally ordered by
    /// stream, timestamp, and sequence.
    pub async fn read_all(&self) -> BpResult<Vec<MarketEvent>> {
        let mut events = Vec::new();
        for file in self.handoff.files() {
            let (bucket, key) = S3::parse_uri(&file.file_path)?;
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
                        ArchiveRecord::MarketEvent(event) => Some(event.event),
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

/// Selects the archive files to replay for a rebuild.
pub async fn discover_event_files(
    catalog: &dyn ArchiveCatalog,
    source_layer: RebuildSourceLayer,
) -> BpResult<(RebuildSourceLayer, ArchiveHandoff)> {
    match source_layer {
        RebuildSourceLayer::Raw => {
            let mut raw = catalog
                .data_files(ArchiveIcebergTable::raw(ArchiveKind::Events))
                .await?;
            raw.sort_by(|left, right| left.file_path.cmp(&right.file_path));
            Ok((
                RebuildSourceLayer::Raw,
                ArchiveHandoff {
                    compacted: Vec::new(),
                    raw_tail: raw,
                },
            ))
        }
        RebuildSourceLayer::Compacted => {
            let mut compacted = catalog
                .data_files(ArchiveIcebergTable::compacted(ArchiveKind::Events))
                .await?;
            compacted.sort_by(|left, right| left.file_path.cmp(&right.file_path));
            Ok((
                RebuildSourceLayer::Compacted,
                ArchiveHandoff {
                    compacted,
                    raw_tail: Vec::new(),
                },
            ))
        }
        RebuildSourceLayer::Auto => {
            let handoff = ArchiveHandoff::discover(catalog, ArchiveKind::Events).await?;
            Ok((RebuildSourceLayer::Auto, handoff))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashSet;

    #[derive(Default)]
    struct FakeCatalog {
        raw: Vec<IcebergDataFile>,
        compacted: Vec<IcebergDataFile>,
        processed: HashSet<String>,
    }

    #[async_trait]
    impl ArchiveCatalog for FakeCatalog {
        async fn register_files(
            &self,
            _table: ArchiveIcebergTable,
            _files: &[IcebergDataFile],
            _source_files: Option<&[String]>,
        ) -> BpResult<()> {
            Ok(())
        }

        async fn registered_data_files(
            &self,
            _table: ArchiveIcebergTable,
        ) -> BpResult<HashSet<String>> {
            Ok(HashSet::new())
        }

        async fn data_files(&self, table: ArchiveIcebergTable) -> BpResult<Vec<IcebergDataFile>> {
            let files = match table.layer() {
                platform_archive::ArchiveTableLayer::Raw => &self.raw,
                platform_archive::ArchiveTableLayer::Compacted => &self.compacted,
            };
            Ok(files.clone())
        }

        async fn processed_source_files(
            &self,
            table: ArchiveIcebergTable,
        ) -> BpResult<HashSet<String>> {
            if table.layer() == platform_archive::ArchiveTableLayer::Compacted {
                Ok(self.processed.clone())
            } else {
                Ok(HashSet::new())
            }
        }
    }

    fn file(path: &str) -> IcebergDataFile {
        IcebergDataFile::new(path, 10, 1)
    }

    #[tokio::test]
    async fn auto_reads_compacted_files_and_only_the_uncompacted_raw_tail() {
        let covered = "s3://archive/market-data/raw/events/covered.parquet";
        let tail = "s3://archive/market-data/raw/events/tail.parquet";
        let compacted = "s3://archive/market-data/compacted/events/a.parquet";
        let catalog = FakeCatalog {
            raw: vec![file(covered), file(tail)],
            compacted: vec![file(compacted)],
            processed: HashSet::from([covered.to_string()]),
        };

        let (layer, handoff) = discover_event_files(&catalog, RebuildSourceLayer::Auto)
            .await
            .unwrap();

        assert_eq!(layer, RebuildSourceLayer::Auto);
        assert_eq!(handoff.compacted, vec![file(compacted)]);
        assert_eq!(handoff.raw_tail, vec![file(tail)]);
    }

    #[tokio::test]
    async fn explicit_layers_do_not_mix_sources() {
        let raw = file("s3://archive/market-data/raw/events/a.parquet");
        let compacted = file("s3://archive/market-data/compacted/events/a.parquet");
        let catalog = FakeCatalog {
            raw: vec![raw.clone()],
            compacted: vec![compacted.clone()],
            processed: HashSet::new(),
        };

        let (_, raw_handoff) = discover_event_files(&catalog, RebuildSourceLayer::Raw)
            .await
            .unwrap();
        let (_, compacted_handoff) = discover_event_files(&catalog, RebuildSourceLayer::Compacted)
            .await
            .unwrap();

        assert_eq!(raw_handoff.raw_tail, vec![raw]);
        assert!(raw_handoff.compacted.is_empty());
        assert_eq!(compacted_handoff.compacted, vec![compacted]);
        assert!(compacted_handoff.raw_tail.is_empty());
    }
}
