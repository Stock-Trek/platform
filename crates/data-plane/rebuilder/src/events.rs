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
        let (source_layer, handoff) = Self::discover(catalog, source_layer).await?;
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

impl IcebergEventReader {
    /// Selects the archive files to replay for a rebuild.
    pub async fn discover(
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
}

#[cfg(test)]
mod tests;
