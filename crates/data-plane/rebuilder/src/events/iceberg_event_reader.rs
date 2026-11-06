use aws_sdk_s3_transfer_manager::Client as TransferClient;
use platform_archive::{
    ArchiveBatch, ArchiveCatalog, ArchiveHandoff, ArchiveIcebergTable, ArchiveKind, ArchiveParquet,
    ArchiveParquetStream, IcebergDataFile, ParquetReader, S3,
};
use platform_errors::{BpError, BpResult};
use tracing::info;

use crate::config::RebuildSourceLayer;

/// Reads events from the compacted archive plus the un-compacted raw tail.
///
/// Each selected object is streamed and decoded one bounded batch at a time,
/// so a rebuild never holds a whole Parquet object in memory.
pub struct IcebergEventReader {
    transfer_client: TransferClient,
    source_layer: RebuildSourceLayer,
    handoff: ArchiveHandoff,
    files: Vec<IcebergDataFile>,
    next_file: usize,
    stream: Option<ArchiveParquetStream<ParquetReader>>,
    file_records: usize,
}

impl IcebergEventReader {
    /// Discovers which archive files must be replayed.
    ///
    /// `Auto` uses the explicit compacted/raw handoff contract: all compacted
    /// event files plus only the raw objects that are not yet represented by
    /// the compacted snapshot.
    pub async fn connect(
        catalog: &dyn ArchiveCatalog,
        source_layer: RebuildSourceLayer,
    ) -> BpResult<Self> {
        let (source_layer, handoff) = Self::discover(catalog, source_layer).await?;
        let transfer_client = S3::transfer_client().await;
        let files = handoff.files().cloned().collect::<Vec<_>>();
        info!(
            source_layer = source_layer.label(),
            compacted_files = handoff.compacted.len(),
            raw_tail_files = handoff.raw_tail.len(),
            files = files.len(),
            "discovered rebuild event partitions"
        );
        Ok(Self {
            transfer_client,
            source_layer,
            handoff,
            files,
            next_file: 0,
            stream: None,
            file_records: 0,
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

    pub async fn next_batch(&mut self) -> BpResult<Option<ArchiveBatch>> {
        loop {
            let mut stream = match self.stream.take() {
                Some(stream) => stream,
                None => {
                    let Some(file) = self.files.get(self.next_file) else {
                        return Ok(None);
                    };
                    self.next_file += 1;
                    self.file_records = 0;
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
                    let reader =
                        ParquetReader::new(&self.transfer_client, &bucket, &key, file_size);
                    ArchiveParquet::decode_reader_batches(ArchiveKind::Events, reader).await?
                }
            };

            match stream.next_batch().await? {
                Some(batch) => {
                    self.file_records += batch.len();
                    self.stream = Some(stream);
                    return Ok(Some(batch));
                }
                None => {
                    info!(
                        file = self.files[self.next_file - 1].file_path,
                        records = self.file_records,
                        "read rebuild event partition"
                    );
                }
            }
        }
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
