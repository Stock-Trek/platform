use crate::spill::{PartitionItem, PartitionRecords};
use platform_archive::{ArchiveBatch, ArchiveKind, ArchiveParquetWriter, ContentHash};
use platform_errors::{PlatformError, PlatformResult};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

const OUTPUT_BATCH_SIZE: usize = 8_192;

pub struct PartitionOutput {
    pub partition: String,
    pub path: PathBuf,
    pub records: usize,
    pub content_hash: ContentHash,
    pub length: u64,
}

impl PartitionOutput {
    fn finish(mut self, writer: ArchiveParquetWriter<File>) -> PlatformResult<Self> {
        writer.close()?;
        let (content_hash, length) = ContentHash::from_file(&self.path).map_err(|error| {
            PlatformError::io(
                "read compacted parquet file",
                self.path.display().to_string(),
                error,
            )
        })?;
        self.content_hash = content_hash;
        self.length = length;
        Ok(self)
    }

    pub fn write_all(
        kind: ArchiveKind,
        mut records: PartitionRecords,
        directory: &Path,
    ) -> PlatformResult<Vec<Self>> {
        let mut outputs = Vec::new();
        let mut current: Option<(Self, ArchiveParquetWriter<File>)> = None;
        let mut buffer = ArchiveBatch::new(kind, Vec::with_capacity(OUTPUT_BATCH_SIZE))?;

        while let Some(PartitionItem {
            partition, record, ..
        }) = records.next_item()?
        {
            let start_new_partition = current
                .as_ref()
                .is_none_or(|(output, _)| output.partition != partition);
            if start_new_partition {
                if let Some((output, mut writer)) = current.take() {
                    if !buffer.is_empty() {
                        writer.write(&buffer)?;
                        buffer.clear();
                    }
                    outputs.push(output.finish(writer)?);
                }

                let path = directory.join(format!("compacted-{}.parquet", outputs.len()));
                let file = File::create(&path).map_err(|error| {
                    PlatformError::io(
                        "create compacted partition file",
                        path.display().to_string(),
                        error,
                    )
                })?;
                current = Some((
                    Self {
                        partition,
                        path,
                        records: 0,
                        content_hash: ContentHash::from_bytes([]),
                        length: 0,
                    },
                    ArchiveParquetWriter::try_new(kind, file)?,
                ));
            }

            if let Some((output, writer)) = current.as_mut() {
                output.records += 1;
                buffer.push(record)?;
                if buffer.len() >= OUTPUT_BATCH_SIZE {
                    writer.write(&buffer)?;
                    buffer.clear();
                }
            }
        }

        if let Some((output, mut writer)) = current.take() {
            if !buffer.is_empty() {
                writer.write(&buffer)?;
            }
            outputs.push(output.finish(writer)?);
        }

        Ok(outputs)
    }
}
