use crate::{
    content_id::ContentId,
    spill::{PartitionItem, PartitionRecords},
};
use platform_archive::{ArchiveBatch, ArchiveKind, ArchiveParquetWriter};
use platform_errors::{BpError, BpResult};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

const OUTPUT_BATCH_SIZE: usize = 8_192;

pub(crate) struct PartitionOutput {
    pub(crate) partition: String,
    pub(crate) path: PathBuf,
    pub(crate) records: usize,
    pub(crate) content_id: u64,
    pub(crate) length: u64,
    content: ContentId,
}

impl PartitionOutput {
    fn finish(mut self, writer: ArchiveParquetWriter<File>) -> BpResult<Self> {
        writer.close()?;
        self.content_id = self.content.value();
        self.length = std::fs::metadata(&self.path)
            .map_err(|error| {
                BpError::io(
                    "read compacted parquet file size",
                    self.path.display().to_string(),
                    error,
                )
            })?
            .len();
        Ok(self)
    }

    pub(crate) fn write_all(
        kind: ArchiveKind,
        mut records: PartitionRecords,
        directory: &Path,
    ) -> BpResult<Vec<Self>> {
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
                    BpError::io(
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
                        content_id: 0,
                        length: 0,
                        content: ContentId::new(),
                    },
                    ArchiveParquetWriter::try_new(kind, file)?,
                ));
            }

            if let Some((output, writer)) = current.as_mut() {
                output.content.update(&record)?;
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
