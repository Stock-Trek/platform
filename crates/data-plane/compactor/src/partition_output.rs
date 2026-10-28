use crate::spill::{PartitionItem, PartitionRecords};
use platform_archive::{ArchiveKind, ArchiveParquetWriter, ArchiveRecord};
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
}

impl PartitionOutput {
    pub(crate) fn write_all(
        kind: ArchiveKind,
        mut records: PartitionRecords,
        directory: &Path,
    ) -> BpResult<Vec<Self>> {
        let mut outputs = Vec::new();
        let mut current: Option<(Self, ArchiveParquetWriter<File>)> = None;
        let mut buffer = Vec::<ArchiveRecord>::with_capacity(OUTPUT_BATCH_SIZE);

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
                    writer.close()?;
                    outputs.push(output);
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
                    },
                    ArchiveParquetWriter::try_new(kind, file)?,
                ));
            }

            buffer.push(record);
            if let Some((output, writer)) = current.as_mut() {
                output.records += 1;
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
            writer.close()?;
            outputs.push(output);
        }

        Ok(outputs)
    }
}
