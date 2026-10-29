use crate::spill::{PartitionItem, PartitionRecords};
use platform_archive::{ArchiveBatch, ArchiveKind, ArchiveParquetWriter};
use platform_errors::{BpError, BpResult};
use std::{
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const OUTPUT_BATCH_SIZE: usize = 8_192;
const CONTENT_ID_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const CONTENT_ID_PRIME: u64 = 0x0000_0100_0000_01b3;

pub(crate) struct PartitionOutput {
    pub(crate) partition: String,
    pub(crate) path: PathBuf,
    pub(crate) records: usize,
    pub(crate) content_id: u64,
    pub(crate) length: u64,
    content: Arc<Mutex<ContentId>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ContentId {
    hash: u64,
    length: u64,
}

impl ContentId {
    fn new() -> Self {
        Self {
            hash: CONTENT_ID_OFFSET_BASIS,
            length: 0,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        self.length = self.length.saturating_add(bytes.len() as u64);
        for byte in bytes {
            self.hash ^= u64::from(*byte);
            self.hash = self.hash.wrapping_mul(CONTENT_ID_PRIME);
        }
    }
}

struct HashingFile {
    file: File,
    content: Arc<Mutex<ContentId>>,
}

impl Write for HashingFile {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.file.write(buffer)?;
        let mut content = self
            .content
            .lock()
            .map_err(|_| io::Error::other("content id lock poisoned"))?;
        content.update(&buffer[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl PartitionOutput {
    fn finish(mut self, writer: ArchiveParquetWriter<HashingFile>) -> BpResult<Self> {
        writer.close()?;
        let content = {
            let content = self.content.lock().map_err(|_| {
                BpError::io(
                    "read compacted parquet content id",
                    self.path.display().to_string(),
                    io::Error::other("content id lock poisoned"),
                )
            })?;
            *content
        };
        self.content_id = content.hash;
        self.length = content.length;
        Ok(self)
    }

    pub(crate) fn write_all(
        kind: ArchiveKind,
        mut records: PartitionRecords,
        directory: &Path,
    ) -> BpResult<Vec<Self>> {
        let mut outputs = Vec::new();
        let mut current: Option<(Self, ArchiveParquetWriter<HashingFile>)> = None;
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
                let content = Arc::new(Mutex::new(ContentId::new()));
                let writer = ArchiveParquetWriter::try_new(
                    kind,
                    HashingFile {
                        file,
                        content: Arc::clone(&content),
                    },
                )?;
                current = Some((
                    Self {
                        partition,
                        path,
                        records: 0,
                        content_id: 0,
                        length: 0,
                        content,
                    },
                    writer,
                ));
            }

            buffer.push(record)?;
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
            outputs.push(output.finish(writer)?);
        }

        Ok(outputs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content_id_for(path: PathBuf, bytes: &[u8]) -> ContentId {
        let file = File::create(path).unwrap();
        let content = Arc::new(Mutex::new(ContentId::new()));
        let mut file = HashingFile {
            file,
            content: Arc::clone(&content),
        };
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
        *content.lock().unwrap()
    }

    #[test]
    fn hashing_file_produces_stable_content_ids() {
        let dir = tempfile::tempdir().unwrap();
        let first = content_id_for(dir.path().join("first.parquet"), b"same bytes");
        let second = content_id_for(dir.path().join("second.parquet"), b"same bytes");
        let different = content_id_for(dir.path().join("different.parquet"), b"other bytes");

        assert_eq!(first, second);
        assert_eq!(first.length, 10);
        assert_eq!(first.hash, 0x12ce_fa2c_7d8e_00f8);
        assert_ne!(first, different);
    }
}
