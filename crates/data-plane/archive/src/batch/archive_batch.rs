use crate::record::{ArchiveKind, ArchiveRecord};
use platform_errors::{BpError, BpResult};
use std::io::ErrorKind;

#[derive(Debug, Clone)]
pub struct ArchiveBatch {
    pub(super) kind: ArchiveKind,
    pub(super) records: Vec<ArchiveRecord>,
}

impl ArchiveBatch {
    pub fn new(kind: ArchiveKind, records: Vec<ArchiveRecord>) -> BpResult<Self> {
        if let Some(record) = records.iter().find(|record| record.kind() != kind) {
            return Err(Self::kind_mismatch(kind, record));
        }
        Ok(Self { kind, records })
    }

    pub fn push(&mut self, record: ArchiveRecord) -> BpResult<()> {
        if record.kind() != self.kind {
            return Err(Self::kind_mismatch(self.kind, &record));
        }
        self.records.push(record);
        Ok(())
    }

    pub fn clear(&mut self) {
        self.records.clear();
    }

    pub fn kind(&self) -> ArchiveKind {
        self.kind
    }

    pub fn records(&self) -> &[ArchiveRecord] {
        &self.records
    }

    pub fn into_records(self) -> Vec<ArchiveRecord> {
        self.records
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub(super) fn kind_mismatch(expected: ArchiveKind, record: &ArchiveRecord) -> BpError {
        BpError::io(
            "encode archived parquet",
            expected.label(),
            std::io::Error::new(
                ErrorKind::InvalidData,
                format!(
                    "record kind {} does not match batch kind {}",
                    record.kind().label(),
                    expected.label()
                ),
            ),
        )
    }
}
