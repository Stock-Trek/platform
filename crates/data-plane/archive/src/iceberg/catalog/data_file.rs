use crate::ArchiveKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IcebergFilePartition {
    pub kind: ArchiveKind,
    pub hour_millis: i64,
}

impl IcebergFilePartition {
    pub fn new(kind: ArchiveKind, hour_millis: i64) -> Self {
        Self { kind, hour_millis }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergDataFile {
    pub file_path: String,
    pub file_size_in_bytes: i64,
    pub record_count: i64,
    pub partition: Option<IcebergFilePartition>,
}

impl IcebergDataFile {
    pub fn new(file_path: impl Into<String>, file_size_in_bytes: i64, record_count: i64) -> Self {
        Self {
            file_path: file_path.into(),
            file_size_in_bytes,
            record_count,
            partition: None,
        }
    }

    pub fn partitioned(
        file_path: impl Into<String>,
        file_size_in_bytes: i64,
        record_count: i64,
        partition: IcebergFilePartition,
    ) -> Self {
        Self {
            file_path: file_path.into(),
            file_size_in_bytes,
            record_count,
            partition: Some(partition),
        }
    }
}
