use crate::iceberg::catalog::IcebergDataFile;
use iceberg::{
    spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Struct},
    table::Table,
};
use platform_errors::{PlatformError, PlatformResult};
use uuid::Uuid;

pub(super) struct SnapshotFile;

impl SnapshotFile {
    pub(super) fn snapshot_id(table: &Table) -> i64 {
        loop {
            let (left, right) = Uuid::new_v4().as_u64_pair();
            let candidate = (left ^ right) as i64 & i64::MAX;
            let candidate = candidate.max(1);
            if !table
                .metadata()
                .snapshots()
                .any(|snapshot| snapshot.snapshot_id() == candidate)
            {
                return candidate;
            }
        }
    }

    pub(super) fn data_file(table: &Table, file: &IcebergDataFile) -> PlatformResult<DataFile> {
        let record_count = u64::try_from(file.record_count).map_err(|_| {
            PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("negative record count for {}", file.file_path),
            ))
        })?;
        let file_size_in_bytes = u64::try_from(file.file_size_in_bytes).map_err(|_| {
            PlatformError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("negative file size for {}", file.file_path),
            ))
        })?;
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file.file_path.clone())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .record_count(record_count)
            .file_size_in_bytes(file_size_in_bytes)
            .build()
            .map_err(|error| {
                PlatformError::external(std::io::Error::other(format!(
                    "build Iceberg data file: {error}"
                )))
            })
    }
}
