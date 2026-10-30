use super::IcebergDataFile;
use crate::iceberg::ArchiveIcebergTable;
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use std::collections::HashSet;

#[async_trait]
pub trait ArchiveCatalog: Send + Sync {
    async fn register_files(
        &self,
        table: ArchiveIcebergTable,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> BpResult<()>;

    async fn registered_data_files(&self, table: ArchiveIcebergTable) -> BpResult<HashSet<String>>;

    /// Lists the live data files in a table.
    ///
    /// Rebuild tooling needs file sizes as well as paths so it can construct a
    /// bounded S3 reader without issuing an extra HEAD request per object.
    async fn data_files(&self, table: ArchiveIcebergTable) -> BpResult<Vec<IcebergDataFile>> {
        Err(BpError::external(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!(
                "archive catalog does not expose data files for {}",
                table.name()
            ),
        )))
    }

    async fn processed_source_files(&self, table: ArchiveIcebergTable)
    -> BpResult<HashSet<String>>;

    /// Reads the compacted data files and the processed raw-source set from one
    /// table snapshot.
    ///
    /// The two values form the compacted/raw handoff contract. They must come
    /// from the same snapshot, otherwise a concurrent compaction commit can mark
    /// a raw object as processed before the matching compacted files are visible
    /// to the recovery reader.
    async fn compacted_snapshot(
        &self,
        table: ArchiveIcebergTable,
    ) -> BpResult<(Vec<IcebergDataFile>, HashSet<String>)> {
        Ok((
            self.data_files(table).await?,
            self.processed_source_files(table).await?,
        ))
    }
}
