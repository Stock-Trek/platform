use crate::iceberg::catalog::IcebergDataFile;
use crate::iceberg::{ArchiveIcebergTable, ArchiveRegistration, CommitContext, RegisterOutcome};
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};
use std::collections::HashSet;

#[async_trait]
pub trait ArchiveCatalog: Send + Sync {
    async fn register_files(
        &self,
        table: ArchiveIcebergTable,
        registration: &ArchiveRegistration,
        context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome>;

    async fn registered_data_files(
        &self,
        table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>>;

    /// Lists the live data files in a table.
    ///
    /// Rebuild tooling needs file sizes as well as paths so it can construct a
    /// bounded S3 reader without issuing an extra HEAD request per object.
    async fn data_files(&self, table: ArchiveIcebergTable) -> PlatformResult<Vec<IcebergDataFile>> {
        Err(PlatformError::external(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!(
                "archive catalog does not expose data files for {}",
                table.name()
            ),
        )))
    }

    async fn processed_source_files(
        &self,
        table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>>;

    async fn claimed_sources(
        &self,
        table: ArchiveIcebergTable,
        sources: &[String],
    ) -> PlatformResult<HashSet<String>> {
        let processed = self.processed_source_files(table).await?;
        Ok(sources
            .iter()
            .filter(|source| processed.contains(*source))
            .cloned()
            .collect())
    }

    async fn live_outputs(
        &self,
        table: ArchiveIcebergTable,
        outputs: &[String],
    ) -> PlatformResult<HashSet<String>> {
        let live = self.registered_data_files(table).await?;
        Ok(outputs
            .iter()
            .filter(|output| live.contains(*output))
            .cloned()
            .collect())
    }

    /// Reads the compacted data files and the processed raw-source set from one
    /// table snapshot.
    ///
    /// The two values form the compacted/raw handoff contract. They must come
    /// from the same snapshot, otherwise a concurrent compaction commit can mark
    /// a raw object as processed before the matching compacted files are visible
    /// to the recovery reader.
    async fn consolidate_claim_index(
        &self,
        _table: ArchiveIcebergTable,
        _context: &CommitContext,
    ) -> PlatformResult<()> {
        Ok(())
    }

    async fn compacted_snapshot(
        &self,
        table: ArchiveIcebergTable,
    ) -> PlatformResult<(Vec<IcebergDataFile>, HashSet<String>)> {
        Ok((
            self.data_files(table).await?,
            self.processed_source_files(table).await?,
        ))
    }
}
