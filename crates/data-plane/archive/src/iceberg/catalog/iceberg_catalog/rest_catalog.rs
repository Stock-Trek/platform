use crate::ObjectStore;
use crate::iceberg::catalog::iceberg_catalog::claim_index::{ClaimIndex, ClaimIndexCache};
use crate::iceberg::catalog::iceberg_catalog::retention::ClaimRetention;
use crate::iceberg::catalog::{ArchiveCatalog, IcebergDataFile};
use crate::iceberg::{ArchiveIcebergTable, ArchiveRegistration, CommitContext, RegisterOutcome};
use crate::{ArchiveKind, CompactionId, LedgerEntry};
use async_trait::async_trait;
use iceberg::{
    Catalog, ErrorKind, NamespaceIdent, TableCreation, spec::FormatVersion, table::Table,
};
use platform_errors::{PlatformError, PlatformResult};
use platform_types::TimestampMilliseconds;
use std::collections::HashSet;

pub struct IcebergCatalog {
    pub(super) catalog: iceberg_catalog_rest::RestCatalog,
    pub(super) namespace: NamespaceIdent,
    pub(super) warehouse: String,
    pub(super) namespace_name: String,
    pub(super) catalog_uri: String,
    pub(super) prefix: Option<String>,
    pub(super) token: Option<String>,
    pub(super) client: reqwest::Client,
    pub(super) claim_index_cache: ClaimIndexCache,
}

impl IcebergCatalog {
    pub async fn ensure_table(&self, archive_table: ArchiveIcebergTable) -> PlatformResult<Table> {
        self.ensure_table_with_context(archive_table, None).await
    }

    pub(super) async fn ensure_table_with_context(
        &self,
        archive_table: ArchiveIcebergTable,
        context: Option<&CommitContext>,
    ) -> PlatformResult<Table> {
        let ident = self.table_ident(archive_table);
        match self.catalog.load_table(&ident).await {
            Ok(table) => {
                let table = self.converge_properties(table, context).await?;
                Self::validate_table(&table, archive_table)?;
                return Ok(table);
            }
            Err(error) if error.kind() == ErrorKind::TableNotFound => {}
            Err(error) => return Err(Self::catalog_error("load Iceberg table", error)),
        }

        self.ensure_namespace().await?;
        let properties = ClaimRetention::properties(false);
        let creation = TableCreation::builder()
            .name(archive_table.name().to_string())
            .location(self.table_location(archive_table))
            .schema(archive_table.table_schema()?)
            .partition_spec_opt(archive_table.partition_spec()?)
            .properties(properties)
            .format_version(FormatVersion::V2)
            .build();

        match self.catalog.create_table(&self.namespace, creation).await {
            Ok(table) => {
                Self::validate_table(&table, archive_table)?;
                Ok(table)
            }
            Err(error) if error.kind() == ErrorKind::TableAlreadyExists => {
                let table = self
                    .catalog
                    .load_table(&ident)
                    .await
                    .map_err(|error| Self::catalog_error("reload Iceberg table", error))?;
                let table = self.converge_properties(table, context).await?;
                Self::validate_table(&table, archive_table)?;
                Ok(table)
            }
            Err(error) => Err(Self::catalog_error("create Iceberg table", error)),
        }
    }

    async fn converge_properties(
        &self,
        table: Table,
        context: Option<&CommitContext>,
    ) -> PlatformResult<Table> {
        match context {
            Some(context) => self.ensure_required_properties(table, context).await,
            None => Ok(table),
        }
    }

    pub fn catalog_error(operation: &'static str, error: iceberg::Error) -> PlatformError {
        PlatformError::external(std::io::Error::other(format!("{operation}: {error}")))
    }
}

#[async_trait]
impl ArchiveCatalog for IcebergCatalog {
    async fn register_files(
        &self,
        table: ArchiveIcebergTable,
        registration: &ArchiveRegistration,
        context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        self.commit_files(table, registration, context).await
    }

    async fn registered_data_files(
        &self,
        table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        let table = self.ensure_table(table).await?;
        Ok(self.claim_state(&table).await?.outputs)
    }

    async fn data_files(&self, table: ArchiveIcebergTable) -> PlatformResult<Vec<IcebergDataFile>> {
        let table = self.ensure_table(table).await?;
        self.data_files_from_table(&table).await
    }

    async fn processed_source_files(
        &self,
        table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        let loaded = self.ensure_table(table).await?;
        let mut processed = self.claim_state(&loaded).await?.sources;
        if table.layer() == crate::iceberg::ArchiveTableLayer::Compacted {
            processed.extend(IcebergCatalog::ledger_source_files(self, table.kind()).await?);
        }
        Ok(processed)
    }

    async fn batch_registered(
        &self,
        table: ArchiveIcebergTable,
        files: &[IcebergDataFile],
    ) -> PlatformResult<bool> {
        let table = self.ensure_table(table).await?;
        let digest = Self::batch_fingerprint(files);
        Ok(
            ClaimIndex::find_batch(&table, Some(&self.claim_index_cache), &digest)
                .await?
                .is_some(),
        )
    }

    async fn claimed_sources(
        &self,
        archive_table: ArchiveIcebergTable,
        sources: &[String],
    ) -> PlatformResult<HashSet<String>> {
        let table = self.ensure_table(archive_table).await?;
        let mut claimed = self.claim_matches(&table, sources, &[]).await?.sources;
        if archive_table.layer() == crate::ArchiveTableLayer::Compacted {
            let ledger = IcebergCatalog::ledger_source_files(self, archive_table.kind()).await?;
            claimed.extend(
                sources
                    .iter()
                    .filter(|source| ledger.contains(*source))
                    .cloned(),
            );
        }
        Ok(claimed)
    }

    async fn live_outputs(
        &self,
        table: ArchiveIcebergTable,
        outputs: &[String],
    ) -> PlatformResult<HashSet<String>> {
        let table = self.ensure_table(table).await?;
        Ok(self.claim_matches(&table, &[], outputs).await?.outputs)
    }

    async fn consolidate_claim_index(
        &self,
        table: ArchiveIcebergTable,
        context: &CommitContext,
    ) -> PlatformResult<()> {
        IcebergCatalog::consolidate_claim_index(self, table, context).await
    }

    async fn sweep_metadata(
        &self,
        table: ArchiveIcebergTable,
        client: &dyn ObjectStore,
        safety_window: std::time::Duration,
    ) -> PlatformResult<usize> {
        let table = self.ensure_table(table).await?;
        IcebergCatalog::sweep_metadata(self, &table, client, safety_window).await
    }

    async fn sweep_orphans(
        &self,
        table: ArchiveIcebergTable,
        client: &dyn ObjectStore,
        safety_window: std::time::Duration,
    ) -> PlatformResult<usize> {
        let loaded = self.ensure_table(table).await?;
        IcebergCatalog::sweep_orphans(self, &loaded, table, client, safety_window).await
    }

    async fn expire_snapshots(
        &self,
        table: ArchiveIcebergTable,
        older_than: platform_types::TimestampMilliseconds,
        retain_last: usize,
        context: &CommitContext,
    ) -> PlatformResult<usize> {
        IcebergCatalog::expire_snapshots(self, table, older_than, retain_last, context).await
    }

    async fn compacted_snapshot(
        &self,
        table: ArchiveIcebergTable,
    ) -> PlatformResult<(Vec<IcebergDataFile>, HashSet<String>)> {
        let loaded = self.ensure_table(table).await?;
        let files = self.data_files_from_table(&loaded).await?;
        let mut processed = self.claim_state(&loaded).await?.sources;
        processed.extend(IcebergCatalog::ledger_source_files(self, table.kind()).await?);
        Ok((files, processed))
    }

    async fn remove_raw_entries(
        &self,
        table: ArchiveIcebergTable,
        entries: &HashSet<String>,
        context: &CommitContext,
    ) -> PlatformResult<usize> {
        IcebergCatalog::remove_raw_entries(self, table, entries, context).await
    }

    async fn record_compaction(
        &self,
        kind: ArchiveKind,
        compaction_id: CompactionId,
        bucket_start: TimestampMilliseconds,
        sources: &[String],
        processed_at: TimestampMilliseconds,
        context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        IcebergCatalog::record_compaction(
            self,
            kind,
            compaction_id,
            bucket_start,
            sources,
            processed_at,
            context,
        )
        .await
    }

    async fn ledger_entries(&self, kind: ArchiveKind) -> PlatformResult<Vec<LedgerEntry>> {
        IcebergCatalog::ledger_entries(self, kind).await
    }

    async fn ledger_source_files(&self, kind: ArchiveKind) -> PlatformResult<HashSet<String>> {
        IcebergCatalog::ledger_source_files(self, kind).await
    }

    async fn compaction_recorded(
        &self,
        kind: ArchiveKind,
        compaction_id: CompactionId,
    ) -> PlatformResult<bool> {
        IcebergCatalog::compaction_recorded(self, kind, compaction_id).await
    }

    fn supports_compaction_ledger(&self) -> bool {
        true
    }
}
