use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::catalog::iceberg_catalog::claim_index::{
    ClaimIndex, INDEX_FORMAT_VERSION_KEY, INDEX_HEAD_DIGEST_KEY, INDEX_HEAD_KEY,
};
use crate::iceberg::catalog::iceberg_catalog::claim_index_page::ClaimIndexPage;
use crate::iceberg::{ArchiveIcebergTable, CommitContext};
use iceberg::{
    spec::{FormatVersion, MAIN_BRANCH},
    table::Table,
};
use platform_errors::{PlatformError, PlatformResult};

impl IcebergCatalog {
    pub async fn validate_contract(&self) -> PlatformResult<()> {
        for table in ArchiveIcebergTable::all() {
            self.ensure_table(table).await?;
        }
        Ok(())
    }

    pub(super) fn validate_table(table: &Table, kind: crate::ArchiveKind) -> PlatformResult<()> {
        if table.metadata().format_version() != FormatVersion::V2 {
            return Err(PlatformError::config_invalid(
                "archive Iceberg table",
                "must use Iceberg format version 2",
            ));
        }
        if !table
            .metadata()
            .default_partition_spec()
            .fields()
            .is_empty()
        {
            return Err(PlatformError::config_invalid(
                "archive Iceberg table",
                "must be unpartitioned; archive rows are ordered within data files",
            ));
        }
        let expected = ArchiveIcebergTable::schema(kind)?;
        let current = table.metadata().current_schema();
        let current = current.as_struct();
        let expected = expected.as_struct();
        let matches = current.fields().len() == expected.fields().len()
            && current
                .fields()
                .iter()
                .zip(expected.fields())
                .all(|(left, right)| {
                    left.name == right.name
                        && left.required == right.required
                        && left.field_type == right.field_type
                });
        if !matches {
            return Err(PlatformError::config_invalid(
                "archive Iceberg table",
                "schema does not match the archive writer contract",
            ));
        }
        Self::validate_claim_index(table)?;
        Ok(())
    }

    pub(super) fn validate_claim_index(table: &Table) -> PlatformResult<()> {
        let properties = table.metadata().properties();
        let head = properties.get(INDEX_HEAD_KEY);
        let digest = properties.get(INDEX_HEAD_DIGEST_KEY);
        if head.is_none() && digest.is_none() && !properties.contains_key(INDEX_FORMAT_VERSION_KEY)
        {
            return Ok(());
        }
        if head.is_none() || digest.is_none() {
            return Err(PlatformError::config_invalid(
                "archive Iceberg table",
                "claim index head is only partially configured",
            ));
        }
        let version = properties.get(INDEX_FORMAT_VERSION_KEY).ok_or_else(|| {
            PlatformError::config_invalid(
                "archive Iceberg table",
                "claim index head is missing its format version",
            )
        })?;
        if version != &ClaimIndexPage::format_version().to_string() {
            return Err(PlatformError::config_invalid(
                "archive Iceberg table",
                format!("claim index uses unsupported format version {version}"),
            ));
        }
        Ok(())
    }

    pub(super) async fn consolidate_claim_index(
        &self,
        archive: ArchiveIcebergTable,
        context: &CommitContext,
    ) -> PlatformResult<()> {
        context.require_authorizer()?;
        let table = self
            .ensure_table_with_context(archive, Some(context))
            .await?;
        if !ClaimIndex::needs_consolidation(&table) {
            return Ok(());
        }
        let live_outputs = self.registered_paths(&table).await?;
        let updates = ClaimIndex::consolidate(&table, context, &live_outputs).await?;
        let request = serde_json::json!({
            "requirements": [
                {
                    "type": "assert-table-uuid",
                    "uuid": table.metadata().uuid().to_string()
                },
                {
                    "type": "assert-ref-snapshot-id",
                    "ref": MAIN_BRANCH,
                    "snapshot-id": table.metadata().current_snapshot_id()
                }
            ],
            "updates": [{
                "action": "set-properties",
                "updates": updates
            }]
        });
        context.authorize().await?;
        self.send_table_commit(table.identifier(), &request).await
    }
}
