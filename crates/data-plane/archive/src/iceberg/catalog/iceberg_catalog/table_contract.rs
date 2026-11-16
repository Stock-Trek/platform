use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::catalog::iceberg_catalog::claim_index::ClaimIndex;
use crate::iceberg::{ArchiveIcebergTable, CommitContext};
use iceberg::{
    spec::{FormatVersion, MAIN_BRANCH},
    table::Table,
};
use platform_errors::{PlatformError, PlatformResult};

impl IcebergCatalog {
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
        Ok(())
    }

    pub async fn consolidate_claim_index(
        &self,
        archive: ArchiveIcebergTable,
        context: &CommitContext,
    ) -> PlatformResult<()> {
        context.authorize().await?;
        let table = self.ensure_table(archive).await?;
        let updates = ClaimIndex::consolidate(&table, context).await?;
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
