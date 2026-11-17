use crate::MIN_SNAPSHOT_RETENTION;
use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::{ArchiveIcebergTable, CommitContext};
use iceberg::spec::MAIN_BRANCH;
use iceberg::{TableRequirement, TableUpdate};
use platform_errors::PlatformResult;
use platform_types::TimestampMilliseconds;
use serde_json::json;
use std::collections::HashSet;

impl IcebergCatalog {
    pub async fn expire_archive_snapshots(
        &self,
        archive_table: ArchiveIcebergTable,
        config: &crate::MaintenanceConfig,
        context: &CommitContext,
    ) -> PlatformResult<usize> {
        config.validate()?;
        let retention_millis =
            i64::try_from(config.snapshot_retention.as_millis()).unwrap_or(i64::MAX);
        let older_than = TimestampMilliseconds(
            TimestampMilliseconds::now().0.saturating_sub(
                retention_millis
                    .max(i64::try_from(MIN_SNAPSHOT_RETENTION.as_millis()).unwrap_or(i64::MAX)),
            ),
        );
        self.expire_snapshots(archive_table, older_than, config.snapshots_to_keep, context)
            .await
    }

    pub async fn expire_snapshots(
        &self,
        archive_table: ArchiveIcebergTable,
        older_than: TimestampMilliseconds,
        retain_last: usize,
        context: &CommitContext,
    ) -> PlatformResult<usize> {
        if retain_last == 0 {
            return Err(platform_errors::PlatformError::config_invalid(
                "archive snapshot expiry",
                "must retain at least one snapshot",
            ));
        }
        let table = self
            .ensure_table_with_context(archive_table, Some(context))
            .await?;
        let metadata = table.metadata();
        let mut protected = metadata
            .snapshot_for_ref(MAIN_BRANCH)
            .map(|snapshot| HashSet::from([snapshot.snapshot_id()]))
            .unwrap_or_default();
        let mut current = metadata.current_snapshot();
        let mut retained = 0;
        while let Some(snapshot) = current {
            if retained >= retain_last {
                break;
            }
            protected.insert(snapshot.snapshot_id());
            retained += 1;
            current = snapshot
                .parent_snapshot_id()
                .and_then(|id| metadata.snapshot_by_id(id));
        }
        let mut snapshot_ids = metadata
            .snapshots()
            .filter(|snapshot| {
                snapshot.timestamp_ms() < older_than.0
                    && !protected.contains(&snapshot.snapshot_id())
            })
            .map(|snapshot| snapshot.snapshot_id())
            .collect::<Vec<_>>();
        snapshot_ids.sort_unstable();
        if snapshot_ids.is_empty() {
            return Ok(0);
        }
        let expired = snapshot_ids.len();
        let updates = vec![TableUpdate::RemoveSnapshots { snapshot_ids }];
        let requirements = vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: metadata.current_snapshot_id(),
            },
        ];
        context.authorize().await?;
        self.send_table_commit(
            table.identifier(),
            &json!({
                "requirements": requirements,
                "updates": updates,
            }),
        )
        .await?;
        Ok(expired)
    }
}
