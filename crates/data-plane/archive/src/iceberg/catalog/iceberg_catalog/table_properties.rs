use crate::iceberg::catalog::iceberg_catalog::claim_index::ClaimIndex;
use crate::iceberg::catalog::iceberg_catalog::rest_catalog::IcebergCatalog;
use crate::iceberg::catalog::iceberg_catalog::retention::ClaimRetention;
use iceberg::Catalog;
use iceberg::spec::MAIN_BRANCH;
use iceberg::table::Table;
use platform_errors::PlatformResult;
use platform_time::RetryBackoff;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

const PROPERTY_COMMIT_ATTEMPTS: usize = 4;
const PROPERTY_COMMIT_BACKOFF: Duration = Duration::from_millis(100);
const PROPERTY_COMMIT_MAX_BACKOFF: Duration = Duration::from_secs(1);

impl IcebergCatalog {
    pub(super) fn missing_required_properties(table: &Table) -> HashMap<String, String> {
        let current = table.metadata().properties();
        ClaimRetention::properties(ClaimIndex::has_head(table))
            .into_iter()
            .filter(|(key, value)| current.get(key) != Some(value))
            .collect()
    }

    pub(super) async fn ensure_required_properties(
        &self,
        mut table: Table,
    ) -> PlatformResult<Table> {
        let mut backoff =
            RetryBackoff::linear(PROPERTY_COMMIT_BACKOFF, PROPERTY_COMMIT_MAX_BACKOFF)
                .with_jitter();
        let mut last_error = None;
        for attempt in 1..=PROPERTY_COMMIT_ATTEMPTS {
            let missing = Self::missing_required_properties(&table);
            if missing.is_empty() {
                return Ok(table);
            }
            match self
                .send_table_commit(
                    table.identifier(),
                    &Self::property_commit_request(&table, missing),
                )
                .await
            {
                Ok(()) => {
                    table = self
                        .catalog
                        .load_table(table.identifier())
                        .await
                        .map_err(|error| {
                            Self::catalog_error("reload Iceberg table properties", error)
                        })?;
                }
                Err(error) => {
                    last_error = Some(error);
                    table = self
                        .catalog
                        .load_table(table.identifier())
                        .await
                        .map_err(|error| {
                            Self::catalog_error("reload Iceberg table properties", error)
                        })?;
                    if attempt < PROPERTY_COMMIT_ATTEMPTS {
                        tokio::time::sleep(backoff.next_delay()).await;
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            platform_errors::PlatformError::external(std::io::Error::other(
                "Iceberg table properties remained unchanged",
            ))
        }))
    }

    pub(super) fn property_commit_request(
        table: &Table,
        updates: HashMap<String, String>,
    ) -> serde_json::Value {
        json!({
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
        })
    }
}
