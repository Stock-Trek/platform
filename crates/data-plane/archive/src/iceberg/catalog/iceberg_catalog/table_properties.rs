use crate::iceberg::catalog::iceberg_catalog::rest_catalog::IcebergCatalog;
use crate::iceberg::catalog::iceberg_catalog::retention::ClaimRetention;
use iceberg::Catalog;
use iceberg::table::Table;
use platform_errors::PlatformResult;
use serde_json::json;
use std::collections::HashMap;

impl IcebergCatalog {
    pub(super) fn missing_required_properties(table: &Table) -> HashMap<String, String> {
        let current = table.metadata().properties();
        ClaimRetention::properties()
            .into_iter()
            .filter(|(key, value)| current.get(key) != Some(value))
            .collect()
    }

    pub(super) async fn ensure_required_properties(&self, table: Table) -> PlatformResult<Table> {
        let missing = Self::missing_required_properties(&table);
        if missing.is_empty() {
            return Ok(table);
        }
        let request = json!({
            "requirements": [{
                "type": "assert-table-uuid",
                "uuid": table.metadata().uuid().to_string()
            }],
            "updates": [{
                "action": "set-properties",
                "updates": missing
            }]
        });
        self.send_table_commit(table.identifier(), &request).await?;
        self.catalog
            .load_table(table.identifier())
            .await
            .map_err(|error| Self::catalog_error("reload Iceberg table properties", error))
    }
}
