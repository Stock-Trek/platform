use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};

const ICEBERG_CATALOG_URI: &str = "ICEBERG_CATALOG_URI";
const ICEBERG_CATALOG_WAREHOUSE: &str = "ICEBERG_CATALOG_WAREHOUSE";
const ICEBERG_CATALOG_TOKEN: &str = "ICEBERG_CATALOG_TOKEN";
const ICEBERG_NAMESPACE: &str = "ICEBERG_NAMESPACE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergConfig {
    pub catalog_uri: String,
    pub warehouse: String,
    pub token: Option<String>,
    pub namespace: String,
}

impl IcebergConfig {
    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let catalog_uri = vars.required_url(ICEBERG_CATALOG_URI, &["http", "https"])?;
        let warehouse = vars.required(ICEBERG_CATALOG_WAREHOUSE)?;
        if !warehouse.starts_with("s3://") {
            return Err(BpError::config_invalid(
                ICEBERG_CATALOG_WAREHOUSE,
                "must be an s3:// URI",
            ));
        }
        let token = vars.optional(ICEBERG_CATALOG_TOKEN)?;
        let namespace = vars.required(ICEBERG_NAMESPACE)?;

        Ok(Self {
            catalog_uri,
            warehouse,
            token,
            namespace,
        })
    }

    /// Reads catalog configuration when `ICEBERG_CATALOG_URI` is present.
    ///
    /// Services that can operate without Iceberg use this to enable the
    /// compacted archive handoff only when a catalog has been configured.
    pub fn optional_from_vars(vars: &EnvVars) -> BpResult<Option<Self>> {
        if vars.optional(ICEBERG_CATALOG_URI)?.is_none() {
            return Ok(None);
        }
        Self::from_vars(vars).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(values: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_map(values.iter().copied())
    }

    #[test]
    fn reads_rest_catalog_configuration() {
        let config = IcebergConfig::from_vars(&vars(&[
            ("ICEBERG_CATALOG_URI", "https://catalog.test"),
            ("ICEBERG_CATALOG_WAREHOUSE", "s3://archive/iceberg"),
            ("ICEBERG_CATALOG_TOKEN", "secret"),
            ("ICEBERG_NAMESPACE", "market"),
        ]))
        .unwrap();

        assert_eq!(config.catalog_uri, "https://catalog.test");
        assert_eq!(config.warehouse, "s3://archive/iceberg");
        assert_eq!(config.token.as_deref(), Some("secret"));
        assert_eq!(config.namespace, "market");
    }

    #[test]
    fn requires_the_warehouse_and_namespace() {
        assert!(
            IcebergConfig::from_vars(&vars(&[("ICEBERG_CATALOG_URI", "http://catalog.test")]))
                .is_err()
        );
        assert!(
            IcebergConfig::from_vars(&vars(&[
                ("ICEBERG_CATALOG_URI", "http://catalog.test"),
                ("ICEBERG_CATALOG_WAREHOUSE", "s3://archive/iceberg"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn optional_configuration_is_only_enabled_by_the_catalog_uri() {
        assert_eq!(IcebergConfig::optional_from_vars(&vars(&[])).unwrap(), None);
        assert!(
            IcebergConfig::optional_from_vars(&vars(&[
                ("ICEBERG_CATALOG_URI", "https://catalog.test"),
                ("ICEBERG_CATALOG_WAREHOUSE", "s3://archive/iceberg"),
                ("ICEBERG_NAMESPACE", "market_data"),
            ]))
            .unwrap()
            .is_some()
        );
    }

    #[test]
    fn rejects_non_http_catalogs_and_non_s3_warehouses() {
        assert!(
            IcebergConfig::from_vars(&vars(&[
                ("ICEBERG_CATALOG_URI", "ftp://catalog.test"),
                ("ICEBERG_CATALOG_WAREHOUSE", "s3://archive/iceberg"),
                ("ICEBERG_NAMESPACE", "market_data"),
            ]))
            .is_err()
        );
        assert!(
            IcebergConfig::from_vars(&vars(&[
                ("ICEBERG_CATALOG_URI", "https://catalog.test"),
                ("ICEBERG_CATALOG_WAREHOUSE", "file:///tmp/warehouse"),
                ("ICEBERG_NAMESPACE", "market_data"),
            ]))
            .is_err()
        );
    }
}
