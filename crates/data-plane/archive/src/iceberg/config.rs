use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};

const ICEBERG_CATALOG_URI: &str = "ICEBERG_CATALOG_URI";
const ICEBERG_CATALOG_WAREHOUSE: &str = "ICEBERG_CATALOG_WAREHOUSE";
const ICEBERG_CATALOG_TOKEN: &str = "ICEBERG_CATALOG_TOKEN";
const ICEBERG_NAMESPACE: &str = "ICEBERG_NAMESPACE";

const DEFAULT_NAMESPACE: &str = "market_data";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergConfig {
    pub catalog_uri: String,
    pub warehouse: String,
    pub token: Option<String>,
    pub namespace: String,
}

impl IcebergConfig {
    pub fn from_vars(vars: &EnvVars, default_warehouse: &str) -> BpResult<Self> {
        let catalog_uri = vars.required_url(ICEBERG_CATALOG_URI, &["http", "https"])?;
        let warehouse = vars
            .optional(ICEBERG_CATALOG_WAREHOUSE)?
            .unwrap_or_else(|| default_warehouse.to_string());
        if !warehouse.starts_with("s3://") {
            return Err(BpError::config_invalid(
                ICEBERG_CATALOG_WAREHOUSE,
                "must be an s3:// URI",
            ));
        }
        let token = vars.optional(ICEBERG_CATALOG_TOKEN)?;
        let namespace = vars
            .optional(ICEBERG_NAMESPACE)?
            .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string());

        Ok(Self {
            catalog_uri,
            warehouse,
            token,
            namespace,
        })
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
        let config = IcebergConfig::from_vars(
            &vars(&[
                ("ICEBERG_CATALOG_URI", "https://catalog.test"),
                ("ICEBERG_CATALOG_WAREHOUSE", "s3://archive/iceberg"),
                ("ICEBERG_CATALOG_TOKEN", "secret"),
                ("ICEBERG_NAMESPACE", "market"),
            ]),
            "s3://archive/default",
        )
        .unwrap();

        assert_eq!(config.catalog_uri, "https://catalog.test");
        assert_eq!(config.warehouse, "s3://archive/iceberg");
        assert_eq!(config.token.as_deref(), Some("secret"));
        assert_eq!(config.namespace, "market");
    }

    #[test]
    fn falls_back_to_the_service_warehouse_and_namespace() {
        let config = IcebergConfig::from_vars(
            &vars(&[("ICEBERG_CATALOG_URI", "http://catalog.test")]),
            "s3://archive/iceberg",
        )
        .unwrap();

        assert_eq!(config.warehouse, "s3://archive/iceberg");
        assert_eq!(config.namespace, "market_data");
        assert_eq!(config.token, None);
    }

    #[test]
    fn rejects_non_http_catalogs_and_non_s3_warehouses() {
        assert!(
            IcebergConfig::from_vars(
                &vars(&[("ICEBERG_CATALOG_URI", "ftp://catalog.test")]),
                "s3://archive/iceberg"
            )
            .is_err()
        );
        assert!(
            IcebergConfig::from_vars(
                &vars(&[
                    ("ICEBERG_CATALOG_URI", "https://catalog.test"),
                    ("ICEBERG_CATALOG_WAREHOUSE", "file:///tmp/warehouse"),
                ]),
                "s3://archive/iceberg"
            )
            .is_err()
        );
    }
}
