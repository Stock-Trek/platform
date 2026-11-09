use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use platform_types::url_scheme::UrlScheme;

pub const ICEBERG_CATALOG_URI: &str = "ICEBERG_CATALOG_URI";
pub const ICEBERG_CATALOG_WAREHOUSE: &str = "ICEBERG_CATALOG_WAREHOUSE";
pub const ICEBERG_CATALOG_TOKEN: &str = "ICEBERG_CATALOG_TOKEN";
pub const ICEBERG_NAMESPACE: &str = "ICEBERG_NAMESPACE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergConfig {
    pub catalog_uri: String,
    pub warehouse: String,
    pub token: Option<String>,
    pub namespace: String,
}

impl IcebergConfig {
    pub fn from_vars(vars: &EnvVars) -> BpResult<Self> {
        let catalog_uri = vars.required_url(ICEBERG_CATALOG_URI, &UrlScheme::HTTP)?;
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
    /// Used by starts that do not replay the archive; a partially configured
    /// catalog still fails validation instead of being silently ignored.
    pub fn optional_from_vars(vars: &EnvVars) -> BpResult<Option<Self>> {
        if vars.optional(ICEBERG_CATALOG_URI)?.is_none() {
            return Ok(None);
        }
        Self::from_vars(vars).map(Some)
    }
}
