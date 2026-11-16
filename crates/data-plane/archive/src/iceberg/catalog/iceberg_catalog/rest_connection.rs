use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::{ArchiveIcebergTable, IcebergConfig};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use platform_errors::{PlatformError, PlatformResult};
use std::{collections::HashMap, sync::Arc, time::Duration};

const CATALOG_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

impl IcebergCatalog {
    pub async fn connect(config: &IcebergConfig) -> PlatformResult<Self> {
        let (catalog_uri, prefix) =
            Self::resolve_endpoint(&config.catalog_uri, config.token.as_deref()).await?;
        let warehouse = config.warehouse.trim_end_matches('/').to_string();
        let namespace = NamespaceIdent::new(config.namespace.clone());
        let mut props = HashMap::from([
            (REST_CATALOG_PROP_URI.to_string(), catalog_uri.clone()),
            (REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone()),
        ]);
        if let Some(token) = &config.token {
            props.insert("token".to_string(), token.clone());
        }

        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }))
            .load("platform_archive", props)
            .await
            .map_err(|error| Self::catalog_error("build Iceberg REST catalog", error))?;

        Ok(Self {
            catalog,
            namespace,
            warehouse,
            namespace_name: config.namespace.clone(),
            catalog_uri,
            prefix,
            token: config.token.clone(),
            client: Self::http_client(),
            claim_index_cache: Default::default(),
        })
    }

    pub(super) fn http_client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(CATALOG_HTTP_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    }

    async fn resolve_endpoint(
        catalog_uri: &str,
        token: Option<&str>,
    ) -> PlatformResult<(String, Option<String>)> {
        let url = format!("{}/v1/config", catalog_uri.trim_end_matches('/'));
        let mut request = Self::http_client().get(&url);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(|error| {
            PlatformError::external(std::io::Error::other(format!(
                "read Iceberg catalog configuration from {url}: {error}"
            )))
        })?;
        if !response.status().is_success() {
            return Err(PlatformError::external(std::io::Error::other(format!(
                "read Iceberg catalog configuration from {url}: {}",
                response.status()
            ))));
        }
        let config: serde_json::Value = response.json().await.map_err(|error| {
            PlatformError::external(std::io::Error::other(format!(
                "decode Iceberg catalog configuration from {url}: {error}"
            )))
        })?;
        let defaults = config.get("defaults");
        let overrides = config.get("overrides");
        let uri = overrides
            .and_then(|value| value.get("uri"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| catalog_uri.to_string());
        let prefix = overrides
            .and_then(|value| value.get("prefix"))
            .or_else(|| defaults.and_then(|value| value.get("prefix")))
            .and_then(serde_json::Value::as_str)
            .map(|prefix| prefix.trim_matches('/').to_string())
            .filter(|prefix| !prefix.is_empty());
        Ok((uri, prefix))
    }

    pub(super) fn table_ident(&self, table: ArchiveIcebergTable) -> TableIdent {
        TableIdent::new(self.namespace.clone(), table.name().to_string())
    }

    pub(super) fn table_location(&self, table: ArchiveIcebergTable) -> String {
        format!(
            "{}/{}/{}",
            self.warehouse,
            self.namespace_name.trim_matches('/'),
            table.name()
        )
    }

    pub(super) async fn ensure_namespace(&self) -> PlatformResult<()> {
        if self
            .catalog
            .namespace_exists(&self.namespace)
            .await
            .map_err(|error| Self::catalog_error("check Iceberg namespace", error))?
        {
            return Ok(());
        }

        match self
            .catalog
            .create_namespace(&self.namespace, HashMap::new())
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NamespaceAlreadyExists => Ok(()),
            Err(error) => Err(Self::catalog_error("create Iceberg namespace", error)),
        }
    }
}
