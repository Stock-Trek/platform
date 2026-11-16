use crate::iceberg::catalog::iceberg_catalog::retention::ClaimRetention;
use crate::iceberg::catalog::{ArchiveCatalog, IcebergDataFile};
use crate::iceberg::{
    ArchiveIcebergTable, ArchiveRegistration, CommitContext, IcebergConfig, RegisterOutcome,
};
use async_trait::async_trait;
use iceberg::{
    Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent,
    spec::FormatVersion, table::Table,
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use platform_errors::{PlatformError, PlatformResult};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

const CATALOG_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

pub struct IcebergCatalog {
    pub(super) catalog: iceberg_catalog_rest::RestCatalog,
    pub(super) namespace: NamespaceIdent,
    warehouse: String,
    namespace_name: String,
    pub(super) catalog_uri: String,
    pub(super) prefix: Option<String>,
    pub(super) token: Option<String>,
    pub(super) client: reqwest::Client,
}

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

    fn table_location(&self, table: ArchiveIcebergTable) -> String {
        format!(
            "{}/{}/{}",
            self.warehouse,
            self.namespace_name.trim_matches('/'),
            table.name()
        )
    }

    async fn ensure_namespace(&self) -> PlatformResult<()> {
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

    pub async fn ensure_table(&self, table: ArchiveIcebergTable) -> PlatformResult<Table> {
        let ident = self.table_ident(table);
        match self.catalog.load_table(&ident).await {
            Ok(table) => return self.ensure_required_properties(table).await,
            Err(error) if error.kind() == ErrorKind::TableNotFound => {}
            Err(error) => return Err(Self::catalog_error("load Iceberg table", error)),
        }

        self.ensure_namespace().await?;
        let properties = ClaimRetention::properties();
        let creation = TableCreation::builder()
            .name(table.name().to_string())
            .location(self.table_location(table))
            .schema(ArchiveIcebergTable::schema(table.kind())?)
            .properties(properties)
            .format_version(FormatVersion::V2)
            .build();

        match self.catalog.create_table(&self.namespace, creation).await {
            Ok(table) => Ok(table),
            Err(error) if error.kind() == ErrorKind::TableAlreadyExists => {
                let table = self
                    .catalog
                    .load_table(&ident)
                    .await
                    .map_err(|error| Self::catalog_error("reload Iceberg table", error))?;
                self.ensure_required_properties(table).await
            }
            Err(error) => Err(Self::catalog_error("create Iceberg table", error)),
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
        self.registered_paths(&table).await
    }

    async fn data_files(&self, table: ArchiveIcebergTable) -> PlatformResult<Vec<IcebergDataFile>> {
        let table = self.ensure_table(table).await?;
        self.data_files_from_table(&table).await
    }

    async fn processed_source_files(
        &self,
        table: ArchiveIcebergTable,
    ) -> PlatformResult<HashSet<String>> {
        let table = self.ensure_table(table).await?;
        Ok(Self::source_files_from_metadata(&table))
    }

    async fn compacted_snapshot(
        &self,
        table: ArchiveIcebergTable,
    ) -> PlatformResult<(Vec<IcebergDataFile>, HashSet<String>)> {
        // Load the table once and derive both views from that metadata so the
        // processed set can never describe a newer snapshot than the files.
        let table = self.ensure_table(table).await?;
        let files = self.data_files_from_table(&table).await?;
        let processed = Self::source_files_from_metadata(&table);
        Ok((files, processed))
    }
}
