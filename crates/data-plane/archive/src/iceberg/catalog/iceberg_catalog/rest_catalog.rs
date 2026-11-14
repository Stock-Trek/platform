use crate::iceberg::catalog::{ArchiveCatalog, IcebergDataFile};
use crate::iceberg::{ArchiveIcebergTable, IcebergConfig};
use async_trait::async_trait;
use iceberg::{
    Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent,
    spec::FormatVersion, table::Table,
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use platform_errors::{BuildError, BuildResult};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub struct IcebergCatalog {
    pub catalog: iceberg_catalog_rest::RestCatalog,
    namespace: NamespaceIdent,
    warehouse: String,
    namespace_name: String,
}

impl IcebergCatalog {
    pub async fn connect(config: &IcebergConfig) -> BuildResult<Self> {
        let warehouse = config.warehouse.trim_end_matches('/').to_string();
        let namespace = NamespaceIdent::new(config.namespace.clone());
        let mut props = HashMap::from([
            (
                REST_CATALOG_PROP_URI.to_string(),
                config.catalog_uri.clone(),
            ),
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
        })
    }

    fn table_ident(&self, table: ArchiveIcebergTable) -> TableIdent {
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

    async fn ensure_namespace(&self) -> BuildResult<()> {
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

    pub async fn ensure_table(&self, table: ArchiveIcebergTable) -> BuildResult<Table> {
        let ident = self.table_ident(table);
        match self.catalog.load_table(&ident).await {
            Ok(table) => return Ok(table),
            Err(error) if error.kind() == ErrorKind::TableNotFound => {}
            Err(error) => return Err(Self::catalog_error("load Iceberg table", error)),
        }

        self.ensure_namespace().await?;
        let creation = TableCreation::builder()
            .name(table.name().to_string())
            .location(self.table_location(table))
            .schema(ArchiveIcebergTable::schema(table.kind())?)
            .properties(HashMap::<String, String>::new())
            .format_version(FormatVersion::V2)
            .build();

        match self.catalog.create_table(&self.namespace, creation).await {
            Ok(table) => Ok(table),
            Err(error) if error.kind() == ErrorKind::TableAlreadyExists => self
                .catalog
                .load_table(&ident)
                .await
                .map_err(|error| Self::catalog_error("reload Iceberg table", error)),
            Err(error) => Err(Self::catalog_error("create Iceberg table", error)),
        }
    }

    pub fn catalog_error(operation: &'static str, error: iceberg::Error) -> BuildError {
        BuildError::external(std::io::Error::other(format!("{operation}: {error}")))
    }
}

#[async_trait]
impl ArchiveCatalog for IcebergCatalog {
    async fn register_files(
        &self,
        table: ArchiveIcebergTable,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> BuildResult<()> {
        self.commit_files(table, files, source_files).await
    }

    async fn registered_data_files(
        &self,
        table: ArchiveIcebergTable,
    ) -> BuildResult<HashSet<String>> {
        let table = self.ensure_table(table).await?;
        self.registered_paths(&table).await
    }

    async fn data_files(&self, table: ArchiveIcebergTable) -> BuildResult<Vec<IcebergDataFile>> {
        let table = self.ensure_table(table).await?;
        self.data_files_from_table(&table).await
    }

    async fn processed_source_files(
        &self,
        table: ArchiveIcebergTable,
    ) -> BuildResult<HashSet<String>> {
        let table = self.ensure_table(table).await?;
        Ok(Self::source_files_from_metadata(&table))
    }

    async fn compacted_snapshot(
        &self,
        table: ArchiveIcebergTable,
    ) -> BuildResult<(Vec<IcebergDataFile>, HashSet<String>)> {
        // Load the table once and derive both views from that metadata so the
        // processed set can never describe a newer snapshot than the files.
        let table = self.ensure_table(table).await?;
        let files = self.data_files_from_table(&table).await?;
        let processed = Self::source_files_from_metadata(&table);
        Ok((files, processed))
    }
}
