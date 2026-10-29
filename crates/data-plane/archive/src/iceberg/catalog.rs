use super::{ArchiveIcebergTable, IcebergConfig};
use async_trait::async_trait;
use futures::TryStreamExt;
use iceberg::{
    Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent,
    spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Struct},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use platform_errors::{BpError, BpResult};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::time::sleep;
use tracing::{info, warn};

const MAX_COMMIT_ATTEMPTS: usize = 4;
const COMMIT_BACKOFF: Duration = Duration::from_millis(250);
const SOURCE_FILE_SUMMARY_PREFIX: &str = "source-file:";
const ARCHIVE_BATCH_SUMMARY_KEY: &str = "archive-batch";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IcebergDataFile {
    pub file_path: String,
    pub file_size_in_bytes: i64,
    pub record_count: i64,
}

impl IcebergDataFile {
    pub fn new(file_path: impl Into<String>, file_size_in_bytes: i64, record_count: i64) -> Self {
        Self {
            file_path: file_path.into(),
            file_size_in_bytes,
            record_count,
        }
    }
}

#[async_trait]
pub trait ArchiveCatalog: Send + Sync {
    async fn register_files(
        &self,
        table: ArchiveIcebergTable,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> BpResult<()>;

    async fn registered_data_files(&self, table: ArchiveIcebergTable) -> BpResult<HashSet<String>>;

    /// Lists the live data files in a table.
    ///
    /// Rebuild tooling needs file sizes as well as paths so it can construct a
    /// bounded S3 reader without issuing an extra HEAD request per object.
    async fn data_files(&self, table: ArchiveIcebergTable) -> BpResult<Vec<IcebergDataFile>> {
        Err(BpError::external(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!(
                "archive catalog does not expose data files for {}",
                table.name()
            ),
        )))
    }

    async fn processed_source_files(&self, table: ArchiveIcebergTable)
    -> BpResult<HashSet<String>>;
}

pub struct IcebergCatalog {
    catalog: iceberg_catalog_rest::RestCatalog,
    namespace: NamespaceIdent,
    warehouse: String,
    namespace_name: String,
}

impl IcebergCatalog {
    pub async fn connect(config: &IcebergConfig) -> BpResult<Self> {
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

    async fn ensure_namespace(&self) -> BpResult<()> {
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

    async fn ensure_table(&self, table: ArchiveIcebergTable) -> BpResult<Table> {
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

    async fn registered_paths(&self, table: &Table) -> BpResult<HashSet<String>> {
        let scan = table
            .scan()
            .build()
            .map_err(|error| Self::catalog_error("build Iceberg table scan", error))?;
        let mut tasks = scan
            .plan_files()
            .await
            .map_err(|error| Self::catalog_error("plan Iceberg table scan", error))?;
        let mut paths = HashSet::new();
        while let Some(task) = tasks
            .try_next()
            .await
            .map_err(|error| Self::catalog_error("read Iceberg table scan", error))?
        {
            paths.insert(task.data_file_path().to_string());
        }
        Ok(paths)
    }

    async fn already_registered(
        &self,
        table: &Table,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> BpResult<bool> {
        if let Some(source_files) = source_files {
            let processed = Self::source_files_from_metadata(table);
            return Ok(source_files.iter().all(|file| processed.contains(file)));
        }
        if files.is_empty() {
            return Ok(false);
        }

        let paths = self.registered_paths(table).await?;
        Ok(files.iter().all(|file| paths.contains(&file.file_path)))
    }

    fn data_file(&self, table: &Table, file: &IcebergDataFile) -> BpResult<DataFile> {
        let record_count = u64::try_from(file.record_count).map_err(|_| {
            BpError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Iceberg data file {} has a negative record count {}",
                    file.file_path, file.record_count
                ),
            ))
        })?;
        let file_size_in_bytes = u64::try_from(file.file_size_in_bytes).map_err(|_| {
            BpError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Iceberg data file {} has a negative size {}",
                    file.file_path, file.file_size_in_bytes
                ),
            ))
        })?;

        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file.file_path.clone())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .record_count(record_count)
            .file_size_in_bytes(file_size_in_bytes)
            .build()
            .map_err(|error| {
                BpError::external(std::io::Error::other(format!(
                    "build Iceberg data file: {error}"
                )))
            })
    }

    fn source_files_from_metadata(table: &Table) -> HashSet<String> {
        table
            .metadata()
            .snapshots()
            .flat_map(|snapshot| snapshot.summary().additional_properties.keys())
            .filter_map(|key| key.strip_prefix(SOURCE_FILE_SUMMARY_PREFIX))
            .map(str::to_string)
            .collect()
    }

    fn batch_fingerprint(files: &[IcebergDataFile]) -> String {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for file in files {
            for byte in file.file_path.as_bytes().iter().chain(&[0]) {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        format!("{hash:016x}")
    }

    fn catalog_error(operation: &'static str, error: iceberg::Error) -> BpError {
        BpError::external(std::io::Error::other(format!("{operation}: {error}")))
    }
}

#[async_trait]
impl ArchiveCatalog for IcebergCatalog {
    async fn register_files(
        &self,
        table: ArchiveIcebergTable,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> BpResult<()> {
        if files.is_empty() && source_files.is_none() {
            return Ok(());
        }

        let mut last_error = None;
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            let current = match self.ensure_table(table).await {
                Ok(table) => table,
                Err(error) => {
                    last_error = Some(error);
                    if attempt < MAX_COMMIT_ATTEMPTS {
                        sleep(COMMIT_BACKOFF * attempt as u32).await;
                    }
                    continue;
                }
            };

            match self.already_registered(&current, files, source_files).await {
                Ok(true) => {
                    info!(table = table.name(), "Iceberg commit was already applied");
                    return Ok(());
                }
                Ok(false) => {}
                Err(error) => {
                    last_error = Some(error);
                    if attempt < MAX_COMMIT_ATTEMPTS {
                        sleep(COMMIT_BACKOFF * attempt as u32).await;
                    }
                    continue;
                }
            }

            let data_files = files
                .iter()
                .map(|file| self.data_file(&current, file))
                .collect::<BpResult<Vec<_>>>()?;
            let mut snapshot_properties = HashMap::from([(
                ARCHIVE_BATCH_SUMMARY_KEY.to_string(),
                Self::batch_fingerprint(files),
            )]);
            if let Some(source_files) = source_files {
                snapshot_properties.extend(source_files.iter().map(|file| {
                    (
                        format!("{SOURCE_FILE_SUMMARY_PREFIX}{file}"),
                        "1".to_string(),
                    )
                }));
            }

            let transaction = Transaction::new(&current);
            let action = transaction
                .fast_append()
                .add_data_files(data_files)
                .set_snapshot_properties(snapshot_properties);
            let transaction = action
                .apply(transaction)
                .map_err(|error| Self::catalog_error("prepare Iceberg commit", error))?;

            match transaction.commit(&self.catalog).await {
                Ok(_) => {
                    info!(
                        table = table.name(),
                        files = files.len(),
                        "registered archive files in Iceberg"
                    );
                    return Ok(());
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        table = table.name(),
                        attempt,
                        "Iceberg commit failed"
                    );
                    last_error = Some(Self::catalog_error("commit Iceberg table", error));
                }
            }

            if let Ok(current) = self.ensure_table(table).await
                && self
                    .already_registered(&current, files, source_files)
                    .await
                    .unwrap_or(false)
            {
                return Ok(());
            }

            if attempt < MAX_COMMIT_ATTEMPTS {
                sleep(COMMIT_BACKOFF * attempt as u32).await;
            }
        }

        Err(last_error.unwrap_or_else(|| {
            BpError::external(std::io::Error::other(
                "Iceberg commit failed without a reported error",
            ))
        }))
    }

    async fn registered_data_files(&self, table: ArchiveIcebergTable) -> BpResult<HashSet<String>> {
        let table = self.ensure_table(table).await?;
        self.registered_paths(&table).await
    }

    async fn data_files(&self, table: ArchiveIcebergTable) -> BpResult<Vec<IcebergDataFile>> {
        let table = self.ensure_table(table).await?;
        let scan = table
            .scan()
            .build()
            .map_err(|error| Self::catalog_error("build Iceberg table scan", error))?;
        let mut tasks = scan
            .plan_files()
            .await
            .map_err(|error| Self::catalog_error("plan Iceberg table scan", error))?;
        let mut files = Vec::new();
        while let Some(task) = tasks
            .try_next()
            .await
            .map_err(|error| Self::catalog_error("read Iceberg table scan", error))?
        {
            files.push(IcebergDataFile::new(
                task.data_file_path().to_string(),
                i64::try_from(task.file_size_in_bytes).unwrap_or(i64::MAX),
                i64::try_from(task.record_count.unwrap_or_default()).unwrap_or(i64::MAX),
            ));
        }
        files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
        Ok(files)
    }

    async fn processed_source_files(
        &self,
        table: ArchiveIcebergTable,
    ) -> BpResult<HashSet<String>> {
        let table = self.ensure_table(table).await?;
        Ok(Self::source_files_from_metadata(&table))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_fingerprints_are_stable_and_order_sensitive() {
        let first = IcebergDataFile::new("s3://archive/a.parquet", 1, 2);
        let second = IcebergDataFile::new("s3://archive/b.parquet", 3, 4);

        assert_eq!(
            IcebergCatalog::batch_fingerprint(&[first.clone(), second.clone()]),
            IcebergCatalog::batch_fingerprint(&[first.clone(), second.clone()])
        );
        assert_ne!(
            IcebergCatalog::batch_fingerprint(&[first.clone(), second.clone()]),
            IcebergCatalog::batch_fingerprint(&[second, first])
        );
    }
}
