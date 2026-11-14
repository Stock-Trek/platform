use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::{ArchiveIcebergTable, catalog::IcebergDataFile};
use iceberg::{
    spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Struct},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
};
use platform_errors::{BuildError, BuildResult};
use platform_time::RetryPolicy;
use std::time::Duration;
use tracing::{info, warn};

const MAX_COMMIT_ATTEMPTS: usize = 4;
const COMMIT_BACKOFF: Duration = Duration::from_millis(250);
const COMMIT_MAX_BACKOFF: Duration = Duration::from_secs(1);

enum CommitOutcome {
    Applied,
    Retry(BuildError),
}

impl IcebergCatalog {
    pub async fn commit_files(
        &self,
        table: ArchiveIcebergTable,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> BuildResult<()> {
        if files.is_empty() && source_files.is_none() {
            return Ok(());
        }

        let retry = RetryPolicy::linear(MAX_COMMIT_ATTEMPTS, COMMIT_BACKOFF, COMMIT_MAX_BACKOFF);
        let mut last_error = None;
        for attempt in 1..=retry.max_attempts() {
            match self
                .commit_attempt(table, files, source_files, attempt)
                .await?
            {
                CommitOutcome::Applied => return Ok(()),
                CommitOutcome::Retry(error) => last_error = Some(error),
            }
            if retry.should_retry(attempt) {
                tokio::time::sleep(retry.delay(attempt - 1)).await;
            }
        }

        Err(last_error.unwrap_or_else(|| {
            BuildError::external(std::io::Error::other(
                "Iceberg commit failed without a reported error",
            ))
        }))
    }

    async fn commit_attempt(
        &self,
        table: ArchiveIcebergTable,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
        attempt: usize,
    ) -> BuildResult<CommitOutcome> {
        let current = match self.ensure_table(table).await {
            Ok(current) => current,
            Err(error) => return Ok(CommitOutcome::Retry(error)),
        };

        match self.already_registered(&current, files, source_files).await {
            Ok(true) => {
                info!(table = table.name(), "Iceberg commit was already applied");
                return Ok(CommitOutcome::Applied);
            }
            Ok(false) => {}
            Err(error) => return Ok(CommitOutcome::Retry(error)),
        }

        let data_files = files
            .iter()
            .map(|file| self.data_file(&current, file))
            .collect::<BuildResult<Vec<_>>>()?;
        let snapshot_properties = Self::snapshot_properties(files, source_files);

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
                Ok(CommitOutcome::Applied)
            }
            Err(error) => {
                warn!(
                    error = %error,
                    table = table.name(),
                    attempt,
                    "Iceberg commit failed"
                );
                let error = Self::catalog_error("commit Iceberg table", error);
                if let Ok(current) = self.ensure_table(table).await
                    && self
                        .already_registered(&current, files, source_files)
                        .await
                        .unwrap_or(false)
                {
                    return Ok(CommitOutcome::Applied);
                }
                Ok(CommitOutcome::Retry(error))
            }
        }
    }

    async fn already_registered(
        &self,
        table: &Table,
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> BuildResult<bool> {
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

    fn data_file(&self, table: &Table, file: &IcebergDataFile) -> BuildResult<DataFile> {
        let record_count = u64::try_from(file.record_count).map_err(|_| {
            BuildError::external(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Iceberg data file {} has a negative record count {}",
                    file.file_path, file.record_count
                ),
            ))
        })?;
        let file_size_in_bytes = u64::try_from(file.file_size_in_bytes).map_err(|_| {
            BuildError::external(std::io::Error::new(
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
                BuildError::external(std::io::Error::other(format!(
                    "build Iceberg data file: {error}"
                )))
            })
    }
}
