use crate::iceberg::catalog::IcebergDataFile;
use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::{ContentHasher, SourceBatch};
use iceberg::table::Table;
use platform_errors::{PlatformError, PlatformResult};
use std::collections::{HashMap, HashSet};

const SOURCE_FILE_SUMMARY_PREFIX: &str = "source-file:";
const ARCHIVE_BATCH_SUMMARY_KEY: &str = "archive-batch";
const SOURCE_BATCH_SUMMARY_KEY: &str = "source-batch";

impl IcebergCatalog {
    pub fn source_files_from_metadata(table: &Table) -> HashSet<String> {
        table
            .metadata()
            .snapshots()
            .flat_map(|snapshot| snapshot.summary().additional_properties.keys())
            .filter_map(|key| key.strip_prefix(SOURCE_FILE_SUMMARY_PREFIX))
            .map(str::to_string)
            .collect()
    }

    pub fn batch_registered_in_metadata(table: &Table, files: &[IcebergDataFile]) -> bool {
        if files.is_empty() {
            return false;
        }
        let fingerprint = Self::batch_fingerprint(files);
        table.metadata().snapshots().any(|snapshot| {
            snapshot
                .summary()
                .additional_properties
                .get(ARCHIVE_BATCH_SUMMARY_KEY)
                .is_some_and(|value| value == &fingerprint)
        })
    }

    pub fn snapshot_properties(
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> PlatformResult<HashMap<String, String>> {
        let mut properties = HashMap::from([(
            ARCHIVE_BATCH_SUMMARY_KEY.to_string(),
            Self::batch_fingerprint(files),
        )]);
        if let Some(source_files) = source_files {
            properties.insert(
                SOURCE_BATCH_SUMMARY_KEY.to_string(),
                Self::source_batch_digest(source_files)?,
            );
            properties.extend(source_files.iter().map(|file| {
                (
                    format!("{SOURCE_FILE_SUMMARY_PREFIX}{file}"),
                    "1".to_string(),
                )
            }));
        }
        Ok(properties)
    }

    pub fn batch_fingerprint(files: &[IcebergDataFile]) -> String {
        let mut hasher = ContentHasher::new();
        for file in files {
            hasher.update(file.file_path.as_bytes());
            hasher.update([0_u8]);
        }
        hasher.finish().to_string()
    }
}

impl IcebergCatalog {
    pub fn source_batch_digest(source_files: &[String]) -> PlatformResult<String> {
        if source_files.is_empty() {
            return Err(PlatformError::config_invalid(
                "archive source batch",
                "must contain at least one source object",
            ));
        }
        Ok(SourceBatch::digest_files(source_files))
    }
}
