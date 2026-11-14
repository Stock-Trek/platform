use crate::ContentHasher;
use crate::iceberg::catalog::IcebergDataFile;
use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use iceberg::table::Table;
use std::collections::{HashMap, HashSet};

const SOURCE_FILE_SUMMARY_PREFIX: &str = "source-file:";
const ARCHIVE_BATCH_SUMMARY_KEY: &str = "archive-batch";

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

    pub fn snapshot_properties(
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> HashMap<String, String> {
        let mut properties = HashMap::from([(
            ARCHIVE_BATCH_SUMMARY_KEY.to_string(),
            Self::batch_fingerprint(files),
        )]);
        if let Some(source_files) = source_files {
            properties.extend(source_files.iter().map(|file| {
                (
                    format!("{SOURCE_FILE_SUMMARY_PREFIX}{file}"),
                    "1".to_string(),
                )
            }));
        }
        properties
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
