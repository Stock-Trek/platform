use super::super::super::IcebergDataFile;
use super::super::IcebergCatalog;
use iceberg::table::Table;
use std::collections::{HashMap, HashSet};

const SOURCE_FILE_SUMMARY_PREFIX: &str = "source-file:";
const ARCHIVE_BATCH_SUMMARY_KEY: &str = "archive-batch";

impl IcebergCatalog {
    pub(in crate::iceberg::catalog::iceberg_catalog) fn source_files_from_metadata(
        table: &Table,
    ) -> HashSet<String> {
        table
            .metadata()
            .snapshots()
            .flat_map(|snapshot| snapshot.summary().additional_properties.keys())
            .filter_map(|key| key.strip_prefix(SOURCE_FILE_SUMMARY_PREFIX))
            .map(str::to_string)
            .collect()
    }

    pub(in crate::iceberg::catalog::iceberg_catalog) fn snapshot_properties(
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

    pub(super) fn batch_fingerprint(files: &[IcebergDataFile]) -> String {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for file in files {
            for byte in file.file_path.as_bytes().iter().chain(&[0]) {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        format!("{hash:016x}")
    }
}
