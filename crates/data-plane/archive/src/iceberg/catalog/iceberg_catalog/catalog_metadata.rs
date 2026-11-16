use crate::iceberg::catalog::IcebergDataFile;
use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::{ContentHasher, SourceBatch};
use iceberg::table::Table;
use platform_errors::{PlatformError, PlatformResult};
use std::collections::{HashMap, HashSet};

const SOURCE_FILE_SUMMARY_PREFIX: &str = "source-file:";
const ARCHIVE_BATCH_SUMMARY_KEY: &str = "archive-batch";
const SOURCE_BATCH_SUMMARY_KEY: &str = "source-batch";
const SOURCE_CLAIM_COUNT_KEY: &str = "source-claim-count";
const CLAIM_VERSION_KEY: &str = "archive-claim-version";
const CLAIM_VERSION: &str = "1";

impl IcebergCatalog {
    pub fn source_files_from_metadata(table: &Table) -> PlatformResult<HashSet<String>> {
        let mut sources = HashSet::new();
        for snapshot in table.metadata().snapshots() {
            let properties = &snapshot.summary().additional_properties;
            let source_files = Self::snapshot_source_files(properties);
            let has_claim = properties.contains_key(CLAIM_VERSION_KEY)
                || properties.contains_key(SOURCE_BATCH_SUMMARY_KEY)
                || properties.contains_key(SOURCE_CLAIM_COUNT_KEY)
                || !source_files.is_empty();
            if !has_claim {
                continue;
            }
            Self::validate_claim(properties, &source_files)?;
            sources.extend(source_files);
        }
        Ok(sources)
    }

    pub fn snapshot_properties(
        files: &[IcebergDataFile],
        source_files: Option<&[String]>,
    ) -> PlatformResult<HashMap<String, String>> {
        let mut properties = HashMap::from([(
            ARCHIVE_BATCH_SUMMARY_KEY.to_string(),
            Self::batch_fingerprint(files),
        )]);
        let Some(source_files) = source_files else {
            return Ok(properties);
        };
        if source_files.is_empty() {
            return Err(PlatformError::config_invalid(
                "archive source batch",
                "must contain at least one source object",
            ));
        }
        let mut sources = source_files.to_vec();
        sources.sort();
        sources.dedup();
        properties.insert(CLAIM_VERSION_KEY.to_string(), CLAIM_VERSION.to_string());
        properties.insert(
            SOURCE_BATCH_SUMMARY_KEY.to_string(),
            Self::source_batch_digest(&sources)?,
        );
        properties.insert(
            SOURCE_CLAIM_COUNT_KEY.to_string(),
            sources.len().to_string(),
        );
        properties.extend(sources.into_iter().map(|file| {
            (
                format!("{SOURCE_FILE_SUMMARY_PREFIX}{file}"),
                "1".to_string(),
            )
        }));
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

    pub fn source_batch_digest(source_files: &[String]) -> PlatformResult<String> {
        if source_files.is_empty() {
            return Err(PlatformError::config_invalid(
                "archive source batch",
                "must contain at least one source object",
            ));
        }
        Ok(SourceBatch::digest_files(source_files))
    }

    fn snapshot_source_files(properties: &HashMap<String, String>) -> Vec<String> {
        let mut sources = properties
            .keys()
            .filter_map(|key| key.strip_prefix(SOURCE_FILE_SUMMARY_PREFIX))
            .map(str::to_string)
            .collect::<Vec<_>>();
        sources.sort();
        sources
    }

    fn validate_claim(
        properties: &HashMap<String, String>,
        source_files: &[String],
    ) -> PlatformResult<()> {
        let version = properties
            .get(CLAIM_VERSION_KEY)
            .ok_or_else(|| Self::claim_error("is missing a claim version"))?;
        if version != CLAIM_VERSION {
            return Err(Self::claim_error(format!(
                "uses unsupported claim version {version}"
            )));
        }
        let source_batch = properties
            .get(SOURCE_BATCH_SUMMARY_KEY)
            .ok_or_else(|| Self::claim_error("is missing a source batch digest"))?;
        let count = properties
            .get(SOURCE_CLAIM_COUNT_KEY)
            .ok_or_else(|| Self::claim_error("is missing a source claim count"))?
            .parse::<usize>()
            .map_err(|_| Self::claim_error("has an invalid source claim count"))?;
        if count == 0 || count != source_files.len() {
            return Err(Self::claim_error(format!(
                "declares {count} source claims but carries {}",
                source_files.len()
            )));
        }
        if source_batch != &Self::source_batch_digest(source_files)? {
            return Err(Self::claim_error(
                "has a source batch digest that does not match its source files",
            ));
        }
        if properties.get(ARCHIVE_BATCH_SUMMARY_KEY).is_none() {
            return Err(Self::claim_error("is missing an output fingerprint"));
        }
        Ok(())
    }

    fn claim_error(reason: impl std::fmt::Display) -> PlatformError {
        PlatformError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("archive claim metadata {reason}"),
        ))
    }
}
