use super::claim_index::ClaimIndexCache;
use super::claim_index_block::{ClaimIndexBlock, IndexBlockRef};
use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use bytes::Bytes;
use iceberg::table::Table;
use platform_errors::{PlatformError, PlatformResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

pub(super) const INDEX_FORMAT_VERSION: u32 = 2;
const INDEX_PAGE_MAX_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct IndexParent {
    pub(super) location: String,
    pub(super) digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ClaimIndexPage {
    pub(super) format_version: u32,
    pub(super) parent: Option<IndexParent>,
    pub(super) sources: Vec<String>,
    pub(super) outputs: Vec<String>,
    pub(super) blocks: Vec<IndexBlockRef>,
    pub(super) archive_batch: String,
}

impl ClaimIndexPage {
    pub(super) fn from_claims(
        parent: Option<IndexParent>,
        sources: HashSet<String>,
        outputs: HashSet<String>,
    ) -> Self {
        Self::new(
            parent,
            Self::sorted_set(sources),
            Self::sorted_set(outputs),
            Vec::new(),
        )
    }

    pub(super) async fn write_consolidated(
        table: &Table,
        sources: HashSet<String>,
        outputs: HashSet<String>,
        depth: u64,
    ) -> PlatformResult<HashMap<String, String>> {
        let blocks =
            ClaimIndexBlock::write_all(table, Self::sorted_set(sources), Self::sorted_set(outputs))
                .await?;
        Self::new(None, Vec::new(), Vec::new(), blocks)
            .write(table, depth)
            .await
    }

    pub(super) async fn read(
        table: &Table,
        parent: &IndexParent,
        cache: Option<&ClaimIndexCache>,
    ) -> PlatformResult<Self> {
        if let Some(page) = cache.and_then(|cache| cache.get_page(&parent.digest)) {
            return Ok((*page).clone());
        }
        let bytes = table
            .file_io()
            .new_input(&parent.location)
            .map_err(|error| Self::error(format!("could not open index page: {error}")))?
            .read()
            .await
            .map_err(|error| Self::error(format!("could not read index page: {error}")))?;
        if bytes.len() > INDEX_PAGE_MAX_BYTES {
            return Err(Self::error("index page exceeds the size limit"));
        }
        if Self::digest(&bytes) != parent.digest {
            return Err(Self::error("index page digest does not match its parent"));
        }
        let page: Self = serde_json::from_slice(&bytes)
            .map_err(|error| Self::error(format!("could not decode index page: {error}")))?;
        page.validate()?;
        if let Some(cache) = cache {
            cache.insert_page(parent.digest.clone(), page.clone());
        }
        Ok(page)
    }

    pub(super) async fn write(
        self,
        table: &Table,
        depth: u64,
    ) -> PlatformResult<HashMap<String, String>> {
        self.validate()?;
        let bytes = serde_json::to_vec(&self)
            .map_err(|error| Self::error(format!("could not encode an index page: {error}")))?;
        if bytes.len() > INDEX_PAGE_MAX_BYTES {
            return Err(Self::error("index page exceeds the size limit"));
        }
        let digest = Self::digest(&bytes);
        let location = format!(
            "{}/archive-index-{digest}.json",
            Self::metadata_directory(table)?
        );
        table
            .file_io()
            .new_output(&location)
            .map_err(|error| Self::error(format!("could not open index page: {error}")))?
            .write(Bytes::from(bytes))
            .await
            .map_err(|error| Self::error(format!("could not write index page: {error}")))?;
        Ok(HashMap::from([
            (super::claim_index::INDEX_HEAD_KEY.to_string(), location),
            (
                super::claim_index::INDEX_HEAD_DIGEST_KEY.to_string(),
                digest,
            ),
            (
                super::claim_index::INDEX_FORMAT_VERSION_KEY.to_string(),
                INDEX_FORMAT_VERSION.to_string(),
            ),
            (
                super::claim_index::INDEX_DEPTH_KEY.to_string(),
                depth.to_string(),
            ),
        ]))
    }

    pub(super) fn format_version() -> u32 {
        INDEX_FORMAT_VERSION
    }

    fn new(
        parent: Option<IndexParent>,
        sources: Vec<String>,
        outputs: Vec<String>,
        blocks: Vec<IndexBlockRef>,
    ) -> Self {
        let archive_batch = Self::paths_fingerprint(
            outputs
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .as_slice(),
        );
        Self {
            format_version: INDEX_FORMAT_VERSION,
            parent,
            sources,
            outputs,
            blocks,
            archive_batch,
        }
    }

    fn validate(&self) -> PlatformResult<()> {
        if self.format_version != INDEX_FORMAT_VERSION {
            return Err(Self::error("index page uses an unsupported format version"));
        }
        if !Self::is_sorted(&self.sources) || !Self::is_sorted(&self.outputs) {
            return Err(Self::error("index page entries are not sorted"));
        }
        if self.archive_batch
            != Self::paths_fingerprint(
                self.outputs
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .as_slice(),
            )
        {
            return Err(Self::error("index page output fingerprint does not match"));
        }
        Ok(())
    }

    fn sorted_set(entries: HashSet<String>) -> Vec<String> {
        let mut entries = entries.into_iter().collect::<Vec<_>>();
        entries.sort();
        entries
    }

    fn is_sorted(entries: &[String]) -> bool {
        entries.windows(2).all(|pair| pair[0] <= pair[1])
    }

    fn metadata_directory(table: &Table) -> PlatformResult<String> {
        let location = table.metadata().metadata_location().map_err(|error| {
            IcebergCatalog::catalog_error("read Iceberg metadata directory", error)
        })?;
        let location = location.trim_end_matches('/');
        if let Some((parent, file)) = location.rsplit_once('/')
            && file.ends_with(".json")
        {
            return Ok(parent.to_string());
        }
        Ok(location.to_string())
    }

    fn paths_fingerprint(paths: &[&str]) -> String {
        let mut paths = paths.to_vec();
        paths.sort();
        let mut hasher = Sha256::new();
        for path in paths {
            hasher.update(path.as_bytes());
            hasher.update([0_u8]);
        }
        Self::hex(hasher.finalize().as_slice())
    }

    fn digest(bytes: &[u8]) -> String {
        Self::hex(Sha256::digest(bytes).as_slice())
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn error(reason: impl std::fmt::Display) -> PlatformError {
        PlatformError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("archive claim index {reason}"),
        ))
    }
}
