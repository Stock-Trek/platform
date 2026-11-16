use super::claim_index::ClaimIndexCache;
use super::claim_index_page::INDEX_FORMAT_VERSION;
use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use bytes::Bytes;
use iceberg::table::Table;
use platform_errors::{PlatformError, PlatformResult};
use platform_metrics::MarketDataMetrics;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const INDEX_BLOCK_MAX_BYTES: usize = 2 * 1024 * 1024;
const INDEX_BLOCK_TARGET_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct IndexBlockRef {
    pub(super) location: String,
    pub(super) digest: String,
    pub(super) source_first: Option<String>,
    pub(super) source_last: Option<String>,
    pub(super) output_first: Option<String>,
    pub(super) output_last: Option<String>,
}

impl IndexBlockRef {
    pub(super) fn may_contain_source(&self, source: &str) -> bool {
        Self::in_range(
            source,
            self.source_first.as_deref(),
            self.source_last.as_deref(),
        )
    }

    pub(super) fn may_contain_output(&self, output: &str) -> bool {
        Self::in_range(
            output,
            self.output_first.as_deref(),
            self.output_last.as_deref(),
        )
    }

    fn in_range(value: &str, first: Option<&str>, last: Option<&str>) -> bool {
        first.is_some_and(|first| first <= value) && last.is_some_and(|last| value <= last)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ClaimIndexBlock {
    format_version: u32,
    sources: Vec<String>,
    outputs: Vec<String>,
}

impl ClaimIndexBlock {
    pub(super) async fn write_all(
        table: &Table,
        sources: Vec<String>,
        outputs: Vec<String>,
    ) -> PlatformResult<Vec<IndexBlockRef>> {
        let mut references = Vec::new();
        for chunk in Self::chunks(sources) {
            references.push(Self::write(table, chunk, Vec::new()).await?);
        }
        for chunk in Self::chunks(outputs) {
            references.push(Self::write(table, Vec::new(), chunk).await?);
        }
        Ok(references)
    }

    pub(super) async fn read(
        table: &Table,
        reference: &IndexBlockRef,
        cache: Option<&ClaimIndexCache>,
    ) -> PlatformResult<Self> {
        if let Some(block) = cache.and_then(|cache| cache.get_block(&reference.digest)) {
            MarketDataMetrics::record_index_cache_hit("block");
            return Ok((*block).clone());
        }
        MarketDataMetrics::record_index_cache_miss("block");
        let bytes = table
            .file_io()
            .new_input(&reference.location)
            .map_err(|error| Self::error(format!("could not open index block: {error}")))?
            .read()
            .await
            .map_err(|error| Self::error(format!("could not read index block: {error}")))?;
        if bytes.len() > INDEX_BLOCK_MAX_BYTES {
            return Err(Self::error("index block exceeds the size limit"));
        }
        if Self::digest(&bytes) != reference.digest {
            return Err(Self::error(
                "index block digest does not match its reference",
            ));
        }
        let block: Self = serde_json::from_slice(&bytes)
            .map_err(|error| Self::error(format!("could not decode index block: {error}")))?;
        if block.format_version != INDEX_FORMAT_VERSION {
            return Err(Self::error(
                "index block uses an unsupported format version",
            ));
        }
        if !Self::sorted(&block.sources) || !Self::sorted(&block.outputs) {
            return Err(Self::error("index block entries are not sorted"));
        }
        MarketDataMetrics::record_index_block_read();
        if let Some(cache) = cache {
            cache.insert_block(reference.digest.clone(), block.clone());
        }
        Ok(block)
    }

    pub(super) fn contains_source(&self, source: &str) -> bool {
        self.sources
            .binary_search_by(|entry| entry.as_str().cmp(source))
            .is_ok()
    }

    pub(super) fn contains_output(&self, output: &str) -> bool {
        self.outputs
            .binary_search_by(|entry| entry.as_str().cmp(output))
            .is_ok()
    }

    pub(super) fn sources_for_load(&self) -> Vec<String> {
        self.sources.clone()
    }

    pub(super) fn outputs_for_load(&self) -> Vec<String> {
        self.outputs.clone()
    }

    async fn write(
        table: &Table,
        sources: Vec<String>,
        outputs: Vec<String>,
    ) -> PlatformResult<IndexBlockRef> {
        let block = Self {
            format_version: INDEX_FORMAT_VERSION,
            sources,
            outputs,
        };
        let bytes = serde_json::to_vec(&block)
            .map_err(|error| Self::error(format!("could not encode an index block: {error}")))?;
        if bytes.len() > INDEX_BLOCK_MAX_BYTES {
            return Err(Self::error("index block exceeds the size limit"));
        }
        let digest = Self::digest(&bytes);
        let location = format!(
            "{}/archive-index-block-{digest}.json",
            Self::metadata_directory(table)?
        );
        table
            .file_io()
            .new_output(&location)
            .map_err(|error| Self::error(format!("could not open index block: {error}")))?
            .write(Bytes::from(bytes))
            .await
            .map_err(|error| Self::error(format!("could not write index block: {error}")))?;
        Ok(IndexBlockRef {
            location,
            digest,
            source_first: block.sources.first().cloned(),
            source_last: block.sources.last().cloned(),
            output_first: block.outputs.first().cloned(),
            output_last: block.outputs.last().cloned(),
        })
    }

    fn chunks(mut entries: Vec<String>) -> Vec<Vec<String>> {
        entries.sort();
        entries.dedup();
        let mut chunks = Vec::new();
        let mut current = Vec::new();
        let mut current_bytes = 0_usize;
        for entry in entries {
            let entry_bytes = entry.len().saturating_add(8);
            if !current.is_empty()
                && current_bytes.saturating_add(entry_bytes) > INDEX_BLOCK_TARGET_BYTES
            {
                chunks.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current_bytes = current_bytes.saturating_add(entry_bytes);
            current.push(entry);
        }
        if !current.is_empty() {
            chunks.push(current);
        }
        chunks
    }

    fn sorted(entries: &[String]) -> bool {
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

    fn digest(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn error(reason: impl std::fmt::Display) -> PlatformError {
        PlatformError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("archive claim index block {reason}"),
        ))
    }
}
