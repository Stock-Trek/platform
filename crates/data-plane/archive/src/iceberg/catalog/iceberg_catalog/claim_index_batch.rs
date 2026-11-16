use super::claim_index::ClaimIndexCache;
use super::claim_index_page::INDEX_FORMAT_VERSION;
use crate::ContentHasher;
use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use bytes::Bytes;
use iceberg::table::Table;
use platform_errors::{PlatformError, PlatformResult};
use platform_metrics::MarketDataMetrics;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

const BATCH_BLOCK_MAX_BYTES: usize = 2 * 1024 * 1024;
const BATCH_BLOCK_TARGET_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ClaimBatch {
    pub(super) digest: String,
    pub(super) sources: Vec<String>,
    pub(super) outputs: Vec<String>,
}

impl ClaimBatch {
    pub(super) fn from_claims(sources: HashSet<String>, outputs: HashSet<String>) -> Self {
        let sources = Self::sorted(sources);
        let outputs = Self::sorted(outputs);
        let digest = Self::digest(&outputs);
        Self {
            digest,
            sources,
            outputs,
        }
    }

    pub(super) fn is_live(&self, live_outputs: &HashSet<String>) -> bool {
        self.outputs.is_empty()
            || self
                .outputs
                .iter()
                .any(|output| live_outputs.contains(output))
    }

    pub(super) fn validate(&self) -> PlatformResult<()> {
        if !Self::sorted_ok(&self.sources) || !Self::sorted_ok(&self.outputs) {
            return Err(Self::error("batch entries are not sorted"));
        }
        if self.digest != Self::digest(&self.outputs) {
            return Err(Self::error("batch digest does not match its outputs"));
        }
        Ok(())
    }

    fn digest(outputs: &[String]) -> String {
        let mut hasher = ContentHasher::new();
        for output in outputs {
            hasher.update(output.as_bytes());
            hasher.update([0_u8]);
        }
        hasher.finish().to_string()
    }

    fn sorted(entries: HashSet<String>) -> Vec<String> {
        let mut entries = entries.into_iter().collect::<Vec<_>>();
        entries.sort();
        entries
    }

    fn sorted_ok(entries: &[String]) -> bool {
        entries.windows(2).all(|pair| pair[0] <= pair[1])
    }

    fn error(reason: impl std::fmt::Display) -> PlatformError {
        PlatformError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("archive claim batch {reason}"),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct IndexBatchBlockRef {
    pub(super) location: String,
    pub(super) digest: String,
    pub(super) batch_first: String,
    pub(super) batch_last: String,
}

impl IndexBatchBlockRef {
    #[cfg(test)]
    pub(super) fn may_contain_batch(&self, digest: &str) -> bool {
        self.batch_first.as_str() <= digest && digest <= self.batch_last.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ClaimIndexBatchBlock {
    format_version: u32,
    batches: Vec<ClaimBatch>,
}

impl ClaimIndexBatchBlock {
    pub(super) async fn write_all(
        table: &Table,
        mut batches: Vec<ClaimBatch>,
    ) -> PlatformResult<Vec<IndexBatchBlockRef>> {
        batches.sort_by(|left, right| left.digest.cmp(&right.digest));
        batches.dedup_by(|left, right| left.digest == right.digest);
        let mut references = Vec::new();
        for chunk in Self::chunks(batches) {
            references.push(Self::write(table, chunk).await?);
        }
        Ok(references)
    }

    pub(super) async fn read(
        table: &Table,
        reference: &IndexBatchBlockRef,
        cache: Option<&ClaimIndexCache>,
    ) -> PlatformResult<Self> {
        if let Some(block) = cache.and_then(|cache| cache.get_batch_block(&reference.digest)) {
            MarketDataMetrics::record_index_cache_hit("batch-block");
            return Ok((*block).clone());
        }
        MarketDataMetrics::record_index_cache_miss("batch-block");
        let bytes = table
            .file_io()
            .new_input(&reference.location)
            .map_err(|error| Self::error(format!("could not open index batch block: {error}")))?
            .read()
            .await
            .map_err(|error| Self::error(format!("could not read index batch block: {error}")))?;
        if bytes.len() > BATCH_BLOCK_MAX_BYTES {
            return Err(Self::error("index batch block exceeds the size limit"));
        }
        if Self::digest(&bytes) != reference.digest {
            return Err(Self::error(
                "index batch block digest does not match its reference",
            ));
        }
        let block: Self = serde_json::from_slice(&bytes).map_err(|error| {
            Self::error(format!("could not decode an index batch block: {error}"))
        })?;
        block.validate()?;
        MarketDataMetrics::record_index_block_read();
        if let Some(cache) = cache {
            cache.insert_batch_block(reference.digest.clone(), block.clone());
        }
        Ok(block)
    }

    pub(super) fn batches_for_load(&self) -> Vec<ClaimBatch> {
        self.batches.clone()
    }

    #[cfg(test)]
    pub(super) fn contains_batch(&self, digest: &str) -> Option<ClaimBatch> {
        self.batches
            .iter()
            .find(|batch| batch.digest == digest)
            .cloned()
    }

    fn validate(&self) -> PlatformResult<()> {
        if self.format_version != INDEX_FORMAT_VERSION {
            return Err(Self::error(
                "index batch block uses an unsupported format version",
            ));
        }
        if !self
            .batches
            .windows(2)
            .all(|pair| pair[0].digest < pair[1].digest)
        {
            return Err(Self::error("index batch block entries are not sorted"));
        }
        for batch in &self.batches {
            batch.validate()?;
        }
        Ok(())
    }

    async fn write(table: &Table, batches: Vec<ClaimBatch>) -> PlatformResult<IndexBatchBlockRef> {
        let block = Self {
            format_version: INDEX_FORMAT_VERSION,
            batches,
        };
        let bytes = serde_json::to_vec(&block).map_err(|error| {
            Self::error(format!("could not encode an index batch block: {error}"))
        })?;
        if bytes.len() > BATCH_BLOCK_MAX_BYTES {
            return Err(Self::error("index batch block exceeds the size limit"));
        }
        let digest = Self::digest(&bytes);
        let location = format!(
            "{}/archive-index-batch-{digest}.json",
            Self::metadata_directory(table)?
        );
        table
            .file_io()
            .new_output(&location)
            .map_err(|error| Self::error(format!("could not open index batch block: {error}")))?
            .write(Bytes::from(bytes))
            .await
            .map_err(|error| Self::error(format!("could not write index batch block: {error}")))?;
        let batch_first = block
            .batches
            .first()
            .map(|batch| batch.digest.clone())
            .unwrap_or_default();
        let batch_last = block
            .batches
            .last()
            .map(|batch| batch.digest.clone())
            .unwrap_or_default();
        Ok(IndexBatchBlockRef {
            location,
            digest,
            batch_first,
            batch_last,
        })
    }

    fn chunks(mut batches: Vec<ClaimBatch>) -> Vec<Vec<ClaimBatch>> {
        let mut chunks = Vec::new();
        let mut current = Vec::new();
        let mut current_bytes = 0_usize;
        for batch in batches.drain(..) {
            let batch_bytes = batch
                .sources
                .iter()
                .chain(&batch.outputs)
                .map(|entry| entry.len().saturating_add(8))
                .sum::<usize>()
                .saturating_add(batch.digest.len());
            if !current.is_empty()
                && current_bytes.saturating_add(batch_bytes) > BATCH_BLOCK_TARGET_BYTES
            {
                chunks.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current_bytes = current_bytes.saturating_add(batch_bytes);
            current.push(batch);
        }
        if !current.is_empty() {
            chunks.push(current);
        }
        chunks
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
            format!("archive claim index batch block {reason}"),
        ))
    }
}

pub(super) type BatchMap = HashMap<String, ClaimBatch>;
