use crate::iceberg::catalog::iceberg_catalog::claim_index_block::ClaimIndexBlock;
use crate::iceberg::catalog::iceberg_catalog::claim_index_page::{ClaimIndexPage, IndexParent};
use crate::iceberg::{ArchiveRegistration, CommitContext};
use iceberg::table::Table;
use platform_errors::{PlatformError, PlatformResult};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

pub(super) const INDEX_HEAD_KEY: &str = "archive.index-head";
pub(super) const INDEX_HEAD_DIGEST_KEY: &str = "archive.index-head-digest";
pub(super) const INDEX_FORMAT_VERSION_KEY: &str = "archive.index-format-version";
pub(super) const INDEX_DEPTH_KEY: &str = "archive.index-depth";
const INDEX_CONSOLIDATION_DEPTH: u64 = 64;
const CLAIM_INDEX_CACHE_CAPACITY: usize = 512;

pub(super) struct ClaimIndex;

pub(super) struct ClaimState {
    pub(super) sources: HashSet<String>,
    pub(super) outputs: HashSet<String>,
}

impl ClaimState {
    pub(super) fn new(
        sources: impl IntoIterator<Item = String>,
        outputs: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            sources: sources.into_iter().collect(),
            outputs: outputs.into_iter().collect(),
        }
    }
}

impl ClaimIndex {
    pub(super) fn has_head(table: &Table) -> bool {
        table.metadata().properties().contains_key(INDEX_HEAD_KEY)
    }

    pub(super) async fn load(table: &Table) -> PlatformResult<ClaimState> {
        Self::load_cached(table, None).await
    }

    pub(super) async fn load_cached(
        table: &Table,
        cache: Option<&ClaimIndexCache>,
    ) -> PlatformResult<ClaimState> {
        let properties = table.metadata().properties();
        let Some(location) = properties.get(INDEX_HEAD_KEY).cloned() else {
            return Ok(ClaimState::new(Vec::new(), Vec::new()));
        };
        let Some(digest) = properties.get(INDEX_HEAD_DIGEST_KEY).cloned() else {
            return Err(Self::error("index head is missing its digest"));
        };
        let version = properties
            .get(INDEX_FORMAT_VERSION_KEY)
            .ok_or_else(|| Self::error("index head is missing its format version"))?;
        if version != &ClaimIndexPage::format_version().to_string() {
            return Err(Self::error(format!(
                "index head uses unsupported format version {version}"
            )));
        }
        let mut sources = HashSet::new();
        let mut outputs = HashSet::new();
        let mut visited = HashSet::new();
        let mut next = Some(IndexParent { location, digest });
        while let Some(parent) = next {
            if !visited.insert(parent.digest.clone()) {
                return Err(Self::error("index chain contains a cycle"));
            }
            let page = ClaimIndexPage::read(table, &parent, cache).await?;
            sources.extend(page.sources);
            outputs.extend(page.outputs);
            for reference in &page.blocks {
                let block = ClaimIndexBlock::read(table, reference, cache).await?;
                sources.extend(block.sources_for_load());
                outputs.extend(block.outputs_for_load());
            }
            next = page.parent;
        }
        Ok(ClaimState::new(sources, outputs))
    }

    pub(super) async fn append(
        table: &Table,
        registration: &ArchiveRegistration,
        baseline: Option<ClaimState>,
    ) -> PlatformResult<HashMap<String, String>> {
        let properties = table.metadata().properties();
        let (parent, depth) = match (
            properties.get(INDEX_HEAD_KEY).cloned(),
            properties.get(INDEX_HEAD_DIGEST_KEY).cloned(),
        ) {
            (Some(location), Some(digest)) => (
                Some(IndexParent { location, digest }),
                properties
                    .get(INDEX_DEPTH_KEY)
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_default(),
            ),
            (None, None) => (None, 0),
            _ => return Err(Self::error("index head is only partially configured")),
        };
        let new_sources = registration
            .source_files()
            .unwrap_or_default()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let new_outputs = registration
            .files()
            .iter()
            .map(|file| file.file_path.clone())
            .collect::<HashSet<_>>();
        if parent.is_none() {
            let baseline = baseline.unwrap_or_else(|| ClaimState::new(Vec::new(), Vec::new()));
            return ClaimIndexPage::write_consolidated(
                table,
                baseline.sources.into_iter().chain(new_sources).collect(),
                baseline.outputs.into_iter().chain(new_outputs).collect(),
                1,
            )
            .await;
        }
        if depth >= INDEX_CONSOLIDATION_DEPTH {
            let mut state = Self::load(table).await?;
            state.sources.extend(new_sources);
            state.outputs.extend(new_outputs);
            return ClaimIndexPage::write_consolidated(table, state.sources, state.outputs, 1)
                .await;
        }
        ClaimIndexPage::from_claims(parent, new_sources, new_outputs)
            .write(table, depth.saturating_add(1))
            .await
    }

    pub(super) async fn consolidate(
        table: &Table,
        context: &CommitContext,
    ) -> PlatformResult<HashMap<String, String>> {
        context.authorize().await?;
        let state = Self::load(table).await?;
        ClaimIndexPage::write_consolidated(table, state.sources, state.outputs, 1).await
    }

    pub(super) fn error(reason: impl std::fmt::Display) -> PlatformError {
        PlatformError::external(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("archive claim index {reason}"),
        ))
    }
}

#[derive(Default)]
pub(super) struct ClaimIndexCache {
    pages: Mutex<HashMap<String, Arc<ClaimIndexPage>>>,
    blocks: Mutex<HashMap<String, Arc<ClaimIndexBlock>>>,
}

impl ClaimIndexCache {
    pub(super) fn get_page(&self, digest: &str) -> Option<Arc<ClaimIndexPage>> {
        self.pages
            .lock()
            .expect("claim index cache lock poisoned")
            .get(digest)
            .cloned()
    }

    pub(super) fn get_block(&self, digest: &str) -> Option<Arc<ClaimIndexBlock>> {
        self.blocks
            .lock()
            .expect("claim index cache lock poisoned")
            .get(digest)
            .cloned()
    }

    pub(super) fn insert_page(&self, digest: String, page: ClaimIndexPage) {
        let mut pages = self.pages.lock().expect("claim index cache lock poisoned");
        Self::evict_if_full(&mut pages);
        pages.insert(digest, Arc::new(page));
    }

    pub(super) fn insert_block(&self, digest: String, block: ClaimIndexBlock) {
        let mut blocks = self.blocks.lock().expect("claim index cache lock poisoned");
        Self::evict_if_full(&mut blocks);
        blocks.insert(digest, Arc::new(block));
    }

    fn evict_if_full<T>(entries: &mut HashMap<String, Arc<T>>) {
        if entries.len() < CLAIM_INDEX_CACHE_CAPACITY {
            return;
        }
        if let Some(key) = entries.keys().next().cloned() {
            entries.remove(&key);
        }
    }
}
