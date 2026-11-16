use super::claim_index::{ClaimIndex, ClaimIndexCache, ClaimState, INDEX_HEAD_DIGEST_KEY};
use super::claim_index::{INDEX_FORMAT_VERSION_KEY, INDEX_HEAD_KEY};
use super::claim_index_batch::{ClaimBatch, ClaimIndexBatchBlock};
use super::claim_index_block::ClaimIndexBlock;
use super::claim_index_page::{ClaimIndexPage, IndexParent};
use iceberg::table::Table;
use platform_errors::PlatformResult;
use std::collections::HashSet;

impl ClaimIndex {
    pub(super) async fn find_cached(
        table: &Table,
        cache: Option<&ClaimIndexCache>,
        sources: &[String],
        outputs: &[String],
    ) -> PlatformResult<Option<ClaimState>> {
        let properties = table.metadata().properties();
        let Some(location) = properties.get(INDEX_HEAD_KEY).cloned() else {
            return Ok(None);
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
        let mut wanted_sources = sources.iter().cloned().collect::<HashSet<_>>();
        let mut wanted_outputs = outputs.iter().cloned().collect::<HashSet<_>>();
        let mut found_sources = HashSet::new();
        let mut found_outputs = HashSet::new();
        let mut visited = HashSet::new();
        let mut visited_blocks = HashSet::new();
        let mut next = Some(IndexParent { location, digest });
        while let Some(parent) = next {
            if !visited.insert(parent.digest.clone()) {
                return Err(Self::error("index chain contains a cycle"));
            }
            let page = ClaimIndexPage::read(table, &parent, cache).await?;
            Self::collect_entries(
                &page.sources,
                &page.outputs,
                &mut wanted_sources,
                &mut wanted_outputs,
                &mut found_sources,
                &mut found_outputs,
            );
            for reference in &page.blocks {
                if !visited_blocks.insert(reference.digest.clone()) {
                    continue;
                }
                let relevant = wanted_sources
                    .iter()
                    .any(|source| reference.may_contain_source(source))
                    || wanted_outputs
                        .iter()
                        .any(|output| reference.may_contain_output(output));
                if !relevant {
                    continue;
                }
                let block = ClaimIndexBlock::read(table, reference, cache).await?;
                for source in wanted_sources
                    .iter()
                    .filter(|source| block.contains_source(source))
                    .cloned()
                    .collect::<Vec<_>>()
                {
                    wanted_sources.remove(&source);
                    found_sources.insert(source);
                }
                for output in wanted_outputs
                    .iter()
                    .filter(|output| block.contains_output(output))
                    .cloned()
                    .collect::<Vec<_>>()
                {
                    wanted_outputs.remove(&output);
                    found_outputs.insert(output);
                }
            }
            if wanted_sources.is_empty() && wanted_outputs.is_empty() {
                break;
            }
            next = page.parent;
        }
        Ok(Some(ClaimState::new(found_sources, found_outputs)))
    }

    pub(super) async fn find_batch(
        table: &Table,
        cache: Option<&ClaimIndexCache>,
        digest: &str,
    ) -> PlatformResult<Option<ClaimBatch>> {
        let properties = table.metadata().properties();
        let Some(location) = properties.get(INDEX_HEAD_KEY).cloned() else {
            return Ok(None);
        };
        let Some(head_digest) = properties.get(INDEX_HEAD_DIGEST_KEY).cloned() else {
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
        let mut visited = HashSet::new();
        let mut next = Some(IndexParent {
            location,
            digest: head_digest,
        });
        while let Some(parent) = next {
            if !visited.insert(parent.digest.clone()) {
                return Err(Self::error("index chain contains a cycle"));
            }
            let page = ClaimIndexPage::read(table, &parent, cache).await?;
            if let Some(batch) = page
                .batches
                .iter()
                .find(|batch| batch.digest == digest)
                .cloned()
            {
                return Ok(Some(batch));
            }
            for reference in &page.batch_blocks {
                if !reference.may_contain_batch(digest) {
                    continue;
                }
                let block = ClaimIndexBatchBlock::read(table, reference, cache).await?;
                if let Some(batch) = block.contains_batch(digest) {
                    return Ok(Some(batch));
                }
            }
            next = page.parent;
        }
        Ok(None)
    }

    fn collect_entries(
        page_sources: &[String],
        page_outputs: &[String],
        wanted_sources: &mut HashSet<String>,
        wanted_outputs: &mut HashSet<String>,
        found_sources: &mut HashSet<String>,
        found_outputs: &mut HashSet<String>,
    ) {
        for source in wanted_sources
            .iter()
            .filter(|source| page_sources.binary_search(source).is_ok())
            .cloned()
            .collect::<Vec<_>>()
        {
            wanted_sources.remove(&source);
            found_sources.insert(source);
        }
        for output in wanted_outputs
            .iter()
            .filter(|output| page_outputs.binary_search(output).is_ok())
            .cloned()
            .collect::<Vec<_>>()
        {
            wanted_outputs.remove(&output);
            found_outputs.insert(output);
        }
    }
}
