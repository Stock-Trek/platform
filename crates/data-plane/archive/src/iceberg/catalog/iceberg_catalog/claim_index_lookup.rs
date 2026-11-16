use super::claim_index::{ClaimIndex, ClaimIndexCache, ClaimState, INDEX_HEAD_DIGEST_KEY};
use super::claim_index::{INDEX_FORMAT_VERSION_KEY, INDEX_HEAD_KEY};
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
        let mut next = Some(IndexParent { location, digest });
        while let Some(parent) = next {
            if !visited.insert(parent.digest.clone()) {
                return Err(Self::error("index chain contains a cycle"));
            }
            let page = ClaimIndexPage::read(table, &parent, cache).await?;
            for source in wanted_sources
                .iter()
                .filter(|source| page.sources.binary_search(source).is_ok())
                .cloned()
                .collect::<Vec<_>>()
            {
                wanted_sources.remove(&source);
                found_sources.insert(source);
            }
            for output in wanted_outputs
                .iter()
                .filter(|output| page.outputs.binary_search(output).is_ok())
                .cloned()
                .collect::<Vec<_>>()
            {
                wanted_outputs.remove(&output);
                found_outputs.insert(output);
            }
            if wanted_sources.is_empty() && wanted_outputs.is_empty() {
                break;
            }
            next = page.parent;
        }
        Ok(Some(ClaimState::new(found_sources, found_outputs)))
    }
}
