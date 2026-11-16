use super::claim_index::{ClaimIndex, ClaimIndexCache, INDEX_HEAD_DIGEST_KEY, INDEX_HEAD_KEY};
use super::claim_index_page::{ClaimIndexPage, IndexParent};
use iceberg::table::Table;
use platform_errors::PlatformResult;
use std::collections::HashSet;

impl ClaimIndex {
    pub(super) async fn reachable_locations(table: &Table) -> PlatformResult<HashSet<String>> {
        let properties = table.metadata().properties();
        let Some(location) = properties.get(INDEX_HEAD_KEY).cloned() else {
            return Ok(HashSet::new());
        };
        let Some(digest) = properties.get(INDEX_HEAD_DIGEST_KEY).cloned() else {
            return Err(Self::error("index head is missing its digest"));
        };
        let mut locations = HashSet::new();
        let mut visited = HashSet::new();
        let mut next = Some(IndexParent { location, digest });
        while let Some(parent) = next {
            if !visited.insert(parent.digest.clone()) {
                return Err(Self::error("index chain contains a cycle"));
            }
            locations.insert(parent.location.clone());
            let page = ClaimIndexPage::read(table, &parent, None::<&ClaimIndexCache>).await?;
            for reference in page.blocks {
                locations.insert(reference.location);
            }
            for reference in page.batch_blocks {
                locations.insert(reference.location);
            }
            next = page.parent;
        }
        Ok(locations)
    }
}
