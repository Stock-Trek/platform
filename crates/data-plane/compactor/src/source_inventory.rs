use crate::source::S3ArchiveSource;
use platform_archive::{Bucket, ObjectKey, S3Object};
use platform_errors::PlatformResult;
use std::collections::{HashMap, HashSet};

pub(super) const INVENTORY_RECONCILE_POLLS: u64 = 60;

#[derive(Default)]
pub(super) struct RawInventory {
    pub(super) objects: HashMap<String, S3Object>,
    pub(super) candidate_keys: HashSet<String>,
    pub(super) last_key: Option<String>,
    pub(super) polls: u64,
    pub(super) previous_pending: usize,
}

impl S3ArchiveSource {
    pub(super) async fn refresh_inventory(
        &self,
        bucket: &Bucket,
        prefix: &ObjectKey,
    ) -> PlatformResult<()> {
        let mut inventory = self.inventory.lock().await;
        let reconcile = inventory.objects.is_empty()
            || inventory.polls.is_multiple_of(INVENTORY_RECONCILE_POLLS);
        if reconcile {
            inventory.objects.clear();
            inventory.candidate_keys.clear();
            inventory.last_key = None;
        }
        let mut discovered = Vec::new();
        let mut continuation = None;
        let mut start_after = inventory.last_key.clone();
        loop {
            let page = self
                .lister
                .list_page(
                    bucket,
                    prefix,
                    continuation.as_deref(),
                    start_after.as_deref(),
                    1_000,
                )
                .await?;
            platform_metrics::MarketDataMetrics::record_archive_listing_page(
                "compactor-inventory",
                u64::try_from(page.objects.len()).unwrap_or(u64::MAX),
            );
            for object in page.objects {
                let key = object.key.as_str().to_string();
                if inventory
                    .last_key
                    .as_deref()
                    .is_none_or(|last| key.as_str() > last)
                {
                    inventory.last_key = Some(key.clone());
                }
                if inventory.objects.insert(key.clone(), object).is_none() || reconcile {
                    discovered.push(key);
                }
            }
            start_after = None;
            match page.next_continuation_token {
                Some(next) => continuation = Some(next),
                None => break,
            }
        }
        inventory.candidate_keys.extend(discovered);
        let RawInventory {
            objects,
            candidate_keys,
            ..
        } = &mut *inventory;
        candidate_keys.retain(|key| objects.contains_key(key));
        let examined = inventory.candidate_keys.len();
        let total = inventory.objects.len();
        let growth = examined as i64 - inventory.previous_pending as i64;
        let oldest = inventory
            .candidate_keys
            .iter()
            .filter_map(|key| inventory.objects.get(key))
            .map(|object| object.last_modified_millis().unwrap_or_default())
            .min()
            .unwrap_or_else(|| platform_types::TimestampMilliseconds::now().0);
        let age = platform_types::TimestampMilliseconds::now()
            .0
            .saturating_sub(oldest)
            .max(0) as u64;
        inventory.previous_pending = examined;
        inventory.polls = inventory.polls.wrapping_add(1);
        drop(inventory);
        platform_metrics::MarketDataMetrics::record_compactor_inventory(examined, total);
        platform_metrics::MarketDataMetrics::record_compactor_backlog(
            std::time::Duration::from_millis(age),
            growth,
        );
        Ok(())
    }
}
