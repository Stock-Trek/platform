use crate::source::S3ArchiveSource;
use platform_archive::{Bucket, ObjectKey, S3, S3Object};
use platform_errors::PlatformResult;
use std::collections::{HashMap, HashSet};

const INVENTORY_RECONCILE_POLLS: u64 = 60;

#[derive(Default)]
pub(super) struct RawInventory {
    pub(super) objects: HashMap<String, S3Object>,
    pub(super) candidate_keys: HashSet<String>,
    pub(super) last_key: Option<String>,
    pub(super) polls: u64,
}

impl S3ArchiveSource {
    pub(super) async fn refresh_inventory(
        &self,
        client: &aws_sdk_s3::Client,
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
            let page = S3::list_objects_page(
                client,
                bucket,
                prefix,
                continuation.as_deref(),
                start_after.as_deref(),
                1_000,
            )
            .await?;
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
        inventory.polls = inventory.polls.wrapping_add(1);
        drop(inventory);
        platform_metrics::MarketDataMetrics::record_compactor_inventory(examined, total);
        Ok(())
    }
}
