use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use stock_trek_types::cex::asset_id::AssetId;

pub(super) struct SessionState {
    pub(super) version: u64,
    pub(super) balances: BTreeMap<AssetId, f64>,
    pub(super) last_update: Option<std::time::Instant>,
    pub(super) last_exchange_time: DateTime<Utc>,
}

impl SessionState {
    pub(super) fn new() -> Self {
        Self {
            version: 0,
            balances: BTreeMap::new(),
            last_update: None,
            last_exchange_time: Utc::now(),
        }
    }

    pub(super) fn replace_snapshot(
        &mut self,
        balances: BTreeMap<AssetId, f64>,
        occurred_at: DateTime<Utc>,
    ) {
        self.balances = balances;
        self.last_exchange_time = occurred_at;
    }

    pub(super) fn next_version(current: u64, exchange_version: u64) -> u64 {
        let now = Utc::now().timestamp_millis().max(0) as u64;
        current.max(exchange_version).max(now).saturating_add(1)
    }

    pub(super) fn merge_balances(
        target: &mut BTreeMap<AssetId, f64>,
        updates: &BTreeMap<AssetId, f64>,
    ) {
        target.extend(updates.iter().map(|(asset, total)| (*asset, *total)));
    }
}

impl Default for SessionState {
    fn default() -> Self {
        Self::new()
    }
}
