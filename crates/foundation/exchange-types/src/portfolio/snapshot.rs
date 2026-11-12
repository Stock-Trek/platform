use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use stock_trek_types::cex::asset_id::AssetId;

#[derive(Debug, Clone, PartialEq)]
pub struct AccountSnapshot {
    balances: BTreeMap<AssetId, f64>,
    occurred_at: DateTime<Utc>,
}

impl AccountSnapshot {
    pub fn new(balances: BTreeMap<AssetId, f64>, occurred_at: DateTime<Utc>) -> Self {
        Self {
            balances,
            occurred_at,
        }
    }

    pub fn balances(&self) -> &BTreeMap<AssetId, f64> {
        &self.balances
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
}
