use async_trait::async_trait;
use chrono::{DateTime, Utc};
use platform_errors::BpResult;
use std::collections::BTreeMap;
use stock_trek_types::cex::asset_id::AssetId;

#[derive(Debug, Clone, PartialEq)]
pub struct AccountPositionEvent {
    version: u64,
    balances: BTreeMap<AssetId, f64>,
    occurred_at: DateTime<Utc>,
}

impl AccountPositionEvent {
    pub fn new(version: u64, balances: BTreeMap<AssetId, f64>, occurred_at: DateTime<Utc>) -> Self {
        Self {
            version,
            balances,
            occurred_at,
        }
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn balances(&self) -> &BTreeMap<AssetId, f64> {
        &self.balances
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AccountEvent {
    Position(AccountPositionEvent),
    SessionExpired,
}

#[async_trait]
pub trait AccountEventStream: Send {
    async fn next(&mut self) -> BpResult<Option<AccountEvent>>;
}
