use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectorPortfolioUpdate {
    cex_id: CexId,
    account_id: AccountId,
    balances: BTreeMap<AssetId, f64>,
    version: u64,
    occurred_at: DateTime<Utc>,
    exchange_time: Option<DateTime<Utc>>,
}

impl ConnectorPortfolioUpdate {
    pub fn new(
        cex_id: CexId,
        account_id: AccountId,
        balances: BTreeMap<AssetId, f64>,
        version: u64,
        occurred_at: DateTime<Utc>,
    ) -> Self {
        Self {
            cex_id,
            account_id,
            balances,
            version,
            occurred_at,
            exchange_time: None,
        }
    }

    pub fn with_exchange_time(mut self, exchange_time: DateTime<Utc>) -> Self {
        self.exchange_time = Some(exchange_time);
        self
    }

    pub fn with_optional_exchange_time(mut self, exchange_time: Option<DateTime<Utc>>) -> Self {
        self.exchange_time = exchange_time;
        self
    }

    pub fn cex_id(&self) -> CexId {
        self.cex_id
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub fn balances(&self) -> &BTreeMap<AssetId, f64> {
        &self.balances
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    pub fn exchange_time(&self) -> Option<DateTime<Utc>> {
        self.exchange_time
    }
}
