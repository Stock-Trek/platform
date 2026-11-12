use async_trait::async_trait;
use chrono::{DateTime, Utc};
use platform_errors::BpResult;
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
use std::{collections::BTreeMap, time::Duration};
use stock_trek_types::cex::{account_id::AccountId, asset_id::AssetId, cex_id::CexId};

#[derive(Debug, Clone, PartialEq)]
pub struct PortfolioSnapshot {
    balances: BTreeMap<AssetId, f64>,
    occurred_at: DateTime<Utc>,
}

impl PortfolioSnapshot {
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

#[derive(Debug, Clone, PartialEq)]
pub struct PortfolioPosition {
    version: u64,
    balances: BTreeMap<AssetId, f64>,
    occurred_at: DateTime<Utc>,
}

impl PortfolioPosition {
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
pub enum PortfolioEvent {
    Position(PortfolioPosition),
    SessionExpired,
}

#[async_trait]
pub trait PortfolioSession: Send {
    async fn snapshot(&self) -> BpResult<PortfolioSnapshot>;

    async fn refresh(&self) -> BpResult<()>;

    async fn close(&mut self) -> BpResult<()>;

    async fn next(&mut self) -> BpResult<Option<PortfolioEvent>>;
}

#[async_trait]
pub trait PortfolioExchangeTransport: Send + Sync {
    fn cex_id(&self) -> CexId;

    fn refresh_interval(&self) -> Duration;

    fn reconnect_backoff(&self) -> Duration;

    async fn open_session(
        &self,
        account: &AccountId,
        credentials: &ApiKeyCredentials,
    ) -> BpResult<Box<dyn PortfolioSession>>;
}
