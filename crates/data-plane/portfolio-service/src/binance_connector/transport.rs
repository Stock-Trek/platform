use async_trait::async_trait;
use chrono::{DateTime, Utc};
use platform_errors::BpResult;
use platform_exchange_types::api_key_credential::ApiKeyCredentials;
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
pub enum BinanceAccountEvent {
    Position(AccountPositionEvent),
    ListenKeyExpired,
}

#[async_trait]
pub trait BinanceEventStream: Send {
    async fn next(&mut self) -> BpResult<Option<BinanceAccountEvent>>;
}

#[async_trait]
pub trait BinanceTransport: Send + Sync {
    async fn create_listen_key(&self, credentials: &ApiKeyCredentials) -> BpResult<String>;

    async fn keepalive_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BpResult<()>;

    async fn close_listen_key(
        &self,
        credentials: &ApiKeyCredentials,
        listen_key: &str,
    ) -> BpResult<()>;

    async fn account_snapshot(&self, credentials: &ApiKeyCredentials) -> BpResult<AccountSnapshot>;

    async fn open_stream(&self, listen_key: &str) -> BpResult<Box<dyn BinanceEventStream>>;
}
