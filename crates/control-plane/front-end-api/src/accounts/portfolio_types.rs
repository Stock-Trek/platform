use crate::api::timestamp::milliseconds;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use stock_trek_types::cex::{
    asset_id::AssetId, cex_id::CexId, side::Side, tag::Tag, trading_pair::TradingPair,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingOrder {
    pub tag: Tag,
    pub symbol: TradingPair,
    pub side: Side,
    #[serde(rename = "type")]
    pub order_type: OrderType,
    pub quantity: String,
    pub price: String,
    #[serde(with = "milliseconds")]
    pub placed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortfolioAsset {
    pub asset: AssetId,
    pub total: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortfolioSnapshot {
    pub account_id: String,
    pub cex_id: CexId,
    #[serde(with = "milliseconds::option")]
    pub as_of: Option<DateTime<Utc>>,
    pub stream_sequence: u64,
    pub assets: Vec<PortfolioAsset>,
    pub pending_orders: Vec<PendingOrder>,
}

impl PortfolioSnapshot {
    pub fn pending(account_id: &str, cex_id: CexId) -> Self {
        Self {
            account_id: account_id.to_string(),
            cex_id,
            as_of: None,
            stream_sequence: 0,
            assets: Vec::new(),
            pending_orders: Vec::new(),
        }
    }
}
