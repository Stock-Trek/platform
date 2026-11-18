use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Symbol {
    pub base: String,
    pub quote: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingOrder {
    pub tag: String,
    pub symbol: Symbol,
    pub side: OrderSide,
    #[serde(rename = "type")]
    pub order_type: OrderType,
    pub quantity: String,
    pub price: String,
    #[serde(with = "crate::api::timestamp::milliseconds")]
    pub placed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortfolioAsset {
    pub asset: String,
    pub total: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PortfolioSnapshot {
    pub account_id: String,
    pub cex_id: String,
    #[serde(with = "crate::api::timestamp::milliseconds::option")]
    pub as_of: Option<DateTime<Utc>>,
    pub stream_sequence: u64,
    pub assets: Vec<PortfolioAsset>,
    pub pending_orders: Vec<PendingOrder>,
}

impl PortfolioSnapshot {
    pub fn pending(account_id: &str, cex_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            cex_id: cex_id.to_string(),
            as_of: None,
            stream_sequence: 0,
            assets: Vec::new(),
            pending_orders: Vec::new(),
        }
    }
}
