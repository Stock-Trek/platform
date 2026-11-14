use serde_json::{Value, json};
use stock_trek::types::{AccountId, AssetId, CexId, OrderRequest, Tag};

pub struct OrderEvents;

impl OrderEvents {
    #[allow(clippy::too_many_arguments)]
    pub fn placed(
        kind: &str,
        cex_id: &CexId,
        account_id: &AccountId,
        order_request: &OrderRequest<AssetId, f64>,
        fill_price: f64,
        filled_base: f64,
        filled_quote: f64,
        partial: bool,
        slipped: bool,
    ) -> Value {
        json!({
            "type": kind,
            "cex": cex_id.to_string(),
            "account": account_id.0,
            "orderRequest": Self::request_value(order_request),
            "fillPrice": fill_price,
            "filledBaseQuantity": filled_base,
            "filledQuoteQuantity": filled_quote,
            "partialFill": partial,
            "slippage": slipped,
        })
    }

    pub fn cancelled(
        cex_id: Option<&CexId>,
        account_id: Option<&AccountId>,
        tag: &Option<Tag>,
    ) -> Value {
        json!({
            "type": "cancel_orders",
            "cex": cex_id.map(|cex| cex.to_string()),
            "account": account_id.map(|account| account.0.clone()),
            "tag": tag.as_ref().map(|tag| tag.0.clone()),
        })
    }

    pub fn request_value(order_request: &OrderRequest<AssetId, f64>) -> Value {
        serde_json::to_value(order_request).unwrap_or(Value::Null)
    }
}
