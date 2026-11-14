use stock_trek_types::cex::{asset_id::AssetId, order_request::OrderRequest};

#[derive(serde::Serialize, serde::Deserialize)]
pub struct PendingOrderWire {
    pub request: OrderRequest<AssetId, f64>,
    pub filled_base_quantity: f64,
    pub filled_quote_quantity: f64,
}
