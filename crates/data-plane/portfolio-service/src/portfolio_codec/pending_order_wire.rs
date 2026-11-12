use stock_trek_types::cex::{asset_id::AssetId, order_request::OrderRequest};

#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct PendingOrderWire {
    pub(super) request: OrderRequest<AssetId, f64>,
    pub(super) filled_base_quantity: f64,
    pub(super) filled_quote_quantity: f64,
}
