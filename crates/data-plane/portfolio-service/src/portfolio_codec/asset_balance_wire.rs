use stock_trek_types::cex::asset_id::AssetId;

#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct AssetBalanceWire {
    pub(super) asset: AssetId,
    pub(super) total: f64,
}
