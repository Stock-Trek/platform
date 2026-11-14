use stock_trek_types::cex::asset_id::AssetId;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AssetBalanceWire {
    pub asset: AssetId,
    pub total: f64,
}
