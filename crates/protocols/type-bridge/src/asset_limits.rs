use rust_decimal::Decimal;
use std::collections::HashMap;
use stock_trek_types::cex::asset_id::AssetId;

#[derive(Debug, Clone)]
pub struct TBAssetLimits {
    pub asset_limits: HashMap<AssetId, Decimal>,
}
