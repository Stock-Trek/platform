use crate::cex::asset_id::AssetId;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TradingPair {
    pub base: AssetId,
    pub quote: AssetId,
}
impl TradingPair {
    pub fn new(base: AssetId, quote: AssetId) -> Self {
        if base == quote {
            panic!("Base {:?} cannot be the same as Quote {:?}", base, quote);
        }
        Self { base, quote }
    }
}
