use crate::{convert::string::JsonString, generated::dto::AssetIdDto};
use stock_trek::{errors::StockTrekError, types::AssetId};

impl TryFrom<AssetIdDto> for AssetId {
    type Error = StockTrekError;
    fn try_from(value: AssetIdDto) -> Result<Self, Self::Error> {
        JsonString::deserialize(&value.asset_id)
    }
}
