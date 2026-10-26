use crate::{
    asset_limits::TBAssetLimits, exchange_info::TBExchangeInfo,
    session_authentication::TBSessionAuthentication,
};
use stock_trek_types::cex::order_response::OrderResponse;

#[derive(Debug, Clone)]
pub enum TBResponse {
    ExchangeInfo(TBExchangeInfo),
    AssetLimits(TBAssetLimits),
    SessionAuthentication(TBSessionAuthentication),
    SpotOrder(OrderResponse),
}
