use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use strum::Display;

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinanceExpiryReason {
    EXCHANGE_CANCELED,
    EXECUTION_RULE_PRICE_RANGE_EXCEEDED,
    INSUFFICIENT_LIQUIDITY,
    NONE,
    OCO_TRIGGER,
    OTO_PHASE_ONE_EXPIRED,
    REJECTED,
    UNFILLED_FOK_ORDER_EXPIRED,
    UNFILLED_IOC_QUANTITY_EXPIRED,
    #[serde(other)]
    Unknown,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceOrderStatusResponse {
    pub clientOrderId: String,
    pub cummulativeQuoteQty: Option<Decimal>,
    pub executedQty: Option<Decimal>,
    pub expiryReason: Option<BinanceExpiryReason>,
    pub icebergQty: Option<Decimal>,
    pub isWorking: Option<bool>,
    pub orderId: i64,
    pub orderListId: i64,
    pub origQty: Option<Decimal>,
    pub origQuoteOrderQty: Option<Decimal>,
    pub pegOffsetType: Option<BinancePegOffsetType>,
    pub pegOffsetValue: Option<i32>,
    pub pegPriceType: Option<BinancePegPriceType>,
    pub peggedPrice: Option<Decimal>,
    pub preventedMatchId: Option<i64>,
    pub preventedQuantity: Option<Decimal>,
    pub price: Option<Decimal>,
    pub selfTradePreventionMode: Option<BinanceSelfTradeProtection>,
    pub side: BinanceSide,
    pub status: BinanceOrderStatus,
    pub stopPrice: Option<Decimal>,
    pub strategyId: Option<i64>,
    pub strategyType: Option<i32>,
    pub symbol: String,
    pub time: Option<i64>,
    pub timeInForce: Option<BinanceTimeInForce>,
    pub trailingDelta: Option<i64>,
    pub trailingTime: Option<i64>,
    #[serde(rename = "type")]
    pub r#type: BinanceOrderType,
    pub updateTime: Option<i64>,
    pub usedSor: Option<bool>,
    pub workingFloor: Option<BinanceWorkingFloor>,
    pub workingTime: Option<i64>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceOrderListOrder {
    pub clientOrderId: String,
    pub orderId: i64,
    pub symbol: String,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceOrderListStatus {
    pub contingencyType: String,
    pub listClientOrderId: String,
    pub listOrderStatus: String,
    pub orderListId: i64,
    #[serde(default)]
    pub orders: Vec<BinanceOrderListOrder>,
    pub symbol: String,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceCancelOrderResponse {
    pub clientOrderId: String,
    pub cummulativeQuoteQty: Option<Decimal>,
    pub executedQty: Option<Decimal>,
    pub expiryReason: Option<BinanceExpiryReason>,
    pub icebergQty: Option<Decimal>,
    pub orderId: i64,
    pub orderListId: i64,
    pub origClientOrderId: String,
    pub origQty: Option<Decimal>,
    pub origQuoteOrderQty: Option<Decimal>,
    pub pegOffsetType: Option<BinancePegOffsetType>,
    pub pegOffsetValue: Option<i32>,
    pub pegPriceType: Option<BinancePegPriceType>,
    pub peggedPrice: Option<Decimal>,
    pub preventedMatchId: Option<i64>,
    pub preventedQuantity: Option<Decimal>,
    pub price: Option<Decimal>,
    pub selfTradePreventionMode: Option<BinanceSelfTradeProtection>,
    pub side: BinanceSide,
    pub status: BinanceOrderStatus,
    pub stopPrice: Option<Decimal>,
    pub strategyId: Option<i64>,
    pub strategyType: Option<i32>,
    pub symbol: String,
    pub timeInForce: Option<BinanceTimeInForce>,
    pub trailingDelta: Option<i64>,
    pub trailingTime: Option<i64>,
    pub transactTime: Option<i64>,
    #[serde(rename = "type")]
    pub r#type: BinanceOrderType,
    pub usedSor: Option<bool>,
    pub workingFloor: Option<BinanceWorkingFloor>,
    pub workingTime: Option<i64>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceCancelOrderListResponse {
    pub contingencyType: String,
    pub listClientOrderId: String,
    pub listOrderStatus: String,
    pub listStatusType: String,
    pub orderListId: i64,
    #[serde(default)]
    pub orderReports: Vec<BinanceCancelOrderResponse>,
    #[serde(default)]
    pub orders: Vec<BinanceOrderListOrder>,
    pub symbol: String,
    pub transactionTime: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
#[non_exhaustive]
pub enum BinanceCancelResponse {
    Order(BinanceCancelOrderResponse),
    OrderList(BinanceCancelOrderListResponse),
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinanceOrderStatus {
    CANCELED,
    EXPIRED,
    EXPIRED_IN_MATCH,
    FILLED,
    NEW,
    PARTIALLY_FILLED,
    PENDING_CANCEL,
    PENDING_NEW,
    REJECTED,
    #[serde(other)]
    Unknown,
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug, Display, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinanceOrderType {
    LIMIT,
    LIMIT_MAKER,
    MARKET,
    STOP_LOSS,
    STOP_LOSS_LIMIT,
    TAKE_PROFIT,
    TAKE_PROFIT_LIMIT,
    #[default]
    #[serde(other)]
    Unknown,
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinancePegOffsetType {
    PRICE_LEVEL,
    #[serde(other)]
    Unknown,
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinancePegPriceType {
    PRIMARY_PEG,
    MARKET_PEG,
    #[serde(other)]
    Unknown,
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinanceSelfTradeProtection {
    EXPIRE_BOTH,
    EXPIRE_MAKER,
    EXPIRE_TAKER,
    DECREMENT,
    NONE,
    TRANSFER,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Deserialize, Debug, Display, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinanceSide {
    BUY,
    SELL,
    #[default]
    #[serde(other)]
    Unknown,
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinanceTimeInForce {
    FOK,
    GTC,
    IOC,
    #[serde(other)]
    Unknown,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinanceWorkingFloor {
    EXCHANGE,
    SOR,
    #[serde(other)]
    Unknown,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceWebsocketSessionResponse {
    pub authorizedSince: Option<i64>,
    pub connectedSince: i64,
    pub returnRateLimits: bool,
    pub serverTime: i64,
    pub userDataStream: bool,
}
