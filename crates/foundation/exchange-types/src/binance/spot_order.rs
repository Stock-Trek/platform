use crate::{
    binance::{
        exchange::BinanceExchange,
        order_status::BinanceOrderStatusRequest,
        recv_window::BinanceRecvWindow,
        request::BinanceRequestFactory,
        response::BinanceResponse,
        supporting_types::{
            BinanceExpiryReason, BinanceOrderStatus, BinanceOrderType, BinancePegOffsetType,
            BinancePegPriceType, BinanceSelfTradeProtection, BinanceSide, BinanceTimeInForce,
            BinanceWorkingFloor,
        },
    },
    http::{HttpMethod, HttpRequest},
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETRequest, ETWebsocketRequest, WebsocketResponseMatcher},
    signer::Signer,
    ticker::Ticker,
    websocket_id::ETWebsocketId,
};
use platform_errors::BpResult;
use platform_types::new_types::{TimestampMilliseconds, UsageCount};
use query_params::QueryParams;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use strum::Display;

#[allow(non_snake_case)]
#[skip_serializing_none]
#[derive(Serialize, Debug, Clone, Hash, QueryParams)]
pub struct BinanceSpotOrderRequest {
    apiKey: Option<String>,
    timestamp: i64,
    pub icebergQty: Option<Decimal>,
    pub newClientOrderId: Option<String>,
    pub newOrderRespType: Option<BinanceNewOrderResponseType>,
    pub pegPriceType: Option<BinancePegPriceType>,
    pub pegOffsetValue: Option<i32>,
    pub pegOffsetType: Option<BinancePegOffsetType>,
    pub price: Option<Decimal>,
    pub quantity: Option<Decimal>,
    pub quoteOrderQty: Option<Decimal>,
    pub recvWindow: Option<BinanceRecvWindow>,
    pub selfTradePreventionMode: Option<BinanceSelfTradeProtection>,
    pub side: BinanceSide,
    pub stopPrice: Option<Decimal>,
    pub strategyId: Option<i64>,
    pub strategyType: Option<i32>,
    pub symbol: String,
    pub timeInForce: Option<BinanceTimeInForce>,
    pub trailingDelta: Option<i64>,
    #[serde(rename = "type")]
    pub r#type: BinanceOrderType,
}

impl BinanceSpotOrderRequest {
    pub fn new(
        symbol: impl Into<String>,
        side: BinanceSide,
        r#type: BinanceOrderType,
        quantity: Decimal,
    ) -> Self {
        Self {
            apiKey: None,
            timestamp: 0,
            icebergQty: None,
            newClientOrderId: None,
            newOrderRespType: None,
            pegOffsetType: None,
            pegOffsetValue: None,
            pegPriceType: None,
            price: None,
            quantity: Some(quantity),
            quoteOrderQty: None,
            recvWindow: None,
            selfTradePreventionMode: None,
            side,
            stopPrice: None,
            strategyId: None,
            strategyType: None,
            symbol: symbol.into(),
            timeInForce: None,
            trailingDelta: None,
            r#type,
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinanceNewOrderResponseType {
    ACK,
    RESULT,
    FULL,
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize)]
#[skip_serializing_none]
pub struct BinanceSpotOrderResponse {
    pub clientOrderId: String,
    pub cummulativeQuoteQty: Option<Decimal>,
    pub executedQty: Option<Decimal>,
    pub expiryReason: Option<BinanceExpiryReason>,
    pub fills: Option<Vec<BinanceFill>>,
    pub icebergQty: Option<Decimal>,
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
    pub side: Option<BinanceSide>,
    pub status: Option<BinanceOrderStatus>,
    pub stopPrice: Option<Decimal>,
    pub strategyId: Option<i64>,
    pub strategyType: Option<i32>,
    pub symbol: String,
    pub timeInForce: Option<BinanceTimeInForce>,
    pub trailingDelta: Option<i64>,
    pub trailingTime: Option<i64>,
    pub transactTime: i64,
    #[serde(rename = "type")]
    pub r#type: Option<BinanceOrderType>,
    pub usedSor: Option<bool>,
    pub workingFloor: Option<BinanceWorkingFloor>,
    pub workingTime: Option<i64>,
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BinanceFill {
    pub allocId: Option<i64>,
    pub commission: Decimal,
    pub commissionAsset: Ticker,
    pub matchType: Option<String>,
    pub price: Decimal,
    pub qty: Decimal,
    pub tradeId: i64,
}

impl ETRequest for BinanceSpotOrderRequest {
    type Exchange = BinanceExchange;

    fn is_signed(&self) -> bool {
        true
    }
    fn rate_limit_usage(&self, restriction: RateLimitRestriction) -> UsageCount {
        match restriction {
            RateLimitRestriction::OrderCount | RateLimitRestriction::Weight => UsageCount::ONE,
            _ => UsageCount::ZERO,
        }
    }
    fn set_api_key(&mut self, api_key: Option<String>) {
        self.apiKey = api_key;
    }
    fn set_timestamp(&mut self, timestamp: TimestampMilliseconds) {
        self.timestamp = timestamp.0;
    }
    fn query_params(&self, percent_encode: bool) -> String {
        self.query_params(true, percent_encode)
    }
}

impl ETHttpRequest for BinanceSpotOrderRequest {
    type Response = BinanceResponse<BinanceSpotOrderResponse>;
    type VerificationRequest = BinanceOrderStatusRequest;

    fn http_endpoint(&self) -> &'static str {
        "order"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::POST
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
    fn verification_request_http(&self) -> Option<BinanceOrderStatusRequest> {
        let mut request = BinanceOrderStatusRequest::new(self.symbol.clone());
        request.origClientOrderId = self.newClientOrderId.clone();
        Some(request)
    }
}

impl ETWebsocketRequest for BinanceSpotOrderRequest {
    type Response = BinanceResponse<BinanceSpotOrderResponse>;
    type VerificationRequest = BinanceOrderStatusRequest;

    fn websocket_method_name(&self) -> &'static str {
        "order.place"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
    fn verification_request_websocket(&self) -> Option<BinanceOrderStatusRequest> {
        let mut request = BinanceOrderStatusRequest::new(self.symbol.clone());
        request.origClientOrderId = self.newClientOrderId.clone();
        Some(request)
    }
}
