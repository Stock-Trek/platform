use crate::{
    binance::{
        macros::binance_signed_request,
        order_status::BinanceOrderStatusRequest,
        request::BinanceRequestFactory,
        response::BinanceResponse,
        supporting_types::{
            BinanceExpiryReason, BinanceOrderListStatus, BinanceOrderStatus, BinanceOrderType,
            BinancePegOffsetType, BinancePegPriceType, BinanceSelfTradeProtection, BinanceSide,
            BinanceTimeInForce, BinanceWorkingFloor,
        },
    },
    http::{HttpMethod, HttpRequest},
    new_types::UsageCount,
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETWebsocketRequest, WebsocketResponseMatcher},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use platform_errors::BpResult;
use query_params::QueryParams;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

binance_signed_request! {
    #[allow(non_snake_case)]
    #[skip_serializing_none]
    #[derive(Serialize, Debug, Clone, Hash, QueryParams)]
    pub struct BinanceAmendOrderRequest {
        pub newClientOrderId: Option<String>,
        pub newQty: Decimal,
        pub orderId: Option<i64>,
        pub origClientOrderId: Option<String>,
        pub symbol: String,
    }
    idempotent: true,
    rate_limit: {
        RateLimitRestriction::Weight => UsageCount(4),
        _ => UsageCount::ZERO,
    }
}

impl BinanceAmendOrderRequest {
    pub fn new(symbol: impl Into<String>, new_qty: Decimal) -> Self {
        Self {
            apiKey: None,
            timestamp: 0,
            newClientOrderId: None,
            newQty: new_qty,
            orderId: None,
            origClientOrderId: None,
            recvWindow: None,
            symbol: symbol.into(),
        }
    }
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceAmendOrderResponse {
    pub amendedOrder: BinanceAmendedOrder,
    pub executionId: i64,
    pub listStatus: Option<BinanceOrderListStatus>,
    pub transactTime: i64,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceAmendedOrder {
    pub clientOrderId: String,
    pub cumulativeQuoteQty: Decimal,
    pub executedQty: Decimal,
    pub expiryReason: Option<BinanceExpiryReason>,
    pub icebergQty: Option<Decimal>,
    pub orderId: i64,
    pub orderListId: i64,
    pub origClientOrderId: String,
    pub pegOffsetType: Option<BinancePegOffsetType>,
    pub pegOffsetValue: Option<i32>,
    pub pegPriceType: Option<BinancePegPriceType>,
    pub peggedPrice: Option<Decimal>,
    pub preventedQty: Decimal,
    pub price: Decimal,
    pub qty: Decimal,
    pub quoteOrderQty: Decimal,
    pub selfTradePreventionMode: BinanceSelfTradeProtection,
    pub side: BinanceSide,
    pub status: BinanceOrderStatus,
    pub stopPrice: Option<Decimal>,
    pub strategyId: Option<i64>,
    pub strategyType: Option<i32>,
    pub symbol: String,
    pub timeInForce: BinanceTimeInForce,
    pub trailingDelta: Option<i64>,
    pub trailingTime: Option<i64>,
    #[serde(rename = "type")]
    pub r#type: BinanceOrderType,
    pub usedSor: Option<bool>,
    pub workingFloor: Option<BinanceWorkingFloor>,
    pub workingTime: Option<i64>,
}

impl ETHttpRequest for BinanceAmendOrderRequest {
    type Response = BinanceResponse<BinanceAmendOrderResponse>;
    type VerificationRequest = BinanceOrderStatusRequest;

    fn http_endpoint(&self) -> &'static str {
        "order/amend/keepPriority"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::PUT
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
    fn verification_request_http(&self) -> Option<Self::VerificationRequest> {
        let mut request = BinanceOrderStatusRequest::new(self.symbol.clone());
        request.orderId = self.orderId;
        request.recvWindow = self.recvWindow;
        Some(request)
    }
}

impl ETWebsocketRequest for BinanceAmendOrderRequest {
    type Response = BinanceResponse<BinanceAmendOrderResponse>;
    type VerificationRequest = BinanceOrderStatusRequest;

    fn websocket_method_name(&self) -> &'static str {
        "order.amend.keepPriority"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
    fn verification_request_websocket(&self) -> Option<Self::VerificationRequest> {
        let mut request = BinanceOrderStatusRequest::new(self.symbol.clone());
        request.orderId = self.orderId;
        request.recvWindow = self.recvWindow;
        Some(request)
    }
}
