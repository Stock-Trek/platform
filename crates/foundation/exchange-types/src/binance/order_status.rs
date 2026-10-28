use crate::{
    binance::{
        macros::binance_signed_request, request::BinanceRequestFactory, response::BinanceResponse,
        supporting_types::BinanceOrderStatusResponse,
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
use serde::Serialize;
use serde_with::skip_serializing_none;

binance_signed_request! {
    #[allow(non_snake_case)]
    #[skip_serializing_none]
    #[derive(Serialize, Debug, Clone, Hash, QueryParams)]
    pub struct BinanceOrderStatusRequest {
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

impl BinanceOrderStatusRequest {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            apiKey: None,
            timestamp: 0,
            orderId: None,
            origClientOrderId: None,
            recvWindow: None,
            symbol: symbol.into(),
        }
    }
}

impl ETHttpRequest for BinanceOrderStatusRequest {
    type Response = BinanceResponse<BinanceOrderStatusResponse>;
    type VerificationRequest = Self;

    fn http_endpoint(&self) -> &'static str {
        "order"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::GET
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
}

impl ETWebsocketRequest for BinanceOrderStatusRequest {
    type Response = BinanceResponse<BinanceOrderStatusResponse>;
    type VerificationRequest = Self;

    fn websocket_method_name(&self) -> &'static str {
        "order.status"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
}
