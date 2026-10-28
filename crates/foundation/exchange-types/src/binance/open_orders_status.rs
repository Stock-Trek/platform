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
    pub struct BinanceOpenOrdersStatusRequest {
        pub symbol: Option<String>,
    }
    idempotent: true,
    rate_limit_method: rate_limit_usage_inner
}

impl BinanceOpenOrdersStatusRequest {
    pub fn new() -> Self {
        Self {
            apiKey: None,
            timestamp: 0,
            recvWindow: None,
            symbol: None,
        }
    }

    fn rate_limit_usage_inner(&self, restriction: RateLimitRestriction) -> UsageCount {
        match restriction {
            RateLimitRestriction::Weight => {
                if self.symbol.is_some() {
                    UsageCount(6)
                } else {
                    UsageCount(80)
                }
            }
            _ => UsageCount::ZERO,
        }
    }
}

impl Default for BinanceOpenOrdersStatusRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl ETHttpRequest for BinanceOpenOrdersStatusRequest {
    type Response = BinanceResponse<Vec<BinanceOrderStatusResponse>>;
    type VerificationRequest = Self;

    fn http_endpoint(&self) -> &'static str {
        "openOrders"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::GET
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
}

impl ETWebsocketRequest for BinanceOpenOrdersStatusRequest {
    type Response = BinanceResponse<Vec<BinanceOrderStatusResponse>>;
    type VerificationRequest = Self;

    fn websocket_method_name(&self) -> &'static str {
        "openOrders.status"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
}
