use crate::{
    binance::{
        exchange::BinanceExchange, recv_window::BinanceRecvWindow, request::BinanceRequestFactory,
        response::BinanceResponse, supporting_types::BinanceOrderStatusResponse,
    },
    http::{HttpMethod, HttpRequest},
    new_types::{TimestampMilliseconds, UsageCount},
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETRequest, ETWebsocketRequest, WebsocketResponseMatcher},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use platform_errors::BpResult;
use query_params::QueryParams;
use serde::Serialize;
use serde_with::skip_serializing_none;

#[allow(non_snake_case)]
#[skip_serializing_none]
#[derive(Serialize, Debug, Clone, Hash, QueryParams)]
pub struct BinanceOpenOrdersStatusRequest {
    apiKey: Option<String>,
    timestamp: i64,
    pub recvWindow: Option<BinanceRecvWindow>,
    pub symbol: Option<String>,
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
}

impl Default for BinanceOpenOrdersStatusRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl ETRequest for BinanceOpenOrdersStatusRequest {
    type Exchange = BinanceExchange;

    fn is_signed(&self) -> bool {
        true
    }
    fn is_idempotent(&self) -> bool {
        true
    }
    fn rate_limit_usage(&self, restriction: RateLimitRestriction) -> UsageCount {
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
