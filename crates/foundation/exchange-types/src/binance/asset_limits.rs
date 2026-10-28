use crate::{
    binance::{
        exchange::BinanceExchange,
        filters::{BinanceAssetFilter, BinanceExchangeFilter, BinanceSymbolFilter},
        recv_window::BinanceRecvWindow,
        request::BinanceRequestFactory,
        response::BinanceResponse,
    },
    http::{HttpMethod, HttpRequest},
    new_types::{TimestampMilliseconds, UsageCount},
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETRequest, ETWebsocketRequest, WebsocketResponseMatcher},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use platform_errors::bp::BpResult;
use query_params::QueryParams;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[allow(non_snake_case)]
#[skip_serializing_none]
#[derive(Serialize, Debug, Clone, Hash, QueryParams)]
pub struct BinanceAssetLimitsRequest {
    apiKey: Option<String>,
    timestamp: i64,
    pub recvWindow: Option<BinanceRecvWindow>,
    pub symbol: String,
}

impl BinanceAssetLimitsRequest {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            apiKey: None,
            timestamp: 0,
            recvWindow: None,
            symbol: symbol.into(),
        }
    }
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceAssetLimitsResponse {
    pub assetFilters: Vec<BinanceAssetFilter>,
    pub exchangeFilters: Vec<BinanceExchangeFilter>,
    pub symbolFilters: Vec<BinanceSymbolFilter>,
}

impl ETRequest for BinanceAssetLimitsRequest {
    type Exchange = BinanceExchange;

    fn is_signed(&self) -> bool {
        true
    }
    fn is_idempotent(&self) -> bool {
        true
    }
    fn rate_limit_usage(&self, restriction: RateLimitRestriction) -> UsageCount {
        match restriction {
            RateLimitRestriction::Weight => UsageCount(40),
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

impl ETHttpRequest for BinanceAssetLimitsRequest {
    type Response = BinanceResponse<BinanceAssetLimitsResponse>;
    type VerificationRequest = Self;

    fn http_endpoint(&self) -> &'static str {
        "myFilters"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::GET
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
}

impl ETWebsocketRequest for BinanceAssetLimitsRequest {
    type Response = BinanceResponse<BinanceAssetLimitsResponse>;
    type VerificationRequest = Self;

    fn websocket_method_name(&self) -> &'static str {
        "myFilters"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
}
