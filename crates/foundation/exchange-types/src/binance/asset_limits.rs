use crate::{
    binance::{
        filters::{BinanceAssetFilter, BinanceExchangeFilter, BinanceSymbolFilter},
        macros::binance_signed_request,
        request::BinanceRequestFactory,
        response::BinanceResponse,
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
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

binance_signed_request! {
    #[allow(non_snake_case)]
    #[skip_serializing_none]
    #[derive(Serialize, Debug, Clone, Hash, QueryParams)]
    pub struct BinanceAssetLimitsRequest {
        pub symbol: String,
    }
    idempotent: true,
    rate_limit: {
        RateLimitRestriction::Weight => UsageCount(40),
        _ => UsageCount::ZERO,
    }
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
