use crate::{
    binance::{
        exchange::BinanceExchange,
        request::BinanceRequestFactory,
        response::{BinanceResponse, BinanceResponsePayload},
    },
    http::{HttpMethod, HttpRequest},
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETRequest},
    signer::Signer,
};
use platform_errors::{BpError, BpResult};
use platform_types::new_types::{TimestampMilliseconds, UsageCount};
use query_params::QueryParams;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[allow(non_snake_case)]
#[skip_serializing_none]
#[derive(Serialize, Debug, Clone, Hash, QueryParams)]
pub struct BinanceDepthRequest {
    pub symbol: String,
    pub limit: Option<u32>,
}

impl BinanceDepthRequest {
    pub fn new(symbol: String, limit: Option<u32>) -> Self {
        Self { symbol, limit }
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceDepthResponse {
    pub lastUpdateId: u64,
    #[serde(default)]
    pub bids: Vec<[String; 2]>,
    #[serde(default)]
    pub asks: Vec<[String; 2]>,
}

impl BinanceResponse<BinanceDepthResponse> {
    pub fn depth(self) -> BpResult<BinanceDepthResponse> {
        match self.payload {
            BinanceResponsePayload::Success(payload) => Ok(payload),
            BinanceResponsePayload::Failure(error) => {
                let retry_after = self.metadata.retry_after.or_else(|| error.retry_after());
                Err(BpError::Exchange(
                    error.exchange_error(self.metadata.status, retry_after),
                ))
            }
        }
    }
}

impl ETRequest for BinanceDepthRequest {
    type Exchange = BinanceExchange;

    fn is_signed(&self) -> bool {
        false
    }
    fn is_idempotent(&self) -> bool {
        true
    }
    fn rate_limit_usage(&self, restriction: RateLimitRestriction) -> UsageCount {
        match restriction {
            RateLimitRestriction::Weight => UsageCount(50),
            _ => UsageCount::ZERO,
        }
    }
    fn set_api_key(&mut self, _api_key: Option<String>) {}
    fn set_timestamp(&mut self, _timestamp: TimestampMilliseconds) {}
    fn query_params(&self, percent_encode: bool) -> String {
        self.query_params(true, percent_encode)
    }
}

impl ETHttpRequest for BinanceDepthRequest {
    type Response = BinanceResponse<BinanceDepthResponse>;
    type VerificationRequest = Self;

    fn http_endpoint(&self) -> &'static str {
        "depth"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::GET
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
}
