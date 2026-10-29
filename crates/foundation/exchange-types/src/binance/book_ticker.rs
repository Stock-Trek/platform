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
pub struct BinanceBookTickerRequest {
    pub symbol: Option<String>,
}

impl BinanceBookTickerRequest {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol: Some(symbol),
        }
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceBookTickerResponse {
    pub symbol: String,
    pub bidPrice: String,
    pub bidQty: String,
    pub askPrice: String,
    pub askQty: String,
}

impl BinanceResponse<BinanceBookTickerResponse> {
    pub fn book_ticker(self) -> BpResult<BinanceBookTickerResponse> {
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

impl ETRequest for BinanceBookTickerRequest {
    type Exchange = BinanceExchange;

    fn is_signed(&self) -> bool {
        false
    }
    fn is_idempotent(&self) -> bool {
        true
    }
    fn rate_limit_usage(&self, restriction: RateLimitRestriction) -> UsageCount {
        match restriction {
            RateLimitRestriction::Weight => UsageCount(2),
            _ => UsageCount::ZERO,
        }
    }
    fn set_api_key(&mut self, _api_key: Option<String>) {}
    fn set_timestamp(&mut self, _timestamp: TimestampMilliseconds) {}
    fn query_params(&self, percent_encode: bool) -> String {
        self.query_params(true, percent_encode)
    }
}

impl ETHttpRequest for BinanceBookTickerRequest {
    type Response = BinanceResponse<BinanceBookTickerResponse>;
    type VerificationRequest = Self;

    fn http_endpoint(&self) -> &'static str {
        "ticker/bookTicker"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::GET
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
}
