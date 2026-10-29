use crate::{
    binance::{
        exchange::BinanceExchange,
        request::BinanceRequestFactory,
        response::{BinanceResponse, BinanceResponsePayload},
    },
    http::{HttpMethod, HttpRequest},
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETRequest, ETWebsocketRequest, WebsocketResponseMatcher},
    server_time::{ServerTimeHttpRequest, ServerTimeResponse, ServerTimeWebsocketRequest},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use platform_errors::{BpError, BpResult};
use platform_types::new_types::{TimestampMilliseconds, UsageCount};
use query_params::QueryParams;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug, Clone, Hash, QueryParams)]
pub struct BinanceTimeRequest {}

#[allow(clippy::new_without_default)]
impl BinanceTimeRequest {
    pub fn new() -> Self {
        Self {}
    }
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceTimeResponse {
    pub serverTime: i64,
}

impl ServerTimeHttpRequest<BinanceResponse<BinanceTimeResponse>> for BinanceTimeRequest {}
impl ServerTimeWebsocketRequest<BinanceResponse<BinanceTimeResponse>> for BinanceTimeRequest {}

impl ServerTimeResponse for BinanceResponse<BinanceTimeResponse> {
    fn server_time(&self) -> BpResult<TimestampMilliseconds> {
        match &self.payload {
            BinanceResponsePayload::Failure(error) => {
                let retry_after = self.metadata.retry_after.or_else(|| error.retry_after());
                Err(BpError::Exchange(
                    error.exchange_error(self.metadata.status, retry_after),
                ))
            }
            BinanceResponsePayload::Success(payload) => {
                Ok(TimestampMilliseconds(payload.serverTime))
            }
        }
    }
}

impl ETRequest for BinanceTimeRequest {
    type Exchange = BinanceExchange;

    fn is_signed(&self) -> bool {
        false
    }
    fn is_idempotent(&self) -> bool {
        true
    }
    fn rate_limit_usage(&self, restriction: RateLimitRestriction) -> UsageCount {
        match restriction {
            RateLimitRestriction::Weight => UsageCount::ONE,
            _ => UsageCount::ZERO,
        }
    }
    fn set_api_key(&mut self, _api_key: Option<String>) {}
    fn set_timestamp(&mut self, _timestamp: TimestampMilliseconds) {}
    fn query_params(&self, percent_encode: bool) -> String {
        self.query_params(true, percent_encode)
    }
}

impl ETHttpRequest for BinanceTimeRequest {
    type Response = BinanceResponse<BinanceTimeResponse>;
    type VerificationRequest = Self;

    fn http_endpoint(&self) -> &'static str {
        "time"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::GET
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
}

impl ETWebsocketRequest for BinanceTimeRequest {
    type Response = BinanceResponse<BinanceTimeResponse>;
    type VerificationRequest = Self;

    fn websocket_method_name(&self) -> &'static str {
        "time"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
}
