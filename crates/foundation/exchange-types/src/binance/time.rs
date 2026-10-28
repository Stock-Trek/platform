use crate::{
    binance::{
        macros::binance_unsigned_request,
        request::BinanceRequestFactory,
        response::{BinanceResponse, BinanceResponsePayload},
    },
    http::{HttpMethod, HttpRequest},
    new_types::{TimestampMilliseconds, UsageCount},
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETWebsocketRequest, WebsocketResponseMatcher},
    server_time::{ServerTimeHttpRequest, ServerTimeResponse, ServerTimeWebsocketRequest},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use platform_errors::{BpError, BpResult};
use query_params::QueryParams;
use serde::{Deserialize, Serialize};

binance_unsigned_request! {
    #[derive(Serialize, Debug, Clone, Hash, QueryParams)]
    pub struct BinanceTimeRequest {}
    idempotent: true,
    rate_limit: {
        RateLimitRestriction::Weight => UsageCount::ONE,
        _ => UsageCount::ZERO,
    }
}

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
