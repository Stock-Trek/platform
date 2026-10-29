use crate::{
    binance::{
        exchange::BinanceExchange, request::BinanceRequestFactory, response::BinanceResponse,
        supporting_types::BinanceWebsocketSessionResponse,
    },
    rate_limited::RateLimitRestriction,
    request::{ETRequest, ETWebsocketRequest, WebsocketResponseMatcher},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use platform_errors::BpResult;
use platform_types::new_types::{TimestampMilliseconds, UsageCount};
use query_params::QueryParams;
use serde::Serialize;

#[derive(Serialize, Debug, Clone, Hash, QueryParams)]
pub struct BinanceSessionLogoutRequest {}

#[allow(clippy::new_without_default)]
impl BinanceSessionLogoutRequest {
    pub fn new() -> Self {
        Self {}
    }
}

impl ETRequest for BinanceSessionLogoutRequest {
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

impl ETWebsocketRequest for BinanceSessionLogoutRequest {
    type Response = BinanceResponse<BinanceWebsocketSessionResponse>;
    type VerificationRequest = Self;

    fn websocket_method_name(&self) -> &'static str {
        "session.logout"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
}
