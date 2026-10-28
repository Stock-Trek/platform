use crate::{
    binance::{
        macros::binance_unsigned_request, request::BinanceRequestFactory,
        response::BinanceResponse, supporting_types::BinanceWebsocketSessionResponse,
    },
    new_types::UsageCount,
    rate_limited::RateLimitRestriction,
    request::{ETWebsocketRequest, WebsocketResponseMatcher},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use platform_errors::BpResult;
use query_params::QueryParams;
use serde::Serialize;

binance_unsigned_request! {
    #[derive(Serialize, Debug, Clone, Hash, QueryParams)]
    pub struct BinanceSessionLogoutRequest {}
    idempotent: true,
    rate_limit: {
        RateLimitRestriction::Weight => UsageCount(2),
        _ => UsageCount::ZERO,
    }
}

#[allow(clippy::new_without_default)]
impl BinanceSessionLogoutRequest {
    pub fn new() -> Self {
        Self {}
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
