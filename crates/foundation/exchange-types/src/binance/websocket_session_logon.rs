use crate::{
    binance::{
        macros::binance_signed_request, request::BinanceRequestFactory, response::BinanceResponse,
        supporting_types::BinanceWebsocketSessionResponse,
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
use serde_with::skip_serializing_none;

binance_signed_request! {
    #[allow(non_snake_case)]
    #[skip_serializing_none]
    #[derive(Serialize, Debug, Clone, Hash, QueryParams)]
    pub struct BinanceSessionLogonRequest {}
    idempotent: true,
    rate_limit: {
        RateLimitRestriction::Weight => UsageCount(2),
        _ => UsageCount::ZERO,
    }
}

impl BinanceSessionLogonRequest {
    pub fn new() -> Self {
        Self {
            apiKey: None,
            timestamp: 0,
            recvWindow: None,
        }
    }
}

impl Default for BinanceSessionLogonRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl ETWebsocketRequest for BinanceSessionLogonRequest {
    type Response = BinanceResponse<BinanceWebsocketSessionResponse>;
    type VerificationRequest = Self;

    fn websocket_method_name(&self) -> &'static str {
        "session.logon"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
}
