use crate::{
    binance::{
        exchange::BinanceExchange, recv_window::BinanceRecvWindow, request::BinanceRequestFactory,
        response::BinanceResponse, supporting_types::BinanceWebsocketSessionResponse,
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
use serde_with::skip_serializing_none;

#[allow(non_snake_case)]
#[skip_serializing_none]
#[derive(Serialize, Debug, Clone, Hash, QueryParams)]
pub struct BinanceSessionLogonRequest {
    apiKey: Option<String>,
    timestamp: i64,
    pub recvWindow: Option<BinanceRecvWindow>,
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

impl ETRequest for BinanceSessionLogonRequest {
    type Exchange = BinanceExchange;

    fn is_signed(&self) -> bool {
        true
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
