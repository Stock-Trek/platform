use crate::{
    binance::{
        macros::binance_signed_request, order_status::BinanceOrderStatusRequest,
        request::BinanceRequestFactory, response::BinanceResponse,
        supporting_types::BinanceCancelResponse,
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
use serde::Serialize;
use serde_with::skip_serializing_none;
use strum::Display;

binance_signed_request! {
    #[allow(non_snake_case)]
    #[skip_serializing_none]
    #[derive(Serialize, Debug, Clone, Hash, QueryParams)]
    pub struct BinanceCancelOrderRequest {
        pub cancelRestrictions: Option<BinanceCancelRestrictions>,
        pub newClientOrderId: Option<String>,
        pub orderId: Option<i64>,
        pub origClientOrderId: Option<String>,
        pub symbol: String,
    }
    idempotent: true,
    rate_limit: {
        RateLimitRestriction::Weight => UsageCount::ONE,
        _ => UsageCount::ZERO,
    }
}

impl BinanceCancelOrderRequest {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            apiKey: None,
            timestamp: 0,
            cancelRestrictions: None,
            newClientOrderId: None,
            orderId: None,
            origClientOrderId: None,
            recvWindow: None,
            symbol: symbol.into(),
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq, Hash, Display)]
pub enum BinanceCancelRestrictions {
    ONLY_NEW,
    ONLY_PARTIALLY_FILLED,
}

impl ETHttpRequest for BinanceCancelOrderRequest {
    type Response = BinanceResponse<BinanceCancelResponse>;
    type VerificationRequest = BinanceOrderStatusRequest;

    fn http_endpoint(&self) -> &'static str {
        "order"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::DELETE
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
    fn verification_request_http(&self) -> Option<Self::VerificationRequest> {
        let mut request = BinanceOrderStatusRequest::new(self.symbol.clone());
        request.orderId = self.orderId;
        request.recvWindow = self.recvWindow;
        Some(request)
    }
}

impl ETWebsocketRequest for BinanceCancelOrderRequest {
    type Response = BinanceResponse<BinanceCancelResponse>;
    type VerificationRequest = BinanceOrderStatusRequest;

    fn websocket_method_name(&self) -> &'static str {
        "order.cancel"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
    fn verification_request_websocket(&self) -> Option<Self::VerificationRequest> {
        let mut request = BinanceOrderStatusRequest::new(self.symbol.clone());
        request.orderId = self.orderId;
        request.recvWindow = self.recvWindow;
        Some(request)
    }
}
