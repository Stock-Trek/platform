use crate::{
    binance::{
        macros::binance_signed_request, open_orders_status::BinanceOpenOrdersStatusRequest,
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

binance_signed_request! {
    #[allow(non_snake_case)]
    #[skip_serializing_none]
    #[derive(Serialize, Debug, Clone, Hash, QueryParams)]
    pub struct BinanceCancelAllOpenOrdersRequest {
        pub symbol: String,
    }
    idempotent: false,
    rate_limit: {
        RateLimitRestriction::Weight => UsageCount::ONE,
        _ => UsageCount::ZERO,
    }
}

impl BinanceCancelAllOpenOrdersRequest {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            apiKey: None,
            timestamp: 0,
            recvWindow: None,
            symbol: symbol.into(),
        }
    }
}

impl ETHttpRequest for BinanceCancelAllOpenOrdersRequest {
    type Response = BinanceResponse<Vec<BinanceCancelResponse>>;
    type VerificationRequest = BinanceOpenOrdersStatusRequest;

    fn http_endpoint(&self) -> &'static str {
        "openOrders"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::DELETE
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
    fn verification_request_http(&self) -> Option<Self::VerificationRequest> {
        let mut request = BinanceOpenOrdersStatusRequest::default();
        request.symbol = Some(self.symbol.clone());
        request.recvWindow = self.recvWindow;
        Some(request)
    }
}

impl ETWebsocketRequest for BinanceCancelAllOpenOrdersRequest {
    type Response = BinanceResponse<Vec<BinanceCancelResponse>>;
    type VerificationRequest = BinanceOpenOrdersStatusRequest;

    fn websocket_method_name(&self) -> &'static str {
        "openOrders.cancelAll"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
    fn verification_request_websocket(&self) -> Option<Self::VerificationRequest> {
        let mut request = BinanceOpenOrdersStatusRequest::default();
        request.symbol = Some(self.symbol.clone());
        request.recvWindow = self.recvWindow;
        Some(request)
    }
}
