use crate::{
    exchange::ETExchange,
    http::{HttpMethod, HttpRequest},
    new_types::{TimestampMilliseconds, UsageCount},
    rate_limited::RateLimitRestriction,
    response::{ETHttpResponse, ETWebsocketResponse},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use platform_errors::BpResult;
use serde::Serialize;
use std::{fmt::Debug, sync::Arc};

pub trait ETRequest: Clone + Debug + Serialize {
    type Exchange: ETExchange;

    fn rate_limit_usage(&self, restriction: RateLimitRestriction) -> UsageCount;
    fn is_signed(&self) -> bool;
    fn is_idempotent(&self) -> bool {
        false
    }
    fn set_api_key(&mut self, api_key: Option<String>);
    fn set_timestamp(&mut self, timestamp: TimestampMilliseconds);
    fn query_params(&self, percent_encode: bool) -> String;
}

pub trait ETHttpRequest: ETRequest
where
    Self: Sized,
{
    type Response: ETHttpResponse;
    type VerificationRequest: ETHttpRequest;

    fn http_method(&self) -> HttpMethod;
    fn http_endpoint(&self) -> &'static str;
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest>;
    fn verification_request_http(&self) -> Option<Self::VerificationRequest> {
        None as Option<Self::VerificationRequest>
    }
}

pub trait ETWebsocketRequest: ETRequest
where
    Self: Sized,
{
    type Response: ETWebsocketResponse;
    type VerificationRequest: ETWebsocketRequest;

    fn websocket_method_name(&self) -> &'static str;
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)>;
    fn verification_request_websocket(&self) -> Option<Self::VerificationRequest> {
        None as Option<Self::VerificationRequest>
    }
}

pub type WebsocketResponseMatcher = Arc<dyn Fn(&serde_json::Value) -> bool + Send + Sync>;
