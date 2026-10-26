use crate::{
    http::HttpResponse,
    new_types::Seconds,
    rate_limited::{RateLimit, RateUsage},
};
use platform_errors::BpResult;
use std::{collections::HashMap, fmt::Debug};

pub trait ETResponse: Clone + Debug {
    fn rate_limit_usage(&self) -> Option<&HashMap<RateLimit, RateUsage>>;
    fn retry_after(&self) -> Option<Seconds>;
}

pub trait ETHttpResponse: ETResponse
where
    Self: Sized,
{
    fn try_from_http(response: HttpResponse) -> BpResult<Self>;
}

pub trait ETWebsocketResponse: ETResponse
where
    Self: Sized,
{
    fn try_from_websocket(response: serde_json::Value) -> BpResult<Self>;
}
