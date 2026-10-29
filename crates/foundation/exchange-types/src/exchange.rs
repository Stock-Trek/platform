use crate::{
    klines::{KlinesHttpRequest, KlinesResponse, KlinesWebsocketRequest},
    rate_limited::RateLimit,
    response::{ETHttpResponse, ETWebsocketResponse},
    server_time::{ServerTimeHttpRequest, ServerTimeResponse, ServerTimeWebsocketRequest},
    urls::Urls,
};
use platform_types::new_types::UsageCount;
use std::collections::HashMap;

pub trait ETExchange {
    type Credentials;
    type KlinesRequestHttp: KlinesHttpRequest<Self::KlinesResponseHttp>;
    type KlinesResponseHttp: ETHttpResponse + KlinesResponse;
    type KlinesRequestWebsocket: KlinesWebsocketRequest<Self::KlinesResponseWebsocket>;
    type KlinesResponseWebsocket: ETWebsocketResponse + KlinesResponse;
    type ServerTimeRequestHttp: ServerTimeHttpRequest<Self::ServerTimeResponseHttp>;
    type ServerTimeResponseHttp: ETHttpResponse + ServerTimeResponse;
    type ServerTimeRequestWebsocket: ServerTimeWebsocketRequest<Self::ServerTimeResponseWebsocket>;
    type ServerTimeResponseWebsocket: ETWebsocketResponse + ServerTimeResponse;

    fn name(&self) -> &'static str;
    fn default_capacity(&self) -> HashMap<RateLimit, UsageCount>;
    fn server_time_request_http(&self) -> Self::ServerTimeRequestHttp;
    fn server_time_request_websocket(&self) -> Self::ServerTimeRequestWebsocket;
    fn urls(&self) -> &impl Urls;
}
