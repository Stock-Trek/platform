use crate::{
    exchange::ETExchange,
    response::{ETHttpResponse, ETWebsocketResponse},
    server_time::{ServerTimeHttpRequest, ServerTimeResponse, ServerTimeWebsocketRequest},
};

pub trait ETExchangeServerTime: ETExchange {
    type ServerTimeRequestHttp: ServerTimeHttpRequest<Self::ServerTimeResponseHttp>;
    type ServerTimeResponseHttp: ETHttpResponse + ServerTimeResponse;
    type ServerTimeRequestWebsocket: ServerTimeWebsocketRequest<Self::ServerTimeResponseWebsocket>;
    type ServerTimeResponseWebsocket: ETWebsocketResponse + ServerTimeResponse;

    fn server_time_request_http(&self) -> Self::ServerTimeRequestHttp;
    fn server_time_request_websocket(&self) -> Self::ServerTimeRequestWebsocket;
}
