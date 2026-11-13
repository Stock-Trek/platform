use crate::{
    exchange::ETExchange,
    response::{ETHttpResponse, ETWebsocketResponse},
    server_time::{ServerTimeHttpRequest, ServerTimeResponse, ServerTimeWebsocketRequest},
};

pub trait ETExchangeServerTimeHttp: ETExchange {
    type ServerTimeRequestHttp: ServerTimeHttpRequest<Self::ServerTimeResponseHttp>;
    type ServerTimeResponseHttp: ETHttpResponse + ServerTimeResponse;

    fn server_time_request_http(&self) -> Self::ServerTimeRequestHttp;
}

pub trait ETExchangeServerTimeWebsocket: ETExchange {
    type ServerTimeRequestWebsocket: ServerTimeWebsocketRequest<Self::ServerTimeResponseWebsocket>;
    type ServerTimeResponseWebsocket: ETWebsocketResponse + ServerTimeResponse;

    fn server_time_request_websocket(&self) -> Self::ServerTimeRequestWebsocket;
}
