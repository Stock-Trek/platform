use crate::{
    exchange::ETExchange,
    klines::{KlinesHttpRequest, KlinesResponse, KlinesWebsocketRequest},
    response::{ETHttpResponse, ETWebsocketResponse},
};

pub trait ETExchangeKlines: ETExchange {
    type KlinesRequestHttp: KlinesHttpRequest<Self::KlinesResponseHttp>;
    type KlinesResponseHttp: ETHttpResponse + KlinesResponse;
    type KlinesRequestWebsocket: KlinesWebsocketRequest<Self::KlinesResponseWebsocket>;
    type KlinesResponseWebsocket: ETWebsocketResponse + KlinesResponse;
}
