use crate::{
    exchange::ETExchange,
    klines::{KlinesHttpRequest, KlinesResponse, KlinesWebsocketRequest},
    response::{ETHttpResponse, ETWebsocketResponse},
};

pub trait ETExchangeKlinesHttp: ETExchange {
    type KlinesRequestHttp: KlinesHttpRequest<Self::KlinesResponseHttp>;
    type KlinesResponseHttp: ETHttpResponse + KlinesResponse;
}

pub trait ETExchangeKlinesWebsocket: ETExchange {
    type KlinesRequestWebsocket: KlinesWebsocketRequest<Self::KlinesResponseWebsocket>;
    type KlinesResponseWebsocket: ETWebsocketResponse + KlinesResponse;
}
