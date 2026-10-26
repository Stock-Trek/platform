use crate::{
    new_types::TimestampMilliseconds,
    request::{ETHttpRequest, ETWebsocketRequest},
    response::{ETHttpResponse, ETWebsocketResponse},
};
use platform_errors::BpResult;
use rust_decimal::Decimal;

pub trait KlinesHttpRequest<Response>: ETHttpRequest<Response = Response>
where
    Response: ETHttpResponse + KlinesResponse,
{
}

pub trait KlinesWebsocketRequest<Response>: ETWebsocketRequest<Response = Response>
where
    Response: ETWebsocketResponse + KlinesResponse,
{
}

pub trait KlinesResponse {
    fn klines(&self) -> BpResult<Vec<Kline>>;
}

#[derive(Debug, Clone)]
pub struct Kline {
    pub open_time: TimestampMilliseconds,
    pub close_time: TimestampMilliseconds,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub trades: u32,
}
