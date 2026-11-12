use crate::{
    request::{ETHttpRequest, ETWebsocketRequest},
    response::{ETHttpResponse, ETWebsocketResponse},
};
use platform_errors::BuildResult;
use platform_types::TimestampMilliseconds;

pub trait ServerTimeHttpRequest<Response>: ETHttpRequest<Response = Response>
where
    Response: ETHttpResponse + ServerTimeResponse,
{
}

pub trait ServerTimeWebsocketRequest<Response>: ETWebsocketRequest<Response = Response>
where
    Response: ETWebsocketResponse + ServerTimeResponse,
{
}

pub trait ServerTimeResponse {
    fn server_time(&self) -> BuildResult<TimestampMilliseconds>;
}
