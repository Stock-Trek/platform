use async_trait::async_trait;
use http_body_util::Full;
use hyper::{
    Request, Response,
    body::{Bytes, Incoming},
};
use std::net::SocketAddr;

#[async_trait]
pub(crate) trait RequestHandler: Send + Sync {
    async fn handle(&self, request: Request<Incoming>, peer: SocketAddr) -> Response<Full<Bytes>>;
}
