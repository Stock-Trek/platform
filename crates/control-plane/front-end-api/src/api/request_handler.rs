use async_trait::async_trait;
use http_body_util::Full;
use hyper::{
    Request, Response,
    body::{Bytes, Incoming},
};

#[async_trait]
pub trait RequestHandler: Send + Sync {
    async fn handle(&self, request: Request<Incoming>) -> Response<Full<Bytes>>;
}
