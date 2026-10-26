use async_trait::async_trait;
use platform_errors::BpResult;
use platform_exchange_types::http::{HttpRequest, HttpResponse};
use std::time::Duration;

#[async_trait]
pub trait HttpClient {
    async fn send(&self, request: HttpRequest, timeout: Duration) -> BpResult<HttpResponse>;
}

#[async_trait]
pub trait WebsocketClient: Sized + Send + Sync {
    async fn connect(&self) -> BpResult<()>;
    fn is_connected(&self) -> bool;
    async fn send(&self, message: String, timeout: Duration) -> BpResult<()>;
    async fn disconnect(&self) -> BpResult<()>;
}
