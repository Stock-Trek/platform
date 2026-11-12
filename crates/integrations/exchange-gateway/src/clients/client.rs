use async_trait::async_trait;
use platform_errors::BuildResult;
use platform_exchange_types::http::{HttpRequest, HttpResponse};
use std::time::Duration;

#[async_trait]
pub trait HttpClient {
    async fn send(&self, request: HttpRequest, timeout: Duration) -> BuildResult<HttpResponse>;
}

#[async_trait]
pub trait WebsocketClient: Sized + Send + Sync {
    async fn connect(&self) -> BuildResult<()>;
    fn is_connected(&self) -> bool;
    async fn send(&self, message: String, timeout: Duration) -> BuildResult<()>;
    async fn disconnect(&self) -> BuildResult<()>;
}
