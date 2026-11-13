use async_trait::async_trait;
use platform_errors::BuildResult;

pub use platform_exchange_types::http::HttpClient;

#[async_trait]
pub trait WebsocketClient: Sized + Send + Sync {
    async fn connect(&self) -> BuildResult<()>;
    fn is_connected(&self) -> bool;
    async fn send(&self, message: String, timeout: std::time::Duration) -> BuildResult<()>;
    async fn disconnect(&self) -> BuildResult<()>;
}
