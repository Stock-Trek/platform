use async_trait::async_trait;
use platform_errors::PlatformResult;

#[async_trait]
pub trait WebsocketClient: Sized + Send + Sync {
    async fn connect(&self) -> PlatformResult<()>;
    fn is_connected(&self) -> bool;
    async fn send(&self, message: String, timeout: std::time::Duration) -> PlatformResult<()>;
    async fn disconnect(&self) -> PlatformResult<()>;
}
