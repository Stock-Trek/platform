use crate::connector::portfolio::ConnectorHealth;
use async_trait::async_trait;
use platform_errors::PlatformResult;

#[async_trait]
pub trait ConnectorHealthSink: Send + Sync {
    async fn publish(&self, health: ConnectorHealth) -> PlatformResult<()>;
}
