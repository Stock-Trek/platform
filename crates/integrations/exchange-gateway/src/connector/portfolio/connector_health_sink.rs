use super::ConnectorHealth;
use async_trait::async_trait;
use platform_errors::BuildResult;

#[async_trait]
pub trait ConnectorHealthSink: Send + Sync {
    async fn publish(&self, health: ConnectorHealth) -> BuildResult<()>;
}
