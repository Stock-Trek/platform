use super::ConnectorHealth;
use async_trait::async_trait;
use platform_errors::BpResult;

#[async_trait]
pub trait ConnectorHealthSink: Send + Sync {
    async fn publish(&self, health: ConnectorHealth) -> BpResult<()>;
}
