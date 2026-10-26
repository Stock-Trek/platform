use async_trait::async_trait;
use platform_errors::BpResult;

#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()>;
}
