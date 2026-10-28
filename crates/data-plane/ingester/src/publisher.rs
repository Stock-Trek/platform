use async_trait::async_trait;
use platform_errors::BpResult;
use platform_nats::NatsPublisher;

#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()>;
}

#[async_trait]
impl EventPublisher for NatsPublisher {
    async fn publish(&self, subject: &str, payload: &[u8]) -> BpResult<()> {
        NatsPublisher::publish(self, subject, payload).await
    }
}
