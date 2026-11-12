use async_trait::async_trait;
use platform_errors::BuildResult;
use platform_exchange_gateway::connector::{ConnectorHealth, ConnectorHealthSink};
use platform_nats::EventPublisher;
use std::sync::Arc;

pub struct NatsConnectorHealthSink {
    publisher: Arc<dyn EventPublisher>,
    subject: String,
}

impl NatsConnectorHealthSink {
    pub fn new(publisher: Arc<dyn EventPublisher>, subject: impl Into<String>) -> Self {
        Self {
            publisher,
            subject: subject.into(),
        }
    }
}

#[async_trait]
impl ConnectorHealthSink for NatsConnectorHealthSink {
    async fn publish(&self, health: ConnectorHealth) -> BuildResult<()> {
        self.publisher
            .publish(&self.subject, &health.to_bytes()?)
            .await
    }
}
