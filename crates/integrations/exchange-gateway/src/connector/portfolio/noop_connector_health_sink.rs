use crate::connector::portfolio::{ConnectorHealth, ConnectorHealthSink};
use async_trait::async_trait;
use platform_errors::BuildResult;

#[derive(Debug, Default)]
pub struct NoopConnectorHealthSink;

impl NoopConnectorHealthSink {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ConnectorHealthSink for NoopConnectorHealthSink {
    async fn publish(&self, _health: ConnectorHealth) -> BuildResult<()> {
        Ok(())
    }
}
