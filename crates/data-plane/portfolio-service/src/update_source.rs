use crate::PortfolioUpdate;
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_nats::Message;

#[derive(Debug, Clone)]
pub struct PortfolioUpdateMessage {
    update: PortfolioUpdate,
    stream_sequence: Option<u64>,
    raw: Option<Message>,
}

impl PortfolioUpdateMessage {
    pub fn new(
        update: PortfolioUpdate,
        stream_sequence: Option<u64>,
        raw: Option<Message>,
    ) -> Self {
        Self {
            update,
            stream_sequence,
            raw,
        }
    }

    pub fn update(&self) -> &PortfolioUpdate {
        &self.update
    }

    pub fn stream_sequence(&self) -> Option<u64> {
        self.stream_sequence
    }

    pub fn raw(&self) -> Option<&Message> {
        self.raw.as_ref()
    }
}

#[async_trait]
pub trait PortfolioUpdateSource: Send + Sync {
    async fn next(&self) -> BpResult<Option<PortfolioUpdateMessage>>;

    async fn checkpoint(&self, sequence: u64) -> BpResult<()>;

    async fn acknowledge(&self, message: &PortfolioUpdateMessage) -> BpResult<()>;
}
