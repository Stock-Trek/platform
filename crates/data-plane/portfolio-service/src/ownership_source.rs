use async_trait::async_trait;
use platform_errors::BpResult;
use platform_nats::Message;
use platform_portfolio_ownership::OwnershipEvent;

#[derive(Debug, Clone)]
pub struct OwnershipMessage {
    event: OwnershipEvent,
    stream_sequence: Option<u64>,
    raw: Option<Message>,
}

impl OwnershipMessage {
    pub fn new(event: OwnershipEvent, stream_sequence: Option<u64>, raw: Option<Message>) -> Self {
        Self {
            event,
            stream_sequence,
            raw,
        }
    }

    pub fn event(&self) -> &OwnershipEvent {
        &self.event
    }

    pub fn stream_sequence(&self) -> Option<u64> {
        self.stream_sequence
    }

    pub fn raw(&self) -> Option<&Message> {
        self.raw.as_ref()
    }
}

#[async_trait]
pub trait OwnershipEventSource: Send + Sync {
    async fn next(&self) -> BpResult<Option<OwnershipMessage>>;

    async fn checkpoint(&self, sequence: u64) -> BpResult<()>;

    async fn acknowledge(&self, message: &OwnershipMessage) -> BpResult<()>;
}
