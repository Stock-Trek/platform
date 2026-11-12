use crate::PortfolioUpdate;
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
