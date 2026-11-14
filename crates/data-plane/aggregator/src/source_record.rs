use crate::market_input::MarketInput;
use platform_nats::Message;

#[derive(Debug, Clone)]
pub struct SourceRecord {
    pub input: MarketInput,
    pub message: Message,
}

impl SourceRecord {
    pub fn new(input: MarketInput, message: Message) -> Self {
        Self { input, message }
    }
}
