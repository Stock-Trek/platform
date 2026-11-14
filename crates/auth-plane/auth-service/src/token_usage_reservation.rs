use crate::token_usage::TokenUsage;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenUsageReservation {
    id: String,
    usage: TokenUsage,
}

impl TokenUsageReservation {
    pub fn new(id: impl Into<String>, usage: TokenUsage) -> Self {
        Self {
            id: id.into(),
            usage,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn usage(&self) -> &TokenUsage {
        &self.usage
    }

    pub fn into_usage(self) -> TokenUsage {
        self.usage
    }
}
