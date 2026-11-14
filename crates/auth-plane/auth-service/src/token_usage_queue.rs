use crate::{
    auth_service::AuthService, token_usage::TokenUsage,
    token_usage_reservation::TokenUsageReservation,
};
use async_trait::async_trait;
use platform_errors::PlatformResult;

#[async_trait]
pub trait TokenUsageQueue: Send + Sync {
    async fn enqueue(&self, usage: TokenUsage) -> PlatformResult<()>;

    async fn reserve(&self, limit: usize) -> PlatformResult<Vec<TokenUsageReservation>>;

    async fn acknowledge(&self, reservation: &TokenUsageReservation) -> PlatformResult<()>;

    async fn release(&self, reservation: &TokenUsageReservation) -> PlatformResult<()>;
}

impl AuthService {
    pub async fn enqueue_usage(&self, mut usage: TokenUsage) -> PlatformResult<()> {
        usage.ensure_event_ids();
        self.usage.enqueue(usage).await
    }
}
