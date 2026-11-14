use crate::{
    consume_result::ConsumeResult, replay_key::ReplayKey, replay_reservation::ReplayReservation,
    replay_store_error::ReplayStoreError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[async_trait]
pub trait ReplayStore: Send + Sync {
    async fn consume(
        &self,
        key: ReplayKey,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<ConsumeResult, ReplayStoreError>;

    async fn release(&self, reservation: &ReplayReservation) -> Result<(), ReplayStoreError>;
}
