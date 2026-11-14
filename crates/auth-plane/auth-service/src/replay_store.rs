use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::error::Error as StdError;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReplayKey {
    token_id: String,
    nonce: String,
}

impl ReplayKey {
    pub fn new(token_id: impl Into<String>, nonce: impl Into<String>) -> Self {
        Self {
            token_id: token_id.into(),
            nonce: nonce.into(),
        }
    }

    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    pub fn nonce(&self) -> &str {
        &self.nonce
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayReservation {
    key: ReplayKey,
    id: u64,
}

impl ReplayReservation {
    pub fn new(key: ReplayKey, id: u64) -> Self {
        Self { key, id }
    }

    pub fn key(&self) -> &ReplayKey {
        &self.key
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumeResult {
    Consumed(ReplayReservation),
    Replayed,
    Expired,
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayStoreError {
    #[error("replay store capacity of {capacity} has been reached")]
    CapacityExceeded { capacity: usize },
    #[error("replay store {operation} failed: {source}")]
    Unavailable {
        operation: &'static str,
        retryable: bool,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
}

impl ReplayStoreError {
    pub fn capacity_exceeded(capacity: usize) -> Self {
        Self::CapacityExceeded { capacity }
    }

    pub fn unavailable<E>(operation: &'static str, retryable: bool, source: E) -> Self
    where
        E: Into<Box<dyn StdError + Send + Sync>>,
    {
        Self::Unavailable {
            operation,
            retryable,
            source: source.into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            Self::CapacityExceeded { .. } => true,
            Self::Unavailable { retryable, .. } => *retryable,
        }
    }
}

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
