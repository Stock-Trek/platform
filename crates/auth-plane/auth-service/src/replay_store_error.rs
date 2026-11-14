use std::error::Error as StdError;

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
