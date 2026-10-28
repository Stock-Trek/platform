use crate::retryability::Retryability;
use platform_types::new_types::{DurationNanoseconds, UsageCount};

#[derive(Debug, thiserror::Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum RateLimitError {
    #[error("Rate limiter capacity must be greater than zero")]
    InvalidRateLimitCapacity,
    #[error("Rate limiter interval must be greater than zero")]
    InvalidRateLimitInterval,
    #[error("Rate limit exceeded")]
    RateLimited,
    #[error(
        "Request rate limit cost {cost} exceeds the maximum capacity {capacity} for interval {interval_nanos}ns"
    )]
    RequestExceedsRateLimit {
        cost: UsageCount,
        capacity: UsageCount,
        interval_nanos: DurationNanoseconds,
    },
}

impl RateLimitError {
    pub fn error_type(&self) -> &'static str {
        self.into()
    }

    pub fn retryability(&self) -> Retryability {
        match self {
            Self::RateLimited => Retryability::Retryable,
            Self::InvalidRateLimitCapacity
            | Self::InvalidRateLimitInterval
            | Self::RequestExceedsRateLimit { .. } => Retryability::Permanent,
        }
    }
}
