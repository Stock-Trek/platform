use platform_types::{
    new_types::{DurationNanoseconds, UsageCount},
    protocol::Protocol,
};
use serde::Deserialize;
use strum::{Display, EnumIter};

pub trait RateLimited {
    fn limit(&self, protocol: Protocol, restriction: RateLimitRestriction) -> UsageCount;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
pub struct RateLimit {
    pub restriction: RateLimitRestriction,
    pub interval_nanos: DurationNanoseconds,
}

#[derive(Debug, Clone, Copy, Display, EnumIter, PartialEq, Eq, Hash, Deserialize)]
pub enum RateLimitRestriction {
    Connection,
    OrderCount,
    RawRequests,
    Weight,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct RateUsage {
    pub limit: Option<UsageCount>,
    pub used: Option<UsageCount>,
}
