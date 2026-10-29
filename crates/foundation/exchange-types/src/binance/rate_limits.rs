use crate::rate_limited::RateLimitRestriction;
use platform_errors::{BpError, ExchangeError};
use platform_types::new_types::DurationNanoseconds;
use serde::Deserialize;
use strum::Display;

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceRateLimit {
    pub count: Option<i64>,
    pub interval: BinanceRateLimitInterval,
    pub intervalNum: u32,
    pub limit: i64,
    pub rateLimitType: BinanceRateLimitType,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug, Clone, Copy, Display, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinanceRateLimitInterval {
    DAY,
    HOUR,
    MINUTE,
    SECOND,
    #[serde(other)]
    Unknown,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug, Clone, Copy, Display, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BinanceRateLimitType {
    CONNECTIONS,
    ORDERS,
    RAW_REQUESTS,
    REQUEST_WEIGHT,
    #[serde(other)]
    Unknown,
}

pub struct BinanceRateLimits;

impl TryFrom<char> for BinanceRateLimitInterval {
    type Error = BpError;

    fn try_from(value: char) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase() {
            's' => Ok(Self::SECOND),
            'm' => Ok(Self::MINUTE),
            'h' => Ok(Self::HOUR),
            'd' => Ok(Self::DAY),
            other => Err(BpError::Exchange(ExchangeError::ParseError(other.into()))),
        }
    }
}

impl TryFrom<BinanceRateLimitInterval> for DurationNanoseconds {
    type Error = BpError;

    fn try_from(value: BinanceRateLimitInterval) -> Result<Self, Self::Error> {
        match value {
            BinanceRateLimitInterval::DAY => Ok(DurationNanoseconds(24 * 60 * 60 * 1_000_000_000)),
            BinanceRateLimitInterval::HOUR => Ok(DurationNanoseconds(60 * 60 * 1_000_000_000)),
            BinanceRateLimitInterval::MINUTE => Ok(DurationNanoseconds(60 * 1_000_000_000)),
            BinanceRateLimitInterval::SECOND => Ok(DurationNanoseconds(1_000_000_000)),
            BinanceRateLimitInterval::Unknown => Err(BpError::Exchange(
                ExchangeError::UnknownValue("BinanceRateLimitInterval".into()),
            )),
        }
    }
}

impl TryFrom<BinanceRateLimitType> for RateLimitRestriction {
    type Error = BpError;

    fn try_from(value: BinanceRateLimitType) -> Result<Self, Self::Error> {
        match value {
            BinanceRateLimitType::CONNECTIONS => Ok(RateLimitRestriction::Connection),
            BinanceRateLimitType::ORDERS => Ok(RateLimitRestriction::OrderCount),
            BinanceRateLimitType::RAW_REQUESTS => Ok(RateLimitRestriction::RawRequests),
            BinanceRateLimitType::REQUEST_WEIGHT => Ok(RateLimitRestriction::Weight),
            BinanceRateLimitType::Unknown => Err(BpError::Exchange(ExchangeError::UnknownValue(
                "BinanceRateLimitType".into(),
            ))),
        }
    }
}
