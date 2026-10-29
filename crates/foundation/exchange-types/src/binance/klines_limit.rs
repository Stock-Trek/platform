use platform_errors::{BpError, BpResult};
use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct BinanceKlinesLimit(u32);

impl BinanceKlinesLimit {
    pub const MINIMUM: u32 = 1;
    pub const MAXIMUM: u32 = 1000;

    pub fn try_new(klines_limit: u32) -> BpResult<Self> {
        if Self::is_valid(klines_limit) {
            Ok(Self(klines_limit))
        } else {
            Err(BpError::bad_value(
                "klines_limit",
                klines_limit.to_string(),
                format!("Out of bounds >={} <={}", Self::MINIMUM, Self::MAXIMUM),
            ))
        }
    }

    pub fn is_valid(klines_limit: u32) -> bool {
        (Self::MINIMUM..=Self::MAXIMUM).contains(&klines_limit)
    }
}

impl fmt::Display for BinanceKlinesLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
