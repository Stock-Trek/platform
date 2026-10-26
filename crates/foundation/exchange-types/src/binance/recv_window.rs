use crate::new_types::Milliseconds;
use platform_errors::{BpError, BpResult, ExchangeError};
use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct BinanceRecvWindow(Milliseconds);

impl BinanceRecvWindow {
    pub const DEFAULT: Self = Self(Milliseconds(5000));
    pub const MAXIMUM: Milliseconds = Milliseconds(60_000);

    pub fn try_new(recv_window: Milliseconds) -> BpResult<Self> {
        if Self::is_valid(recv_window) {
            Ok(Self(recv_window))
        } else {
            Err(BpError::Exchange(ExchangeError::BadValue {
                name: "recv_window".into(),
                value: recv_window.to_string(),
                reason: format!("Out of bounds >{} <={}", Milliseconds::ZERO, Self::MAXIMUM),
            }))
        }
    }

    pub fn is_valid(recv_window: Milliseconds) -> bool {
        recv_window > Milliseconds::ZERO && recv_window <= Self::MAXIMUM
    }
}

impl Default for BinanceRecvWindow {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl fmt::Display for BinanceRecvWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
