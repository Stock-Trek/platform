use platform_errors::{BuildError, BuildResult};
use platform_types::DurationMilliseconds;
use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct BinanceRecvWindow(DurationMilliseconds);

impl BinanceRecvWindow {
    pub const DEFAULT: Self = Self(DurationMilliseconds(5000));
    pub const MAXIMUM: DurationMilliseconds = DurationMilliseconds(60_000);

    pub fn try_new(recv_window: DurationMilliseconds) -> BuildResult<Self> {
        if Self::is_valid(recv_window) {
            Ok(Self(recv_window))
        } else {
            Err(BuildError::bad_value(
                "recv_window",
                recv_window.to_string(),
                format!(
                    "Out of bounds >{} <={}",
                    DurationMilliseconds::ZERO,
                    Self::MAXIMUM
                ),
            ))
        }
    }

    pub fn is_valid(recv_window: DurationMilliseconds) -> bool {
        recv_window > DurationMilliseconds::ZERO && recv_window <= Self::MAXIMUM
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
