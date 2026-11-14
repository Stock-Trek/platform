use super::{ArithmeticError, DurationMilliseconds};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct TimestampMilliseconds(pub i64);

impl TimestampMilliseconds {
    pub const ZERO: Self = Self(0);

    pub fn now() -> Self {
        Self(chrono::Utc::now().timestamp_millis())
    }

    pub fn checked_add(self, rhs: DurationMilliseconds) -> Result<Self, ArithmeticError> {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or(ArithmeticError::Overflow)
    }

    pub fn saturating_add(self, rhs: DurationMilliseconds) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    pub fn checked_sub(self, rhs: DurationMilliseconds) -> Result<Self, ArithmeticError> {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or(ArithmeticError::Overflow)
    }

    pub fn saturating_sub(self, rhs: DurationMilliseconds) -> Self {
        Self(self.0.saturating_sub(rhs.0))
    }

    pub fn checked_duration_since(
        self,
        earlier: Self,
    ) -> Result<DurationMilliseconds, ArithmeticError> {
        self.0
            .checked_sub(earlier.0)
            .map(DurationMilliseconds)
            .ok_or(ArithmeticError::Overflow)
    }

    pub fn saturating_duration_since(self, earlier: Self) -> DurationMilliseconds {
        DurationMilliseconds(self.0.saturating_sub(earlier.0))
    }
}

impl std::fmt::Display for TimestampMilliseconds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
