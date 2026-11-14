use serde::{Deserialize, Serialize};

use super::{
    arithmetic::{ArithmeticError, CheckedSub, SaturatingSub},
    duration::DurationMilliseconds,
};

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct TimestampMilliseconds(pub i64);

impl TimestampMilliseconds {
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(1);
    pub const TWO: Self = Self(2);
    pub const TEN: Self = Self(10);

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

    pub fn checked_sub<Rhs>(
        self,
        rhs: Rhs,
    ) -> Result<<Self as CheckedSub<Rhs>>::Output, ArithmeticError>
    where
        Self: CheckedSub<Rhs>,
    {
        <Self as CheckedSub<Rhs>>::checked_sub(self, rhs)
    }

    pub fn saturating_sub<Rhs>(self, rhs: Rhs) -> <Self as SaturatingSub<Rhs>>::Output
    where
        Self: SaturatingSub<Rhs>,
    {
        <Self as SaturatingSub<Rhs>>::saturating_sub(self, rhs)
    }

    pub fn checked_duration_since(
        self,
        earlier: TimestampMilliseconds,
    ) -> Result<DurationMilliseconds, ArithmeticError> {
        <Self as CheckedSub<TimestampMilliseconds>>::checked_sub(self, earlier)
    }

    pub fn saturating_duration_since(self, earlier: TimestampMilliseconds) -> DurationMilliseconds {
        <Self as SaturatingSub<TimestampMilliseconds>>::saturating_sub(self, earlier)
    }
}

impl CheckedSub<DurationMilliseconds> for TimestampMilliseconds {
    type Output = TimestampMilliseconds;

    fn checked_sub(self, rhs: DurationMilliseconds) -> Result<Self::Output, ArithmeticError> {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or(ArithmeticError::Overflow)
    }
}

impl CheckedSub<TimestampMilliseconds> for TimestampMilliseconds {
    type Output = DurationMilliseconds;

    fn checked_sub(self, rhs: TimestampMilliseconds) -> Result<Self::Output, ArithmeticError> {
        self.0
            .checked_sub(rhs.0)
            .map(DurationMilliseconds)
            .ok_or(ArithmeticError::Overflow)
    }
}

impl SaturatingSub<DurationMilliseconds> for TimestampMilliseconds {
    type Output = TimestampMilliseconds;

    fn saturating_sub(self, rhs: DurationMilliseconds) -> Self::Output {
        Self(self.0.saturating_sub(rhs.0))
    }
}

impl SaturatingSub<TimestampMilliseconds> for TimestampMilliseconds {
    type Output = DurationMilliseconds;

    fn saturating_sub(self, rhs: TimestampMilliseconds) -> Self::Output {
        DurationMilliseconds(self.0.saturating_sub(rhs.0))
    }
}

impl std::fmt::Display for TimestampMilliseconds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Add<DurationMilliseconds> for TimestampMilliseconds {
    type Output = TimestampMilliseconds;

    fn add(self, rhs: DurationMilliseconds) -> Self::Output {
        self.saturating_add(rhs)
    }
}

impl std::ops::AddAssign<DurationMilliseconds> for TimestampMilliseconds {
    fn add_assign(&mut self, rhs: DurationMilliseconds) {
        *self = self.saturating_add(rhs);
    }
}

impl std::ops::Sub<DurationMilliseconds> for TimestampMilliseconds {
    type Output = TimestampMilliseconds;

    fn sub(self, rhs: DurationMilliseconds) -> Self::Output {
        self.saturating_sub(rhs)
    }
}

impl std::ops::SubAssign<DurationMilliseconds> for TimestampMilliseconds {
    fn sub_assign(&mut self, rhs: DurationMilliseconds) {
        *self = self.saturating_sub(rhs);
    }
}

impl std::ops::Sub<TimestampMilliseconds> for TimestampMilliseconds {
    type Output = DurationMilliseconds;

    fn sub(self, rhs: TimestampMilliseconds) -> Self::Output {
        self.saturating_duration_since(rhs)
    }
}

impl std::ops::Add<TimestampMilliseconds> for DurationMilliseconds {
    type Output = TimestampMilliseconds;

    fn add(self, rhs: TimestampMilliseconds) -> Self::Output {
        rhs.saturating_add(self)
    }
}
