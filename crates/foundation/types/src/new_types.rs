use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors produced by checked arithmetic on the domain newtypes.
///
/// The operators on the newtypes are total: addition, subtraction, and scalar
/// multiplication saturate. Operations that cannot have a meaningful total
/// result (division and remainder) are only available through checked methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ArithmeticError {
    #[error("attempt to divide by zero")]
    DivisionByZero,
    #[error("arithmetic overflow")]
    Overflow,
}

/// A value that is guaranteed not to be the zero value of its type.
///
/// Construct one with [`NonZero::new`] (or `try_from`) before using it as a
/// divisor. Keeping the non-zero invariant in the type prevents division and
/// remainder by zero at compile time once a value has been validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonZero<T>(T);

impl<T: Copy> NonZero<T> {
    /// Creates a `NonZero` without checking the value.
    ///
    /// Prefer [`NonZero::new`] or `try_from`; this exists for values whose
    /// non-zero invariant has already been established by construction.
    pub const fn new_unchecked(value: T) -> Self {
        Self(value)
    }

    /// Returns the wrapped value.
    pub const fn get(self) -> T {
        self.0
    }
}

impl<T: Copy + Default + PartialEq> NonZero<T> {
    /// Creates a `NonZero` after checking that the value is not zero.
    pub fn new(value: T) -> Result<Self, ArithmeticError> {
        if value == T::default() {
            Err(ArithmeticError::DivisionByZero)
        } else {
            Ok(Self(value))
        }
    }
}

/// Checked division for a newtype.
///
/// Implementations exist for both same-unit divisors (which produce a raw,
/// dimensionless ratio) and raw scalar divisors (which produce the same
/// newtype). The result is fallible because the quotient may not fit in the
/// underlying integer type.
pub trait CheckedDiv<Rhs> {
    type Output;

    fn checked_div(self, rhs: Rhs) -> Result<Self::Output, ArithmeticError>;
}

/// Checked remainder for a newtype.
///
/// A remainder always retains the newtype unit, regardless of whether the
/// divisor is another newtype or a raw scalar.
pub trait CheckedRem<Rhs> {
    type Output;

    fn checked_rem(self, rhs: Rhs) -> Result<Self::Output, ArithmeticError>;
}

macro_rules! new_type {
    ($name:ident, $raw_type:ty) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub $raw_type);

        impl $name {
            pub const ZERO: $name = $name(0);
            pub const ONE: $name = $name(1);
            pub const TWO: $name = $name(2);
            pub const TEN: $name = $name(10);

            /// Checked addition. Returns an error instead of overflowing.
            pub fn checked_add(self, rhs: Self) -> Result<Self, ArithmeticError> {
                self.0
                    .checked_add(rhs.0)
                    .map($name)
                    .ok_or(ArithmeticError::Overflow)
            }

            /// Saturating addition. This is the behaviour of the `+` operator.
            pub fn saturating_add(self, rhs: Self) -> Self {
                $name(self.0.saturating_add(rhs.0))
            }

            /// Checked subtraction. Returns an error instead of overflowing.
            pub fn checked_sub(self, rhs: Self) -> Result<Self, ArithmeticError> {
                self.0
                    .checked_sub(rhs.0)
                    .map($name)
                    .ok_or(ArithmeticError::Overflow)
            }

            /// Saturating subtraction. This is the behaviour of the `-` operator.
            pub fn saturating_sub(self, rhs: Self) -> Self {
                $name(self.0.saturating_sub(rhs.0))
            }

            /// Checked scalar multiplication. Returns an error instead of overflowing.
            pub fn checked_mul(self, rhs: $raw_type) -> Result<Self, ArithmeticError> {
                self.0
                    .checked_mul(rhs)
                    .map($name)
                    .ok_or(ArithmeticError::Overflow)
            }

            /// Saturating scalar multiplication. This is the behaviour of the `*` operator.
            pub fn saturating_mul(self, rhs: $raw_type) -> Self {
                $name(self.0.saturating_mul(rhs))
            }

            /// Checked division.
            ///
            /// Accepts either a `NonZero` newtype (returning the raw ratio) or a
            /// raw scalar divisor (returning the same newtype). A plain zero
            /// divisor is rejected with [`ArithmeticError::DivisionByZero`].
            pub fn checked_div<Rhs>(
                self,
                rhs: Rhs,
            ) -> Result<<Self as CheckedDiv<Rhs>>::Output, ArithmeticError>
            where
                Self: CheckedDiv<Rhs>,
            {
                <Self as CheckedDiv<Rhs>>::checked_div(self, rhs)
            }

            /// Checked remainder. The result retains this newtype's unit.
            ///
            /// Accepts either a `NonZero` newtype or a raw scalar divisor. A
            /// plain zero divisor is rejected with
            /// [`ArithmeticError::DivisionByZero`].
            pub fn checked_rem<Rhs>(
                self,
                rhs: Rhs,
            ) -> Result<<Self as CheckedRem<Rhs>>::Output, ArithmeticError>
            where
                Self: CheckedRem<Rhs>,
            {
                <Self as CheckedRem<Rhs>>::checked_rem(self, rhs)
            }
        }

        impl CheckedDiv<$name> for $name {
            type Output = $raw_type;

            fn checked_div(self, rhs: $name) -> Result<Self::Output, ArithmeticError> {
                if rhs.0 == 0 {
                    return Err(ArithmeticError::DivisionByZero);
                }
                self.0.checked_div(rhs.0).ok_or(ArithmeticError::Overflow)
            }
        }

        impl CheckedDiv<$raw_type> for $name {
            type Output = $name;

            fn checked_div(self, rhs: $raw_type) -> Result<Self::Output, ArithmeticError> {
                if rhs == 0 {
                    return Err(ArithmeticError::DivisionByZero);
                }
                self.0
                    .checked_div(rhs)
                    .map($name)
                    .ok_or(ArithmeticError::Overflow)
            }
        }

        impl CheckedDiv<NonZero<$name>> for $name {
            type Output = $raw_type;

            fn checked_div(self, rhs: NonZero<$name>) -> Result<Self::Output, ArithmeticError> {
                self.0
                    .checked_div(rhs.get().0)
                    .ok_or(ArithmeticError::Overflow)
            }
        }

        impl CheckedDiv<NonZero<$raw_type>> for $name {
            type Output = $name;

            fn checked_div(self, rhs: NonZero<$raw_type>) -> Result<Self::Output, ArithmeticError> {
                self.0
                    .checked_div(rhs.get())
                    .map($name)
                    .ok_or(ArithmeticError::Overflow)
            }
        }

        impl CheckedRem<$name> for $name {
            type Output = $name;

            fn checked_rem(self, rhs: $name) -> Result<Self::Output, ArithmeticError> {
                if rhs.0 == 0 {
                    return Err(ArithmeticError::DivisionByZero);
                }
                self.0.raw_checked_rem(rhs.0).map($name)
            }
        }

        impl CheckedRem<$raw_type> for $name {
            type Output = $name;

            fn checked_rem(self, rhs: $raw_type) -> Result<Self::Output, ArithmeticError> {
                if rhs == 0 {
                    return Err(ArithmeticError::DivisionByZero);
                }
                self.0.raw_checked_rem(rhs).map($name)
            }
        }

        impl CheckedRem<NonZero<$name>> for $name {
            type Output = $name;

            fn checked_rem(self, rhs: NonZero<$name>) -> Result<Self::Output, ArithmeticError> {
                self.0.raw_checked_rem(rhs.get().0).map($name)
            }
        }

        impl CheckedRem<NonZero<$raw_type>> for $name {
            type Output = $name;

            fn checked_rem(self, rhs: NonZero<$raw_type>) -> Result<Self::Output, ArithmeticError> {
                self.0.raw_checked_rem(rhs.get()).map($name)
            }
        }

        impl From<NonZero<$name>> for $name {
            fn from(value: NonZero<$name>) -> Self {
                value.get()
            }
        }

        impl TryFrom<$name> for NonZero<$name> {
            type Error = ArithmeticError;

            fn try_from(value: $name) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

macro_rules! numeric_ops {
    ($name:ident, $raw_type:ty) => {
        impl std::ops::Add for $name {
            type Output = $name;

            fn add(self, rhs: Self) -> Self::Output {
                self.saturating_add(rhs)
            }
        }

        impl std::ops::AddAssign for $name {
            fn add_assign(&mut self, rhs: Self) {
                *self = self.saturating_add(rhs);
            }
        }

        impl std::ops::Sub for $name {
            type Output = $name;

            fn sub(self, rhs: Self) -> Self::Output {
                self.saturating_sub(rhs)
            }
        }

        impl std::ops::SubAssign for $name {
            fn sub_assign(&mut self, rhs: Self) {
                *self = self.saturating_sub(rhs);
            }
        }

        impl std::ops::Mul<$raw_type> for $name {
            type Output = $name;

            fn mul(self, rhs: $raw_type) -> Self::Output {
                self.saturating_mul(rhs)
            }
        }

        impl std::ops::MulAssign<$raw_type> for $name {
            fn mul_assign(&mut self, rhs: $raw_type) {
                *self = self.saturating_mul(rhs);
            }
        }
    };
}

/// Checked remainder on the raw integer component of a newtype.
trait RawCheckedRem: Copy {
    fn raw_checked_rem(self, divisor: Self) -> Result<Self, ArithmeticError>;
}

impl RawCheckedRem for i64 {
    fn raw_checked_rem(self, divisor: Self) -> Result<Self, ArithmeticError> {
        // `i64::MIN % -1` is mathematically zero, even though the corresponding
        // division cannot be represented in `i64`.
        if self == i64::MIN && divisor == -1 {
            Ok(0)
        } else {
            self.checked_rem(divisor).ok_or(ArithmeticError::Overflow)
        }
    }
}

impl RawCheckedRem for u64 {
    fn raw_checked_rem(self, divisor: Self) -> Result<Self, ArithmeticError> {
        self.checked_rem(divisor).ok_or(ArithmeticError::Overflow)
    }
}

new_type!(DurationSeconds, i64);
numeric_ops!(DurationSeconds, i64);

new_type!(DurationMilliseconds, i64);
numeric_ops!(DurationMilliseconds, i64);

new_type!(DurationMicroseconds, i64);
numeric_ops!(DurationMicroseconds, i64);

new_type!(DurationNanoseconds, i64);
numeric_ops!(DurationNanoseconds, i64);

new_type!(UsageCount, u64);
numeric_ops!(UsageCount, u64);

new_type!(TimestampMilliseconds, i64);

impl std::ops::Add<DurationMilliseconds> for TimestampMilliseconds {
    type Output = TimestampMilliseconds;

    fn add(self, rhs: DurationMilliseconds) -> Self::Output {
        TimestampMilliseconds(self.0.saturating_add(rhs.0))
    }
}

impl std::ops::AddAssign<DurationMilliseconds> for TimestampMilliseconds {
    fn add_assign(&mut self, rhs: DurationMilliseconds) {
        self.0 = self.0.saturating_add(rhs.0);
    }
}

impl std::ops::Sub<DurationMilliseconds> for TimestampMilliseconds {
    type Output = TimestampMilliseconds;

    fn sub(self, rhs: DurationMilliseconds) -> Self::Output {
        TimestampMilliseconds(self.0.saturating_sub(rhs.0))
    }
}

impl std::ops::SubAssign<DurationMilliseconds> for TimestampMilliseconds {
    fn sub_assign(&mut self, rhs: DurationMilliseconds) {
        self.0 = self.0.saturating_sub(rhs.0);
    }
}

impl std::ops::Sub<TimestampMilliseconds> for TimestampMilliseconds {
    type Output = DurationMilliseconds;

    fn sub(self, rhs: TimestampMilliseconds) -> Self::Output {
        DurationMilliseconds(self.0.saturating_sub(rhs.0))
    }
}

impl std::ops::Add<TimestampMilliseconds> for DurationMilliseconds {
    type Output = TimestampMilliseconds;

    fn add(self, rhs: TimestampMilliseconds) -> Self::Output {
        TimestampMilliseconds(self.0.saturating_add(rhs.0))
    }
}

pub use DurationMicroseconds as Microseconds;
pub use DurationMilliseconds as Milliseconds;
pub use DurationNanoseconds as Nanoseconds;
pub use DurationSeconds as Seconds;
pub use TimestampMilliseconds as Timestamp;

/// Non-zero duration newtypes, useful as divisor types.
pub type NonZeroDurationSeconds = NonZero<DurationSeconds>;
pub type NonZeroDurationMilliseconds = NonZero<DurationMilliseconds>;
pub type NonZeroDurationMicroseconds = NonZero<DurationMicroseconds>;
pub type NonZeroDurationNanoseconds = NonZero<DurationNanoseconds>;
pub type NonZeroUsageCount = NonZero<UsageCount>;
pub type NonZeroTimestampMilliseconds = NonZero<TimestampMilliseconds>;

pub type NonZeroSeconds = NonZeroDurationSeconds;
pub type NonZeroMilliseconds = NonZeroDurationMilliseconds;
pub type NonZeroMicroseconds = NonZeroDurationMicroseconds;
pub type NonZeroNanoseconds = NonZeroDurationNanoseconds;
pub type NonZeroTimestamp = NonZeroTimestampMilliseconds;
