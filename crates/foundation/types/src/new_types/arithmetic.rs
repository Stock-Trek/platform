use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ArithmeticError {
    #[error("attempt to divide by zero")]
    DivisionByZero,
    #[error("arithmetic overflow")]
    Overflow,
}

pub trait CheckedDiv<Rhs> {
    type Output;

    fn checked_div(self, rhs: Rhs) -> Result<Self::Output, ArithmeticError>;
}

pub trait CheckedRem<Rhs> {
    type Output;

    fn checked_rem(self, rhs: Rhs) -> Result<Self::Output, ArithmeticError>;
}

pub(super) trait RawCheckedRem: Copy {
    fn raw_checked_rem(self, divisor: Self) -> Result<Self, ArithmeticError>;
}

impl RawCheckedRem for i64 {
    fn raw_checked_rem(self, divisor: Self) -> Result<Self, ArithmeticError> {
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
