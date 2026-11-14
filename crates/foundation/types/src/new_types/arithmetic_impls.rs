macro_rules! arithmetic_impls {
    ($name:ident, $raw_type:ty) => {
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
                    .map(Self)
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
                    .map(Self)
                    .ok_or(ArithmeticError::Overflow)
            }
        }

        impl CheckedRem<$name> for $name {
            type Output = $name;

            fn checked_rem(self, rhs: $name) -> Result<Self::Output, ArithmeticError> {
                if rhs.0 == 0 {
                    return Err(ArithmeticError::DivisionByZero);
                }
                self.0.raw_checked_rem(rhs.0).map(Self)
            }
        }

        impl CheckedRem<$raw_type> for $name {
            type Output = $name;

            fn checked_rem(self, rhs: $raw_type) -> Result<Self::Output, ArithmeticError> {
                if rhs == 0 {
                    return Err(ArithmeticError::DivisionByZero);
                }
                self.0.raw_checked_rem(rhs).map(Self)
            }
        }

        impl CheckedRem<NonZero<$name>> for $name {
            type Output = $name;

            fn checked_rem(self, rhs: NonZero<$name>) -> Result<Self::Output, ArithmeticError> {
                self.0.raw_checked_rem(rhs.get().0).map(Self)
            }
        }

        impl CheckedRem<NonZero<$raw_type>> for $name {
            type Output = $name;

            fn checked_rem(self, rhs: NonZero<$raw_type>) -> Result<Self::Output, ArithmeticError> {
                self.0.raw_checked_rem(rhs.get()).map(Self)
            }
        }

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

pub(crate) use arithmetic_impls;
