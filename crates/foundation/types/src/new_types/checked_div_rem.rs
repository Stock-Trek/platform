macro_rules! checked_div_rem_impls {
    ($name:ident, $raw_type:ty) => {
        impl $crate::new_types::CheckedDiv<$name> for $name {
            type Output = $raw_type;

            fn checked_div(
                self,
                rhs: $name,
            ) -> Result<Self::Output, $crate::new_types::ArithmeticError> {
                if rhs.0 == 0 {
                    return Err($crate::new_types::ArithmeticError::DivisionByZero);
                }
                self.0
                    .checked_div(rhs.0)
                    .ok_or($crate::new_types::ArithmeticError::Overflow)
            }
        }

        impl $crate::new_types::CheckedDiv<$raw_type> for $name {
            type Output = $name;

            fn checked_div(
                self,
                rhs: $raw_type,
            ) -> Result<Self::Output, $crate::new_types::ArithmeticError> {
                if rhs == 0 {
                    return Err($crate::new_types::ArithmeticError::DivisionByZero);
                }
                self.0
                    .checked_div(rhs)
                    .map($name)
                    .ok_or($crate::new_types::ArithmeticError::Overflow)
            }
        }

        impl $crate::new_types::CheckedDiv<$crate::new_types::NonZero<$name>> for $name {
            type Output = $raw_type;

            fn checked_div(
                self,
                rhs: $crate::new_types::NonZero<$name>,
            ) -> Result<Self::Output, $crate::new_types::ArithmeticError> {
                self.0
                    .checked_div(rhs.get().0)
                    .ok_or($crate::new_types::ArithmeticError::Overflow)
            }
        }

        impl $crate::new_types::CheckedDiv<$crate::new_types::NonZero<$raw_type>> for $name {
            type Output = $name;

            fn checked_div(
                self,
                rhs: $crate::new_types::NonZero<$raw_type>,
            ) -> Result<Self::Output, $crate::new_types::ArithmeticError> {
                self.0
                    .checked_div(rhs.get())
                    .map($name)
                    .ok_or($crate::new_types::ArithmeticError::Overflow)
            }
        }

        impl $crate::new_types::CheckedRem<$name> for $name {
            type Output = $name;

            fn checked_rem(
                self,
                rhs: $name,
            ) -> Result<Self::Output, $crate::new_types::ArithmeticError> {
                if rhs.0 == 0 {
                    return Err($crate::new_types::ArithmeticError::DivisionByZero);
                }
                self.0.raw_checked_rem(rhs.0).map($name)
            }
        }

        impl $crate::new_types::CheckedRem<$raw_type> for $name {
            type Output = $name;

            fn checked_rem(
                self,
                rhs: $raw_type,
            ) -> Result<Self::Output, $crate::new_types::ArithmeticError> {
                if rhs == 0 {
                    return Err($crate::new_types::ArithmeticError::DivisionByZero);
                }
                self.0.raw_checked_rem(rhs).map($name)
            }
        }

        impl $crate::new_types::CheckedRem<$crate::new_types::NonZero<$name>> for $name {
            type Output = $name;

            fn checked_rem(
                self,
                rhs: $crate::new_types::NonZero<$name>,
            ) -> Result<Self::Output, $crate::new_types::ArithmeticError> {
                self.0.raw_checked_rem(rhs.get().0).map($name)
            }
        }

        impl $crate::new_types::CheckedRem<$crate::new_types::NonZero<$raw_type>> for $name {
            type Output = $name;

            fn checked_rem(
                self,
                rhs: $crate::new_types::NonZero<$raw_type>,
            ) -> Result<Self::Output, $crate::new_types::ArithmeticError> {
                self.0.raw_checked_rem(rhs.get()).map($name)
            }
        }
    };
}

pub(crate) use checked_div_rem_impls;
