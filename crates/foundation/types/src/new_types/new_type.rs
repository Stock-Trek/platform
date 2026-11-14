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

            pub fn checked_add(
                self,
                rhs: Self,
            ) -> Result<Self, $crate::new_types::ArithmeticError> {
                self.0
                    .checked_add(rhs.0)
                    .map($name)
                    .ok_or($crate::new_types::ArithmeticError::Overflow)
            }

            pub fn saturating_add(self, rhs: Self) -> Self {
                $name(self.0.saturating_add(rhs.0))
            }

            pub fn checked_sub(
                self,
                rhs: Self,
            ) -> Result<Self, $crate::new_types::ArithmeticError> {
                self.0
                    .checked_sub(rhs.0)
                    .map($name)
                    .ok_or($crate::new_types::ArithmeticError::Overflow)
            }

            pub fn saturating_sub(self, rhs: Self) -> Self {
                $name(self.0.saturating_sub(rhs.0))
            }

            pub fn checked_mul(
                self,
                rhs: $raw_type,
            ) -> Result<Self, $crate::new_types::ArithmeticError> {
                self.0
                    .checked_mul(rhs)
                    .map($name)
                    .ok_or($crate::new_types::ArithmeticError::Overflow)
            }

            pub fn saturating_mul(self, rhs: $raw_type) -> Self {
                $name(self.0.saturating_mul(rhs))
            }

            pub fn checked_div<Rhs>(
                self,
                rhs: Rhs,
            ) -> Result<
                <Self as $crate::new_types::CheckedDiv<Rhs>>::Output,
                $crate::new_types::ArithmeticError,
            >
            where
                Self: $crate::new_types::CheckedDiv<Rhs>,
            {
                <Self as $crate::new_types::CheckedDiv<Rhs>>::checked_div(self, rhs)
            }

            pub fn checked_rem<Rhs>(
                self,
                rhs: Rhs,
            ) -> Result<
                <Self as $crate::new_types::CheckedRem<Rhs>>::Output,
                $crate::new_types::ArithmeticError,
            >
            where
                Self: $crate::new_types::CheckedRem<Rhs>,
            {
                <Self as $crate::new_types::CheckedRem<Rhs>>::checked_rem(self, rhs)
            }
        }

        $crate::new_types::checked_div_rem::checked_div_rem_impls!($name, $raw_type);

        impl From<$crate::new_types::NonZero<$name>> for $name {
            fn from(value: $crate::new_types::NonZero<$name>) -> Self {
                value.get()
            }
        }

        impl TryFrom<$name> for $crate::new_types::NonZero<$name> {
            type Error = $crate::new_types::ArithmeticError;

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

pub(crate) use new_type;
