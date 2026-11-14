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
            pub const ZERO: Self = Self(0);
            pub const ONE: Self = Self(1);
            pub const TWO: Self = Self(2);
            pub const TEN: Self = Self(10);

            pub fn checked_add(self, rhs: Self) -> Result<Self, ArithmeticError> {
                self.0
                    .checked_add(rhs.0)
                    .map(Self)
                    .ok_or(ArithmeticError::Overflow)
            }

            pub fn saturating_add(self, rhs: Self) -> Self {
                Self(self.0.saturating_add(rhs.0))
            }

            pub fn checked_sub(self, rhs: Self) -> Result<Self, ArithmeticError> {
                self.0
                    .checked_sub(rhs.0)
                    .map(Self)
                    .ok_or(ArithmeticError::Overflow)
            }

            pub fn saturating_sub(self, rhs: Self) -> Self {
                Self(self.0.saturating_sub(rhs.0))
            }

            pub fn checked_mul(self, rhs: $raw_type) -> Result<Self, ArithmeticError> {
                self.0
                    .checked_mul(rhs)
                    .map(Self)
                    .ok_or(ArithmeticError::Overflow)
            }

            pub fn saturating_mul(self, rhs: $raw_type) -> Self {
                Self(self.0.saturating_mul(rhs))
            }

            pub fn checked_div<Rhs>(
                self,
                rhs: Rhs,
            ) -> Result<<Self as CheckedDiv<Rhs>>::Output, ArithmeticError>
            where
                Self: CheckedDiv<Rhs>,
            {
                <Self as CheckedDiv<Rhs>>::checked_div(self, rhs)
            }

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

pub(crate) use new_type;
