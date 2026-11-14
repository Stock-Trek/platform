use super::ArithmeticError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonZero<T>(T);

impl<T: Copy> NonZero<T> {
    pub const fn get(self) -> T {
        self.0
    }
}

impl<T: Copy + Default + PartialEq> NonZero<T> {
    pub fn new(value: T) -> Result<Self, ArithmeticError> {
        if value == T::default() {
            Err(ArithmeticError::DivisionByZero)
        } else {
            Ok(Self(value))
        }
    }
}

pub type NonZeroDuration = NonZero<std::time::Duration>;

impl From<NonZeroDuration> for std::time::Duration {
    fn from(value: NonZeroDuration) -> Self {
        value.get()
    }
}
