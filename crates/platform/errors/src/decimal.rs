use rust_decimal::Decimal;
use std::{error::Error as StdError, fmt};

#[derive(Debug)]
pub struct DecimalScaleError {
    value: Decimal,
    scale: u32,
}

impl DecimalScaleError {
    pub fn new(value: Decimal, scale: u32) -> Self {
        Self { value, scale }
    }
}

impl fmt::Display for DecimalScaleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "value {} cannot be represented with scale {}",
            self.value, self.scale
        )
    }
}

impl StdError for DecimalScaleError {}
