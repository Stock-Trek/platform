use crate::{convert::required::IsRequired, generated::dto::DecimalDto};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use stock_trek::errors::{StockTrekError, StockTrekResult, ValueError};

pub trait ToDecimal {
    fn to_decimal(self) -> StockTrekResult<Decimal>;
}

impl ToDecimal for DecimalDto {
    fn to_decimal(self) -> StockTrekResult<Decimal> {
        if self.scale > Decimal::MAX_SCALE {
            return Err(StockTrekError::Value(ValueError::IncorrectType {
                expected: format!("scale <= {}", Decimal::MAX_SCALE),
                found: self.scale.to_string(),
            }));
        }
        Ok(Decimal::from_parts(
            self.lo,
            self.mid,
            self.hi,
            self.negative,
            self.scale,
        ))
    }
}

impl TryFrom<DecimalDto> for Decimal {
    type Error = StockTrekError;
    fn try_from(value: DecimalDto) -> Result<Self, Self::Error> {
        value.to_decimal()
    }
}

impl TryFrom<DecimalDto> for f64 {
    type Error = StockTrekError;
    fn try_from(value: DecimalDto) -> Result<Self, Self::Error> {
        value.to_decimal()?.to_f64().required("f64")
    }
}
