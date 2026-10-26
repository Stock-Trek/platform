use crate::{BpError, BpResult};
use platform_errors::ValidationError;
use rust_decimal::{Decimal, RoundingStrategy, prelude::FromPrimitive};

#[derive(Debug, Clone, Copy)]
pub struct TradingPrecision {
    pub base_limit: MinMaxStep,
    pub base_market: MinMaxStep,
    pub quote: MinMaxStep,
}

#[derive(Debug, Clone, Copy)]
pub struct MinMaxStep {
    pub min: Decimal,
    pub max: Decimal,
    pub step: Decimal,
}

impl TradingPrecision {
    pub fn new(base_limit: MinMaxStep, base_market: MinMaxStep, quote: MinMaxStep) -> Self {
        Self {
            base_limit,
            base_market,
            quote,
        }
    }
    pub(crate) fn to_valid_base_limit(
        self,
        base: f64,
        strategy: RoundingStrategy,
    ) -> BpResult<Decimal> {
        Self::to_valid_decimal(base, self.base_limit, strategy)
    }
    pub(crate) fn to_valid_base_market(
        self,
        base: f64,
        strategy: RoundingStrategy,
    ) -> BpResult<Decimal> {
        Self::to_valid_decimal(base, self.base_market, strategy)
    }
    pub(crate) fn to_valid_quote(
        self,
        quote: f64,
        strategy: RoundingStrategy,
    ) -> BpResult<Decimal> {
        Self::to_valid_decimal(quote, self.quote, strategy)
    }
    fn to_valid_decimal(
        value: f64,
        min_max_step: MinMaxStep,
        strategy: RoundingStrategy,
    ) -> BpResult<Decimal> {
        let MinMaxStep { min, max, step } = min_max_step;
        if min > max {
            return Err(BpError::Validation(ValidationError::InvalidValue {
                name: "Trading precision bounds".into(),
                value: format!("min {min} is greater than max {max}"),
            }));
        }
        let value_as_decimal = Decimal::from_f64(value).ok_or_else(|| {
            BpError::Validation(ValidationError::InvalidValue {
                name: "Trading value".into(),
                value: value.to_string(),
            })
        })?;
        if value_as_decimal < min || value_as_decimal > max {
            return Err(BpError::Validation(ValidationError::InvalidValue {
                name: "Trading value".into(),
                value: value.to_string(),
            }));
        }
        let rounded_value = if step == Decimal::ZERO {
            value_as_decimal
        } else {
            let step_count = value_as_decimal / step;
            step_count.round_dp_with_strategy(0, strategy) * step
        };
        if rounded_value < min || rounded_value > max {
            return Err(BpError::Validation(ValidationError::InvalidValue {
                name: "Trading value".into(),
                value: value.to_string(),
            }));
        }
        Ok(rounded_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn precision_with(min: Decimal, max: Decimal, step: Decimal) -> TradingPrecision {
        let limits = MinMaxStep { min, max, step };
        TradingPrecision::new(limits, limits, limits)
    }

    fn precision() -> TradingPrecision {
        precision_with(Decimal::ZERO, Decimal::from(100), Decimal::new(1, 2))
    }

    #[test]
    fn non_finite_values_return_an_error() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let result = precision().to_valid_quote(value, RoundingStrategy::MidpointNearestEven);
            assert!(matches!(
                result,
                Err(BpError::Validation(ValidationError::InvalidValue { .. }))
            ));
        }
    }

    #[test]
    fn finite_values_are_rounded() {
        let precision = precision();
        let result = precision
            .to_valid_quote(1.234, RoundingStrategy::MidpointNearestEven)
            .unwrap();
        assert_eq!(result, Decimal::new(123, 2));
    }

    #[test]
    fn values_outside_the_bounds_return_an_error() {
        let precision = precision();
        for value in [-1.0, 100.01, 1_000.0] {
            let result = precision.to_valid_quote(value, RoundingStrategy::MidpointNearestEven);
            assert!(matches!(
                result,
                Err(BpError::Validation(ValidationError::InvalidValue { .. }))
            ));
        }
    }

    #[test]
    fn values_that_only_round_outside_the_bounds_return_an_error() {
        let precision = precision_with(Decimal::ZERO, Decimal::new(995, 1), Decimal::new(1, 1));
        let result = precision.to_valid_quote(99.6, RoundingStrategy::MidpointNearestEven);
        assert!(matches!(
            result,
            Err(BpError::Validation(ValidationError::InvalidValue { .. }))
        ));
    }

    #[test]
    fn values_below_the_minimum_that_would_round_up_return_an_error() {
        let precision = precision_with(Decimal::ONE, Decimal::from(100), Decimal::ONE);
        let result = precision.to_valid_quote(0.6, RoundingStrategy::MidpointNearestEven);
        assert!(matches!(
            result,
            Err(BpError::Validation(ValidationError::InvalidValue { .. }))
        ));
    }

    #[test]
    fn invalid_bounds_return_an_error_instead_of_panicking() {
        let precision = precision_with(Decimal::from(10), Decimal::from(5), Decimal::ONE);
        let result = precision.to_valid_quote(7.0, RoundingStrategy::MidpointNearestEven);
        assert!(matches!(
            result,
            Err(BpError::Validation(ValidationError::InvalidValue { .. }))
        ));
    }
}
