use super::*;
use platform_errors::{BpError, ValidationError};
use rust_decimal::{Decimal, RoundingStrategy};

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
