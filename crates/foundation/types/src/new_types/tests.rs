use super::{
    ArithmeticError, DurationMilliseconds, DurationNanoseconds, NonZero, TimestampMilliseconds,
    UsageCount,
};

#[test]
fn checked_arithmetic_reports_overflow() {
    assert_eq!(
        UsageCount(u64::MAX).checked_add(UsageCount::ONE),
        Err(ArithmeticError::Overflow)
    );
    assert_eq!(
        UsageCount::ZERO.checked_sub(UsageCount::ONE),
        Err(ArithmeticError::Overflow)
    );
    assert_eq!(
        UsageCount::TWO.checked_mul(u64::MAX),
        Err(ArithmeticError::Overflow)
    );
}

#[test]
fn saturating_arithmetic_clamps_at_bounds() {
    assert_eq!(
        UsageCount(u64::MAX).saturating_add(UsageCount::ONE),
        UsageCount(u64::MAX)
    );
    assert_eq!(
        UsageCount::ZERO.saturating_sub(UsageCount::ONE),
        UsageCount::ZERO
    );
    assert_eq!(
        UsageCount::TWO.saturating_mul(u64::MAX),
        UsageCount(u64::MAX)
    );
}

#[test]
fn checked_division_and_remainder_reject_zero() {
    assert_eq!(
        UsageCount::TEN.checked_div(UsageCount::ZERO),
        Err(ArithmeticError::DivisionByZero)
    );
    assert_eq!(
        UsageCount::TEN.checked_rem(UsageCount::ZERO),
        Err(ArithmeticError::DivisionByZero)
    );
}

#[test]
fn checked_division_accepts_non_zero_divisors() {
    let divisor = NonZero::new(UsageCount(3)).expect("non-zero divisor");
    assert_eq!(UsageCount::TEN.checked_div(divisor), Ok(3));
    assert_eq!(UsageCount::TEN.checked_rem(divisor), Ok(UsageCount::ONE));
}

#[test]
fn timestamp_arithmetic_is_checked() {
    let start = TimestampMilliseconds(1_000);
    let duration = DurationMilliseconds(500);

    assert_eq!(
        start.checked_add(duration),
        Ok(TimestampMilliseconds(1_500))
    );
    assert_eq!(start.checked_sub(duration), Ok(TimestampMilliseconds(500)));
    assert_eq!(
        TimestampMilliseconds(i64::MAX).checked_add(DurationMilliseconds::ONE),
        Err(ArithmeticError::Overflow)
    );
}

#[test]
fn timestamp_arithmetic_saturates() {
    assert_eq!(
        TimestampMilliseconds(i64::MAX).saturating_add(DurationMilliseconds::ONE),
        TimestampMilliseconds(i64::MAX)
    );
    assert_eq!(
        TimestampMilliseconds(i64::MIN).saturating_sub(DurationMilliseconds::ONE),
        TimestampMilliseconds(i64::MIN)
    );
    assert_eq!(
        TimestampMilliseconds(1_000).saturating_duration_since(TimestampMilliseconds(400)),
        DurationMilliseconds(600)
    );
}

#[test]
fn duration_remainder_handles_negative_extremes() {
    assert_eq!(
        DurationNanoseconds(i64::MIN).checked_rem(DurationNanoseconds(-1)),
        Ok(DurationNanoseconds::ZERO)
    );
}
