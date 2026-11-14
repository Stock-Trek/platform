use super::{
    ArithmeticError, DurationMilliseconds, DurationNanoseconds, DurationSeconds, NonZero,
    TimestampMilliseconds, UsageCount,
};

#[test]
fn usage_count_checked_arithmetic_reports_overflow() {
    assert_eq!(
        UsageCount(u64::MAX).checked_add(UsageCount::ONE),
        Err(ArithmeticError::Overflow)
    );
    assert_eq!(
        UsageCount::ZERO.checked_sub(UsageCount::ONE),
        Err(ArithmeticError::Overflow)
    );
    assert_eq!(
        UsageCount::TEN.checked_mul(u64::MAX),
        Err(ArithmeticError::Overflow)
    );
}

#[test]
fn usage_count_saturating_arithmetic_clamps() {
    assert_eq!(
        UsageCount(u64::MAX).saturating_add(UsageCount::ONE),
        UsageCount(u64::MAX)
    );
    assert_eq!(
        UsageCount::ZERO.saturating_sub(UsageCount::ONE),
        UsageCount::ZERO
    );
    assert_eq!(
        UsageCount::TEN.saturating_mul(u64::MAX),
        UsageCount(u64::MAX)
    );
}

#[test]
fn usage_count_operators_saturate() {
    assert_eq!(UsageCount(u64::MAX) + UsageCount::ONE, UsageCount(u64::MAX));
    assert_eq!(UsageCount::ZERO - UsageCount::ONE, UsageCount::ZERO);
    assert_eq!(UsageCount::TEN * u64::MAX, UsageCount(u64::MAX));
}

#[test]
fn duration_checked_arithmetic_reports_overflow() {
    assert_eq!(
        DurationSeconds(i64::MAX).checked_add(DurationSeconds::ONE),
        Err(ArithmeticError::Overflow)
    );
    assert_eq!(
        DurationSeconds(i64::MIN).checked_sub(DurationSeconds::ONE),
        Err(ArithmeticError::Overflow)
    );
    assert_eq!(
        DurationSeconds(i64::MIN).checked_mul(-1),
        Err(ArithmeticError::Overflow)
    );
}

#[test]
fn duration_saturating_arithmetic_clamps() {
    assert_eq!(
        DurationSeconds(i64::MAX).saturating_add(DurationSeconds::ONE),
        DurationSeconds(i64::MAX)
    );
    assert_eq!(
        DurationSeconds(i64::MIN).saturating_sub(DurationSeconds::ONE),
        DurationSeconds(i64::MIN)
    );
    assert_eq!(
        DurationSeconds(i64::MIN).saturating_mul(-1),
        DurationSeconds(i64::MAX)
    );
}

#[test]
fn duration_operators_saturate() {
    assert_eq!(
        DurationSeconds(i64::MAX) + DurationSeconds::ONE,
        DurationSeconds(i64::MAX)
    );
    assert_eq!(
        DurationSeconds(i64::MIN) - DurationSeconds::ONE,
        DurationSeconds(i64::MIN)
    );
    assert_eq!(DurationSeconds(i64::MIN) * -1, DurationSeconds(i64::MAX));
}

#[test]
fn division_and_remainder_are_checked() {
    let divisor = NonZero::new(DurationNanoseconds(3)).expect("non-zero divisor");
    assert_eq!(DurationNanoseconds::TEN.checked_div(divisor), Ok(3));
    assert_eq!(
        DurationNanoseconds::TEN.checked_rem(divisor),
        Ok(DurationNanoseconds::ONE)
    );
    assert_eq!(
        DurationNanoseconds::TEN.checked_div(DurationNanoseconds::ZERO),
        Err(ArithmeticError::DivisionByZero)
    );
    assert_eq!(
        DurationNanoseconds::TEN.checked_rem(DurationNanoseconds::ZERO),
        Err(ArithmeticError::DivisionByZero)
    );
    assert_eq!(
        DurationNanoseconds(i64::MIN).checked_rem(DurationNanoseconds(-1)),
        Ok(DurationNanoseconds::ZERO)
    );
}

#[test]
fn non_zero_rejects_the_zero_value() {
    assert_eq!(
        NonZero::new(UsageCount::ZERO),
        Err(ArithmeticError::DivisionByZero)
    );
    assert_eq!(
        NonZero::new(UsageCount::ONE).map(NonZero::get),
        Ok(UsageCount::ONE)
    );
    assert!(NonZero::<UsageCount>::try_from(UsageCount::ZERO).is_err());
}

#[test]
fn timestamp_checked_arithmetic_reports_overflow() {
    assert_eq!(
        TimestampMilliseconds(i64::MAX).checked_add(DurationMilliseconds::ONE),
        Err(ArithmeticError::Overflow)
    );
    assert_eq!(
        TimestampMilliseconds(i64::MIN).checked_sub(DurationMilliseconds::ONE),
        Err(ArithmeticError::Overflow)
    );
    assert_eq!(
        TimestampMilliseconds(i64::MIN).checked_sub(TimestampMilliseconds(i64::MAX)),
        Err(ArithmeticError::Overflow)
    );
}

#[test]
fn timestamp_saturating_arithmetic_clamps() {
    assert_eq!(
        TimestampMilliseconds(i64::MAX).saturating_add(DurationMilliseconds::ONE),
        TimestampMilliseconds(i64::MAX)
    );
    assert_eq!(
        TimestampMilliseconds(i64::MIN).saturating_sub(DurationMilliseconds::ONE),
        TimestampMilliseconds(i64::MIN)
    );
    assert_eq!(
        TimestampMilliseconds::ZERO.saturating_duration_since(TimestampMilliseconds::ONE),
        DurationMilliseconds(-1)
    );
}

#[test]
fn timestamp_operators_saturate() {
    assert_eq!(
        TimestampMilliseconds(i64::MAX) + DurationMilliseconds::ONE,
        TimestampMilliseconds(i64::MAX)
    );
    assert_eq!(
        TimestampMilliseconds(i64::MIN) - DurationMilliseconds::ONE,
        TimestampMilliseconds(i64::MIN)
    );
    assert_eq!(
        TimestampMilliseconds(10) - TimestampMilliseconds(3),
        DurationMilliseconds(7)
    );
    assert_eq!(
        DurationMilliseconds(3) + TimestampMilliseconds(10),
        TimestampMilliseconds(13)
    );
}

#[test]
fn timestamp_duration_since_is_checked() {
    assert_eq!(
        TimestampMilliseconds(10).checked_duration_since(TimestampMilliseconds(3)),
        Ok(DurationMilliseconds(7))
    );
    assert_eq!(
        TimestampMilliseconds(3).checked_duration_since(TimestampMilliseconds(10)),
        Ok(DurationMilliseconds(-7))
    );
}
