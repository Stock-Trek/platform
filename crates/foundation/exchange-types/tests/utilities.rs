use platform_exchange_types::{
    binance::{klines_limit::BinanceKlinesLimit, recv_window::BinanceRecvWindow},
    encode::ByteEncoder,
};
use platform_types::new_types::{DurationMilliseconds, NonZero, UsageCount};

#[test]
fn percent_encoding_preserves_unreserved_bytes() {
    assert_eq!(ByteEncoder::Percent.encode(b"AZaz09-._~"), "AZaz09-._~");
}

#[test]
fn percent_encoding_escapes_reserved_and_non_ascii_bytes() {
    assert_eq!(ByteEncoder::Percent.encode(b"a b&c=d"), "a%20b%26c%3Dd");
    assert_eq!(ByteEncoder::Percent.encode(b"\xFF\x7F"), "%FF%7F");
}

#[test]
fn klines_limit_accepts_only_one_to_maximum() {
    assert!(BinanceKlinesLimit::try_new(0).is_err());
    assert!(BinanceKlinesLimit::try_new(1).is_ok());
    assert!(BinanceKlinesLimit::try_new(1_000).is_ok());
    assert!(BinanceKlinesLimit::try_new(1_001).is_err());
}

#[test]
fn recv_window_accepts_only_one_to_maximum_milliseconds() {
    assert!(BinanceRecvWindow::try_new(DurationMilliseconds::ZERO).is_err());
    assert!(BinanceRecvWindow::try_new(DurationMilliseconds::ONE).is_ok());
    assert!(BinanceRecvWindow::try_new(DurationMilliseconds(60_000)).is_ok());
    assert!(BinanceRecvWindow::try_new(DurationMilliseconds(60_001)).is_err());
}

#[test]
#[should_panic(expected = "arithmetic overflow")]
fn new_type_addition_panics_on_overflow() {
    let _ = UsageCount(u64::MAX) + UsageCount::ONE;
}

#[test]
#[should_panic(expected = "arithmetic overflow")]
fn new_type_subtraction_panics_on_underflow() {
    let _ = UsageCount::ZERO - UsageCount::ONE;
}

#[test]
#[should_panic(expected = "arithmetic overflow")]
fn new_type_scalar_multiplication_panics_on_overflow() {
    let _ = UsageCount::TWO * u64::MAX;
}

#[test]
fn new_type_saturating_arithmetic_is_explicit() {
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
fn new_type_division_and_remainder_are_checked() {
    let divisor = NonZero::new(UsageCount(3)).expect("non-zero divisor");
    assert_eq!(UsageCount::TEN.checked_div(divisor), Ok(3));
    assert_eq!(UsageCount::TEN.checked_rem(divisor), Ok(UsageCount::ONE));
    assert!(UsageCount::TEN.checked_div(UsageCount::ZERO).is_err());
    assert!(UsageCount::TEN.checked_rem(UsageCount::ZERO).is_err());
}
