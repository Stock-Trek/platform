use crate::retry_backoff::RetryBackoff;
use std::time::Duration;

#[test]
fn first_delay_is_jittered_within_the_upper_half_of_the_base() {
    let base = Duration::from_secs(8);

    for _ in 0..64 {
        let mut backoff = RetryBackoff::new(base);
        let delay = backoff.delay();
        assert!(delay >= base / 2);
        assert!(delay <= base);
    }
}

#[test]
fn delays_grow_exponentially_up_to_the_cap() {
    let base = Duration::from_secs(1);
    let mut backoff = RetryBackoff::new(base);

    for exponent in 0..=4 {
        let ceiling = base * 2_u32.pow(exponent);
        let delay = backoff.delay();
        assert!(delay >= ceiling / 2);
        assert!(delay <= ceiling);
    }

    for _ in 0..4 {
        let delay = backoff.delay();
        assert!(delay >= base * 8);
        assert!(delay <= base * 16);
    }
}

#[test]
fn reset_returns_to_the_initial_base_delay() {
    let base = Duration::from_secs(2);
    let mut backoff = RetryBackoff::new(base);

    for _ in 0..6 {
        backoff.delay();
    }
    backoff.reset();

    let delay = backoff.delay();
    assert!(delay >= base / 2);
    assert!(delay <= base);
}

#[test]
fn zero_base_stays_zero() {
    let mut backoff = RetryBackoff::new(Duration::ZERO);

    assert_eq!(backoff.delay(), Duration::ZERO);
}
