use crate::{RetryBackoff, RetryPolicy};
use std::time::Duration;

#[test]
fn constant_retry_delays_stay_within_the_upper_half_of_the_delay() {
    let policy = RetryPolicy::constant(4, Duration::from_secs(8));

    for attempt in 0..4 {
        for _ in 0..64 {
            let delay = policy.delay(attempt);
            assert!(delay >= Duration::from_secs(4));
            assert!(delay <= Duration::from_secs(8));
        }
    }
}

#[test]
fn retries_stop_at_the_maximum_attempt_count() {
    let policy = RetryPolicy::constant(4, Duration::from_secs(1));

    assert_eq!(policy.max_attempts(), 4);
    assert!(policy.should_retry(0));
    assert!(policy.should_retry(3));
    assert!(!policy.should_retry(4));
}

#[test]
fn linear_delays_grow_and_are_capped() {
    let policy = RetryPolicy::linear(8, Duration::from_millis(250), Duration::from_millis(1_000))
        .without_jitter();

    assert_eq!(policy.delay(0), Duration::from_millis(250));
    assert_eq!(policy.delay(1), Duration::from_millis(500));
    assert_eq!(policy.delay(2), Duration::from_millis(750));
    assert_eq!(policy.delay(3), Duration::from_millis(1_000));
    assert_eq!(policy.delay(100), Duration::from_millis(1_000));
}

#[test]
fn exponential_delays_grow_and_are_capped() {
    let policy = RetryPolicy::exponential(16, Duration::from_millis(100), Duration::from_secs(1))
        .without_jitter();

    assert_eq!(policy.delay(0), Duration::from_millis(100));
    assert_eq!(policy.delay(1), Duration::from_millis(200));
    assert_eq!(policy.delay(2), Duration::from_millis(400));
    assert_eq!(policy.delay(3), Duration::from_millis(800));
    assert_eq!(policy.delay(4), Duration::from_secs(1));
    assert_eq!(policy.delay(100), Duration::from_secs(1));
}

#[test]
fn exponential_delays_stay_within_jitter_bounds() {
    let policy = RetryPolicy::exponential(8, Duration::from_secs(2), Duration::from_secs(16));

    for attempt in 0..8 {
        let base = Duration::from_secs(2) * 2_u32.pow(u32::try_from(attempt).unwrap());
        let base = base.min(Duration::from_secs(16));
        for _ in 0..32 {
            let delay = policy.delay(attempt);
            assert!(delay >= base / 2);
            assert!(delay <= base);
        }
    }
}

#[test]
fn zero_delays_stay_zero() {
    assert_eq!(
        RetryPolicy::constant(1, Duration::ZERO).delay(0),
        Duration::ZERO
    );
    assert_eq!(
        RetryPolicy::exponential(1, Duration::ZERO, Duration::ZERO).delay(0),
        Duration::ZERO
    );
}

#[test]
fn backoff_sequences_advance_and_reset() {
    let mut backoff =
        RetryBackoff::exponential(Duration::from_millis(100), Duration::from_millis(400))
            .without_jitter();

    assert_eq!(backoff.next_delay(), Duration::from_millis(100));
    assert_eq!(backoff.next_delay(), Duration::from_millis(200));
    assert_eq!(backoff.next_delay(), Duration::from_millis(400));
    assert_eq!(backoff.next_delay(), Duration::from_millis(400));

    backoff.reset();
    assert_eq!(backoff.next_delay(), Duration::from_millis(100));
}

#[test]
fn backoff_uses_the_cap_when_the_initial_delay_exceeds_it() {
    let mut backoff = RetryBackoff::exponential(Duration::from_secs(5), Duration::from_millis(100))
        .without_jitter();

    assert_eq!(backoff.next_delay(), Duration::from_millis(100));
    assert_eq!(backoff.next_delay(), Duration::from_millis(100));
}
