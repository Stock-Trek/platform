use crate::iceberg::catalog::iceberg_catalog::retry::RetryPolicy;
use std::time::Duration;

#[test]
fn backoff_grows_with_attempts_and_stays_within_jitter_bounds() {
    let policy = RetryPolicy::new(4, Duration::from_millis(250));

    for attempt in 1..=policy.max_attempts() {
        let base = Duration::from_millis(250) * u32::try_from(attempt).unwrap();
        for _ in 0..32 {
            let backoff = policy.backoff(attempt);
            assert!(backoff >= base / 2);
            assert!(backoff <= base);
        }
    }
}

#[test]
fn retries_stop_at_the_maximum_attempt_count() {
    let policy = RetryPolicy::new(4, Duration::from_millis(250));

    assert!(policy.should_retry(1));
    assert!(policy.should_retry(3));
    assert!(!policy.should_retry(4));
}
