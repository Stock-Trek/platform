use crate::connector::portfolio::reconnect_backoff::ReconnectBackoff;
use std::time::Duration;

#[test]
fn delay_is_jittered_within_the_upper_half_of_the_base() {
    let base = Duration::from_secs(8);
    let backoff = ReconnectBackoff::new(base);
    for _ in 0..64 {
        let delay = backoff.delay();
        assert!(delay >= base / 2);
        assert!(delay <= base);
    }
}

#[test]
fn delay_of_zero_stays_zero() {
    let backoff = ReconnectBackoff::new(Duration::ZERO);
    assert_eq!(backoff.delay(), Duration::ZERO);
}

#[test]
fn jitter_varies_between_attempts() {
    let backoff = ReconnectBackoff::new(Duration::from_secs(30));
    let delays: Vec<Duration> = (0..16).map(|_| backoff.delay()).collect();
    assert!(delays.windows(2).any(|pair| pair[0] != pair[1]));
}
