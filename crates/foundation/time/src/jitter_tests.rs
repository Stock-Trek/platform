use crate::jitter::Jitter;
use std::time::Duration;

#[test]
fn delay_stays_within_the_upper_half_of_the_base() {
    let base = Duration::from_secs(8);

    for _ in 0..64 {
        let delay = Jitter::delay(base);
        assert!(delay >= base / 2);
        assert!(delay <= base);
    }
}

#[test]
fn delay_of_zero_stays_zero() {
    assert_eq!(Jitter::delay(Duration::ZERO), Duration::ZERO);
}

#[test]
fn delay_varies_between_calls() {
    let delays: Vec<Duration> = (0..16)
        .map(|_| Jitter::delay(Duration::from_secs(30)))
        .collect();
    assert!(delays.windows(2).any(|pair| pair[0] != pair[1]));
}
