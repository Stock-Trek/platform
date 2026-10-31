use super::*;
use platform_types::new_types::{DurationNanoseconds, UsageCount};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

fn state_with_clock(now: Instant) -> RateLimiterState {
    RateLimiterState::with_clock(
        DurationNanoseconds(60_000_000_000),
        UsageCount(10),
        Arc::new(move || now),
    )
    .expect("valid rate limiter state")
}

#[test]
fn zero_duration_does_not_throttle_or_drain() {
    let mut state = state_with_clock(Instant::now());
    state.throttle_after(Duration::ZERO);

    assert!(!state.is_throttled());
    assert_eq!(
        state.remaining_capacity().expect("remaining capacity"),
        UsageCount(10)
    );
}

#[test]
fn past_deadline_does_not_throttle_or_drain() {
    let now = Instant::now();
    let mut state = state_with_clock(now);
    state.throttle(now - Duration::from_secs(1));

    assert!(!state.is_throttled());
    assert_eq!(
        state.remaining_capacity().expect("remaining capacity"),
        UsageCount(10)
    );
}

#[test]
fn future_deadline_still_throttles_and_drains() {
    let mut state = state_with_clock(Instant::now());
    state.throttle_after(Duration::from_secs(60));

    assert!(state.is_throttled());
    assert_eq!(
        state.remaining_capacity().expect("remaining capacity"),
        UsageCount::ZERO
    );
}

#[test]
fn throttle_deadline_discards_partial_interval_progress() {
    let base = Instant::now();
    let now = Arc::new(Mutex::new(base));
    let clock = {
        let now = now.clone();
        Arc::new(move || *now.lock().expect("clock lock")) as Arc<dyn Fn() -> Instant + Send + Sync>
    };
    let mut state =
        RateLimiterState::with_clock(DurationNanoseconds(60_000_000_000), UsageCount(10), clock)
            .expect("valid rate limiter state");

    // Bank 30s of progress towards the next interval.
    *now.lock().expect("clock lock") = base + Duration::from_secs(90);
    assert_eq!(
        state.remaining_capacity().expect("remaining capacity"),
        UsageCount(10)
    );

    // The exchange throttles us for 30s from t=90s, i.e. until t=120s.
    state.throttle_after(Duration::from_secs(30));
    assert_eq!(
        state.remaining_capacity().expect("remaining capacity"),
        UsageCount::ZERO
    );

    // 30s after the throttle deadline only half an interval has elapsed since
    // the bucket was drained, so the progress banked before the throttle must
    // not be reused to refill the bucket early.
    *now.lock().expect("clock lock") = base + Duration::from_secs(150);
    assert!(!state.is_throttled());
    assert_eq!(
        state.remaining_capacity().expect("remaining capacity"),
        UsageCount::ZERO
    );

    // A full interval after the deadline the bucket refills completely.
    *now.lock().expect("clock lock") = base + Duration::from_secs(180);
    assert_eq!(
        state.remaining_capacity().expect("remaining capacity"),
        UsageCount(10)
    );
}
