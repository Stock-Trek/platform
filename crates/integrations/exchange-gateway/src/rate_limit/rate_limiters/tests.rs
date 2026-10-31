use super::*;
use crate::rate_limit::rate_limiter::RateLimiter;
use platform_errors::{BpError, RateLimitError};
use platform_exchange_types::rate_limited::RateLimitRestriction;
use platform_types::new_types::{DurationNanoseconds, UsageCount};
use std::collections::HashMap;
use std::time::Duration;
use crate::rate_limit::rate_limiter_state::RateLimiterState;

fn rate_limiters() -> RateLimiters {
    let state = RateLimiterState::try_new(DurationNanoseconds(60_000_000_000), UsageCount(10))
        .expect("valid rate limiter state");
    RateLimiters::new(HashMap::from([(
        RateLimitRestriction::RawRequests,
        RateLimiter::new(&[state]),
    )]))
}

#[test]
fn retry_after_blocks_zero_cost_requests() {
    let rate_limiters = rate_limiters();
    rate_limiters
        .set_retry_after(Duration::from_secs(60))
        .expect("set retry after");
    assert!(matches!(
        rate_limiters.did_acquire(&[]),
        Err(BpError::RateLimit(RateLimitError::RateLimited))
    ));
}

#[test]
fn zero_retry_after_does_not_drain_capacity() {
    let rate_limiters = rate_limiters();
    rate_limiters
        .set_retry_after(Duration::ZERO)
        .expect("set retry after");

    assert_eq!(remaining(&rate_limiters), UsageCount(10));
    assert!(
        rate_limiters
            .did_acquire(&[(RateLimitRestriction::RawRequests, UsageCount(1))])
            .is_ok()
    );
}

#[test]
fn zero_retry_after_does_not_clear_an_existing_throttle() {
    let rate_limiters = rate_limiters();
    rate_limiters
        .set_retry_after(Duration::from_secs(60))
        .expect("set retry after");
    rate_limiters
        .set_retry_after(Duration::ZERO)
        .expect("set retry after");

    assert!(matches!(
        rate_limiters.did_acquire(&[]),
        Err(BpError::RateLimit(RateLimitError::RateLimited))
    ));
}

#[test]
fn zero_cost_requests_are_allowed_without_retry_after() {
    let rate_limiters = rate_limiters();
    assert!(rate_limiters.did_acquire(&[]).is_ok());
}

fn remaining(rate_limiters: &RateLimiters) -> UsageCount {
    rate_limiters
        .remaining_capacity()
        .expect("remaining capacity")
        .into_iter()
        .find(|(rate_limit, _)| rate_limit.restriction == RateLimitRestriction::RawRequests)
        .map(|(_, remaining)| remaining)
        .expect("raw requests restriction present")
}

#[test]
fn guard_refunds_acquired_cost_on_drop() {
    let rate_limiters = rate_limiters();
    let costs = vec![(RateLimitRestriction::RawRequests, UsageCount(3))];
    rate_limiters.did_acquire(&costs).expect("acquire");
    assert_eq!(remaining(&rate_limiters), UsageCount(7));

    {
        let _guard = RateLimitGuard::new(&rate_limiters, costs);
    }

    assert_eq!(remaining(&rate_limiters), UsageCount(10));
}

#[test]
fn partial_acquisition_failure_refunds_without_deadlock() {
    let raw_requests =
        RateLimiterState::try_new(DurationNanoseconds(60_000_000_000), UsageCount(10))
            .expect("valid rate limiter state");
    let weight = RateLimiterState::try_new(DurationNanoseconds(60_000_000_000), UsageCount(2))
        .expect("valid rate limiter state");
    let rate_limiters = RateLimiters::new(HashMap::from([
        (
            RateLimitRestriction::RawRequests,
            RateLimiter::new(&[raw_requests]),
        ),
        (RateLimitRestriction::Weight, RateLimiter::new(&[weight])),
    ]));

    let costs = vec![
        (RateLimitRestriction::RawRequests, UsageCount(5)),
        (RateLimitRestriction::Weight, UsageCount(5)),
    ];
    assert!(matches!(
        rate_limiters.did_acquire(&costs),
        Err(BpError::RateLimit(
            RateLimitError::RequestExceedsRateLimit { .. }
        ))
    ));

    // RawRequests was acquired before Weight failed and must be rolled back.
    assert_eq!(remaining(&rate_limiters), UsageCount(10));
}

#[test]
fn disarmed_guard_keeps_acquired_cost() {
    let rate_limiters = rate_limiters();
    let costs = vec![(RateLimitRestriction::RawRequests, UsageCount(3))];
    rate_limiters.did_acquire(&costs).expect("acquire");

    RateLimitGuard::new(&rate_limiters, costs).disarm();

    assert_eq!(remaining(&rate_limiters), UsageCount(7));
}
