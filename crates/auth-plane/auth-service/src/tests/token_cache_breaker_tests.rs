use crate::tests::{
    support_tests::TestFixture,
    token_cache_support_tests::{Fixture, TestStore},
};
use crate::{
    BreakerState, TokenStore,
    circuit_breaker::{BreakerDecision, CircuitBreaker},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::task::JoinSet;

#[tokio::test(start_paused = true)]
async fn serves_stale_when_a_read_fails_and_opens_the_breaker() {
    let store = Arc::new(TestStore::new());
    store.insert(&TestFixture::token("token-a")).await;
    let cache = Fixture::cached(store.clone(), Fixture::config(5, 500, 1, 1_000));

    assert!(cache.get("token-a").await.unwrap().is_some());
    assert_eq!(cache.breaker_state(), BreakerState::Closed);
    store.set_fail(true);
    tokio::time::advance(Duration::from_millis(15)).await;

    assert_eq!(cache.get("token-a").await.unwrap().unwrap().id(), "token-a");
    assert_eq!(cache.breaker_state(), BreakerState::Open);
    assert_eq!(store.get_calls(), 2);

    assert_eq!(cache.get("token-a").await.unwrap().unwrap().id(), "token-a");
    assert_eq!(store.get_calls(), 2);
}

#[tokio::test]
async fn fails_closed_without_a_fresh_or_stale_entry() {
    let store = Arc::new(TestStore::new());
    store.set_fail(true);
    let cache = Fixture::cached(store.clone(), Fixture::config(5, 500, 1, 1_000));

    assert!(cache.get("token-a").await.is_err());
    assert_eq!(cache.breaker_state(), BreakerState::Open);
    assert!(cache.get("token-a").await.is_err());
    assert_eq!(store.get_calls(), 1);
}

#[tokio::test(start_paused = true)]
async fn does_not_serve_stale_entries_past_the_maximum_age() {
    let store = Arc::new(TestStore::new());
    store.insert(&TestFixture::token("token-a")).await;
    let cache = Fixture::cached(store.clone(), Fixture::config(5, 20, 1, 1_000));

    assert!(cache.get("token-a").await.unwrap().is_some());
    store.set_fail(true);
    tokio::time::advance(Duration::from_millis(50)).await;

    assert!(cache.get("token-a").await.is_err());
    assert_eq!(store.get_calls(), 2);
}

#[tokio::test(start_paused = true)]
async fn half_open_probe_closes_the_breaker_after_recovery() {
    let store = Arc::new(TestStore::new());
    store.insert(&TestFixture::token("token-a")).await;
    let cache = Fixture::cached(store.clone(), Fixture::config(5, 500, 1, 20));

    assert!(cache.get("token-a").await.unwrap().is_some());
    store.set_fail(true);
    tokio::time::advance(Duration::from_millis(15)).await;
    assert!(cache.get("token-a").await.unwrap().is_some());
    assert_eq!(cache.breaker_state(), BreakerState::Open);

    store.set_fail(false);
    tokio::time::advance(Duration::from_millis(40)).await;
    assert!(cache.get("token-a").await.unwrap().is_some());
    assert_eq!(cache.breaker_state(), BreakerState::Closed);
    assert_eq!(store.get_calls(), 3);
}

#[tokio::test]
async fn permanent_lookup_errors_do_not_open_the_breaker() {
    let store = Arc::new(TestStore::new());
    let cache = Fixture::cached(store.clone(), Fixture::config(5, 500, 1, 1_000));

    assert!(cache.get("bad id!").await.is_err());
    assert_eq!(cache.breaker_state(), BreakerState::Closed);
    assert_eq!(store.get_calls(), 1);
}

#[tokio::test]
async fn permanent_store_errors_do_not_open_the_breaker() {
    let store = Arc::new(TestStore::new());
    store.set_permanent_fail(true);
    let cache = Fixture::cached(store.clone(), Fixture::config(5, 500, 1, 1_000));

    assert!(cache.get("token-a").await.is_err());
    assert_eq!(cache.breaker_state(), BreakerState::Closed);
    assert_eq!(store.get_calls(), 1);
}

#[tokio::test]
async fn late_successes_do_not_close_an_open_breaker() {
    let breaker = CircuitBreaker::new(1, Duration::from_secs(1));
    let early = match breaker.allow_request() {
        BreakerDecision::Allowed(permit) => permit,
        BreakerDecision::Rejected(_) => panic!("breaker should start closed"),
    };
    let failing = match breaker.allow_request() {
        BreakerDecision::Allowed(permit) => permit,
        BreakerDecision::Rejected(_) => panic!("breaker should allow concurrent reads"),
    };

    failing.failure();
    assert_eq!(breaker.state(), BreakerState::Open);
    early.success();
    assert_eq!(breaker.state(), BreakerState::Open);
}

#[tokio::test]
async fn coalesces_concurrent_failures() {
    let store = Arc::new(TestStore::new());
    store.set_fail(true);
    store.hold_reads();
    let cache = Arc::new(Fixture::cached(
        store.clone(),
        Fixture::config(10_000, 20_000, 10, 10_000),
    ));
    let started = Arc::new(AtomicUsize::new(0));
    let mut tasks = JoinSet::new();
    for _ in 0..8 {
        let cache = cache.clone();
        let started = started.clone();
        tasks.spawn(async move {
            started.fetch_add(1, Ordering::SeqCst);
            cache.get("token-a").await.is_err()
        });
    }
    Fixture::wait_for_started(&started, 8).await;
    store.wait_until_read_starts().await;
    store.release_reads();
    while let Some(result) = tasks.join_next().await {
        assert!(result.unwrap());
    }
    assert_eq!(store.get_calls(), 1);
}
