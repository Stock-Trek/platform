use crate::tests::{
    support_tests::TestFixture,
    token_cache_support_tests::{Fixture, TestStore},
};
use crate::{TokenCacheConfig, TokenStore};
use std::{sync::Arc, time::Duration};
use tokio::task::JoinSet;

#[tokio::test]
async fn caches_positive_and_negative_lookups() {
    let store = Arc::new(TestStore::new());
    let token = TestFixture::token("token-a");
    store.insert(&token).await;
    let cache = Fixture::cached(store.clone(), Fixture::config(10_000, 20_000, 3, 10_000));

    assert_eq!(cache.get("token-a").await.unwrap().unwrap().id(), "token-a");
    assert_eq!(cache.get("token-a").await.unwrap().unwrap().id(), "token-a");
    assert_eq!(store.get_calls(), 1);

    assert!(cache.get("token-b").await.unwrap().is_none());
    assert!(cache.get("token-b").await.unwrap().is_none());
    assert_eq!(store.get_calls(), 2);
}

#[tokio::test]
async fn coalesces_concurrent_misses() {
    let store = Arc::new(TestStore::new());
    store.insert(&TestFixture::token("token-d")).await;
    store.set_delay_millis(30);
    let cache = Arc::new(Fixture::cached(
        store.clone(),
        Fixture::config(10_000, 20_000, 3, 10_000),
    ));
    let mut tasks = JoinSet::new();
    for _ in 0..8 {
        let cache = cache.clone();
        tasks.spawn(async move {
            cache
                .get("token-d")
                .await
                .unwrap()
                .unwrap()
                .id()
                .to_string()
        });
    }
    while let Some(result) = tasks.join_next().await {
        assert_eq!(result.unwrap(), "token-d");
    }
    assert_eq!(store.get_calls(), 1);
}

#[tokio::test]
async fn rejects_invalid_cache_configuration() {
    assert!(
        TokenCacheConfig::new(
            0,
            1,
            Duration::from_secs(1),
            Duration::from_secs(1),
            1,
            Duration::from_secs(1),
        )
        .is_err()
    );
    assert!(
        TokenCacheConfig::new(
            1,
            1,
            Duration::ZERO,
            Duration::from_secs(1),
            1,
            Duration::from_secs(1),
        )
        .is_err()
    );
    assert!(
        TokenCacheConfig::new(
            1,
            1,
            Duration::from_secs(2),
            Duration::from_secs(1),
            1,
            Duration::from_secs(1),
        )
        .is_err()
    );
}
