use super::{
    support_tests::TestFixture,
    token_cache_support_tests::{Fixture, TestStore},
};
use crate::{CachedTokenStore, TokenCacheConfig, TokenStore};
use secrecy::ExposeSecret;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn local_mutations_refresh_cached_entries() {
    let store = Arc::new(TestStore::new());
    let cache = Fixture::cached(store.clone(), Fixture::config(10_000, 20_000, 3, 10_000));
    let token = TestFixture::token("token-c");

    assert!(cache.get("token-c").await.unwrap().is_none());
    cache.put(&token).await.unwrap();
    assert_eq!(cache.get("token-c").await.unwrap().unwrap().id(), "token-c");
    assert_eq!(store.get_calls(), 1);

    cache.delete("token-c").await.unwrap();
    assert!(cache.get("token-c").await.unwrap().is_none());
    assert_eq!(store.get_calls(), 1);
}

#[tokio::test]
async fn local_updates_win_over_in_flight_reads() {
    let store = Arc::new(TestStore::new());
    let original = TestFixture::token("token-e");
    let replacement = TestFixture::token("token-e");
    store.insert(&original).await;
    store.set_delay_millis(50);
    let cache = Arc::new(Fixture::cached(
        store.clone(),
        Fixture::config(10_000, 20_000, 3, 10_000),
    ));
    let reader = cache.clone();
    let read = tokio::spawn(async move { reader.get("token-e").await });

    tokio::time::sleep(Duration::from_millis(10)).await;
    cache.update(&original, &replacement).await.unwrap();
    let loaded = read.await.unwrap().unwrap().unwrap();
    assert!(loaded.matches_secret(original.secret().expose_secret()));

    let current = cache.get("token-e").await.unwrap().unwrap();
    assert!(current.matches_secret(replacement.secret().expose_secret()));
    assert_eq!(store.get_calls(), 1);
}

#[tokio::test]
async fn cache_capacity_remains_bounded() {
    let store = Arc::new(TestStore::new());
    let first = TestFixture::token("token-f");
    let second = TestFixture::token("token-g");
    store.insert(&first).await;
    store.insert(&second).await;
    let config = TokenCacheConfig::new(
        1,
        1,
        Duration::from_secs(10),
        Duration::from_secs(20),
        3,
        Duration::from_secs(10),
    )
    .unwrap();
    let cache: CachedTokenStore = Fixture::cached(store.clone(), config);

    assert!(cache.get("token-f").await.unwrap().is_some());
    assert_eq!(store.get_calls(), 1);
    assert!(cache.get("token-f").await.unwrap().is_some());
    assert_eq!(store.get_calls(), 1);

    assert!(cache.get("token-g").await.unwrap().is_some());
    assert_eq!(store.get_calls(), 2);
    assert!(cache.get("token-f").await.unwrap().is_some());
    assert_eq!(store.get_calls(), 3);
}
