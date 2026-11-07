use crate::replay_cache::ReplayCache;
use chrono::{Duration, Utc};

struct ReplayCacheFixture;

impl ReplayCacheFixture {
    fn cache() -> ReplayCache {
        ReplayCache::new()
    }

    fn expiry(now: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
        now + Duration::minutes(5)
    }
}

#[test]
fn nonces_are_scoped_to_a_token() {
    let cache = ReplayCacheFixture::cache();
    let now = Utc::now();
    let expires_at = ReplayCacheFixture::expiry(now);

    assert!(cache.consume("token-a", "nonce-a", expires_at, now));
    assert!(!cache.consume("token-a", "nonce-a", expires_at, now));
    assert!(cache.consume("token-b", "nonce-a", expires_at, now));
}

#[test]
fn expired_nonces_are_evicted() {
    let cache = ReplayCacheFixture::cache();
    let now = Utc::now();
    let expires_at = ReplayCacheFixture::expiry(now);

    assert!(cache.consume("token-a", "nonce-a", expires_at, now));
    let later = now + Duration::minutes(6);
    let later_expires_at = later + Duration::minutes(5);
    assert!(cache.consume("token-a", "nonce-a", later_expires_at, later));
    assert!(!cache.consume("token-a", "nonce-a", later_expires_at, later));
}
