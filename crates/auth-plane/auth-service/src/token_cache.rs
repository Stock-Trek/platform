use crate::{ApiToken, TokenCacheConfig};
use lru::LruCache;
use parking_lot::Mutex;
use platform_metrics::AuthTokenCacheMetrics;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::time::Instant;

#[derive(Debug)]
struct PositiveEntry {
    token: ApiToken,
    stored_at: Instant,
}

#[derive(Debug)]
struct NegativeEntry {
    stored_at: Instant,
}

pub enum CacheLookup {
    Fresh(Option<ApiToken>),
    Stale(ApiToken),
    Miss,
}

pub struct TokenCache {
    positive: Mutex<LruCache<String, PositiveEntry>>,
    negative: Mutex<LruCache<String, NegativeEntry>>,
    generations: Mutex<LruCache<String, u64>>,
    next_generation: AtomicU64,
    epoch: AtomicU64,
    freshness: Duration,
    max_stale_age: Duration,
}

impl TokenCache {
    pub fn new(config: &TokenCacheConfig) -> Self {
        let capacity = NonZeroUsize::new(config.max_entries()).unwrap_or(NonZeroUsize::MIN);
        let negative_capacity =
            NonZeroUsize::new(config.negative_entries()).unwrap_or(NonZeroUsize::MIN);
        Self {
            positive: Mutex::new(LruCache::new(capacity)),
            negative: Mutex::new(LruCache::new(negative_capacity)),
            generations: Mutex::new(LruCache::new(capacity)),
            next_generation: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
            freshness: config.freshness(),
            max_stale_age: config.max_stale_age(),
        }
    }

    pub fn lookup(&self, id: &str, now: Instant) -> CacheLookup {
        let positive = {
            let mut entries = self.positive.lock();
            entries
                .get(id)
                .map(|entry| (entry.token.clone(), entry.stored_at))
        };
        if let Some((token, stored_at)) = positive {
            let age = now.saturating_duration_since(stored_at);
            if age < self.freshness {
                return CacheLookup::Fresh(Some(token));
            }
            if age <= self.max_stale_age {
                return CacheLookup::Stale(token);
            }
            self.positive.lock().pop(id);
        }

        let negative = {
            let mut entries = self.negative.lock();
            entries.get(id).map(|entry| entry.stored_at)
        };
        if let Some(stored_at) = negative {
            let age = now.saturating_duration_since(stored_at);
            if age < self.freshness {
                return CacheLookup::Fresh(None);
            }
            self.negative.lock().pop(id);
        }
        CacheLookup::Miss
    }

    pub fn generation(&self, id: &str) -> u64 {
        self.generations.lock().get(id).copied().unwrap_or_default()
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    pub fn store_token(&self, id: &str, token: ApiToken, now: Instant) {
        self.bump_generation(id);
        self.negative.lock().pop(id);
        self.positive.lock().put(
            id.to_string(),
            PositiveEntry {
                token,
                stored_at: now,
            },
        );
        self.publish_sizes();
    }

    pub fn store_missing(&self, id: &str, now: Instant) {
        self.bump_generation(id);
        self.positive.lock().pop(id);
        self.negative
            .lock()
            .put(id.to_string(), NegativeEntry { stored_at: now });
        self.publish_sizes();
    }

    pub fn invalidate(&self, id: &str) {
        self.bump_generation(id);
        self.positive.lock().pop(id);
        self.negative.lock().pop(id);
        self.publish_sizes();
    }

    pub fn insert_loaded_token(
        &self,
        id: &str,
        token: ApiToken,
        generation: u64,
        epoch: u64,
        observed_at: Instant,
    ) -> bool {
        let mut generations = self.generations.lock();
        if self.epoch() != epoch || generations.get(id).copied().unwrap_or_default() != generation {
            return false;
        }
        self.negative.lock().pop(id);
        self.positive.lock().put(
            id.to_string(),
            PositiveEntry {
                token,
                stored_at: observed_at,
            },
        );
        drop(generations);
        self.publish_sizes();
        true
    }

    pub fn insert_loaded_missing(
        &self,
        id: &str,
        generation: u64,
        epoch: u64,
        observed_at: Instant,
    ) -> bool {
        let mut generations = self.generations.lock();
        if self.epoch() != epoch || generations.get(id).copied().unwrap_or_default() != generation {
            return false;
        }
        self.positive.lock().pop(id);
        self.negative.lock().put(
            id.to_string(),
            NegativeEntry {
                stored_at: observed_at,
            },
        );
        drop(generations);
        self.publish_sizes();
        true
    }

    fn bump_generation(&self, id: &str) -> u64 {
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.epoch.fetch_add(1, Ordering::SeqCst);
        self.generations.lock().put(id.to_string(), generation);
        generation
    }

    fn publish_sizes(&self) {
        let entries = self.positive.lock().len();
        let negative_entries = self.negative.lock().len();
        AuthTokenCacheMetrics::record_size(entries, negative_entries);
    }
}
