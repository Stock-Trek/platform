use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;

pub(crate) struct ReplayCache {
    used: Mutex<HashMap<(String, String), DateTime<Utc>>>,
}

impl ReplayCache {
    pub fn new() -> Self {
        Self {
            used: Mutex::new(HashMap::new()),
        }
    }

    pub fn consume(
        &self,
        token_id: &str,
        nonce: &str,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> bool {
        if expires_at <= now {
            return false;
        }
        let mut used = self.used.lock();
        used.retain(|_, expiry| *expiry > now);
        let key = (token_id.to_string(), nonce.to_string());
        if used.contains_key(&key) {
            return false;
        }
        used.insert(key, expires_at);
        true
    }
}
