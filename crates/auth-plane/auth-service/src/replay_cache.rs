use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;

pub(crate) struct ReplayCache {
    used: Mutex<HashMap<String, DateTime<Utc>>>,
}

impl ReplayCache {
    pub fn new() -> Self {
        Self {
            used: Mutex::new(HashMap::new()),
        }
    }

    pub fn consume(&self, nonce: &str, expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        let mut used = self.used.lock();
        used.retain(|_, expiry| *expiry > now);
        if used.contains_key(nonce) {
            return false;
        }
        used.insert(nonce.to_string(), expires_at);
        true
    }
}
