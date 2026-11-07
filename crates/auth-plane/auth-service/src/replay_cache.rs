use chrono::{DateTime, Utc};
use std::{collections::HashMap, sync::Mutex};

pub(crate) struct ReplayCache {
    used: Mutex<HashMap<String, DateTime<Utc>>>,
}

impl Default for ReplayCache {
    fn default() -> Self {
        Self {
            used: Mutex::new(HashMap::new()),
        }
    }
}

impl ReplayCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn consume(&self, nonce: &str, expires_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        let mut used = self.used.lock().expect("poisoned replay mutex");
        used.retain(|_, expiry| *expiry > now);
        if used.contains_key(nonce) {
            return false;
        }
        used.insert(nonce.to_string(), expires_at);
        true
    }
}
