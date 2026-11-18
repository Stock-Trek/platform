use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;

struct Window {
    count: u64,
    resets_at: DateTime<Utc>,
}

pub struct Throttle {
    window: Duration,
    hits: Mutex<HashMap<String, Window>>,
}

impl Throttle {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            hits: Mutex::new(HashMap::new()),
        }
    }

    pub fn used(&self, key: &str, now: DateTime<Utc>) -> u64 {
        self.hits
            .lock()
            .get(key)
            .filter(|window| window.resets_at > now)
            .map(|window| window.count)
            .unwrap_or_default()
    }

    pub fn blocked(&self, key: &str, limit: u64, now: DateTime<Utc>) -> Option<u64> {
        let hits = self.hits.lock();
        let window = hits.get(key)?;
        if window.resets_at <= now || window.count < limit {
            return None;
        }
        Some(
            (window.resets_at - now)
                .max(Duration::seconds(1))
                .num_seconds()
                .max(1) as u64,
        )
    }

    pub fn hit(&self, key: &str, now: DateTime<Utc>) {
        let mut hits = self.hits.lock();
        let window = hits
            .entry(key.to_string())
            .and_modify(|window| {
                if window.resets_at <= now {
                    *window = Window {
                        count: 0,
                        resets_at: now + self.window,
                    };
                }
            })
            .or_insert_with(|| Window {
                count: 0,
                resets_at: now + self.window,
            });
        window.count += 1;
    }

    pub fn check(&self, key: &str, limit: u64, now: DateTime<Utc>) -> Result<(), u64> {
        if let Some(retry_after) = self.blocked(key, limit, now) {
            return Err(retry_after);
        }
        self.hit(key, now);
        Ok(())
    }
}
