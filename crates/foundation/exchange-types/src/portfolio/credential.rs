use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credential {
    pub value: String,
    pub issued_at: Instant,
    pub expires_at: Option<Instant>,
    pub renewable: bool,
}

impl Credential {
    pub fn new(value: impl Into<String>, issued_at: Instant, expires_at: Option<Instant>) -> Self {
        Self {
            value: value.into(),
            issued_at,
            expires_at,
            renewable: expires_at.is_some(),
        }
    }

    pub fn with_renewable(mut self, renewable: bool) -> Self {
        self.renewable = renewable;
        self
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|expires_at| now >= expires_at)
    }

    pub fn expires_within(&self, now: Instant, window: Duration) -> bool {
        self.expires_at
            .is_some_and(|expires_at| expires_at.saturating_duration_since(now) <= window)
    }

    pub fn lifetime_remaining(&self, now: Instant) -> Option<Duration> {
        self.expires_at
            .map(|expires_at| expires_at.saturating_duration_since(now))
    }
}
