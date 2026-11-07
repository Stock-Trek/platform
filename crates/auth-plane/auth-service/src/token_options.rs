use crate::TokenScope;
use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenExpiry {
    Default,
    Never,
    At(DateTime<Utc>),
    After(Duration),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenOptions {
    expiry: TokenExpiry,
    scopes: Vec<String>,
}

impl Default for TokenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl TokenOptions {
    pub fn new() -> Self {
        Self {
            expiry: TokenExpiry::Default,
            scopes: TokenScope::default_scopes(),
        }
    }

    pub fn with_expiry(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expiry = TokenExpiry::At(expires_at);
        self
    }

    pub fn with_expires_at(self, expires_at: DateTime<Utc>) -> Self {
        self.with_expiry(expires_at)
    }

    pub fn with_lifetime<D: TokenDuration>(self, lifetime: D) -> Self {
        match lifetime.as_chrono_duration() {
            Some(lifetime) => self.after(lifetime),
            None => self,
        }
    }

    pub fn after<D: TokenDuration>(mut self, lifetime: D) -> Self {
        if let Some(lifetime) = lifetime.as_chrono_duration() {
            self.expiry = TokenExpiry::After(lifetime.max(Duration::zero()));
        }
        self
    }

    pub fn without_expiry(mut self) -> Self {
        self.expiry = TokenExpiry::Never;
        self
    }

    pub fn with_scopes<S, I>(mut self, scopes: I) -> Self
    where
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        self.scopes = scopes
            .into_iter()
            .map(|scope| scope.as_ref().to_string())
            .collect();
        self
    }

    pub fn expiry(&self) -> TokenExpiry {
        self.expiry
    }

    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }
}

pub trait TokenDuration {
    fn as_chrono_duration(&self) -> Option<Duration>;
}

impl TokenDuration for Duration {
    fn as_chrono_duration(&self) -> Option<Duration> {
        Some(*self)
    }
}

impl TokenDuration for std::time::Duration {
    fn as_chrono_duration(&self) -> Option<Duration> {
        Duration::from_std(*self).ok()
    }
}

impl TokenDuration for i64 {
    fn as_chrono_duration(&self) -> Option<Duration> {
        Some(Duration::seconds(*self))
    }
}

impl TokenDuration for u64 {
    fn as_chrono_duration(&self) -> Option<Duration> {
        i64::try_from(*self).ok().map(Duration::seconds)
    }
}
