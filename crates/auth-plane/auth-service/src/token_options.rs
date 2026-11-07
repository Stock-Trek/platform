use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenExpiry {
    At(DateTime<Utc>),
    After(Duration),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenOptions {
    expiry: TokenExpiry,
    scopes: Vec<String>,
}

impl TokenOptions {
    pub fn new(expiry: TokenExpiry, scopes: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        Self {
            expiry,
            scopes: scopes
                .into_iter()
                .map(|scope| scope.as_ref().to_string())
                .collect(),
        }
    }

    pub fn expiry(&self) -> TokenExpiry {
        self.expiry
    }

    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }
}
