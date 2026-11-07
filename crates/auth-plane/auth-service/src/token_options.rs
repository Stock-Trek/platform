use crate::TokenScope;
use chrono::{DateTime, Duration, Utc};
use std::borrow::Borrow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenExpiry {
    At(DateTime<Utc>),
    After(Duration),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenOptions {
    expiry: TokenExpiry,
    scopes: Vec<TokenScope>,
}

impl TokenOptions {
    pub fn new(
        expiry: TokenExpiry,
        scopes: impl IntoIterator<Item = impl Borrow<TokenScope>>,
    ) -> Self {
        Self {
            expiry,
            scopes: scopes.into_iter().map(|scope| *scope.borrow()).collect(),
        }
    }

    pub fn expiry(&self) -> TokenExpiry {
        self.expiry
    }

    pub fn scopes(&self) -> &[TokenScope] {
        &self.scopes
    }
}
