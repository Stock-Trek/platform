use crate::{ApiToken, TokenMetadata, TokenScope};
use chrono::{DateTime, Utc};

/// A redacted view of an [`ApiToken`] for read and list APIs.
///
/// The token secret is intentionally not included. Use the token returned by
/// creation or rotation when the plaintext secret is required.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenView {
    id: String,
    owner: String,
    created_at: DateTime<Utc>,
    metadata: TokenMetadata,
}

impl TokenView {
    pub fn from_token(token: &ApiToken) -> Self {
        Self {
            id: token.id().to_string(),
            owner: token.owner().to_string(),
            created_at: token.created_at(),
            metadata: token.metadata().clone(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn metadata(&self) -> &TokenMetadata {
        &self.metadata
    }

    pub fn has_scope(&self, scope: TokenScope) -> bool {
        self.metadata.has_scope(scope)
    }

    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.created_at <= now && self.metadata.is_active(now)
    }
}

impl From<&ApiToken> for TokenView {
    fn from(token: &ApiToken) -> Self {
        Self::from_token(token)
    }
}

impl From<ApiToken> for TokenView {
    fn from(token: ApiToken) -> Self {
        Self::from_token(&token)
    }
}
