use strum::{Display, EnumString};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString)]
pub enum TokenScope {
    #[strum(to_string = "*")]
    All,
    #[strum(to_string = "orders:read")]
    OrdersRead,
    #[strum(to_string = "orders:write")]
    OrdersWrite,
    #[strum(to_string = "orders:*")]
    OrdersAll,
    /// Allows reading token metadata through a redacted [`crate::TokenView`].
    #[strum(to_string = "tokens:read")]
    TokensRead,
    /// Allows administering tokens.
    ///
    /// This is an admin-level scope: it can mint tokens with arbitrary scopes,
    /// including [`TokenScope::Admin`], and rotate or revoke any token.
    #[strum(to_string = "tokens:write")]
    TokensWrite,
    #[strum(to_string = "tokens:*")]
    TokensAll,
    #[strum(to_string = "admin")]
    Admin,
}

impl TokenScope {
    pub const DEFAULT: &'static [Self] = &[Self::OrdersRead, Self::OrdersWrite];

    pub fn matches(self, required: Self) -> bool {
        self == Self::All
            || self == Self::Admin
            || self == required
            || matches!(
                (self, required),
                (Self::OrdersAll, Self::OrdersRead | Self::OrdersWrite)
                    | (Self::TokensAll, Self::TokensRead | Self::TokensWrite)
            )
    }
}
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum RevocationReason {
    Compromised,
    Expired,
    NoLongerNeeded,
    OwnerRequested,
    Rotated,
    Administrative,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum RevokedBy {
    Owner,
    Admin,
    SecurityTeam,
    System,
}
use crate::{ApiToken, TokenMetadata};

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
