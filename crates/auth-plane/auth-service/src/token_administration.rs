use crate::{
    ApiToken, AuthService, RevocationReason, RevokedBy, SecretInput, TokenOptions, TokenScope,
    TokenView,
};
use chrono::{DateTime, Utc};
use platform_errors::BuildResult;

/// Read-only access to token metadata.
///
/// All results are redacted [`TokenView`] values and never contain the plaintext
/// token secret.
pub struct TokenReader<'a> {
    service: &'a AuthService,
}

impl<'a> TokenReader<'a> {
    fn new(service: &'a AuthService) -> Self {
        Self { service }
    }

    pub async fn token(&self, id: &str) -> BuildResult<TokenView> {
        Ok(TokenView::from_token(&self.service.token(id).await?))
    }

    pub async fn tokens_for_owner(&self, owner: &str) -> BuildResult<Vec<TokenView>> {
        Ok(self
            .service
            .tokens_for_owner(owner)
            .await?
            .iter()
            .map(TokenView::from_token)
            .collect())
    }

    pub async fn active_tokens_for_owner(
        &self,
        owner: &str,
        now: DateTime<Utc>,
    ) -> BuildResult<Vec<TokenView>> {
        Ok(self
            .service
            .active_tokens_for_owner(owner, now)
            .await?
            .iter()
            .map(TokenView::from_token)
            .collect())
    }

    pub async fn token_for_owner(&self, owner: &str, now: DateTime<Utc>) -> BuildResult<TokenView> {
        Ok(TokenView::from_token(
            &self.service.token_for_owner(owner, now).await?,
        ))
    }

    pub async fn token_for_owner_with_scope(
        &self,
        owner: &str,
        scope: TokenScope,
        now: DateTime<Utc>,
    ) -> BuildResult<TokenView> {
        Ok(TokenView::from_token(
            &self
                .service
                .token_for_owner_with_scope(owner, scope, now)
                .await?,
        ))
    }
}

/// Token lifecycle administration.
///
/// Token administration is admin-level: `tokens:write` can mint tokens with
/// arbitrary scopes, including [`TokenScope::Admin`], and can rotate or revoke
/// any token. Grant this scope only to trusted principals.
pub struct TokenAdministrator<'a> {
    service: &'a AuthService,
}

impl<'a> TokenAdministrator<'a> {
    fn new(service: &'a AuthService) -> Self {
        Self { service }
    }

    pub async fn add_token(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
        created_at: DateTime<Utc>,
        options: TokenOptions,
        now: DateTime<Utc>,
    ) -> BuildResult<ApiToken> {
        self.service
            .add_token(id, owner, created_at, options, now)
            .await
    }

    pub async fn delete_token(&self, id: &str, now: DateTime<Utc>) -> BuildResult<()> {
        self.service.delete_token(id, now).await
    }

    pub async fn revoke_token(
        &self,
        id: &str,
        reason: RevocationReason,
        revoked_by: RevokedBy,
        now: DateTime<Utc>,
    ) -> BuildResult<ApiToken> {
        self.service.revoke_token(id, reason, revoked_by, now).await
    }

    /// Revokes all active tokens for `owner`. This operation is deliberately
    /// non-atomic, and retries skip tokens that are already revoked.
    pub async fn revoke_tokens_for_owner(
        &self,
        owner: &str,
        reason: RevocationReason,
        revoked_by: RevokedBy,
        now: DateTime<Utc>,
    ) -> BuildResult<Vec<ApiToken>> {
        self.service
            .revoke_tokens_for_owner(owner, reason, revoked_by, now)
            .await
    }

    pub async fn rotate_token(&self, id: &str, now: DateTime<Utc>) -> BuildResult<ApiToken> {
        self.service.rotate_token(id, now).await
    }

    pub async fn rotate_token_to(
        &self,
        id: &str,
        replacement_id: impl Into<String>,
        now: DateTime<Utc>,
    ) -> BuildResult<ApiToken> {
        self.service.rotate_token_to(id, replacement_id, now).await
    }

    pub async fn purge_expired_tokens(&self, now: DateTime<Utc>) -> BuildResult<usize> {
        self.service.purge_expired_tokens(now).await
    }

    pub async fn reconcile_owner_indexes(&self) -> BuildResult<usize> {
        self.service.reconcile_owner_indexes().await
    }
}

impl AuthService {
    /// Authenticates a caller with `tokens:read` and returns a redacted token
    /// reader.
    pub async fn token_reader(
        &self,
        token_id: &str,
        secret: impl SecretInput,
        now: DateTime<Utc>,
    ) -> BuildResult<TokenReader<'_>> {
        self.authenticate(token_id, secret, Some(TokenScope::TokensRead), now)
            .await?;
        Ok(TokenReader::new(self))
    }

    /// Authenticates a caller with `tokens:write` and returns a token
    /// administrator. This grants admin-level control over all tokens, including
    /// issuing tokens with arbitrary scopes.
    pub async fn token_administrator(
        &self,
        token_id: &str,
        secret: impl SecretInput,
        now: DateTime<Utc>,
    ) -> BuildResult<TokenAdministrator<'_>> {
        self.authenticate(token_id, secret, Some(TokenScope::TokensWrite), now)
            .await?;
        Ok(TokenAdministrator::new(self))
    }
}
