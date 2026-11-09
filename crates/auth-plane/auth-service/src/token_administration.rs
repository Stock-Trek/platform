use crate::{
    ApiToken, AuthService, RevocationReason, RevokedBy, SecretInput, TokenOptions, TokenScope,
};
use chrono::{DateTime, Utc};
use platform_errors::BpResult;

pub struct TokenReader<'a> {
    service: &'a AuthService,
}

impl<'a> TokenReader<'a> {
    fn new(service: &'a AuthService) -> Self {
        Self { service }
    }

    pub async fn token(&self, id: &str) -> BpResult<ApiToken> {
        self.service.token(id).await
    }

    pub async fn tokens_for_owner(&self, owner: &str) -> BpResult<Vec<ApiToken>> {
        self.service.tokens_for_owner(owner).await
    }

    pub async fn active_tokens_for_owner(
        &self,
        owner: &str,
        now: DateTime<Utc>,
    ) -> BpResult<Vec<ApiToken>> {
        self.service.active_tokens_for_owner(owner, now).await
    }

    pub async fn token_for_owner(&self, owner: &str, now: DateTime<Utc>) -> BpResult<ApiToken> {
        self.service.token_for_owner(owner, now).await
    }

    pub async fn token_for_owner_with_scope(
        &self,
        owner: &str,
        scope: TokenScope,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        self.service
            .token_for_owner_with_scope(owner, scope, now)
            .await
    }
}

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
    ) -> BpResult<ApiToken> {
        self.service
            .add_token(id, owner, created_at, options, now)
            .await
    }

    pub async fn delete_token(&self, id: &str, now: DateTime<Utc>) -> BpResult<()> {
        self.service.delete_token(id, now).await
    }

    pub async fn revoke_token(
        &self,
        id: &str,
        reason: RevocationReason,
        revoked_by: RevokedBy,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        self.service.revoke_token(id, reason, revoked_by, now).await
    }

    /// Revokes all active tokens for `owner`. See
    /// [`AuthService::revoke_tokens_for_owner`] for the partial-progress and
    /// retry semantics.
    pub async fn revoke_tokens_for_owner(
        &self,
        owner: &str,
        reason: RevocationReason,
        revoked_by: RevokedBy,
        now: DateTime<Utc>,
    ) -> BpResult<Vec<ApiToken>> {
        self.service
            .revoke_tokens_for_owner(owner, reason, revoked_by, now)
            .await
    }

    pub async fn rotate_token(&self, id: &str, now: DateTime<Utc>) -> BpResult<ApiToken> {
        self.service.rotate_token(id, now).await
    }

    pub async fn rotate_token_to(
        &self,
        id: &str,
        replacement_id: impl Into<String>,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        self.service.rotate_token_to(id, replacement_id, now).await
    }

    pub async fn purge_expired_tokens(&self, now: DateTime<Utc>) -> BpResult<usize> {
        self.service.purge_expired_tokens(now).await
    }
}

impl AuthService {
    pub async fn token_reader(
        &self,
        token_id: &str,
        secret: impl SecretInput,
        now: DateTime<Utc>,
    ) -> BpResult<TokenReader<'_>> {
        self.authenticate(token_id, secret, Some(TokenScope::TokensRead), now)
            .await?;
        Ok(TokenReader::new(self))
    }

    pub async fn token_administrator(
        &self,
        token_id: &str,
        secret: impl SecretInput,
        now: DateTime<Utc>,
    ) -> BpResult<TokenAdministrator<'_>> {
        self.authenticate(token_id, secret, Some(TokenScope::TokensWrite), now)
            .await?;
        Ok(TokenAdministrator::new(self))
    }
}
