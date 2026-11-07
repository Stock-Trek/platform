use crate::{
    ApiToken, AuthService, TokenAuditAction, TokenAuditEvent, TokenDuration, TokenMetadata,
    TokenOptions,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub async fn add_token(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
    ) -> BpResult<ApiToken> {
        self.add_token_at(id, owner, Utc::now()).await
    }

    pub async fn add_token_at(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
        created_at: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        self.add_token_with_options_at(id, owner, created_at, TokenOptions::new())
            .await
    }

    pub async fn add_token_with_options(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
        options: TokenOptions,
    ) -> BpResult<ApiToken> {
        self.add_token_with_options_at(id, owner, Utc::now(), options)
            .await
    }

    pub async fn add_token_with_options_at(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
        created_at: DateTime<Utc>,
        options: TokenOptions,
    ) -> BpResult<ApiToken> {
        let token = self.build_token(id, owner, created_at, options)?;
        if let Err(error) = self.store.put(&token).await {
            let event = TokenAuditEvent::new(TokenAuditAction::Issued, created_at)
                .with_token(token.id(), Some(token.owner()))
                .failure(error.to_string());
            self.record(event);
            return Err(error);
        }
        let event = TokenAuditEvent::new(TokenAuditAction::Issued, created_at)
            .with_token(token.id(), Some(token.owner()));
        self.record(event);
        Ok(token)
    }

    pub async fn add_token_with_scopes<S, I>(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
        scopes: I,
    ) -> BpResult<ApiToken>
    where
        S: AsRef<str>,
        I: IntoIterator<Item = S>,
    {
        let options = TokenOptions::new().with_scopes(scopes);
        self.add_token_with_options(id, owner, options).await
    }

    pub async fn add_token_with_expiry(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
        expires_at: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let options = TokenOptions::new().with_expiry(expires_at);
        self.add_token_with_options(id, owner, options).await
    }

    pub async fn add_token_with_lifetime<D: TokenDuration>(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
        lifetime: D,
    ) -> BpResult<ApiToken> {
        let now = Utc::now();
        let options = Self::options_for_lifetime(lifetime, now);
        self.add_token_with_options_at(id, owner, now, options)
            .await
    }

    pub async fn create_token(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
    ) -> BpResult<ApiToken> {
        self.add_token(id, owner).await
    }

    pub async fn delete_token(&self, id: &str) -> BpResult<()> {
        ApiToken::validate_id(id)?;
        let token = self.store.get(id).await?;
        self.store.delete(id).await?;
        let event = TokenAuditEvent::new(TokenAuditAction::Deleted, Utc::now())
            .with_token(id, token.as_ref().map(ApiToken::owner));
        self.record(event);
        Ok(())
    }

    pub async fn token(&self, id: &str) -> BpResult<ApiToken> {
        ApiToken::validate_id(id)?;
        self.store
            .get(id)
            .await?
            .ok_or_else(|| BpError::unknown_token(id))
    }

    pub async fn token_ids(&self) -> BpResult<Vec<String>> {
        self.store.ids().await
    }

    pub async fn tokens_for_owner(&self, owner: &str) -> BpResult<Vec<ApiToken>> {
        Self::validate_owner(owner)?;
        let mut tokens = Vec::new();
        for id in self.store.ids().await? {
            if let Some(token) = self.store.get(&id).await?
                && token.owner() == owner
            {
                tokens.push(token);
            }
        }
        Ok(tokens)
    }

    pub async fn list_tokens_for_owner(&self, owner: &str) -> BpResult<Vec<ApiToken>> {
        self.tokens_for_owner(owner).await
    }

    pub async fn active_tokens_for_owner(&self, owner: &str) -> BpResult<Vec<ApiToken>> {
        let now = Utc::now();
        Ok(self
            .tokens_for_owner(owner)
            .await?
            .into_iter()
            .filter(|token| token.is_active(now))
            .collect())
    }

    pub async fn token_for_owner(&self, owner: &str) -> BpResult<ApiToken> {
        self.token_for_owner_at(owner, Utc::now()).await
    }

    pub async fn token_for_owner_at(&self, owner: &str, now: DateTime<Utc>) -> BpResult<ApiToken> {
        self.tokens_for_owner(owner)
            .await?
            .into_iter()
            .filter(|token| token.is_active(now))
            .min_by(|left, right| {
                left.replaced_by()
                    .is_some()
                    .cmp(&right.replaced_by().is_some())
                    .then_with(|| left.id().cmp(right.id()))
            })
            .ok_or_else(|| BpError::auth_invalid_field("owner", "has no active API token"))
    }

    fn build_token(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
        created_at: DateTime<Utc>,
        options: TokenOptions,
    ) -> BpResult<ApiToken> {
        let expires_at = self.resolve_expiry(options.expiry(), created_at);
        let metadata = TokenMetadata::new(expires_at, options.scopes())?;
        ApiToken::generate_with_metadata(id, owner, created_at, metadata)
    }

    fn options_for_lifetime<D: TokenDuration>(lifetime: D, now: DateTime<Utc>) -> TokenOptions {
        match lifetime.as_chrono_duration() {
            Some(lifetime) => TokenOptions::new().with_expiry(now + lifetime),
            None => TokenOptions::new(),
        }
    }

    fn validate_owner(owner: &str) -> BpResult<()> {
        if owner.trim().is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        Ok(())
    }
}
