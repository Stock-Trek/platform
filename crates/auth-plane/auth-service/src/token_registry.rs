use crate::{
    ApiToken, AuthService, TokenAuditAction, TokenAuditEvent, TokenExpiry, TokenMetadata,
    TokenOptions,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub async fn add_token(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
        created_at: DateTime<Utc>,
        options: TokenOptions,
    ) -> BpResult<ApiToken> {
        let expires_at = match options.expiry() {
            TokenExpiry::At(expires_at) => expires_at,
            TokenExpiry::After(lifetime) => created_at + lifetime,
        };
        let metadata = TokenMetadata::new(expires_at, options.scopes())?;
        let token = ApiToken::generate(id, owner, created_at, metadata)?;
        if let Err(error) = self.store.put(&token).await {
            let event = TokenAuditEvent::new(TokenAuditAction::Issued, created_at)
                .with_token(token.id(), Some(token.owner()))
                .failure(error.to_string());
            self.audit.record(event);
            return Err(error);
        }
        let event = TokenAuditEvent::new(TokenAuditAction::Issued, created_at)
            .with_token(token.id(), Some(token.owner()));
        self.audit.record(event);
        Ok(token)
    }

    pub async fn delete_token(&self, id: &str, now: DateTime<Utc>) -> BpResult<()> {
        ApiToken::validate_id(id)?;
        let token = self.store.get(id).await?;
        self.store.delete(id).await?;
        let event = TokenAuditEvent::new(TokenAuditAction::Deleted, now)
            .with_token(id, token.as_ref().map(ApiToken::owner));
        self.audit.record(event);
        Ok(())
    }

    pub async fn token(&self, id: &str) -> BpResult<ApiToken> {
        ApiToken::validate_id(id)?;
        self.store
            .get(id)
            .await?
            .ok_or_else(|| BpError::unknown_token(id))
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

    pub async fn active_tokens_for_owner(
        &self,
        owner: &str,
        now: DateTime<Utc>,
    ) -> BpResult<Vec<ApiToken>> {
        Ok(self
            .tokens_for_owner(owner)
            .await?
            .into_iter()
            .filter(|token| token.metadata().is_active(now))
            .collect())
    }

    pub async fn token_for_owner(&self, owner: &str, now: DateTime<Utc>) -> BpResult<ApiToken> {
        self.tokens_for_owner(owner)
            .await?
            .into_iter()
            .filter(|token| token.metadata().is_active(now))
            .min_by(|left, right| {
                left.metadata()
                    .replaced_by()
                    .is_some()
                    .cmp(&right.metadata().replaced_by().is_some())
                    .then_with(|| left.id().cmp(right.id()))
            })
            .ok_or_else(|| BpError::auth_invalid_field("owner", "has no active API token"))
    }

    fn validate_owner(owner: &str) -> BpResult<()> {
        if owner.trim().is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        Ok(())
    }
}
