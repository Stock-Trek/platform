use crate::{
    ApiToken, AuthService, TokenAuditAction, TokenAuditEvent, TokenExpiry, TokenMetadata,
    TokenOptions, TokenScope,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub async fn add_token(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
        created_at: DateTime<Utc>,
        options: TokenOptions,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let expires_at = match options.expiry() {
            TokenExpiry::At(expires_at) => {
                if expires_at <= created_at {
                    return Err(BpError::auth_invalid_field(
                        "expiry",
                        "must be after the creation time",
                    ));
                }
                if expires_at <= now {
                    return Err(BpError::auth_invalid_field(
                        "expiry",
                        "must be in the future",
                    ));
                }
                expires_at
            }
            TokenExpiry::After(lifetime) => {
                if lifetime <= Duration::zero() {
                    return Err(BpError::auth_invalid_field(
                        "expiry",
                        "must be greater than zero",
                    ));
                }
                created_at.checked_add_signed(lifetime).ok_or_else(|| {
                    BpError::auth_invalid_field("expiry", "is outside the supported range")
                })?
            }
        };
        let metadata = TokenMetadata::new(expires_at, options.scopes())?;
        let token = ApiToken::generate(id, owner, created_at, metadata)?;
        if let Err(error) = self.store.put(&token).await {
            let event = TokenAuditEvent::new(TokenAuditAction::Issued, created_at)
                .with_token(token.id(), Some(token.owner()))
                .failure(error.to_string());
            self.record_audit_failure(event).await;
            return Err(error);
        }
        let event = TokenAuditEvent::new(TokenAuditAction::Issued, created_at)
            .with_token(token.id(), Some(token.owner()));
        self.record_audit(event).await?;
        Ok(token)
    }

    pub async fn delete_token(&self, id: &str, now: DateTime<Utc>) -> BpResult<()> {
        let token = self.stored_token(id).await?;
        self.store.delete(id).await?;
        if let Some(token) = token {
            let event = TokenAuditEvent::new(TokenAuditAction::Deleted, now)
                .with_token(token.id(), Some(token.owner()));
            self.record_audit(event).await?;
        }
        Ok(())
    }

    pub async fn token(&self, id: &str) -> BpResult<ApiToken> {
        self.stored_token(id)
            .await?
            .ok_or_else(|| BpError::unknown_token(id))
    }

    pub(crate) async fn stored_token(&self, id: &str) -> BpResult<Option<ApiToken>> {
        ApiToken::validate_id(id)?;
        let token = self.store.get(id).await?;
        if let Some(token) = &token {
            if token.id() != id {
                return Err(BpError::auth_store(
                    "read",
                    id,
                    false,
                    "token store returned a token with a different id".to_string(),
                ));
            }
            token
                .validate()
                .map_err(|error| BpError::auth_store("read", id, false, error))?;
        }
        Ok(token)
    }

    pub async fn tokens_for_owner(&self, owner: &str) -> BpResult<Vec<ApiToken>> {
        let owner = ApiToken::validated_owner(owner)?;
        let mut ids = self.store.ids().await?;
        ids.sort();
        ids.dedup();
        let mut tokens = Vec::new();
        for id in ids {
            if let Some(token) = self.stored_token(&id).await?
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
        self.select_token_for_owner(owner, None, now).await
    }

    pub async fn token_for_owner_with_scope(
        &self,
        owner: &str,
        scope: TokenScope,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        self.select_token_for_owner(owner, Some(scope), now).await
    }

    async fn select_token_for_owner(
        &self,
        owner: &str,
        scope: Option<TokenScope>,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let tokens = self.active_tokens_for_owner(owner, now).await?;
        if tokens.is_empty() {
            return Err(BpError::auth_invalid_field(
                "owner",
                "has no active API token",
            ));
        }
        match scope {
            Some(scope) => tokens
                .into_iter()
                .filter(|token| token.metadata().has_scope(scope))
                .min_by(Self::compare_tokens)
                .ok_or_else(|| {
                    BpError::auth_invalid_field(
                        "scope",
                        format!("owner has no active API token with required scope {scope:?}"),
                    )
                }),
            None => tokens
                .into_iter()
                .min_by(Self::compare_tokens)
                .ok_or_else(|| BpError::auth_invalid_field("owner", "has no active API token")),
        }
    }

    fn compare_tokens(left: &ApiToken, right: &ApiToken) -> std::cmp::Ordering {
        left.metadata()
            .replaced_by()
            .is_some()
            .cmp(&right.metadata().replaced_by().is_some())
            .then_with(|| {
                right
                    .metadata()
                    .last_used_at()
                    .cmp(&left.metadata().last_used_at())
            })
            .then_with(|| right.created_at().cmp(&left.created_at()))
            .then_with(|| left.id().cmp(right.id()))
    }
}
