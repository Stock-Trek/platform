use crate::{
    ApiToken, AuthService, TokenAuditAction, TokenAuditEvent, TokenExpiry, TokenMetadata,
    TokenMutation, TokenOptions,
};
use chrono::{DateTime, Duration, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub(crate) async fn add_token(
        &self,
        id: impl Into<String>,
        owner: impl Into<String>,
        created_at: DateTime<Utc>,
        options: TokenOptions,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        if created_at > now {
            return Err(BpError::auth_invalid_field(
                "created_at",
                "must not be in the future",
            ));
        }
        let expires_at = match options.expiry() {
            TokenExpiry::At(expires_at) => {
                if expires_at <= created_at {
                    return Err(BpError::auth_invalid_field(
                        "expires_at",
                        "must be after the creation time",
                    ));
                }
                if expires_at <= now {
                    return Err(BpError::auth_invalid_field(
                        "expires_at",
                        "must be in the future",
                    ));
                }
                expires_at
            }
            TokenExpiry::After(lifetime) => {
                if lifetime <= Duration::zero() {
                    return Err(BpError::auth_invalid_field(
                        "expires_at",
                        "must be greater than zero",
                    ));
                }
                let expires_at = created_at.checked_add_signed(lifetime).ok_or_else(|| {
                    BpError::auth_invalid_field("expires_at", "is outside the supported range")
                })?;
                if expires_at <= now {
                    return Err(BpError::auth_invalid_field(
                        "expires_at",
                        "must be in the future",
                    ));
                }
                expires_at
            }
        };
        let metadata = TokenMetadata::new(expires_at, options.scopes())?;
        let token = ApiToken::generate(id, owner, created_at, metadata)?;
        let event = TokenAuditEvent::new(TokenAuditAction::Issued, now)
            .with_token(token.id(), Some(token.owner()));
        match self
            .commit_mutation(TokenMutation::Create(token.clone()), event)
            .await
        {
            Ok(_) => Ok(token),
            Err(error) => {
                let event = TokenAuditEvent::new(TokenAuditAction::Issued, now)
                    .with_token(token.id(), Some(token.owner()))
                    .failure(error.to_string());
                self.record_audit_failure(event).await;
                Err(error)
            }
        }
    }

    pub(crate) async fn token(&self, id: &str) -> BpResult<ApiToken> {
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
}
