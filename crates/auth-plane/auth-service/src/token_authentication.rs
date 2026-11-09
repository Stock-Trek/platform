use crate::{
    ApiToken, AuthService, SecretInput, TokenAuditAction, TokenAuditEvent, TokenScope,
    auth_service::MAX_UPDATE_ATTEMPTS,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub async fn authenticate(
        &self,
        token_id: &str,
        secret: impl SecretInput,
        scope: Option<TokenScope>,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let candidate = secret.secret_input();
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let mut token = match self.token(token_id).await {
                Ok(token) => token,
                Err(error) => {
                    self.record_authentication_failure(token_id, None, now, &error)
                        .await;
                    return Err(error);
                }
            };
            if !token.matches_secret(candidate) {
                let error = BpError::auth_invalid_secret(token.id());
                self.record_authentication_failure(token_id, Some(&token), now, &error)
                    .await;
                return Err(error);
            }
            if let Err(error) = Self::ensure_active(&token, now) {
                self.record_authentication_failure(token_id, Some(&token), now, &error)
                    .await;
                return Err(error);
            }
            if let Some(scope) = scope
                && let Err(error) = Self::ensure_scope(&token, scope)
            {
                self.record_authentication_failure(token_id, Some(&token), now, &error)
                    .await;
                return Err(error);
            }
            let original = token.clone();
            token.metadata.record_use(now);
            match self.store.update(&original, &token).await {
                Ok(()) => {
                    let event = TokenAuditEvent::new(TokenAuditAction::Authenticated, now)
                        .with_token(token.id(), Some(token.owner()));
                    self.record_audit(event).await?;
                    return Ok(token);
                }
                Err(error) if Self::is_conflict(&error) => continue,
                Err(error) => {
                    self.record_authentication_failure(token_id, Some(&token), now, &error)
                        .await;
                    return Err(error);
                }
            }
        }
        let error = BpError::token_conflict(token_id);
        self.record_authentication_failure(token_id, None, now, &error)
            .await;
        Err(error)
    }

    pub(crate) fn ensure_active(token: &ApiToken, now: DateTime<Utc>) -> BpResult<()> {
        if token.metadata().is_revoked() {
            return Err(BpError::auth_revoked(token.id()));
        }
        if token.created_at() > now {
            return Err(BpError::auth_invalid_field(
                "created_at",
                "must not be in the future",
            ));
        }
        if token.metadata().is_expired(now) {
            return Err(BpError::auth_expired(token.id()));
        }
        if token.metadata().is_rotation_overlap_ended(now) {
            return Err(BpError::auth_rotation_overlap_ended(token.id()));
        }
        Ok(())
    }

    pub(crate) fn ensure_scope(token: &ApiToken, required: TokenScope) -> BpResult<()> {
        if token.metadata().has_scope(required) {
            Ok(())
        } else {
            Err(BpError::auth_missing_scope(
                token.id(),
                required.to_string(),
            ))
        }
    }

    async fn record_authentication_failure(
        &self,
        token_id: &str,
        token: Option<&ApiToken>,
        now: DateTime<Utc>,
        error: &BpError,
    ) {
        let event = TokenAuditEvent::new(TokenAuditAction::AuthenticationFailed, now)
            .with_token(token_id, token.map(ApiToken::owner))
            .failure(error.to_string());
        self.record_audit_failure(event).await;
    }
}
