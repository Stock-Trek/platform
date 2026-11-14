use crate::{
    ApiToken, AuthService, SecretInput, TokenAuditAction, TokenAuditEvent, TokenScope, TokenUsage,
};
use chrono::{DateTime, Utc};
use platform_errors::{BuildError, BuildResult};

impl AuthService {
    pub async fn authenticate(
        &self,
        token_id: &str,
        secret: impl SecretInput,
        scope: Option<TokenScope>,
        now: DateTime<Utc>,
    ) -> BuildResult<ApiToken> {
        let candidate = secret.secret_input();
        let token = match self.token(token_id).await {
            Ok(token) => token,
            Err(error) => {
                self.record_authentication_failure(token_id, None, now, &error)
                    .await;
                return Err(error);
            }
        };
        self.authenticate_loaded(token, candidate, scope, now).await
    }

    pub async fn authenticate_loaded(
        &self,
        mut token: ApiToken,
        candidate: &str,
        scope: Option<TokenScope>,
        now: DateTime<Utc>,
    ) -> BuildResult<ApiToken> {
        let token_id = token.id().to_string();
        if !token.matches_secret(candidate) {
            let error = BuildError::auth_invalid_secret(token.id());
            self.record_authentication_failure(&token_id, Some(&token), now, &error)
                .await;
            return Err(error);
        }
        if let Err(error) = Self::ensure_active(&token, now) {
            self.record_authentication_failure(&token_id, Some(&token), now, &error)
                .await;
            return Err(error);
        }
        if let Some(scope) = scope
            && let Err(error) = Self::ensure_scope(&token, scope)
        {
            self.record_authentication_failure(&token_id, Some(&token), now, &error)
                .await;
            return Err(error);
        }
        let event = TokenAuditEvent::new(TokenAuditAction::Authenticated, now)
            .with_token(token.id(), Some(token.owner()));
        token.metadata.record_use(now);
        let usage = TokenUsage::for_token(token.id(), now, vec![event]);
        match self.enqueue_usage(usage).await {
            Ok(()) => Ok(token),
            Err(error) => {
                tracing::error!(%error, "failed to enqueue token usage");
                Err(error)
            }
        }
    }

    pub fn ensure_active(token: &ApiToken, now: DateTime<Utc>) -> BuildResult<()> {
        if token.metadata().is_revoked() {
            return Err(BuildError::auth_revoked(token.id()));
        }
        if token.created_at() > now {
            return Err(BuildError::auth_invalid_field(
                "created_at",
                "must not be in the future",
            ));
        }
        if token.metadata().is_expired(now) {
            return Err(BuildError::auth_expired(token.id()));
        }
        if token.metadata().is_rotation_overlap_ended(now) {
            return Err(BuildError::auth_rotation_overlap_ended(token.id()));
        }
        Ok(())
    }

    pub fn ensure_scope(token: &ApiToken, required: TokenScope) -> BuildResult<()> {
        if token.metadata().has_scope(required) {
            Ok(())
        } else {
            Err(BuildError::auth_missing_scope(
                token.id(),
                required.to_string(),
            ))
        }
    }

    pub async fn record_authentication_failure(
        &self,
        token_id: &str,
        token: Option<&ApiToken>,
        now: DateTime<Utc>,
        error: &BuildError,
    ) {
        let event = TokenAuditEvent::new(TokenAuditAction::AuthenticationFailed, now)
            .with_token(token_id, token.map(ApiToken::owner))
            .failure(error.to_string());
        self.record_audit_failure(event).await;
    }
}
