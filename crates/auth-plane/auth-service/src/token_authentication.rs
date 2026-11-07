use crate::{ApiToken, AuthService, SignatureInput, TokenAuditAction, TokenAuditEvent, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub async fn authenticate(
        &self,
        token_id: &str,
        secret: impl SignatureInput,
        scope: Option<TokenScope>,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let candidate = secret.signature_input();
        let mut token = match self.token(token_id).await {
            Ok(token) => token,
            Err(error) => {
                self.record_authentication_failure(token_id, None, now, &error);
                return Err(error);
            }
        };
        if !token.matches_secret(candidate) {
            let error = BpError::auth_invalid_field("secret", "does not match the token");
            self.record_authentication_failure(token_id, Some(&token), now, &error);
            return Err(error);
        }
        if let Err(error) = Self::ensure_active(&token, now) {
            self.record_authentication_failure(token_id, Some(&token), now, &error);
            return Err(error);
        }
        if let Some(scope) = scope
            && let Err(error) = Self::ensure_scope(&token, scope)
        {
            self.record_authentication_failure(token_id, Some(&token), now, &error);
            return Err(error);
        }
        token.metadata.record_use(now);
        if let Err(error) = self.store.update(&token).await {
            self.record_authentication_failure(token_id, Some(&token), now, &error);
            return Err(error);
        }
        let event = TokenAuditEvent::new(TokenAuditAction::Authenticated, now)
            .with_token(token.id(), Some(token.owner()));
        self.audit.record(event);
        Ok(token)
    }

    pub(crate) fn ensure_active(token: &ApiToken, now: DateTime<Utc>) -> BpResult<()> {
        if token.metadata().is_revoked() {
            return Err(BpError::auth_invalid_field(
                "token_id",
                format!("{:?} has been revoked", token.id()),
            ));
        }
        if token.metadata().is_expired(now) {
            return Err(BpError::auth_invalid_field(
                "token_id",
                format!("{:?} has expired", token.id()),
            ));
        }
        Ok(())
    }

    pub(crate) fn ensure_scope(token: &ApiToken, required: TokenScope) -> BpResult<()> {
        if token.metadata().has_scope(required) {
            Ok(())
        } else {
            Err(BpError::auth_invalid_field(
                "scope",
                format!(
                    "token {:?} is missing required scope {required:?}",
                    token.id()
                ),
            ))
        }
    }

    fn record_authentication_failure(
        &self,
        token_id: &str,
        token: Option<&ApiToken>,
        now: DateTime<Utc>,
        error: &BpError,
    ) {
        let event = TokenAuditEvent::new(TokenAuditAction::AuthenticationFailed, now)
            .with_token(token_id, token.map(ApiToken::owner))
            .failure(error.to_string());
        self.audit.record(event);
    }
}
