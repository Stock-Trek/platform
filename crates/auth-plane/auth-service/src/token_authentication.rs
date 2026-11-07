use crate::{
    ApiToken, AuthService, SignatureArgument, SignatureInput, TokenAuditAction, TokenAuditEvent,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use std::str;

impl AuthService {
    pub async fn authenticate(
        &self,
        token_id: &str,
        secret: impl SignatureInput,
    ) -> BpResult<ApiToken> {
        self.authenticate_at(token_id, secret, Utc::now()).await
    }

    pub async fn authenticate_at(
        &self,
        token_id: &str,
        secret: impl SignatureInput,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        self.authenticate_for_scope_at(token_id, secret, None, now)
            .await
    }

    pub async fn authenticate_with_scope(
        &self,
        token_id: &str,
        secret: impl SignatureInput,
        scope: &str,
    ) -> BpResult<ApiToken> {
        self.authenticate_with_scope_at(token_id, secret, scope, Utc::now())
            .await
    }

    pub async fn authenticate_with_scope_at(
        &self,
        token_id: &str,
        secret: impl SignatureInput,
        scope: &str,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        self.authenticate_for_scope_at(token_id, secret, Some(scope), now)
            .await
    }

    pub async fn authorize(
        &self,
        token_id: &str,
        secret: impl SignatureInput,
        scope: &str,
    ) -> BpResult<ApiToken> {
        self.authenticate_with_scope(token_id, secret, scope).await
    }

    pub(crate) async fn authenticate_for_scope_at(
        &self,
        token_id: &str,
        secret: impl SignatureInput,
        scope: Option<&str>,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let candidate = match secret.signature_argument() {
            SignatureArgument::Text(secret) => secret,
            SignatureArgument::Bytes(secret) => str::from_utf8(secret)
                .map_err(|e| BpError::auth_invalid_field("secret", e.to_string()))?,
        };
        let token = match self.token(token_id).await {
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
        let token = match self.touch(&token, now).await {
            Ok(token) => token,
            Err(error) => {
                self.record_authentication_failure(token_id, Some(&token), now, &error);
                return Err(error);
            }
        };
        let event = TokenAuditEvent::new(TokenAuditAction::Authenticated, now)
            .with_token(token.id(), Some(token.owner()));
        self.record(event);
        Ok(token)
    }

    pub(crate) fn ensure_active(token: &ApiToken, now: DateTime<Utc>) -> BpResult<()> {
        if token.is_revoked() {
            return Err(BpError::auth_invalid_field(
                "token_id",
                format!("{:?} has been revoked", token.id()),
            ));
        }
        if token.is_expired(now) {
            return Err(BpError::auth_invalid_field(
                "token_id",
                format!("{:?} has expired", token.id()),
            ));
        }
        Ok(())
    }

    pub(crate) fn ensure_scope(token: &ApiToken, required: &str) -> BpResult<()> {
        if token.has_scope(required) {
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
        self.record(event);
    }
}
