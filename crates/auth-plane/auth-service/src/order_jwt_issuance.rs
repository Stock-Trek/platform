use crate::{ApiToken, AuthService, IssuedJwt, OrderJwtClaims, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub async fn issue_jwt_for_owner(
        &self,
        owner: &str,
        now: DateTime<Utc>,
    ) -> BpResult<IssuedJwt> {
        let token = self
            .token_for_owner_with_scope(owner, TokenScope::OrdersWrite, now)
            .await?;
        self.issue_jwt_with_token(&token, now)
    }

    pub async fn issue_jwt(
        &self,
        token_id: &str,
        secret: impl crate::SecretInput,
        now: DateTime<Utc>,
    ) -> BpResult<IssuedJwt> {
        let token = self
            .authenticate(token_id, secret, Some(TokenScope::OrdersWrite), now)
            .await?;
        self.issue_jwt_with_token(&token, now)
    }

    fn issue_jwt_with_token(&self, token: &ApiToken, now: DateTime<Utc>) -> BpResult<IssuedJwt> {
        Self::ensure_active(token, now)?;
        Self::ensure_scope(token, TokenScope::OrdersWrite)?;
        let issued_at = DateTime::from_timestamp(now.timestamp(), 0).unwrap_or(now);
        let requested_expiry = issued_at
            .checked_add_signed(self.signature_validity)
            .ok_or_else(|| {
                BpError::auth_invalid_jwt("expires_at is outside the supported range")
            })?;
        let expires_at = requested_expiry.min(token.metadata().expires_at());
        let claims = OrderJwtClaims::new(token.id(), token.owner(), issued_at, expires_at)?;
        self.jwt_signer.sign(&claims)
    }
}
