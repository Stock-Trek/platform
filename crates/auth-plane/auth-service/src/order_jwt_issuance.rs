use crate::{ApiToken, AuthService, OrderJwtClaims, TokenScope, order_jwt_claims::IssuedJwt};
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};

impl AuthService {
    pub async fn issue_jwt_for_owner(
        &self,
        owner: &str,
        now: DateTime<Utc>,
    ) -> PlatformResult<IssuedJwt> {
        let token = self
            .token_for_owner_with_scope(owner, TokenScope::OrdersWrite, now)
            .await?;
        self.issue_jwt_with_token(&token, now)
    }

    fn issue_jwt_with_token(
        &self,
        token: &ApiToken,
        now: DateTime<Utc>,
    ) -> PlatformResult<IssuedJwt> {
        Self::ensure_active(token, now)?;
        Self::ensure_scope(token, TokenScope::OrdersWrite)?;
        let issued_at = DateTime::from_timestamp(now.timestamp(), 0).unwrap_or(now);
        let requested_expiry = issued_at
            .checked_add_signed(self.signature_validity)
            .ok_or_else(|| {
                PlatformError::auth_invalid_jwt("expires_at is outside the supported range")
            })?;
        let expires_at = requested_expiry.min(token.metadata().expires_at());
        let claims = OrderJwtClaims::new(token.id(), token.owner(), issued_at, expires_at)?;
        self.jwt_signer.sign(&claims)
    }
}
