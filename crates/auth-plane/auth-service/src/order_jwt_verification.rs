use crate::{AuthService, OrderJwtClaims, TokenScope, order_jwt_claims::CLOCK_SKEW_SECONDS};
use chrono::{DateTime, Utc};
use platform_errors::{BuildError, BuildResult};

impl AuthService {
    pub async fn verify_jwt(&self, jwt: &str, now: DateTime<Utc>) -> BuildResult<OrderJwtClaims> {
        let verified = self.jwt_verifier().verify(jwt, now)?;
        let token = self
            .stored_token(verified.token_id())
            .await?
            .ok_or_else(|| BuildError::auth_invalid_jwt("references an unknown token"))?;
        if verified.token_id() != token.id() || verified.owner() != token.owner() {
            return Err(BuildError::auth_invalid_jwt(
                "claims do not match the referenced token",
            ));
        }
        Self::ensure_active(&token, now)?;
        Self::ensure_scope(&token, TokenScope::OrdersWrite)?;
        verified.validate_at(now)?;
        if verified.expires_at().timestamp() > token.metadata().expires_at().timestamp() {
            return Err(BuildError::auth_invalid_jwt(
                "expires after the referenced token",
            ));
        }
        if verified.issued_at().timestamp() < token.created_at().timestamp() - CLOCK_SKEW_SECONDS {
            return Err(BuildError::auth_invalid_jwt(
                "was issued before the referenced token",
            ));
        }
        Ok(verified)
    }
}
