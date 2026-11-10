use crate::{
    AuthService, OrderJwtClaims, TokenScope, order_jwt_claims::CLOCK_SKEW_SECONDS,
    order_jwt_claims::JwtCodec,
};
use chrono::{DateTime, Utc};
use jsonwebtoken::Algorithm;
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub async fn verify_jwt(&self, jwt: &str, now: DateTime<Utc>) -> BpResult<OrderJwtClaims> {
        let untrusted = JwtCodec::decode_untrusted(jwt)?;
        if untrusted.header.alg != Algorithm::HS256 {
            return Err(BpError::auth_invalid_jwt(
                "must use the HS256 signing algorithm",
            ));
        }
        let token_id = untrusted.claims.token_id().to_string();
        let token = self
            .stored_token(&token_id)
            .await?
            .ok_or_else(|| BpError::auth_invalid_jwt("references an unknown token"))?;
        let verified = JwtCodec::decode(token.secret(), jwt)?.claims;
        if verified.token_id() != token.id() || verified.owner() != token.owner() {
            return Err(BpError::auth_invalid_jwt(
                "claims do not match the referenced token",
            ));
        }
        Self::ensure_active(&token, now)?;
        Self::ensure_scope(&token, TokenScope::OrdersWrite)?;
        verified.validate_at(now)?;
        if verified.expires_at().timestamp() > token.metadata().expires_at().timestamp() {
            return Err(BpError::auth_invalid_jwt(
                "expires after the referenced token",
            ));
        }
        if verified.issued_at().timestamp() < token.created_at().timestamp() - CLOCK_SKEW_SECONDS {
            return Err(BpError::auth_invalid_jwt(
                "was issued before the referenced token",
            ));
        }
        Ok(verified)
    }
}
