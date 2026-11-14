use crate::{ApiToken, AuthService, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};
use serde::{Deserialize, Serialize};

pub const CLOCK_SKEW_SECONDS: i64 = 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedJwt {
    token: String,
    expires_at: DateTime<Utc>,
}

impl IssuedJwt {
    pub fn new(token: impl Into<String>, expires_at: DateTime<Utc>) -> Self {
        Self {
            token: token.into(),
            expires_at,
        }
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderJwtClaims {
    #[serde(rename = "sub")]
    token_id: String,
    owner: String,
    #[serde(rename = "iat")]
    issued_at: i64,
    #[serde(rename = "exp")]
    expires_at: i64,
}

impl OrderJwtClaims {
    pub fn new(
        token_id: impl Into<String>,
        owner: impl Into<String>,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> PlatformResult<Self> {
        let token_id = token_id.into();
        let owner = owner.into();
        let owner = ApiToken::validated_owner(&owner)?.to_string();
        ApiToken::validate_id(&token_id)?;
        let issued_at = issued_at.timestamp();
        let expires_at = expires_at.timestamp();
        if expires_at <= issued_at {
            return Err(PlatformError::auth_invalid_jwt(
                "expires_at must be after issued_at",
            ));
        }
        Ok(Self {
            token_id,
            owner,
            issued_at,
            expires_at,
        })
    }

    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn issued_at(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.issued_at, 0).unwrap_or(DateTime::<Utc>::MIN_UTC)
    }

    pub fn expires_at(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.expires_at, 0).unwrap_or(DateTime::<Utc>::MIN_UTC)
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at <= now.timestamp()
    }

    pub fn normalize_owner(&mut self) -> PlatformResult<()> {
        self.owner = ApiToken::validated_owner(&self.owner)?.to_string();
        Ok(())
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> PlatformResult<()> {
        if self.expires_at <= self.issued_at {
            return Err(PlatformError::auth_invalid_jwt(
                "expires_at must be after issued_at",
            ));
        }
        if self.is_expired(now) {
            return Err(PlatformError::auth_invalid_jwt("has expired"));
        }
        if self.issued_at > now.timestamp().saturating_add(CLOCK_SKEW_SECONDS) {
            return Err(PlatformError::auth_invalid_jwt("was issued in the future"));
        }
        Ok(())
    }
}
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
impl AuthService {
    pub async fn verify_jwt(
        &self,
        jwt: &str,
        now: DateTime<Utc>,
    ) -> PlatformResult<OrderJwtClaims> {
        let verified = self.jwt_verifier().verify(jwt, now)?;
        let token = self
            .stored_token(verified.token_id())
            .await?
            .ok_or_else(|| PlatformError::auth_invalid_jwt("references an unknown token"))?;
        if verified.token_id() != token.id() || verified.owner() != token.owner() {
            return Err(PlatformError::auth_invalid_jwt(
                "claims do not match the referenced token",
            ));
        }
        Self::ensure_active(&token, now)?;
        Self::ensure_scope(&token, TokenScope::OrdersWrite)?;
        verified.validate_at(now)?;
        if verified.expires_at().timestamp() > token.metadata().expires_at().timestamp() {
            return Err(PlatformError::auth_invalid_jwt(
                "expires after the referenced token",
            ));
        }
        if verified.issued_at().timestamp() < token.created_at().timestamp() - CLOCK_SKEW_SECONDS {
            return Err(PlatformError::auth_invalid_jwt(
                "was issued before the referenced token",
            ));
        }
        Ok(verified)
    }
}
