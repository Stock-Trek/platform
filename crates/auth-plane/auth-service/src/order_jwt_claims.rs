use crate::ApiToken;
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use serde::{Deserialize, Serialize};

pub(crate) const CLOCK_SKEW_SECONDS: i64 = 60;

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
    ) -> BpResult<Self> {
        let token_id = token_id.into();
        let owner = owner.into();
        let owner = ApiToken::validated_owner(&owner)?.to_string();
        ApiToken::validate_id(&token_id)?;
        let issued_at = issued_at.timestamp();
        let expires_at = expires_at.timestamp();
        if expires_at <= issued_at {
            return Err(BpError::auth_invalid_jwt(
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

    pub(crate) fn normalize_owner(&mut self) -> BpResult<()> {
        self.owner = ApiToken::validated_owner(&self.owner)?.to_string();
        Ok(())
    }

    pub(crate) fn validate_at(&self, now: DateTime<Utc>) -> BpResult<()> {
        if self.expires_at <= self.issued_at {
            return Err(BpError::auth_invalid_jwt(
                "expires_at must be after issued_at",
            ));
        }
        if self.is_expired(now) {
            return Err(BpError::auth_invalid_jwt("has expired"));
        }
        if self.issued_at > now.timestamp().saturating_add(CLOCK_SKEW_SECONDS) {
            return Err(BpError::auth_invalid_jwt("was issued in the future"));
        }
        Ok(())
    }
}
