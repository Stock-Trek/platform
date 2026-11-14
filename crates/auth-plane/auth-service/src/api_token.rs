use crate::TokenMetadata;
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};
use secrecy::{ExposeSecret, SecretString};
use subtle::ConstantTimeEq;

pub const MAX_TOKEN_ID_LENGTH: usize = 256;
pub const MAX_OWNER_LENGTH: usize = 256;
pub const MAX_SECRET_LENGTH: usize = 256;

#[derive(Clone)]
pub struct ApiToken {
    pub id: String,
    pub owner: String,
    pub secret: SecretString,
    pub created_at: DateTime<Utc>,
    pub metadata: TokenMetadata,
}

impl ApiToken {
    pub fn from_parts(
        id: impl Into<String>,
        owner: impl Into<String>,
        secret: SecretString,
        created_at: DateTime<Utc>,
        metadata: TokenMetadata,
    ) -> PlatformResult<Self> {
        let id = id.into();
        let owner = owner.into();
        let owner = Self::validated_owner(&owner)?.to_string();
        let token = Self {
            id,
            owner,
            secret,
            created_at,
            metadata,
        };
        token.validate()?;
        Ok(token)
    }

    pub fn validate(&self) -> PlatformResult<()> {
        Self::validate_id(&self.id)?;
        Self::validated_owner(&self.owner)?;
        Self::validate_secret(self.secret.expose_secret())?;
        self.metadata.validate()?;
        self.validate_temporal()?;
        if self.metadata.rotated_from() == Some(self.id.as_str()) {
            return Err(PlatformError::auth_invalid_field(
                "rotated_from",
                "must refer to a different token",
            ));
        }
        if self.metadata.replaced_by() == Some(self.id.as_str()) {
            return Err(PlatformError::auth_invalid_field(
                "replaced_by",
                "must refer to a different token",
            ));
        }
        Ok(())
    }

    fn validate_secret(secret: &str) -> PlatformResult<()> {
        if secret.trim().is_empty() {
            return Err(PlatformError::auth_invalid_field(
                "secret",
                "must not be empty",
            ));
        }
        if secret.chars().count() > MAX_SECRET_LENGTH {
            return Err(PlatformError::auth_invalid_field(
                "secret",
                format!("must be at most {MAX_SECRET_LENGTH} characters"),
            ));
        }
        if secret.chars().any(char::is_control) {
            return Err(PlatformError::auth_invalid_field(
                "secret",
                "must not contain control characters",
            ));
        }
        Ok(())
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn secret(&self) -> &SecretString {
        &self.secret
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn metadata(&self) -> &TokenMetadata {
        &self.metadata
    }

    pub fn matches_secret(&self, candidate: &str) -> bool {
        self.secret
            .expose_secret()
            .as_bytes()
            .ct_eq(candidate.as_bytes())
            .into()
    }

    pub fn validated_owner(owner: &str) -> PlatformResult<&str> {
        let owner = owner.trim();
        if owner.is_empty() {
            return Err(PlatformError::auth_invalid_field(
                "owner",
                "must not be empty",
            ));
        }
        if owner.chars().count() > MAX_OWNER_LENGTH {
            return Err(PlatformError::auth_invalid_field(
                "owner",
                format!("must be at most {MAX_OWNER_LENGTH} characters"),
            ));
        }
        if owner.chars().any(char::is_control) {
            return Err(PlatformError::auth_invalid_field(
                "owner",
                "must not contain control characters",
            ));
        }
        Ok(owner)
    }

    pub fn validate_id(id: &str) -> PlatformResult<()> {
        if id.is_empty() {
            return Err(PlatformError::auth_invalid_field(
                "token_id",
                "must not be empty",
            ));
        }
        if id.len() > MAX_TOKEN_ID_LENGTH {
            return Err(PlatformError::auth_invalid_field(
                "token_id",
                format!("must be at most {MAX_TOKEN_ID_LENGTH} characters"),
            ));
        }
        let valid = id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if valid {
            Ok(())
        } else {
            Err(PlatformError::auth_invalid_field(
                "token_id",
                "must only contain ASCII letters, digits, '-' and '_'",
            ))
        }
    }
}

impl std::fmt::Debug for ApiToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiToken")
            .field("id", &self.id)
            .field("owner", &self.owner)
            .field("secret", &"<redacted>")
            .field("created_at", &self.created_at)
            .field("metadata", &self.metadata)
            .finish()
    }
}
