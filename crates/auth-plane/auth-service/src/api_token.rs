use crate::{RevocationReason, TokenMetadata};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use secrecy::{ExposeSecret, SecretString};
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub struct ApiToken {
    pub(crate) id: String,
    pub(crate) owner: String,
    pub(crate) secret: SecretString,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) metadata: TokenMetadata,
}

impl ApiToken {
    pub fn new(
        id: impl Into<String>,
        owner: impl Into<String>,
        secret: SecretString,
        created_at: DateTime<Utc>,
    ) -> BpResult<Self> {
        let metadata = TokenMetadata::new(None, crate::TokenScope::default_scopes())?;
        Self::from_parts(id, owner, secret, created_at, metadata)
    }

    pub fn from_parts(
        id: impl Into<String>,
        owner: impl Into<String>,
        secret: SecretString,
        created_at: DateTime<Utc>,
        metadata: TokenMetadata,
    ) -> BpResult<Self> {
        let id = id.into();
        Self::validate_id(&id)?;
        let owner = owner.into();
        if owner.trim().is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        if secret.expose_secret().is_empty() {
            return Err(BpError::auth_invalid_field("secret", "must not be empty"));
        }
        Ok(Self {
            id,
            owner,
            secret,
            created_at,
            metadata,
        })
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

    pub fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.metadata.expires_at()
    }

    pub fn scopes(&self) -> &[String] {
        self.metadata.scopes()
    }

    pub fn revoked_at(&self) -> Option<DateTime<Utc>> {
        self.metadata.revoked_at()
    }

    pub fn revocation_reason(&self) -> Option<&str> {
        self.metadata.revocation_reason()
    }

    pub fn revoked_by(&self) -> Option<&str> {
        self.metadata.revoked_by()
    }

    pub fn last_used_at(&self) -> Option<DateTime<Utc>> {
        self.metadata.last_used_at()
    }

    pub fn rotated_from(&self) -> Option<&str> {
        self.metadata.rotated_from()
    }

    pub fn replaced_by(&self) -> Option<&str> {
        self.metadata.replaced_by()
    }

    pub fn rotation_overlap_until(&self) -> Option<DateTime<Utc>> {
        self.metadata.rotation_overlap_until()
    }

    pub fn effective_expires_at(&self) -> Option<DateTime<Utc>> {
        self.metadata.effective_expires_at()
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.metadata.is_expired(now)
    }

    pub fn is_revoked(&self) -> bool {
        self.metadata.is_revoked()
    }

    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.metadata.is_active(now)
    }

    pub fn has_scope(&self, required: &str) -> bool {
        self.metadata.has_scope(required)
    }

    pub fn has_permission(&self, required: &str) -> bool {
        self.has_scope(required)
    }

    pub fn matches_secret(&self, candidate: &str) -> bool {
        self.secret
            .expose_secret()
            .as_bytes()
            .ct_eq(candidate.as_bytes())
            .into()
    }

    pub fn validate_id(id: &str) -> BpResult<()> {
        let valid = !id.is_empty()
            && id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if valid {
            Ok(())
        } else {
            Err(BpError::auth_invalid_field(
                "token_id",
                format!("{id:?} must only contain ASCII letters, digits, '-' and '_'"),
            ))
        }
    }

    pub(crate) fn record_use(&mut self, now: DateTime<Utc>) {
        self.metadata.record_use(now);
    }

    pub(crate) fn revoke(
        &mut self,
        now: DateTime<Utc>,
        reason: impl RevocationReason,
        revoked_by: Option<String>,
    ) {
        self.metadata.revoke(now, reason, revoked_by);
    }

    pub(crate) fn begin_rotation(
        &mut self,
        replacement_id: impl Into<String>,
        overlap_until: DateTime<Utc>,
    ) {
        self.metadata.begin_rotation(replacement_id, overlap_until);
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
