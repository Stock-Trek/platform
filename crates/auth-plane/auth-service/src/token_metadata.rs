use crate::{RevocationReason, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::BpResult;

#[derive(Clone, Debug)]
pub struct TokenMetadata {
    pub(crate) expires_at: DateTime<Utc>,
    pub(crate) scopes: Vec<String>,
    pub(crate) revoked_at: Option<DateTime<Utc>>,
    pub(crate) revocation_reason: Option<RevocationReason>,
    pub(crate) revoked_by: Option<String>,
    pub(crate) last_used_at: Option<DateTime<Utc>>,
    pub(crate) rotated_from: Option<String>,
    pub(crate) replaced_by: Option<String>,
    pub(crate) rotation_overlap_until: Option<DateTime<Utc>>,
}

impl TokenMetadata {
    pub fn new(
        expires_at: DateTime<Utc>,
        scopes: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> BpResult<Self> {
        let scopes: Vec<String> = scopes
            .into_iter()
            .map(|scope| scope.as_ref().to_string())
            .collect();
        TokenScope::validate_all(&scopes)?;
        Ok(Self {
            expires_at,
            scopes,
            revoked_at: None,
            revocation_reason: None,
            revoked_by: None,
            last_used_at: None,
            rotated_from: None,
            replaced_by: None,
            rotation_overlap_until: None,
        })
    }

    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }

    pub fn revoked_at(&self) -> Option<DateTime<Utc>> {
        self.revoked_at
    }

    pub fn revocation_reason(&self) -> Option<RevocationReason> {
        self.revocation_reason
    }

    pub fn revoked_by(&self) -> Option<&str> {
        self.revoked_by.as_deref()
    }

    pub fn last_used_at(&self) -> Option<DateTime<Utc>> {
        self.last_used_at
    }

    pub fn rotated_from(&self) -> Option<&str> {
        self.rotated_from.as_deref()
    }

    pub fn replaced_by(&self) -> Option<&str> {
        self.replaced_by.as_deref()
    }

    pub fn rotation_overlap_until(&self) -> Option<DateTime<Utc>> {
        self.rotation_overlap_until
    }

    pub fn effective_expires_at(&self) -> DateTime<Utc> {
        self.rotation_overlap_until
            .map_or(self.expires_at, |overlap| self.expires_at.min(overlap))
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.effective_expires_at() <= now
    }

    pub fn is_revoked(&self) -> bool {
        self.revoked_at.is_some()
    }

    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        !self.is_revoked() && !self.is_expired(now)
    }

    pub fn has_scope(&self, required: &str) -> bool {
        self.scopes
            .iter()
            .any(|granted| TokenScope::matches(granted, required))
    }

    pub fn record_use(&mut self, now: DateTime<Utc>) {
        if self.last_used_at.is_none_or(|last_used| now > last_used) {
            self.last_used_at = Some(now);
        }
    }

    pub fn revoke(&mut self, now: DateTime<Utc>, reason: RevocationReason, revoked_by: String) {
        self.revoked_at.get_or_insert(now);
        self.revocation_reason = Some(reason);
        self.revoked_by = Some(revoked_by);
    }

    pub fn begin_rotation(
        &mut self,
        replacement_id: impl Into<String>,
        overlap_until: DateTime<Utc>,
    ) {
        self.replaced_by = Some(replacement_id.into());
        self.rotation_overlap_until = Some(overlap_until);
    }
}
