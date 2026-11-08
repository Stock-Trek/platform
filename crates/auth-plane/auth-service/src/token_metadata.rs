use crate::{ApiToken, RevocationReason, RevokedBy, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use std::{borrow::Borrow, collections::HashSet};

#[derive(Clone, Debug)]
pub struct TokenMetadata {
    pub(crate) expires_at: DateTime<Utc>,
    pub(crate) scopes: Vec<TokenScope>,
    pub(crate) revoked_at: Option<DateTime<Utc>>,
    pub(crate) revocation_reason: Option<RevocationReason>,
    pub(crate) revoked_by: Option<RevokedBy>,
    pub(crate) last_used_at: Option<DateTime<Utc>>,
    pub(crate) rotated_from: Option<String>,
    pub(crate) replaced_by: Option<String>,
    pub(crate) rotation_overlap_until: Option<DateTime<Utc>>,
}

impl TokenMetadata {
    pub fn new(
        expires_at: DateTime<Utc>,
        scopes: impl IntoIterator<Item = impl Borrow<TokenScope>>,
    ) -> BpResult<Self> {
        let scopes: Vec<TokenScope> = scopes.into_iter().map(|scope| *scope.borrow()).collect();
        Self::validate_scopes(&scopes)?;
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

    pub fn scopes(&self) -> &[TokenScope] {
        &self.scopes
    }

    pub fn revoked_at(&self) -> Option<DateTime<Utc>> {
        self.revoked_at
    }

    pub fn revocation_reason(&self) -> Option<RevocationReason> {
        self.revocation_reason
    }

    pub fn revoked_by(&self) -> Option<RevokedBy> {
        self.revoked_by
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
        self.expires_at <= now
    }

    pub(crate) fn is_rotation_overlap_ended(&self, now: DateTime<Utc>) -> bool {
        self.rotation_overlap_until
            .is_some_and(|overlap_until| overlap_until <= now)
    }

    pub fn is_revoked(&self) -> bool {
        self.revoked_at.is_some()
    }

    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        !self.is_revoked() && !self.is_expired(now) && !self.is_rotation_overlap_ended(now)
    }

    pub fn has_scope(&self, required: TokenScope) -> bool {
        self.scopes.iter().any(|granted| granted.matches(required))
    }

    pub fn record_use(&mut self, now: DateTime<Utc>) {
        if self.last_used_at.is_none_or(|last_used| now > last_used) {
            self.last_used_at = Some(now);
        }
    }

    fn validate_scopes(scopes: &[TokenScope]) -> BpResult<()> {
        if scopes.is_empty() {
            return Err(BpError::auth_invalid_field("scopes", "must not be empty"));
        }
        let mut seen = HashSet::new();
        if scopes.iter().any(|scope| !seen.insert(*scope)) {
            return Err(BpError::auth_invalid_field(
                "scopes",
                "must not contain duplicates",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> BpResult<()> {
        Self::validate_scopes(&self.scopes)?;
        if let Some(token_id) = &self.rotated_from {
            ApiToken::validate_id(token_id).map_err(|_| {
                BpError::auth_invalid_field("rotated_from", "must be a valid token id")
            })?;
        }
        if let Some(token_id) = &self.replaced_by {
            ApiToken::validate_id(token_id).map_err(|_| {
                BpError::auth_invalid_field("replaced_by", "must be a valid token id")
            })?;
        }
        if self.rotated_from.is_some() && self.rotated_from == self.replaced_by {
            return Err(BpError::auth_invalid_field(
                "rotated_from",
                "must refer to a different token than replaced_by",
            ));
        }
        match (self.revoked_at, self.revocation_reason, self.revoked_by) {
            (None, None, None) | (Some(_), Some(_), Some(_)) => {}
            (None, _, _) => {
                return Err(BpError::auth_invalid_field(
                    "revoked_at",
                    "is required when revocation metadata is present",
                ));
            }
            (Some(_), None, _) => {
                return Err(BpError::auth_invalid_field(
                    "revocation_reason",
                    "is required for a revoked token",
                ));
            }
            (Some(_), Some(_), None) => {
                return Err(BpError::auth_invalid_field(
                    "revoked_by",
                    "is required for a revoked token",
                ));
            }
        }
        match (&self.replaced_by, self.rotation_overlap_until) {
            (None, None) | (Some(_), Some(_)) => Ok(()),
            (Some(_), None) => Err(BpError::auth_invalid_field(
                "rotation_overlap_until",
                "is required when replaced_by is set",
            )),
            (None, Some(_)) => Err(BpError::auth_invalid_field(
                "replaced_by",
                "is required when rotation_overlap_until is set",
            )),
        }
    }

    pub fn revoke(&mut self, now: DateTime<Utc>, reason: RevocationReason, revoked_by: RevokedBy) {
        self.revoked_at.get_or_insert(now);
        self.revocation_reason.get_or_insert(reason);
        self.revoked_by.get_or_insert(revoked_by);
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
