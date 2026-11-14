use crate::{
    ApiToken, AuthService, TokenAuditAction, TokenAuditEvent, auth_service::MAX_UPDATE_ATTEMPTS,
    token_store::TokenMutation,
};
use chrono::{DateTime, Utc};
use platform_errors::{AuthError, PlatformError, PlatformResult};
use strum::{Display, EnumString};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum RevocationReason {
    Compromised,
    Expired,
    NoLongerNeeded,
    OwnerRequested,
    Rotated,
    Administrative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum RevokedBy {
    Owner,
    Admin,
    SecurityTeam,
    System,
}

impl ApiToken {
    pub fn validate_temporal(&self) -> PlatformResult<()> {
        if self.created_at >= self.metadata.expires_at {
            return Err(PlatformError::auth_invalid_field(
                "expires_at",
                "must be after created_at",
            ));
        }
        Self::ensure_not_before_creation("revoked_at", self.metadata.revoked_at, self.created_at)?;
        Self::ensure_not_before_creation(
            "last_used_at",
            self.metadata.last_used_at,
            self.created_at,
        )?;
        Self::ensure_not_before_creation(
            "rotation_overlap_until",
            self.metadata.rotation_overlap_until,
            self.created_at,
        )
    }

    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.created_at <= now && self.metadata.is_active(now)
    }

    fn ensure_not_before_creation(
        field: &'static str,
        timestamp: Option<DateTime<Utc>>,
        created_at: DateTime<Utc>,
    ) -> PlatformResult<()> {
        if timestamp.is_some_and(|timestamp| timestamp < created_at) {
            return Err(PlatformError::auth_invalid_field(
                field,
                "must not be before created_at",
            ));
        }
        Ok(())
    }
}

impl AuthService {
    pub async fn delete_token(&self, id: &str, now: DateTime<Utc>) -> PlatformResult<()> {
        let Some(token) = self.stored_token(id).await? else {
            return Ok(());
        };
        let event = TokenAuditEvent::new(TokenAuditAction::Deleted, now)
            .with_token(token.id(), Some(token.owner()));
        let deleted = self
            .commit_mutation(TokenMutation::Delete { expected: token }, event)
            .await?;
        if deleted {
            Ok(())
        } else {
            Err(PlatformError::token_conflict(id))
        }
    }

    pub async fn revoke_token(
        &self,
        id: &str,
        reason: RevocationReason,
        revoked_by: RevokedBy,
        now: DateTime<Utc>,
    ) -> PlatformResult<ApiToken> {
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let mut token = self.token(id).await?;
            if token.metadata.is_revoked() {
                return Err(PlatformError::auth_revoked(token.id()));
            }
            let original = token.clone();
            token.metadata.revoke(now, reason, revoked_by);
            let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
                .with_token(token.id(), Some(token.owner()));
            let mutation = TokenMutation::Update {
                expected: original,
                token: token.clone(),
            };
            match self.commit_mutation(mutation, event).await {
                Ok(_) => return Ok(token),
                Err(error) if Self::is_conflict(&error) => continue,
                Err(error) => {
                    self.record_revocation_failure(&token, now, &error).await;
                    return Err(error);
                }
            }
        }
        let error = PlatformError::token_conflict(id);
        let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
            .with_token(id, None)
            .failure(error.to_string());
        self.record_audit_failure(event).await;
        Err(error)
    }

    pub async fn revoke_tokens_for_owner(
        &self,
        owner: &str,
        reason: RevocationReason,
        revoked_by: RevokedBy,
        now: DateTime<Utc>,
    ) -> PlatformResult<Vec<ApiToken>> {
        let mut revoked = Vec::new();
        let mut first_error = None;
        for token in self.tokens_for_owner(owner).await? {
            if token.metadata().is_revoked() {
                continue;
            }
            match self.revoke_token(token.id(), reason, revoked_by, now).await {
                Ok(token) => revoked.push(token),
                Err(error) if Self::is_already_revoked(&error) => continue,
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(revoked),
        }
    }

    fn is_already_revoked(error: &PlatformError) -> bool {
        matches!(error, PlatformError::Auth(AuthError::Revoked { .. }))
    }

    async fn record_revocation_failure(
        &self,
        token: &ApiToken,
        now: DateTime<Utc>,
        error: &PlatformError,
    ) {
        let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
            .with_token(token.id(), Some(token.owner()))
            .failure(error.to_string());
        self.record_audit_failure(event).await;
    }
}
