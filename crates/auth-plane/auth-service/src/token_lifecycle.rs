use crate::{
    ApiToken, AuthService, RevocationReason, RevokedBy, TokenAuditAction, TokenAuditEvent,
    auth_service::MAX_UPDATE_ATTEMPTS,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub async fn delete_token(&self, id: &str, now: DateTime<Utc>) -> BpResult<()> {
        let token = self.stored_token(id).await?;
        self.store.delete(id).await?;
        let Some(token) = token else {
            return Ok(());
        };
        let event = TokenAuditEvent::new(TokenAuditAction::Deleted, now)
            .with_token(token.id(), Some(token.owner()));
        if let Err(error) = self.record_audit(event).await {
            return Err(self.rollback_deleted_token(&token, error).await);
        }
        Ok(())
    }

    pub async fn revoke_token(
        &self,
        id: &str,
        reason: RevocationReason,
        revoked_by: RevokedBy,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let mut token = self.token(id).await?;
            if token.metadata.is_revoked() {
                return Err(BpError::auth_invalid_field(
                    "token_id",
                    "is already revoked",
                ));
            }
            let original = token.clone();
            token.metadata.revoke(now, reason, revoked_by);
            match self.store.update(&original, &token).await {
                Ok(()) => {
                    let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
                        .with_token(token.id(), Some(token.owner()));
                    if let Err(error) = self.record_audit(event).await {
                        return Err(self.rollback_revocation(&token, &original, error).await);
                    }
                    return Ok(token);
                }
                Err(error) if Self::is_conflict(&error) => continue,
                Err(error) => {
                    self.record_revocation_failure(&token, now, &error).await;
                    return Err(error);
                }
            }
        }
        let error = BpError::token_conflict(id);
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
    ) -> BpResult<Vec<ApiToken>> {
        let mut revoked = Vec::new();
        for token in self.tokens_for_owner(owner).await? {
            if token.metadata().is_revoked() {
                continue;
            }
            revoked.push(
                self.revoke_token(token.id(), reason, revoked_by, now)
                    .await?,
            );
        }
        Ok(revoked)
    }

    async fn rollback_deleted_token(&self, token: &ApiToken, failure: BpError) -> BpError {
        match self.store.put(token).await {
            Ok(()) => failure,
            Err(rollback) => {
                Self::rollback_failure("restore token", token.id(), &failure, rollback)
            }
        }
    }

    async fn rollback_revocation(
        &self,
        revoked: &ApiToken,
        original: &ApiToken,
        failure: BpError,
    ) -> BpError {
        match self.store.update(revoked, original).await {
            Ok(()) => failure,
            Err(rollback) => {
                Self::rollback_failure("restore token", revoked.id(), &failure, rollback)
            }
        }
    }

    async fn record_revocation_failure(
        &self,
        token: &ApiToken,
        now: DateTime<Utc>,
        error: &BpError,
    ) {
        let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
            .with_token(token.id(), Some(token.owner()))
            .failure(error.to_string());
        self.record_audit_failure(event).await;
    }
}
