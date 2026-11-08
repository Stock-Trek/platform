use crate::{
    ApiToken, AuthService, RevocationReason, RevokedBy, TokenAuditAction, TokenAuditEvent,
    auth_service::MAX_UPDATE_ATTEMPTS,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
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
                return Ok(token);
            }
            let original = token.clone();
            token.metadata.revoke(now, reason, revoked_by);
            match self.store.update(&original, &token).await {
                Ok(()) => {
                    let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
                        .with_token(token.id(), Some(token.owner()));
                    self.audit.record(event);
                    return Ok(token);
                }
                Err(error) if Self::is_conflict(&error) => continue,
                Err(error) => {
                    self.record_revocation_failure(&token, now, &error);
                    return Err(error);
                }
            }
        }
        let error = BpError::token_conflict(id);
        let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
            .with_token(id, None)
            .failure(error.to_string());
        self.audit.record(event);
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

    pub async fn purge_expired_tokens(&self, now: DateTime<Utc>) -> BpResult<usize> {
        let mut purged = 0;
        let mut ids = self.store.ids().await?;
        ids.sort();
        ids.dedup();
        for id in ids {
            if let Some(token) = self.stored_token(&id).await?
                && token.metadata().is_expired(now)
            {
                self.store.delete(&id).await?;
                let event = TokenAuditEvent::new(TokenAuditAction::Deleted, now)
                    .with_token(token.id(), Some(token.owner()));
                self.audit.record(event);
                purged += 1;
            }
        }
        Ok(purged)
    }

    fn record_revocation_failure(&self, token: &ApiToken, now: DateTime<Utc>, error: &BpError) {
        let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
            .with_token(token.id(), Some(token.owner()))
            .failure(error.to_string());
        self.audit.record(event);
    }
}
