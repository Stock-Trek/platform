use crate::{
    ApiToken, AuthService, RevocationReason, RevokedBy, TokenAuditAction, TokenAuditEvent,
    TokenMutation, auth_service::MAX_UPDATE_ATTEMPTS,
};
use chrono::{DateTime, Utc};
use platform_errors::{AuthError, BpError, BpResult};

impl AuthService {
    pub(crate) async fn delete_token(&self, id: &str, now: DateTime<Utc>) -> BpResult<()> {
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
            Err(BpError::token_conflict(id))
        }
    }

    pub(crate) async fn revoke_token(
        &self,
        id: &str,
        reason: RevocationReason,
        revoked_by: RevokedBy,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let mut token = self.token(id).await?;
            if token.metadata.is_revoked() {
                return Err(BpError::auth_revoked(token.id()));
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
        let error = BpError::token_conflict(id);
        let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
            .with_token(id, None)
            .failure(error.to_string());
        self.record_audit_failure(event).await;
        Err(error)
    }

    /// Revokes every token owned by `owner` that is not already revoked.
    ///
    /// This operation is deliberately non-atomic: each token is revoked with
    /// its own compare-and-swap update. If one token cannot be revoked, the
    /// remaining tokens are still processed and the first failure is returned
    /// afterwards. Tokens revoked before the failure stay revoked.
    ///
    /// The operation is retryable. Already-revoked tokens, including tokens
    /// revoked concurrently by another caller, are skipped on subsequent
    /// attempts, so a retry only needs to revoke the tokens that are still
    /// active.
    pub(crate) async fn revoke_tokens_for_owner(
        &self,
        owner: &str,
        reason: RevocationReason,
        revoked_by: RevokedBy,
        now: DateTime<Utc>,
    ) -> BpResult<Vec<ApiToken>> {
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

    fn is_already_revoked(error: &BpError) -> bool {
        matches!(error, BpError::Auth(AuthError::Revoked { .. }))
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
