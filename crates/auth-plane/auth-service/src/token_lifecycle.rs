use crate::{
    ApiToken, AuthService, RevocationReason, RevokedBy, TokenAuditAction, TokenAuditEvent,
    TokenMetadata,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use rand::{Rng, distributions::Alphanumeric};

impl AuthService {
    pub async fn rotate_token(&self, id: &str, now: DateTime<Utc>) -> BpResult<ApiToken> {
        let replacement_id = self.rotated_token_id(id, now);
        self.rotate_token_to(id, replacement_id, now).await
    }

    pub async fn rotate_token_to(
        &self,
        id: &str,
        replacement_id: impl Into<String>,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let original = self.token(id).await?;
        if let Some(replacement) = original.metadata().replaced_by() {
            return Err(BpError::auth_invalid_field(
                "token_id",
                format!("has already been rotated to {replacement:?}"),
            ));
        }
        Self::ensure_active(&original, now)?;
        let expires_at = now.checked_add_signed(self.token_lifetime).ok_or_else(|| {
            BpError::auth_invalid_field("token_lifetime", "is outside the supported range")
        })?;
        let overlap_until = now
            .checked_add_signed(self.rotation_overlap)
            .ok_or_else(|| {
                BpError::auth_invalid_field("rotation_overlap", "is outside the supported range")
            })?;
        let metadata = TokenMetadata::new(expires_at, original.metadata().scopes().iter())?;
        let mut replacement = ApiToken::generate(replacement_id, original.owner(), now, metadata)?;
        replacement.metadata.rotated_from = Some(original.id().to_string());
        if let Err(error) = self.store.put(&replacement).await {
            let event = TokenAuditEvent::new(TokenAuditAction::Rotated, now)
                .with_token(original.id(), Some(original.owner()))
                .failure(error.to_string());
            self.audit.record(event);
            return Err(error);
        }
        let mut retired = original.clone();
        retired
            .metadata
            .begin_rotation(replacement.id(), overlap_until);
        if let Err(error) = self.store.update(&retired).await {
            let _ = self.store.delete(replacement.id()).await;
            let event = TokenAuditEvent::new(TokenAuditAction::Rotated, now)
                .with_token(original.id(), Some(original.owner()))
                .failure(error.to_string());
            self.audit.record(event);
            return Err(error);
        }
        let event = TokenAuditEvent::new(TokenAuditAction::Rotated, now)
            .with_token(original.id(), Some(original.owner()));
        self.audit.record(event);
        let issued = TokenAuditEvent::new(TokenAuditAction::Issued, now)
            .with_token(replacement.id(), Some(replacement.owner()));
        self.audit.record(issued);
        Ok(replacement)
    }

    pub async fn revoke_token(
        &self,
        id: &str,
        reason: RevocationReason,
        revoked_by: RevokedBy,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let mut token = self.token(id).await?;
        if token.metadata.is_revoked() {
            return Ok(token);
        }
        token.metadata.revoke(now, reason, revoked_by);
        if let Err(error) = self.store.update(&token).await {
            let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
                .with_token(token.id(), Some(token.owner()))
                .failure(error.to_string());
            self.audit.record(event);
            return Err(error);
        }
        let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
            .with_token(token.id(), Some(token.owner()));
        self.audit.record(event);
        Ok(token)
    }

    pub async fn revoke_tokens_for_owner(
        &self,
        owner: &str,
        reason: RevocationReason,
        revoked_by: RevokedBy,
        now: DateTime<Utc>,
    ) -> BpResult<Vec<ApiToken>> {
        let mut revoked = Vec::new();
        for token in self.active_tokens_for_owner(owner, now).await? {
            revoked.push(
                self.revoke_token(token.id(), reason, revoked_by, now)
                    .await?,
            );
        }
        Ok(revoked)
    }

    pub async fn purge_expired_tokens(&self, now: DateTime<Utc>) -> BpResult<usize> {
        let mut purged = 0;
        for id in self.store.ids().await? {
            if let Some(token) = self.store.get(&id).await?
                && token.metadata().is_expired(now)
            {
                self.store.delete(&id).await?;
                purged += 1;
            }
        }
        Ok(purged)
    }

    fn rotated_token_id(&self, id: &str, now: DateTime<Utc>) -> String {
        let suffix: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        format!("{id}-{}-{suffix}", now.timestamp_millis())
    }
}
