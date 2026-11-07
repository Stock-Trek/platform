use crate::{
    ApiToken, AuthService, RevocationReason, TokenAuditAction, TokenAuditEvent, TokenExpiry,
    TokenMetadata,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use rand::{Rng, distributions::Alphanumeric};

impl AuthService {
    pub async fn rotate_token(&self, id: &str) -> BpResult<ApiToken> {
        self.rotate_token_at(id, Utc::now()).await
    }

    pub async fn rotate_token_at(&self, id: &str, now: DateTime<Utc>) -> BpResult<ApiToken> {
        let replacement_id = self.rotated_token_id(id, now);
        self.rotate_token_to_at(id, replacement_id, now).await
    }

    pub async fn rotate_token_to(
        &self,
        id: &str,
        replacement_id: impl Into<String>,
    ) -> BpResult<ApiToken> {
        self.rotate_token_to_at(id, replacement_id, Utc::now())
            .await
    }

    pub async fn rotate_token_with_id(
        &self,
        id: &str,
        replacement_id: impl Into<String>,
    ) -> BpResult<ApiToken> {
        self.rotate_token_to(id, replacement_id).await
    }

    pub async fn rotate_token_to_at(
        &self,
        id: &str,
        replacement_id: impl Into<String>,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let original = self.token(id).await?;
        if let Some(replacement) = original.replaced_by() {
            return Err(BpError::auth_invalid_field(
                "token_id",
                format!("has already been rotated to {replacement:?}"),
            ));
        }
        Self::ensure_active(&original, now)?;
        let expires_at = self.resolve_expiry(TokenExpiry::Default, now);
        let metadata = TokenMetadata::new(expires_at, original.scopes().iter())?;
        let mut replacement =
            ApiToken::generate_with_metadata(replacement_id, original.owner(), now, metadata)?;
        replacement.metadata.rotated_from = Some(original.id().to_string());
        if let Err(error) = self.store.put(&replacement).await {
            let event = TokenAuditEvent::new(TokenAuditAction::Rotated, now)
                .with_token(original.id(), Some(original.owner()))
                .failure(error.to_string());
            self.record(event);
            return Err(error);
        }
        let mut retired = original.clone();
        retired.begin_rotation(replacement.id(), now + self.rotation_overlap);
        if let Err(error) = self.store.update(&retired).await {
            let _ = self.store.delete(replacement.id()).await;
            let event = TokenAuditEvent::new(TokenAuditAction::Rotated, now)
                .with_token(original.id(), Some(original.owner()))
                .failure(error.to_string());
            self.record(event);
            return Err(error);
        }
        let event = TokenAuditEvent::new(TokenAuditAction::Rotated, now)
            .with_token(original.id(), Some(original.owner()))
            .success();
        self.record(event);
        let issued = TokenAuditEvent::new(TokenAuditAction::Issued, now)
            .with_token(replacement.id(), Some(replacement.owner()));
        self.record(issued);
        Ok(replacement)
    }

    pub async fn rotate_token_for_owner(&self, owner: &str) -> BpResult<ApiToken> {
        let token = self.token_for_owner(owner).await?;
        self.rotate_token(token.id()).await
    }

    pub async fn revoke_token(
        &self,
        id: &str,
        reason: impl RevocationReason,
    ) -> BpResult<ApiToken> {
        self.revoke_token_at(id, reason, Utc::now()).await
    }

    pub async fn revoke_token_with_reason(
        &self,
        id: &str,
        reason: impl RevocationReason,
    ) -> BpResult<ApiToken> {
        self.revoke_token(id, reason).await
    }

    pub async fn revoke_token_without_reason(&self, id: &str) -> BpResult<ApiToken> {
        self.revoke_token(id, Option::<String>::None).await
    }

    pub async fn revoke_token_at(
        &self,
        id: &str,
        reason: impl RevocationReason,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        self.revoke_token_with_actor_at(id, reason, None, now).await
    }

    pub async fn revoke_token_with_actor(
        &self,
        id: &str,
        reason: impl RevocationReason,
        revoked_by: impl Into<String>,
    ) -> BpResult<ApiToken> {
        self.revoke_token_with_actor_at(id, reason, Some(revoked_by.into()), Utc::now())
            .await
    }

    pub async fn revoke_token_with_actor_at(
        &self,
        id: &str,
        reason: impl RevocationReason,
        revoked_by: Option<String>,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let mut token = self.token(id).await?;
        token.revoke(now, reason, revoked_by);
        if let Err(error) = self.store.update(&token).await {
            let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
                .with_token(token.id(), Some(token.owner()))
                .failure(error.to_string());
            self.record(event);
            return Err(error);
        }
        let event = TokenAuditEvent::new(TokenAuditAction::Revoked, now)
            .with_token(token.id(), Some(token.owner()));
        self.record(event);
        Ok(token)
    }

    pub async fn revoke_tokens_for_owner(
        &self,
        owner: &str,
        reason: impl RevocationReason,
    ) -> BpResult<Vec<ApiToken>> {
        let reason = reason.into_reason();
        let now = Utc::now();
        let mut revoked = Vec::new();
        for token in self.active_tokens_for_owner(owner).await? {
            revoked.push(
                self.revoke_token_at(token.id(), reason.clone(), now)
                    .await?,
            );
        }
        Ok(revoked)
    }

    pub async fn purge_expired_tokens(&self, now: DateTime<Utc>) -> BpResult<usize> {
        let mut purged = 0;
        for id in self.store.ids().await? {
            if let Some(token) = self.store.get(&id).await?
                && token.is_expired(now)
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
