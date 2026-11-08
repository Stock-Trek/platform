use crate::{
    ApiToken, AuthService, RevocationReason, RevokedBy, TokenAuditAction, TokenAuditEvent,
    TokenMetadata, api_token::MAX_TOKEN_ID_LENGTH,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use rand::{Rng, distributions::Alphanumeric};
use std::collections::HashSet;

impl AuthService {
    pub async fn rotate_token(&self, id: &str, now: DateTime<Utc>) -> BpResult<ApiToken> {
        ApiToken::validate_id(id)?;
        let replacement_id = Self::rotated_token_id(id, now);
        self.rotate_token_to(id, replacement_id, now).await
    }

    pub async fn rotate_token_to(
        &self,
        id: &str,
        replacement_id: impl Into<String>,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let replacement_id = replacement_id.into();
        ApiToken::validate_id(&replacement_id).map_err(|_| {
            BpError::auth_invalid_field("replacement_id", "must be a valid token id")
        })?;
        if replacement_id == id {
            return Err(BpError::auth_invalid_field(
                "replacement_id",
                "must refer to a different token",
            ));
        }
        let original = self.token(id).await?;
        if let Some(replacement) = original.metadata().replaced_by() {
            return Err(BpError::auth_invalid_field(
                "token_id",
                format!("has already been rotated to {replacement:?}"),
            ));
        }
        if original.metadata().rotated_from() == Some(replacement_id.as_str()) {
            return Err(BpError::auth_invalid_field(
                "replacement_id",
                "must not refer to the token this token was rotated from",
            ));
        }
        if self
            .rotation_lineage_contains(&original, &replacement_id)
            .await?
        {
            return Err(BpError::auth_invalid_field(
                "replacement_id",
                "must not reuse a token id from this token's rotation lineage",
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
        let mut retired = original.clone();
        retired
            .metadata
            .begin_rotation(replacement.id(), overlap_until);
        retired.metadata.validate()?;
        if let Err(error) = self.store.put(&replacement).await {
            let event = TokenAuditEvent::new(TokenAuditAction::Rotated, now)
                .with_token(original.id(), Some(original.owner()))
                .failure(error.to_string());
            self.audit.record(event);
            return Err(error);
        }
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

    async fn rotation_lineage_contains(&self, token: &ApiToken, candidate: &str) -> BpResult<bool> {
        let mut next = token.metadata().rotated_from().map(str::to_string);
        let mut visited = HashSet::new();
        while let Some(id) = next {
            if id == candidate {
                return Ok(true);
            }
            if !visited.insert(id.clone()) {
                return Ok(false);
            }
            let Some(ancestor) = self.stored_token(&id).await? else {
                return Ok(false);
            };
            next = ancestor.metadata().rotated_from().map(str::to_string);
        }
        Ok(false)
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

    fn rotated_token_id(id: &str, now: DateTime<Utc>) -> String {
        let random: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        let suffix = format!("-{}-{random}", now.timestamp_millis().unsigned_abs());
        let prefix_length = MAX_TOKEN_ID_LENGTH.saturating_sub(suffix.len());
        let prefix = &id[..id.len().min(prefix_length)];
        format!("{prefix}{suffix}")
    }
}
