use crate::{ApiToken, AuthService, TokenMetadata};
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};
use std::collections::HashSet;

impl AuthService {
    pub async fn replacement_for(
        &self,
        original: &ApiToken,
        replacement_id: &str,
        now: DateTime<Utc>,
    ) -> PlatformResult<ApiToken> {
        self.ensure_rotation_allowed(original, replacement_id, now)
            .await?;
        if let Some(existing) = self.stored_token(replacement_id).await? {
            Self::ensure_replacement_usable(original, &existing, now)?;
            return Ok(existing);
        }
        let expires_at = now.checked_add_signed(self.token_lifetime).ok_or_else(|| {
            PlatformError::auth_invalid_field("token_lifetime", "is outside the supported range")
        })?;
        let metadata = TokenMetadata::new(expires_at, original.metadata().scopes().iter())?;
        let mut replacement = ApiToken::generate(replacement_id, original.owner(), now, metadata)?;
        replacement.metadata.rotated_from = Some(original.id().to_string());
        Ok(replacement)
    }

    pub fn retired_for(
        original: &ApiToken,
        replacement_id: &str,
        overlap_until: DateTime<Utc>,
    ) -> PlatformResult<ApiToken> {
        let mut retired = original.clone();
        retired
            .metadata
            .begin_rotation(replacement_id, overlap_until);
        retired.validate()?;
        Ok(retired)
    }

    pub fn ensure_replacement_matches(
        original: &ApiToken,
        replacement: &ApiToken,
    ) -> PlatformResult<()> {
        if replacement.owner() != original.owner()
            || replacement.metadata().rotated_from() != Some(original.id())
        {
            return Err(PlatformError::token_conflict(original.id()));
        }
        Ok(())
    }

    async fn ensure_rotation_allowed(
        &self,
        original: &ApiToken,
        replacement_id: &str,
        now: DateTime<Utc>,
    ) -> PlatformResult<()> {
        if let Some(replacement) = original.metadata().replaced_by() {
            return Err(PlatformError::auth_invalid_field(
                "token_id",
                format!("has already been rotated to {replacement:?}"),
            ));
        }
        if original.metadata().rotated_from() == Some(replacement_id) {
            return Err(PlatformError::auth_invalid_field(
                "replacement_id",
                "must not refer to the token this token was rotated from",
            ));
        }
        if self
            .rotation_lineage_contains(original, replacement_id)
            .await?
        {
            return Err(PlatformError::auth_invalid_field(
                "replacement_id",
                "must not reuse a token id from this token's rotation lineage",
            ));
        }
        Self::ensure_active(original, now)
    }

    async fn rotation_lineage_contains(
        &self,
        token: &ApiToken,
        candidate: &str,
    ) -> PlatformResult<bool> {
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

    pub fn rotation_overlap_until(&self, now: DateTime<Utc>) -> PlatformResult<DateTime<Utc>> {
        now.checked_add_signed(self.rotation_overlap)
            .ok_or_else(|| {
                PlatformError::auth_invalid_field(
                    "rotation_overlap",
                    "is outside the supported range",
                )
            })
    }
}
impl AuthService {
    pub async fn completed_rotation(
        &self,
        original: &ApiToken,
        replacement_id: &str,
        now: DateTime<Utc>,
    ) -> PlatformResult<ApiToken> {
        let replacement = self
            .stored_token(replacement_id)
            .await?
            .ok_or_else(|| PlatformError::unknown_token(replacement_id))?;
        Self::ensure_replacement_usable(original, &replacement, now)?;
        if original.metadata().replaced_by() == Some(replacement_id) {
            self.sync_owner_index_token(original).await;
        } else if let Some(current) = self.stored_token(original.id()).await? {
            self.sync_owner_index_token(&current).await;
        }
        self.sync_owner_index_token(&replacement).await;
        Ok(replacement)
    }

    pub fn ensure_replacement_usable(
        original: &ApiToken,
        replacement: &ApiToken,
        now: DateTime<Utc>,
    ) -> PlatformResult<()> {
        Self::ensure_replacement_matches(original, replacement)?;
        Self::ensure_active(replacement, now)?;
        if let Some(next) = replacement.metadata().replaced_by() {
            return Err(PlatformError::auth_invalid_field(
                "replacement_id",
                format!("has already been rotated to {next:?}"),
            ));
        }
        Ok(())
    }
}
