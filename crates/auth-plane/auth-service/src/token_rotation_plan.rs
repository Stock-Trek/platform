use crate::{ApiToken, AuthService, TokenMetadata, auth_service::MAX_UPDATE_ATTEMPTS};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use std::collections::HashSet;

impl AuthService {
    pub(crate) async fn replacement_for(
        &self,
        original: &ApiToken,
        replacement_id: &str,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        self.ensure_rotation_allowed(original, replacement_id, now)
            .await?;
        let expires_at = now.checked_add_signed(self.token_lifetime).ok_or_else(|| {
            BpError::auth_invalid_field("token_lifetime", "is outside the supported range")
        })?;
        let metadata = TokenMetadata::new(expires_at, original.metadata().scopes().iter())?;
        let mut replacement = ApiToken::generate(replacement_id, original.owner(), now, metadata)?;
        replacement.metadata.rotated_from = Some(original.id().to_string());
        Ok(replacement)
    }

    pub(crate) async fn retire_rotated_token(
        &self,
        original: &ApiToken,
        replacement_id: &str,
        overlap_until: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> BpResult<()> {
        let mut current = original.clone();
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            if current.metadata().replaced_by() == Some(replacement_id) {
                return Ok(());
            }
            self.ensure_rotation_allowed(&current, replacement_id, now)
                .await?;
            let mut retired = current.clone();
            retired
                .metadata
                .begin_rotation(replacement_id, overlap_until);
            retired.validate()?;
            match self.store.update(&current, &retired).await {
                Ok(()) => return Ok(()),
                Err(error) if Self::is_conflict(&error) => {
                    current = self.token(original.id()).await?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(BpError::token_conflict(original.id()))
    }

    pub(crate) fn ensure_replacement_matches(
        original: &ApiToken,
        replacement: &ApiToken,
    ) -> BpResult<()> {
        if replacement.owner() != original.owner()
            || replacement.metadata().rotated_from() != Some(original.id())
        {
            return Err(BpError::token_conflict(original.id()));
        }
        Ok(())
    }

    async fn ensure_rotation_allowed(
        &self,
        original: &ApiToken,
        replacement_id: &str,
        now: DateTime<Utc>,
    ) -> BpResult<()> {
        if let Some(replacement) = original.metadata().replaced_by() {
            return Err(BpError::auth_invalid_field(
                "token_id",
                format!("has already been rotated to {replacement:?}"),
            ));
        }
        if original.metadata().rotated_from() == Some(replacement_id) {
            return Err(BpError::auth_invalid_field(
                "replacement_id",
                "must not refer to the token this token was rotated from",
            ));
        }
        if self
            .rotation_lineage_contains(original, replacement_id)
            .await?
        {
            return Err(BpError::auth_invalid_field(
                "replacement_id",
                "must not reuse a token id from this token's rotation lineage",
            ));
        }
        Self::ensure_active(original, now)
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

    pub(crate) fn rotation_overlap_until(&self, now: DateTime<Utc>) -> BpResult<DateTime<Utc>> {
        now.checked_add_signed(self.rotation_overlap)
            .ok_or_else(|| {
                BpError::auth_invalid_field("rotation_overlap", "is outside the supported range")
            })
    }
}
