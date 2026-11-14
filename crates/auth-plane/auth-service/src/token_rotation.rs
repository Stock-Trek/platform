use crate::{
    ApiToken, AuthService, TokenAuditAction, TokenAuditEvent, TokenRotationTransaction,
    api_token::MAX_TOKEN_ID_LENGTH, random_string::RandomString,
};
use chrono::{DateTime, Utc};
use platform_errors::{PlatformError, PlatformResult};

impl AuthService {
    pub async fn rotate_token(&self, id: &str, now: DateTime<Utc>) -> PlatformResult<ApiToken> {
        ApiToken::validate_id(id)?;
        let replacement_id = Self::rotated_token_id(id, now);
        self.rotate_token_to(id, replacement_id, now).await
    }

    pub async fn rotate_token_to(
        &self,
        id: &str,
        replacement_id: impl Into<String>,
        now: DateTime<Utc>,
    ) -> PlatformResult<ApiToken> {
        let replacement_id = replacement_id.into();
        Self::validate_rotation_ids(id, &replacement_id)?;
        let original = self.token(id).await?;
        if original.metadata().replaced_by() == Some(replacement_id.as_str()) {
            return self
                .completed_rotation(&original, &replacement_id, now)
                .await;
        }
        let replacement = self
            .replacement_for(&original, &replacement_id, now)
            .await?;
        let overlap_until = self.rotation_overlap_until(now)?;
        let retired = Self::retired_for(&original, &replacement_id, overlap_until)?;
        let events = vec![
            TokenAuditEvent::new(TokenAuditAction::Rotated, now)
                .with_token(original.id(), Some(original.owner())),
            TokenAuditEvent::new(TokenAuditAction::Issued, now)
                .with_token(replacement.id(), Some(replacement.owner())),
        ];
        let transaction =
            TokenRotationTransaction::new(original.clone(), retired, replacement.clone(), events)?;
        self.commit_rotation(&transaction).await?;
        if let Err(error) = self.store.materialize_rotation(&transaction).await {
            if let Some(current) = self.stored_token(original.id()).await?
                && current.metadata().replaced_by() == Some(replacement_id.as_str())
            {
                return self
                    .completed_rotation(&original, &replacement_id, now)
                    .await;
            }
            self.record_rotation_failure(&original, now, &error).await;
            return Err(error);
        }
        let replacement = self.token(&replacement_id).await?;
        Self::ensure_replacement_usable(&original, &replacement, now)?;
        if let Some(retired) = self.stored_token(original.id()).await? {
            self.sync_owner_index_token(&retired).await;
        }
        self.sync_owner_index_token(&replacement).await;
        Ok(replacement)
    }

    fn validate_rotation_ids(id: &str, replacement_id: &str) -> PlatformResult<()> {
        ApiToken::validate_id(replacement_id).map_err(|_| {
            PlatformError::auth_invalid_field("replacement_id", "must be a valid token id")
        })?;
        if replacement_id == id {
            return Err(PlatformError::auth_invalid_field(
                "replacement_id",
                "must refer to a different token",
            ));
        }
        Ok(())
    }

    fn rotated_token_id(id: &str, now: DateTime<Utc>) -> String {
        let random = RandomString::generate(8);
        let suffix = format!("-{}-{random}", now.timestamp_millis().unsigned_abs());
        let prefix_length = MAX_TOKEN_ID_LENGTH.saturating_sub(suffix.len());
        let prefix = &id[..id.len().min(prefix_length)];
        format!("{prefix}{suffix}")
    }

    pub async fn record_rotation_failure(
        &self,
        token: &ApiToken,
        now: DateTime<Utc>,
        error: &PlatformError,
    ) {
        let event = TokenAuditEvent::new(TokenAuditAction::Rotated, now)
            .with_token(token.id(), Some(token.owner()))
            .failure(error.to_string());
        self.record_audit_failure(event).await;
    }
}
