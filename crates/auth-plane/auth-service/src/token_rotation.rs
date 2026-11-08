use crate::{
    ApiToken, AuthService, TokenAuditAction, TokenAuditEvent, api_token::MAX_TOKEN_ID_LENGTH,
};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};
use rand::{Rng, distributions::Alphanumeric};

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
        if original.metadata().replaced_by() == Some(replacement_id.as_str()) {
            return self.completed_rotation(&original, &replacement_id).await;
        }
        let replacement = self
            .replacement_for(&original, &replacement_id, now)
            .await?;
        let overlap_until = self.rotation_overlap_until(now)?;
        let replacement = match self.store.put(&replacement).await {
            Ok(()) => replacement,
            Err(error) if Self::is_duplicate(&error) => {
                self.reuse_existing_replacement(&original, &replacement_id, error, now)
                    .await?
            }
            Err(error) => {
                self.record_rotation_failure(&original, now, &error).await;
                return Err(error);
            }
        };
        match self
            .retire_rotated_token(&original, replacement.id(), overlap_until, now)
            .await
        {
            Ok(()) => {
                self.record_rotation_success(&original, &replacement, now)
                    .await?;
                Ok(replacement)
            }
            Err(error) => {
                match self.stored_token(original.id()).await {
                    Ok(Some(current))
                        if current.metadata().replaced_by() == Some(replacement.id()) =>
                    {
                        self.record_rotation_success(&original, &replacement, now)
                            .await?;
                        return Ok(replacement);
                    }
                    Ok(_) => {}
                    Err(_) => {
                        self.record_rotation_failure(&original, now, &error).await;
                        return Err(error);
                    }
                }
                let rollback = self.store.delete(replacement.id()).await;
                self.record_rotation_failure(&original, now, &error).await;
                match rollback {
                    Ok(()) => Err(error),
                    Err(rollback_error) => Err(Self::rotation_rollback_error(
                        replacement.id(),
                        &error,
                        rollback_error,
                    )),
                }
            }
        }
    }

    async fn completed_rotation(
        &self,
        original: &ApiToken,
        replacement_id: &str,
    ) -> BpResult<ApiToken> {
        let replacement = self
            .stored_token(replacement_id)
            .await?
            .ok_or_else(|| BpError::unknown_token(replacement_id))?;
        Self::ensure_replacement_matches(original, &replacement)?;
        Ok(replacement)
    }

    async fn reuse_existing_replacement(
        &self,
        original: &ApiToken,
        replacement_id: &str,
        conflict: BpError,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let existing = match self.stored_token(replacement_id).await {
            Ok(Some(existing)) => existing,
            Ok(None) => {
                self.record_rotation_failure(original, now, &conflict).await;
                return Err(conflict);
            }
            Err(error) => {
                self.record_rotation_failure(original, now, &error).await;
                return Err(error);
            }
        };
        if let Err(error) = Self::ensure_replacement_matches(original, &existing) {
            self.record_rotation_failure(original, now, &error).await;
            return Err(error);
        }
        Ok(existing)
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

    async fn record_rotation_success(
        &self,
        original: &ApiToken,
        replacement: &ApiToken,
        now: DateTime<Utc>,
    ) -> BpResult<()> {
        let event = TokenAuditEvent::new(TokenAuditAction::Rotated, now)
            .with_token(original.id(), Some(original.owner()));
        self.record_audit(event).await?;
        let issued = TokenAuditEvent::new(TokenAuditAction::Issued, now)
            .with_token(replacement.id(), Some(replacement.owner()));
        self.record_audit(issued).await
    }

    async fn record_rotation_failure(&self, token: &ApiToken, now: DateTime<Utc>, error: &BpError) {
        let event = TokenAuditEvent::new(TokenAuditAction::Rotated, now)
            .with_token(token.id(), Some(token.owner()))
            .failure(error.to_string());
        self.record_audit_failure(event).await;
    }

    fn rotation_rollback_error(
        replacement_id: &str,
        failure: &BpError,
        rollback: BpError,
    ) -> BpError {
        BpError::auth_store(
            "rollback",
            replacement_id,
            rollback.retryability().is_retryable(),
            format!(
                "failed to remove the replacement after rotation failed: {rollback}; \
                 original failure: {failure}"
            ),
        )
    }
}
