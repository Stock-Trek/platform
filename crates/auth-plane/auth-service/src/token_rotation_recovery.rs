use crate::{ApiToken, AuthService};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub(crate) async fn completed_rotation(
        &self,
        original: &ApiToken,
        replacement_id: &str,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        let replacement = self
            .stored_token(replacement_id)
            .await?
            .ok_or_else(|| BpError::unknown_token(replacement_id))?;
        Self::ensure_replacement_usable(original, &replacement, now)?;
        Ok(replacement)
    }

    pub(crate) async fn store_rotation_replacement(
        &self,
        original: &ApiToken,
        replacement: ApiToken,
        now: DateTime<Utc>,
    ) -> BpResult<(ApiToken, bool)> {
        match self.store.put(&replacement).await {
            Ok(()) => Ok((replacement, true)),
            Err(error) if Self::is_duplicate(&error) => {
                let existing = self
                    .reuse_existing_replacement(original, replacement.id(), error, now)
                    .await?;
                Ok((existing, false))
            }
            Err(error) => {
                self.record_rotation_failure(original, now, &error).await;
                Err(error)
            }
        }
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
        if let Err(error) = Self::ensure_replacement_usable(original, &existing, now) {
            self.record_rotation_failure(original, now, &error).await;
            return Err(error);
        }
        Ok(existing)
    }

    pub(crate) async fn retire_or_recover_rotation(
        &self,
        original: &ApiToken,
        replacement: &ApiToken,
        overlap_until: DateTime<Utc>,
        created_replacement: bool,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        match self
            .retire_rotated_token(original, replacement.id(), overlap_until, now)
            .await
        {
            Ok(retired) => Ok(retired),
            Err(error) => {
                self.recover_failed_retirement(
                    original,
                    replacement,
                    error,
                    created_replacement,
                    now,
                )
                .await
            }
        }
    }

    async fn recover_failed_retirement(
        &self,
        original: &ApiToken,
        replacement: &ApiToken,
        error: BpError,
        created_replacement: bool,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        match self.stored_token(original.id()).await {
            Ok(Some(current)) if current.metadata().replaced_by() == Some(replacement.id()) => {
                match self
                    .completed_rotation(original, replacement.id(), now)
                    .await
                {
                    Ok(_) => Ok(current),
                    Err(error) => {
                        self.record_rotation_failure(original, now, &error).await;
                        Err(error)
                    }
                }
            }
            Ok(_) => {
                let rollback = if created_replacement {
                    self.store.delete_if(replacement).await.map(|_| ())
                } else {
                    Ok(())
                };
                self.record_rotation_failure(original, now, &error).await;
                match rollback {
                    Ok(()) => Err(error),
                    Err(rollback_error) => Err(Self::rotation_rollback_error(
                        replacement.id(),
                        &error,
                        rollback_error,
                    )),
                }
            }
            Err(read_error) => {
                self.record_rotation_failure(original, now, &read_error)
                    .await;
                Err(error)
            }
        }
    }

    pub(crate) fn ensure_replacement_usable(
        original: &ApiToken,
        replacement: &ApiToken,
        now: DateTime<Utc>,
    ) -> BpResult<()> {
        Self::ensure_replacement_matches(original, replacement)?;
        Self::ensure_active(replacement, now)?;
        if let Some(next) = replacement.metadata().replaced_by() {
            return Err(BpError::auth_invalid_field(
                "replacement_id",
                format!("has already been rotated to {next:?}"),
            ));
        }
        Ok(())
    }

    pub(crate) async fn finish_rotation(
        &self,
        original: &ApiToken,
        replacement: ApiToken,
        retired: &ApiToken,
        created_replacement: bool,
        now: DateTime<Utc>,
    ) -> BpResult<ApiToken> {
        if let Err(error) = self
            .record_rotation_success(original, &replacement, now)
            .await
        {
            let rollback = self
                .rollback_rotation(retired, &replacement, created_replacement)
                .await;
            return Err(match rollback {
                Ok(()) => error,
                Err(rollback_error) => {
                    Self::rotation_rollback_error(replacement.id(), &error, rollback_error)
                }
            });
        }
        Ok(replacement)
    }
}
