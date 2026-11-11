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
        if original.metadata().replaced_by() == Some(replacement_id) {
            self.sync_owner_index_token(original).await;
        } else if let Some(current) = self.stored_token(original.id()).await? {
            self.sync_owner_index_token(&current).await;
        }
        self.sync_owner_index_token(&replacement).await;
        Ok(replacement)
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
}
