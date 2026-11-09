use crate::ApiToken;
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl ApiToken {
    pub(crate) fn validate_temporal(&self) -> BpResult<()> {
        if self.created_at >= self.metadata.expires_at {
            return Err(BpError::auth_invalid_field(
                "expires_at",
                "must be after created_at",
            ));
        }
        Self::ensure_not_before_creation("revoked_at", self.metadata.revoked_at, self.created_at)?;
        Self::ensure_not_before_creation(
            "rotation_overlap_until",
            self.metadata.rotation_overlap_until,
            self.created_at,
        )
    }

    fn ensure_not_before_creation(
        field: &'static str,
        timestamp: Option<DateTime<Utc>>,
        created_at: DateTime<Utc>,
    ) -> BpResult<()> {
        if timestamp.is_some_and(|timestamp| timestamp < created_at) {
            return Err(BpError::auth_invalid_field(
                field,
                "must not be before created_at",
            ));
        }
        Ok(())
    }
}
