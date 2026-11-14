use crate::{AuthService, TokenAuditAction, TokenAuditEvent, TokenMutation};
use chrono::{DateTime, Utc};
use platform_errors::BuildResult;

impl AuthService {
    /// Deletes expired tokens one at a time.
    ///
    /// Each token is removed with a single compare-and-swap commit that also
    /// records its `Deleted` audit event. A failure after some tokens have
    /// already been purged leaves those deletions in place; the caller can
    /// safely retry because the operation is idempotent.
    pub async fn purge_expired_tokens(&self, now: DateTime<Utc>) -> BuildResult<usize> {
        let mut ids = self.store.ids().await?;
        ids.sort();
        ids.dedup();
        let mut deleted = 0;
        for id in ids {
            let Some(token) = self.stored_token(&id).await? else {
                continue;
            };
            if !token.metadata().is_expired(now) {
                continue;
            }
            let event = TokenAuditEvent::new(TokenAuditAction::Deleted, now)
                .with_token(token.id(), Some(token.owner()));
            match self
                .commit_mutation(TokenMutation::Delete { expected: token }, event)
                .await
            {
                Ok(true) => deleted += 1,
                Ok(false) => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(deleted)
    }
}
