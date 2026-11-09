use crate::{ApiToken, AuthService, TokenAuditAction, TokenAuditEvent};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult};

impl AuthService {
    pub async fn purge_expired_tokens(&self, now: DateTime<Utc>) -> BpResult<usize> {
        let mut ids = self.store.ids().await?;
        ids.sort();
        ids.dedup();
        let mut expired = Vec::new();
        for id in ids {
            if let Some(token) = self.stored_token(&id).await?
                && token.metadata().is_expired(now)
            {
                expired.push(token);
            }
        }
        let mut deleted = Vec::new();
        for token in expired {
            match self.delete_expired_if_current(&token, now).await {
                Ok(true) => deleted.push(token),
                Ok(false) => continue,
                Err(error) => return Err(self.rollback_purge(&deleted, error).await),
            }
        }
        if deleted.is_empty() {
            return Ok(0);
        }
        let events = deleted
            .iter()
            .map(|token| {
                TokenAuditEvent::new(TokenAuditAction::Deleted, now)
                    .with_token(token.id(), Some(token.owner()))
            })
            .collect::<Vec<_>>();
        if let Err(error) = self.record_audit_batch(&events).await {
            return Err(self.rollback_purge(&deleted, error).await);
        }
        Ok(deleted.len())
    }

    async fn delete_expired_if_current(
        &self,
        token: &ApiToken,
        now: DateTime<Utc>,
    ) -> BpResult<bool> {
        let Some(current) = self.stored_token(token.id()).await? else {
            return Ok(false);
        };
        if current.to_value() != token.to_value() || !current.metadata().is_expired(now) {
            return Ok(false);
        }
        self.store.delete_if(token).await
    }

    async fn rollback_purge(&self, tokens: &[ApiToken], failure: BpError) -> BpError {
        for token in tokens {
            if let Err(rollback) = self.store.put(token).await {
                return Self::rollback_failure(
                    "restore purged tokens",
                    token.id(),
                    &failure,
                    rollback,
                );
            }
        }
        failure
    }
}
