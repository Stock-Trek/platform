use crate::{
    ApiToken, TokenAuditEvent, TokenMutation,
    token_audit_outbox::TokenAuditOutbox,
    vault_client::{VaultClient, WriteCondition},
};
use platform_errors::{PlatformError, PlatformResult};

impl VaultClient {
    pub async fn commit_events(
        &self,
        mutation: TokenMutation,
        mut events: Vec<TokenAuditEvent>,
    ) -> PlatformResult<bool> {
        for event in &mut events {
            TokenAuditOutbox::ensure_event_id(event);
        }
        Self::validate_mutation(&mutation)?;
        let mut created = Vec::new();
        for event in &events {
            match self.put_standalone_audit(event).await {
                Ok(true) => created.push(TokenAuditOutbox::require_event_id(event)?.to_string()),
                Ok(false) => {}
                Err(error) => {
                    self.remove_audit_events(&created).await;
                    return Err(error);
                }
            }
        }
        let result = match mutation {
            TokenMutation::Create(token) => self
                .put_entry(token.id(), token.to_value(), WriteCondition::Create)
                .await
                .map(|_| true),
            TokenMutation::Update { expected, token } => self
                .commit_update_events(&expected, &token)
                .await
                .map(|_| true),
            TokenMutation::Delete { expected } => self.commit_delete_events(&expected).await,
        };
        match result {
            Ok(true) => Ok(true),
            Ok(false) => {
                self.remove_audit_events(&created).await;
                Ok(false)
            }
            Err(error) => {
                self.remove_audit_events(&created).await;
                Err(error)
            }
        }
    }

    fn validate_mutation(mutation: &TokenMutation) -> PlatformResult<()> {
        match mutation {
            TokenMutation::Create(token) | TokenMutation::Update { token, .. } => token.validate(),
            TokenMutation::Delete { expected } => expected.validate(),
        }
    }

    async fn commit_update_events(
        &self,
        expected: &ApiToken,
        token: &ApiToken,
    ) -> PlatformResult<()> {
        let Some((data, version)) = self.get_value_entry(token.id()).await? else {
            return Err(PlatformError::unknown_token(token.id()));
        };
        if Self::is_tombstone(&data) {
            return Err(PlatformError::unknown_token(token.id()));
        }
        let current = Self::token_from_value(token.id(), &data)?;
        if current.to_value() != expected.to_value() {
            return Err(PlatformError::token_conflict(token.id()));
        }
        self.put_entry(
            token.id(),
            token.to_value(),
            WriteCondition::Update(version),
        )
        .await
    }

    async fn commit_delete_events(&self, expected: &ApiToken) -> PlatformResult<bool> {
        let Some((data, version)) = self.get_value_entry(expected.id()).await? else {
            return Ok(false);
        };
        if Self::is_tombstone(&data) {
            self.delete(expected.id()).await?;
            return Ok(false);
        }
        let current = Self::token_from_value(expected.id(), &data)?;
        if current.to_value() != expected.to_value() {
            return Ok(false);
        }
        match self
            .put_entry(
                expected.id(),
                Self::tombstone_value(),
                WriteCondition::Update(version),
            )
            .await
        {
            Ok(()) => {
                self.delete(expected.id()).await?;
                Ok(true)
            }
            Err(error) if Self::is_conflict(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn remove_audit_events(&self, event_ids: &[String]) {
        for event_id in event_ids {
            let _ = self
                .delete_key(&TokenAuditOutbox::storage_key(event_id))
                .await;
        }
    }
}
