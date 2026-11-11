use crate::{
    ApiToken, TokenAuditEvent, TokenMutation,
    token_audit_outbox::TokenAuditOutbox,
    vault_client::{VaultClient, WriteCondition},
};
use platform_errors::{BpError, BpResult};

impl VaultClient {
    pub(crate) async fn commit_events(
        &self,
        mutation: TokenMutation,
        mut events: Vec<TokenAuditEvent>,
    ) -> BpResult<bool> {
        for event in &mut events {
            TokenAuditOutbox::ensure_event_id(event);
        }
        match mutation {
            TokenMutation::Create(token) => {
                token.validate()?;
                self.put_entry(
                    token.id(),
                    TokenAuditOutbox::token_document(&token, &events)?,
                    WriteCondition::Create,
                )
                .await?;
                Ok(true)
            }
            TokenMutation::Update { expected, token } => {
                self.commit_update_events(&expected, &token, &events)
                    .await?;
                Ok(true)
            }
            TokenMutation::Delete { expected } => {
                self.commit_delete_events(&expected, &events).await
            }
        }
    }

    async fn commit_update_events(
        &self,
        expected: &ApiToken,
        token: &ApiToken,
        events: &[TokenAuditEvent],
    ) -> BpResult<()> {
        token.validate()?;
        let Some((data, version)) = self.get_value_entry(token.id()).await? else {
            return Err(BpError::unknown_token(token.id()));
        };
        if Self::is_tombstone(&data) {
            return Err(BpError::unknown_token(token.id()));
        }
        let current = Self::token_from_value(token.id(), &data)?;
        if current.to_value() != expected.to_value() {
            return Err(BpError::token_conflict(token.id()));
        }
        let mut outbox = TokenAuditOutbox::from_document(&data)?;
        outbox.extend_from_slice(events);
        self.put_entry(
            token.id(),
            TokenAuditOutbox::token_document(token, &outbox)?,
            WriteCondition::Update(version),
        )
        .await
    }

    async fn commit_delete_events(
        &self,
        expected: &ApiToken,
        events: &[TokenAuditEvent],
    ) -> BpResult<bool> {
        expected.validate()?;
        let Some((data, version)) = self.get_value_entry(expected.id()).await? else {
            return Ok(false);
        };
        if Self::is_tombstone(&data) {
            return Ok(false);
        }
        let current = Self::token_from_value(expected.id(), &data)?;
        if current.to_value() != expected.to_value() {
            return Ok(false);
        }
        let mut outbox = TokenAuditOutbox::from_document(&data)?;
        outbox.extend_from_slice(events);
        match self
            .put_entry(
                expected.id(),
                TokenAuditOutbox::tombstone_document(&outbox)?,
                WriteCondition::Update(version),
            )
            .await
        {
            Ok(()) => Ok(true),
            Err(error) if Self::is_conflict(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }
}
