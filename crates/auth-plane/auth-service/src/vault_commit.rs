use crate::{
    ApiToken, TokenAuditEvent, TokenMutation, TokenRotationTransaction,
    auth_service::MAX_UPDATE_ATTEMPTS,
    token_audit_outbox::TokenAuditOutbox,
    vault_client::{VaultClient, WriteCondition},
};
use platform_errors::{BpError, BpResult};

impl VaultClient {
    pub(crate) async fn commit(
        &self,
        mutation: TokenMutation,
        mut event: TokenAuditEvent,
    ) -> BpResult<bool> {
        TokenAuditOutbox::ensure_event_id(&mut event);
        match mutation {
            TokenMutation::Create(token) => {
                token.validate()?;
                self.put_entry(
                    token.id(),
                    TokenAuditOutbox::token_document(&token, std::slice::from_ref(&event)),
                    WriteCondition::Create,
                )
                .await?;
                Ok(true)
            }
            TokenMutation::Update { expected, token } => {
                self.commit_update(&expected, &token, event).await?;
                Ok(true)
            }
            TokenMutation::Delete { expected } => self.commit_delete(&expected, event).await,
        }
    }

    async fn commit_update(
        &self,
        expected: &ApiToken,
        token: &ApiToken,
        event: TokenAuditEvent,
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
        let mut outbox = TokenAuditOutbox::from_document(&data);
        outbox.push(event);
        self.put_entry(
            token.id(),
            TokenAuditOutbox::token_document(token, &outbox),
            WriteCondition::Update(version),
        )
        .await
    }

    async fn commit_delete(&self, expected: &ApiToken, event: TokenAuditEvent) -> BpResult<bool> {
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
        let mut outbox = TokenAuditOutbox::from_document(&data);
        outbox.push(event);
        match self
            .put_entry(
                expected.id(),
                TokenAuditOutbox::tombstone_document(&outbox),
                WriteCondition::Update(version),
            )
            .await
        {
            Ok(()) => Ok(true),
            Err(error) if Self::is_conflict(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn commit_rotation(
        &self,
        transaction: &TokenRotationTransaction,
    ) -> BpResult<()> {
        self.put_entry(
            &transaction.storage_key(),
            transaction.to_value(),
            WriteCondition::Create,
        )
        .await
    }

    pub(crate) async fn pending_audits(&self) -> BpResult<Vec<TokenAuditEvent>> {
        let mut events = Vec::new();
        for key in self.keys().await? {
            if TokenRotationTransaction::is_storage_key(&key) {
                continue;
            }
            if let Some((data, _)) = self.get_value_entry(&key).await? {
                events.extend(TokenAuditOutbox::from_document(&data));
            }
        }
        Ok(events)
    }

    pub(crate) async fn acknowledge_audit(&self, event_id: &str) -> BpResult<()> {
        for key in self.keys().await? {
            if TokenRotationTransaction::is_storage_key(&key) {
                continue;
            }
            if self.acknowledge_event(&key, event_id).await? {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn acknowledge_event(&self, key: &str, event_id: &str) -> BpResult<bool> {
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let Some((data, version)) = self.get_value_entry(key).await? else {
                return Ok(false);
            };
            let mut outbox = TokenAuditOutbox::from_document(&data);
            if !outbox
                .iter()
                .any(|event| event.event_id() == Some(event_id))
            {
                return Ok(false);
            }
            TokenAuditOutbox::remove_events(&mut outbox, event_id);
            if Self::is_tombstone(&data) {
                if outbox.is_empty() {
                    self.delete(key).await?;
                    return Ok(true);
                }
                match self
                    .put_entry(
                        key,
                        TokenAuditOutbox::tombstone_document(&outbox),
                        WriteCondition::Update(version),
                    )
                    .await
                {
                    Ok(()) => return Ok(true),
                    Err(error) if Self::is_conflict(&error) => continue,
                    Err(error) => return Err(error),
                }
            }
            let token = Self::token_from_value(key, &data)?;
            match self
                .put_entry(
                    key,
                    TokenAuditOutbox::token_document(&token, &outbox),
                    WriteCondition::Update(version),
                )
                .await
            {
                Ok(()) => return Ok(true),
                Err(error) if Self::is_conflict(&error) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(BpError::token_conflict(key))
    }

    pub(crate) async fn pending_rotations(&self) -> BpResult<Vec<TokenRotationTransaction>> {
        let mut transactions = Vec::new();
        for key in self.keys().await? {
            if !TokenRotationTransaction::is_storage_key(&key) {
                continue;
            }
            let Some((data, _)) = self.get_value_entry(&key).await? else {
                continue;
            };
            if TokenRotationTransaction::is_document(&data) {
                transactions.push(TokenRotationTransaction::from_value(&data)?);
            }
        }
        transactions.sort_by(|left, right| left.transaction_id().cmp(right.transaction_id()));
        Ok(transactions)
    }

    pub(crate) async fn acknowledge_rotation(&self, transaction_id: &str) -> BpResult<()> {
        let key = format!(
            "{}{}",
            crate::token_rotation_transaction::ROTATION_TRANSACTION_PREFIX,
            transaction_id
        );
        self.delete(&key).await
    }
}
