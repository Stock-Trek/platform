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
        self.commit_events(mutation, vec![event]).await
    }

    /// Updates a token without dropping audit events already waiting in its
    /// outbox.
    pub(crate) async fn update_preserving_outbox(
        &self,
        expected: &ApiToken,
        token: &ApiToken,
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
        let outbox = TokenAuditOutbox::from_document(&data)?;
        self.put_entry(
            token.id(),
            TokenAuditOutbox::token_document(token, &outbox)?,
            WriteCondition::Update(version),
        )
        .await
    }

    pub(crate) async fn append_audit(&self, mut event: TokenAuditEvent) -> BpResult<()> {
        TokenAuditOutbox::ensure_event_id(&mut event);
        self.put_standalone_audit(&event).await.map(|_| ())
    }

    pub(crate) async fn put_standalone_audit(&self, event: &TokenAuditEvent) -> BpResult<bool> {
        let event_id = TokenAuditOutbox::require_event_id(event)?;
        let key = TokenAuditOutbox::storage_key(event_id);
        match self
            .put_entry(
                &key,
                TokenAuditOutbox::standalone_document(event)?,
                WriteCondition::Create,
            )
            .await
        {
            Ok(()) => Ok(true),
            Err(BpError::Auth(platform_errors::AuthError::TokenExists { .. })) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn commit_rotation(
        &self,
        transaction: &TokenRotationTransaction,
    ) -> BpResult<()> {
        let key = transaction.storage_key();
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            match self.get_value_entry(&key).await? {
                None => match self
                    .put_entry(&key, transaction.to_value(), WriteCondition::Create)
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(BpError::Auth(platform_errors::AuthError::TokenExists { .. })) => continue,
                    Err(error) => return Err(error),
                },
                Some((data, version)) => {
                    let existing = TokenRotationTransaction::from_value(&data)?;
                    if !existing.has_same_rotation(transaction) {
                        return Err(BpError::token_conflict(transaction.original().id()));
                    }
                    match self
                        .put_entry(
                            &key,
                            transaction.to_value(),
                            WriteCondition::Update(version),
                        )
                        .await
                    {
                        Ok(()) => return Ok(()),
                        Err(BpError::Auth(platform_errors::AuthError::Conflict { .. })) => continue,
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        Err(BpError::token_conflict(transaction.original().id()))
    }

    pub(crate) async fn pending_rotations(&self) -> BpResult<Vec<TokenRotationTransaction>> {
        let mut transactions = Vec::new();
        for key in self
            .keys_with_prefix(crate::token_rotation_transaction::ROTATION_TRANSACTION_PREFIX)
            .await?
        {
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
        self.delete_key(&key).await
    }
}
