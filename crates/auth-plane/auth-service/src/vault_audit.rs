use crate::{
    TokenAuditEvent,
    auth_service::MAX_UPDATE_ATTEMPTS,
    token_audit_outbox::TokenAuditOutbox,
    token_rotation_transaction::TokenRotationTransaction,
    vault_client::{VaultClient, WriteCondition},
};
use platform_errors::{BpError, BpResult};

impl VaultClient {
    pub(crate) async fn pending_audits(&self) -> BpResult<Vec<TokenAuditEvent>> {
        let mut events = Vec::new();
        for key in self.keys().await? {
            if TokenRotationTransaction::is_storage_key(&key) {
                continue;
            }
            let Some((data, _)) = self.get_value_entry(&key).await? else {
                continue;
            };
            match TokenAuditOutbox::standalone_event(&data)? {
                Some(event) => events.push(event),
                None => events.extend(TokenAuditOutbox::from_document(&data)?),
            }
        }
        Ok(events)
    }

    pub(crate) async fn acknowledge_audit(&self, event_id: &str) -> BpResult<()> {
        let standalone_key = TokenAuditOutbox::storage_key(event_id);
        if let Some((data, _)) = self.get_value_entry(&standalone_key).await?
            && TokenAuditOutbox::standalone_event(&data)?
                .as_ref()
                .is_some_and(|event| event.event_id() == Some(event_id))
        {
            self.delete_key(&standalone_key).await?;
            return Ok(());
        }
        for key in self.keys().await? {
            if TokenRotationTransaction::is_storage_key(&key)
                || TokenAuditOutbox::is_storage_key(&key)
            {
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
            let mut outbox = TokenAuditOutbox::from_document(&data)?;
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
                        TokenAuditOutbox::tombstone_document(&outbox)?,
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
                    TokenAuditOutbox::token_document(&token, &outbox)?,
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
}
