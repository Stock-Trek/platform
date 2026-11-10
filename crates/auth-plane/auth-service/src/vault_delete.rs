use crate::{
    ApiToken, token_audit_outbox::TokenAuditOutbox, vault_client::VaultClient,
    vault_write::WriteCondition,
};
use platform_errors::{AuthError, BpError, BpResult};
use reqwest::{Method, StatusCode};

impl VaultClient {
    pub(crate) async fn delete(&self, id: &str) -> BpResult<()> {
        ApiToken::validate_id(id)?;
        self.delete_key(id).await
    }

    /// Deletes an entry that is not necessarily a token key, such as a rotation
    /// transaction or a standalone audit record.
    pub(crate) async fn delete_key(&self, key: &str) -> BpResult<()> {
        let response = self
            .send(
                "delete",
                key,
                self.request(Method::DELETE, self.config.url(&["metadata", key])?),
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        self.require_success("delete", key, response)
            .await
            .map(|_| ())
    }

    /// Atomically deletes `expected` by first committing a tombstone with a
    /// compare-and-swap write, then removing the metadata.
    ///
    /// Pending audit events are kept on the tombstone and the metadata is only
    /// removed once the outbox is empty. A create always uses CAS 0, so it
    /// cannot succeed while the tombstone exists. That makes the final metadata
    /// delete safe: a newer token can never be recreated between the tombstone
    /// write and the delete.
    pub(crate) async fn delete_if(&self, expected: &ApiToken) -> BpResult<bool> {
        expected.validate()?;
        let Some((data, version)) = self.get_value_entry(expected.id()).await? else {
            return Ok(false);
        };
        if Self::is_tombstone(&data) {
            if TokenAuditOutbox::from_document(&data)?.is_empty() {
                self.delete(expected.id()).await?;
            }
            return Ok(false);
        }
        let current = Self::token_from_value(expected.id(), &data)?;
        if current.to_value() != expected.to_value() {
            return Ok(false);
        }
        let outbox = TokenAuditOutbox::from_document(&data)?;
        match self
            .put_entry(
                expected.id(),
                TokenAuditOutbox::tombstone_document(&outbox)?,
                WriteCondition::Update(version),
            )
            .await
        {
            Ok(()) => {}
            Err(error) if Self::is_conflict(&error) => return Ok(false),
            Err(error) => return Err(error),
        }
        if outbox.is_empty() {
            self.delete(expected.id()).await?;
        }
        Ok(true)
    }

    pub(crate) fn is_conflict(error: &BpError) -> bool {
        matches!(error, BpError::Auth(AuthError::Conflict { .. }))
    }
}
