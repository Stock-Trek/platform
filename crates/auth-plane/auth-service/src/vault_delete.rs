use crate::{
    ApiToken,
    vault_client::{TOMBSTONE_FIELD, VaultClient},
    vault_write::WriteCondition,
};
use platform_errors::{AuthError, BpError, BpResult};
use reqwest::{Method, StatusCode};
use serde_json::json;

impl VaultClient {
    pub(crate) async fn delete(&self, id: &str) -> BpResult<()> {
        ApiToken::validate_id(id)?;
        let response = self
            .send(
                "delete",
                id,
                self.request(Method::DELETE, self.config.url(&["metadata", id])?),
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        self.require_success("delete", id, response)
            .await
            .map(|_| ())
    }

    /// Atomically deletes `expected` by first committing a tombstone with a
    /// compare-and-swap write, then removing the metadata.
    ///
    /// A create always uses CAS 0, so it cannot succeed while the tombstone
    /// exists. That makes the final metadata delete safe: a newer token can
    /// never be recreated between the tombstone write and the delete.
    pub(crate) async fn delete_if(&self, expected: &ApiToken) -> BpResult<bool> {
        expected.validate()?;
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
        let tombstone = json!({ TOMBSTONE_FIELD: true });
        match self
            .put_entry(expected.id(), tombstone, WriteCondition::Update(version))
            .await
        {
            Ok(()) => {}
            Err(error) if Self::is_conflict(&error) => return Ok(false),
            Err(error) => return Err(error),
        }
        self.delete(expected.id()).await?;
        Ok(true)
    }

    pub(crate) fn is_conflict(error: &BpError) -> bool {
        matches!(error, BpError::Auth(AuthError::Conflict { .. }))
    }
}
