use crate::{ApiToken, vault_client::VaultClient, vault_write::WriteCondition};
use platform_errors::{AuthError, BuildError, BuildResult};
use reqwest::{Method, StatusCode};
use serde_json::{Map, Value};

impl VaultClient {
    pub(crate) async fn delete(&self, id: &str) -> BuildResult<()> {
        ApiToken::validate_id(id)?;
        self.delete_key(id).await
    }

    pub(crate) async fn delete_key(&self, key: &str) -> BuildResult<()> {
        let response = self
            .send(
                "delete",
                key,
                self.request(Method::DELETE, self.url_for_path("metadata", key)?),
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        self.require_success("delete", key, response)
            .await
            .map(|_| ())
    }

    pub(crate) async fn delete_if(&self, expected: &ApiToken) -> BuildResult<bool> {
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
        match self
            .put_entry(
                expected.id(),
                Self::tombstone_value(),
                WriteCondition::Update(version),
            )
            .await
        {
            Ok(()) => {}
            Err(error) if Self::is_conflict(&error) => return Ok(false),
            Err(error) => return Err(error),
        }
        self.delete(expected.id()).await?;
        Ok(true)
    }

    pub(crate) fn tombstone_value() -> Value {
        let mut value = Map::new();
        value.insert(
            crate::vault_client::TOMBSTONE_FIELD.to_string(),
            Value::Bool(true),
        );
        Value::Object(value)
    }

    pub(crate) fn is_conflict(error: &BuildError) -> bool {
        matches!(error, BuildError::Auth(AuthError::Conflict { .. }))
    }
}
