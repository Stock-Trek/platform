use crate::{ApiToken, vault_client::VaultClient, vault_error::VaultWriteOutcome};
use platform_errors::{AuthError, PlatformError, PlatformResult};
use reqwest::{Method, StatusCode};
use serde_json::{Map, Value, json};

#[derive(Clone, Copy)]
pub enum WriteCondition {
    Create,
    Update(u64),
}

impl WriteCondition {
    fn cas(self) -> u64 {
        match self {
            Self::Create => 0,
            Self::Update(version) => version,
        }
    }

    fn conflict(self, token_id: &str) -> PlatformError {
        match self {
            Self::Create => PlatformError::token_exists(token_id),
            Self::Update(_) => PlatformError::token_conflict(token_id),
        }
    }
}

impl VaultClient {
    pub async fn put(&self, token: &ApiToken, condition: WriteCondition) -> PlatformResult<()> {
        token.validate()?;
        self.put_entry(token.id(), token.to_value(), condition)
            .await
    }

    pub async fn put_entry(
        &self,
        path: &str,
        data: Value,
        condition: WriteCondition,
    ) -> PlatformResult<()> {
        let body = json!({
            "data": data,
            "options": { "cas": condition.cas() },
        });
        let request = self
            .request(Method::PUT, self.url_for_path("data", path)?)
            .json(&body);
        let response = self.send("write", path, request).await?;
        if VaultWriteOutcome::from_status(response.status()) == VaultWriteOutcome::CasMismatch {
            return Err(condition.conflict(path));
        }
        self.require_success("write", path, response)
            .await
            .map(|_| ())
    }
}
impl VaultClient {
    pub async fn delete(&self, id: &str) -> PlatformResult<()> {
        ApiToken::validate_id(id)?;
        self.delete_key(id).await
    }

    pub async fn delete_key(&self, key: &str) -> PlatformResult<()> {
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

    pub async fn delete_if(&self, expected: &ApiToken) -> PlatformResult<bool> {
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

    pub fn tombstone_value() -> Value {
        let mut value = Map::new();
        value.insert(
            crate::vault_client::TOMBSTONE_FIELD.to_string(),
            Value::Bool(true),
        );
        Value::Object(value)
    }

    pub fn is_conflict(error: &PlatformError) -> bool {
        matches!(error, PlatformError::Auth(AuthError::Conflict { .. }))
    }
}
