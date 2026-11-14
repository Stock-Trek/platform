use crate::{ApiToken, vault_client::VaultClient, vault_error::VaultWriteOutcome};
use platform_errors::{BuildError, BuildResult};
use reqwest::Method;
use serde_json::{Value, json};

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

    fn conflict(self, token_id: &str) -> BuildError {
        match self {
            Self::Create => BuildError::token_exists(token_id),
            Self::Update(_) => BuildError::token_conflict(token_id),
        }
    }
}

impl VaultClient {
    pub async fn put(&self, token: &ApiToken, condition: WriteCondition) -> BuildResult<()> {
        token.validate()?;
        self.put_entry(token.id(), token.to_value(), condition)
            .await
    }

    pub async fn put_entry(
        &self,
        path: &str,
        data: Value,
        condition: WriteCondition,
    ) -> BuildResult<()> {
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
