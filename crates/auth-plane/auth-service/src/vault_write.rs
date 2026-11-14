use crate::{ApiToken, vault_client::VaultClient, vault_error::VaultWriteOutcome};
use platform_errors::{PlatformError, PlatformResult};
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
