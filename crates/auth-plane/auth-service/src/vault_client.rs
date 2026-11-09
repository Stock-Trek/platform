use crate::{
    ApiToken, VaultTokenStoreConfig,
    vault_error::{VaultError, VaultWriteOutcome},
};
use platform_errors::{BpError, BpResult};
use reqwest::{Method, StatusCode, Url};
use secrecy::ExposeSecret;
use serde_json::{Value, json};

const TOKEN_HEADER: &str = "X-Vault-Token";

#[derive(Clone, Copy)]
pub(crate) enum WriteCondition {
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

    fn conflict(self, token_id: &str) -> BpError {
        match self {
            Self::Create => BpError::token_exists(token_id),
            Self::Update(_) => BpError::token_conflict(token_id),
        }
    }
}

pub(crate) struct VaultClient {
    client: reqwest::Client,
    config: VaultTokenStoreConfig,
}

impl VaultClient {
    pub(crate) fn new(config: VaultTokenStoreConfig) -> BpResult<Self> {
        let client = reqwest::Client::builder()
            .timeout(config.timeout())
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| BpError::auth_store("client build", config.address(), false, e))?;
        Ok(Self { client, config })
    }

    pub(crate) async fn put(&self, token: &ApiToken, condition: WriteCondition) -> BpResult<()> {
        token.validate()?;
        let body = json!({
            "data": token.to_value(),
            "options": { "cas": condition.cas() },
        });
        let request = self
            .request(Method::PUT, self.config.url(&["data", token.id()])?)
            .json(&body);
        let response = self.send("write", token.id(), request).await?;
        if VaultWriteOutcome::from_status(response.status()) == VaultWriteOutcome::CasMismatch {
            return Err(condition.conflict(token.id()));
        }
        self.require_success("write", token.id(), response)
            .await
            .map(|_| ())
    }

    pub(crate) async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        Ok(self.get_entry(id).await?.map(|(token, _)| token))
    }

    pub(crate) async fn get_entry(&self, id: &str) -> BpResult<Option<(ApiToken, u64)>> {
        ApiToken::validate_id(id)?;
        let response = self
            .send(
                "read",
                id,
                self.request(Method::GET, self.config.url(&["data", id])?),
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = self.require_success("read", id, response).await?;
        let value = self.parse_json("read", id, response).await?;
        let data = value.pointer("/data/data").cloned().ok_or_else(|| {
            BpError::auth_store("read", id, false, "malformed Vault response".to_string())
        })?;
        let version = value
            .pointer("/data/metadata/version")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                BpError::auth_store("read", id, false, "missing Vault version".to_string())
            })?;
        let token = ApiToken::from_value(id, &data)
            .map_err(|error| BpError::auth_store("read", id, false, error))?;
        Ok(Some((token, version)))
    }

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

    pub(crate) async fn ids(&self) -> BpResult<Vec<String>> {
        let mut url = self.config.url(&["metadata"])?;
        url.query_pairs_mut().append_pair("list", "true");
        let response = self
            .send("list", "tokens", self.request(Method::GET, url))
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        let response = self.require_success("list", "tokens", response).await?;
        let value = self.parse_json("list", "tokens", response).await?;
        let keys = value
            .pointer("/data/keys")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                BpError::auth_store(
                    "list",
                    "tokens",
                    false,
                    "malformed Vault response".to_string(),
                )
            })?;
        let mut ids: Vec<String> = keys
            .iter()
            .filter_map(|key| key.as_str().map(str::to_string))
            .filter(|key| !key.ends_with('/') && ApiToken::validate_id(key).is_ok())
            .collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    fn request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        self.client
            .request(method, url)
            .header(TOKEN_HEADER, self.config.token.expose_secret())
    }

    async fn send(
        &self,
        operation: &'static str,
        target: &str,
        request: reqwest::RequestBuilder,
    ) -> BpResult<reqwest::Response> {
        request
            .send()
            .await
            .map_err(|e| VaultError::transport(operation, target, e))
    }

    async fn require_success(
        &self,
        operation: &'static str,
        target: &str,
        response: reqwest::Response,
    ) -> BpResult<reqwest::Response> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let headers = response.headers().clone();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable body>".to_string());
        Err(VaultError::response(
            operation, target, status, &headers, body,
        ))
    }

    async fn parse_json(
        &self,
        operation: &'static str,
        target: &str,
        response: reqwest::Response,
    ) -> BpResult<Value> {
        response
            .json::<Value>()
            .await
            .map_err(|e| BpError::auth_store(operation, target, false, e))
    }
}
