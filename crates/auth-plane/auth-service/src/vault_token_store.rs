use crate::{ApiToken, token_store::TokenStore};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use std::time::Duration;

const TOKEN_HEADER: &str = "X-Vault-Token";
const NOT_MODIFIED: u16 = 412;
const RATE_LIMITED: u16 = 429;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct VaultTokenStoreConfig {
    address: String,
    token: SecretString,
    mount: String,
    timeout: Duration,
}

impl VaultTokenStoreConfig {
    pub fn new(address: impl Into<String>, token: SecretString, mount: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            token,
            mount: mount.into(),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn mount(&self) -> &str {
        &self.mount
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn validate(&self) -> BpResult<()> {
        if self.address.trim().is_empty() {
            return Err(BpError::config_empty("vault_address"));
        }
        if self.token.expose_secret().is_empty() {
            return Err(BpError::config_empty("vault_token"));
        }
        if self.mount.trim().is_empty() {
            return Err(BpError::config_empty("vault_mount"));
        }
        Ok(())
    }
}

pub struct VaultTokenStore {
    client: reqwest::Client,
    config: VaultTokenStoreConfig,
}

impl VaultTokenStore {
    pub fn new(config: VaultTokenStoreConfig) -> BpResult<Self> {
        config.validate()?;
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| BpError::auth_store("client build", &config.address, false, e))?;
        Ok(Self { client, config })
    }

    fn base_url(&self) -> String {
        self.config.address.trim_end_matches('/').to_string()
    }

    fn data_url(&self, id: &str) -> String {
        format!("{}/v1/{}/data/{}", self.base_url(), self.config.mount, id)
    }

    fn metadata_url(&self, id: &str) -> String {
        if id.is_empty() {
            format!("{}/v1/{}/metadata", self.base_url(), self.config.mount)
        } else {
            format!(
                "{}/v1/{}/metadata/{}",
                self.base_url(),
                self.config.mount,
                id
            )
        }
    }

    fn request(&self, method: reqwest::Method, url: String) -> reqwest::RequestBuilder {
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
            .map_err(|e| BpError::auth_store(operation, target, true, e))
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
        let retryable = status.is_server_error()
            || status.as_u16() == RATE_LIMITED
            || status.as_u16() == NOT_MODIFIED;
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable body>".to_string());
        Err(BpError::auth_store(
            operation,
            target,
            retryable,
            format!("Vault answered {}: {body}", status.as_u16()),
        ))
    }

    async fn parse_json(
        &self,
        operation: &'static str,
        target: &str,
        response: reqwest::Response,
    ) -> BpResult<Value> {
        response.json::<Value>().await.map_err(|e| {
            BpError::auth_store(
                operation,
                target,
                false,
                format!("unreadable response: {e}"),
            )
        })
    }

    async fn read(&self, id: &str) -> BpResult<Option<ApiToken>> {
        let response = self
            .send(
                "read",
                id,
                self.request(reqwest::Method::GET, self.data_url(id)),
            )
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = self.require_success("read", id, response).await?;
        let value = self.parse_json("read", id, response).await?;
        let data = value.pointer("/data/data").cloned().ok_or_else(|| {
            BpError::auth_store("read", id, false, "malformed Vault response".to_string())
        })?;
        ApiToken::from_value(id, &data).map(Some)
    }

    async fn list_ids(&self) -> BpResult<Vec<String>> {
        let url = format!("{}?list=true", self.metadata_url(""));
        let response = self
            .send(
                "list",
                url.as_str(),
                self.request(reqwest::Method::GET, url.clone()),
            )
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        let response = self.require_success("list", url.as_str(), response).await?;
        let value = self.parse_json("list", url.as_str(), response).await?;
        let keys = value
            .pointer("/data/keys")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| {
                BpError::auth_store(
                    "list",
                    url.as_str(),
                    false,
                    "malformed Vault response".to_string(),
                )
            })?;
        Ok(keys
            .iter()
            .filter_map(|key| key.as_str().map(str::to_string))
            .filter(|key| !key.ends_with('/'))
            .collect())
    }
}

#[async_trait]
impl TokenStore for VaultTokenStore {
    async fn put(&self, token: &ApiToken) -> BpResult<()> {
        let body = json!({ "data": token.to_value() });
        let request = self
            .request(reqwest::Method::PUT, self.data_url(token.id()))
            .json(&body);
        let response = self.send("write", token.id(), request).await?;
        self.require_success("write", token.id(), response)
            .await
            .map(|_| ())
    }

    async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        self.read(id).await
    }

    async fn delete(&self, id: &str) -> BpResult<()> {
        let response = self
            .send(
                "delete",
                id,
                self.request(reqwest::Method::DELETE, self.metadata_url(id)),
            )
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(BpError::unknown_token(id));
        }
        self.require_success("delete", id, response)
            .await
            .map(|_| ())
    }

    async fn ids(&self) -> BpResult<Vec<String>> {
        self.list_ids().await
    }
}
