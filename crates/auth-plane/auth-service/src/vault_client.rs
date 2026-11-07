use crate::{ApiToken, VaultTokenStoreConfig, vault_error::VaultError};
use platform_errors::{BpError, BpResult};
use reqwest::{Method, StatusCode, Url};
use secrecy::ExposeSecret;
use serde_json::{Value, json};

const TOKEN_HEADER: &str = "X-Vault-Token";

pub(crate) struct VaultClient {
    client: reqwest::Client,
    config: VaultTokenStoreConfig,
}

impl VaultClient {
    pub(crate) fn new(config: VaultTokenStoreConfig) -> BpResult<Self> {
        config.validate()?;
        let client = reqwest::Client::builder()
            .timeout(config.timeout())
            .build()
            .map_err(|e| BpError::auth_store("client build", config.address(), false, e))?;
        Ok(Self { client, config })
    }

    pub(crate) async fn put(&self, token: &ApiToken, create_only: bool) -> BpResult<()> {
        ApiToken::validate_id(token.id())?;
        let mut body = json!({ "data": token.to_value() });
        if create_only {
            body["options"] = json!({ "cas": 0 });
        }
        let request = self
            .request(Method::PUT, self.url(&["data", token.id()])?)
            .json(&body);
        let response = self.send("write", token.id(), request).await?;
        let status = response.status();
        if create_only && matches!(status, StatusCode::BAD_REQUEST | StatusCode::CONFLICT) {
            let headers = response.headers().clone();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable body>".to_string());
            if VaultError::duplicate_write(status, &body) {
                return Err(BpError::token_exists(token.id()));
            }
            return Err(VaultError::response(
                "write",
                token.id(),
                status,
                &headers,
                body,
            ));
        }
        self.require_success("write", token.id(), response)
            .await
            .map(|_| ())
    }

    pub(crate) async fn get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        ApiToken::validate_id(id)?;
        let response = self
            .send(
                "read",
                id,
                self.request(Method::GET, self.url(&["data", id])?),
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
        ApiToken::from_value(id, &data).map(Some)
    }

    pub(crate) async fn delete(&self, id: &str) -> BpResult<()> {
        ApiToken::validate_id(id)?;
        let response = self
            .send(
                "delete",
                id,
                self.request(Method::DELETE, self.url(&["metadata", id])?),
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
        let mut url = self.url(&["metadata"])?;
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
            .filter(|key| !key.ends_with('/'))
            .collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    fn url(&self, suffix: &[&str]) -> BpResult<Url> {
        let address = self.config.address().trim();
        let mut url = Url::parse(address.trim_end_matches('/'))
            .map_err(|e| BpError::config_invalid_source("vault_address", e))?;
        url.set_query(None);
        url.set_fragment(None);
        {
            let mut segments = url.path_segments_mut().map_err(|_| {
                BpError::config_invalid("vault_address", "must be a hierarchical URL")
            })?;
            segments.pop_if_empty();
            segments.push("v1");
            for segment in self.config.mount().split('/') {
                segments.push(segment);
            }
            for segment in suffix {
                segments.push(segment);
            }
        }
        Ok(url)
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
