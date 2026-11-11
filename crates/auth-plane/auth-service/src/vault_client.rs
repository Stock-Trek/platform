use crate::{VaultTokenStoreConfig, vault_error::VaultError};
use platform_errors::{BpError, BpResult};
use reqwest::{Method, Url};
use secrecy::ExposeSecret;
use serde_json::Value;

const TOKEN_HEADER: &str = "X-Vault-Token";
pub(crate) const TOMBSTONE_FIELD: &str = "__platform_deleted__";

pub(crate) use crate::vault_write::WriteCondition;

pub(crate) struct VaultClient {
    pub(crate) client: reqwest::Client,
    pub(crate) config: VaultTokenStoreConfig,
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

    pub(crate) fn url_for_path(&self, area: &str, path: &str) -> BpResult<Url> {
        let mut segments = vec![area];
        segments.extend(path.split('/').filter(|segment| !segment.is_empty()));
        self.config.url(&segments)
    }

    pub(crate) fn request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        self.client
            .request(method, url)
            .header(TOKEN_HEADER, self.config.token.expose_secret())
    }

    pub(crate) async fn send(
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

    pub(crate) async fn require_success(
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

    pub(crate) async fn parse_json(
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
