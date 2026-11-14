use crate::{VaultTokenStoreConfig, vault_error::VaultError};
use platform_errors::{BuildError, BuildResult};
use reqwest::{Method, Url};
use secrecy::ExposeSecret;
use serde_json::Value;

const TOKEN_HEADER: &str = "X-Vault-Token";
pub const TOMBSTONE_FIELD: &str = "__platform_deleted__";

pub use crate::vault_write::WriteCondition;

pub struct VaultClient {
    pub client: reqwest::Client,
    pub config: VaultTokenStoreConfig,
}

impl VaultClient {
    pub fn new(config: VaultTokenStoreConfig) -> BuildResult<Self> {
        let client = reqwest::Client::builder()
            .timeout(config.timeout())
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| BuildError::auth_store("client build", config.address(), false, e))?;
        Ok(Self { client, config })
    }

    pub fn url_for_path(&self, area: &str, path: &str) -> BuildResult<Url> {
        let mut segments = vec![area];
        segments.extend(path.split('/').filter(|segment| !segment.is_empty()));
        self.config.url(&segments)
    }

    pub fn request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        self.client
            .request(method, url)
            .header(TOKEN_HEADER, self.config.token.expose_secret())
    }

    pub async fn send(
        &self,
        operation: &'static str,
        target: &str,
        request: reqwest::RequestBuilder,
    ) -> BuildResult<reqwest::Response> {
        request
            .send()
            .await
            .map_err(|e| VaultError::transport(operation, target, e))
    }

    pub async fn require_success(
        &self,
        operation: &'static str,
        target: &str,
        response: reqwest::Response,
    ) -> BuildResult<reqwest::Response> {
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

    pub async fn parse_json(
        &self,
        operation: &'static str,
        target: &str,
        response: reqwest::Response,
    ) -> BuildResult<Value> {
        response
            .json::<Value>()
            .await
            .map_err(|e| BuildError::auth_store(operation, target, false, e))
    }
}
