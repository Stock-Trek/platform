use crate::clients::client::HttpClient;
use async_trait::async_trait;
use platform_errors::{BpError, BpResult};
use platform_exchange_types::http::{HttpMethod, HttpRequest, HttpResponse};
use std::time::Duration;

#[derive(Clone)]
pub struct ReqwestHttpClient {
    client: reqwest::Client,
    base_url: String,
}

impl ReqwestHttpClient {
    pub fn try_new(base_url: &str) -> BpResult<Self> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(BpError::external)?;
        Ok(Self::with_client(base_url.trim_end_matches('/'), client))
    }
    pub fn with_client(base_url: &str, client: reqwest::Client) -> Self {
        Self {
            client,
            base_url: base_url.into(),
        }
    }
    fn build_url(&self, path: &str, query: Option<&str>) -> String {
        let mut url = format!("{}/{}", self.base_url, path.trim_start_matches('/'));
        if let Some(query) = query.filter(|query| !query.is_empty()) {
            url.push('?');
            url.push_str(query);
        }
        url
    }
    fn method(method: HttpMethod) -> reqwest::Method {
        match method {
            HttpMethod::GET => reqwest::Method::GET,
            HttpMethod::DELETE => reqwest::Method::DELETE,
            HttpMethod::PATCH => reqwest::Method::PATCH,
            HttpMethod::POST => reqwest::Method::POST,
            HttpMethod::PUT => reqwest::Method::PUT,
        }
    }
}

#[async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn send(&self, request: HttpRequest, timeout: Duration) -> BpResult<HttpResponse> {
        let url = self.build_url(&request.path, request.query.as_deref());
        let mut request_builder = self
            .client
            .request(Self::method(request.method), &url)
            .timeout(timeout);
        for (name, value) in &request.headers {
            request_builder = request_builder.header(name, value);
        }
        if let Some(body) = request.body {
            request_builder = request_builder.body(body);
        }
        let response = request_builder.send().await.map_err(|error| {
            // A connect error proves the request never reached the wire, so it is
            // definitely not sent. Everything else (including a timeout after the
            // connection was established, or a body/decode error) may have reached
            // the exchange, so the outcome is unknown. `is_connect()` is checked
            // first because a connect timeout satisfies both predicates.
            if error.is_connect() || error.is_builder() {
                BpError::send_not_sent_external(error)
            } else {
                BpError::send_unknown_external(error)
            }
        })?;
        let status = response.status();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_ascii_lowercase(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect();
        let body = response
            .bytes()
            .await
            .map_err(BpError::send_unknown_external)?
            .to_vec();
        Ok(HttpResponse {
            status: status.as_u16(),
            headers,
            body,
        })
    }
}

impl std::fmt::Debug for ReqwestHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqwestHttpClient")
            .field("client", &"<reqwest::Client>")
            .field("base_url", &self.base_url)
            .finish()
    }
}
