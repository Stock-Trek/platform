use platform_errors::{BpError, BpResult};
use platform_messages::jwt::{JwtRequest, JwtResponse};
use platform_sigv4::{SigV4Config, SigV4Signer};
use platform_types::url_scheme::UrlScheme;
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

const REFRESH_MARGIN_SECS: u64 = 30;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

struct CachedJwt {
    token: String,
    refresh_at: SystemTime,
}

impl CachedJwt {
    fn is_fresh(&self, now: SystemTime) -> bool {
        now < self.refresh_at
    }
}

pub struct JwtCache {
    url: reqwest::Url,
    owner: String,
    client: reqwest::Client,
    signer: Option<SigV4Signer>,
    cached: Mutex<Option<CachedJwt>>,
}

impl JwtCache {
    #[allow(dead_code)]
    pub fn new(url: &str, owner: impl Into<String>) -> BpResult<Self> {
        Self::with_optional_signer(url, owner.into(), None)
    }

    pub fn with_sigv4(url: &str, owner: impl Into<String>, sigv4: &SigV4Config) -> BpResult<Self> {
        let owner = owner.into();
        if owner.trim().is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        let signer = sigv4.signer_for_owner(&owner)?;
        Self::with_optional_signer(url, owner, Some(signer))
    }

    fn with_optional_signer(
        url: &str,
        owner: String,
        signer: Option<SigV4Signer>,
    ) -> BpResult<Self> {
        let url = reqwest::Url::parse(url)
            .map_err(|error| BpError::config_invalid_source("auth_service_url", error))?;
        if !UrlScheme::from_url(&url).is_some_and(UrlScheme::is_http) {
            return Err(BpError::config_invalid(
                "auth_service_url",
                "must use the http or https scheme",
            ));
        }
        if owner.trim().is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(BpError::external)?;
        Ok(Self {
            url,
            owner,
            client,
            signer,
            cached: Mutex::new(None),
        })
    }

    pub async fn token(&self) -> BpResult<String> {
        if let Some(token) = self.cached_token().await {
            return Ok(token);
        }
        let mut cached = self.cached.lock().await;
        if let Some(value) = cached.as_ref()
            && value.is_fresh(SystemTime::now())
        {
            return Ok(value.token.clone());
        }
        let fetched = self.fetch().await?;
        let token = fetched.token.clone();
        *cached = Some(fetched);
        Ok(token)
    }

    async fn cached_token(&self) -> Option<String> {
        let cached = self.cached.lock().await;
        cached
            .as_ref()
            .filter(|value| value.is_fresh(SystemTime::now()))
            .map(|value| value.token.clone())
    }

    async fn fetch(&self) -> BpResult<CachedJwt> {
        let body =
            serde_json::to_vec(&JwtRequest::new(self.owner.clone())).map_err(BpError::external)?;
        let mut headers =
            BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
        if let Some(signer) = &self.signer {
            signer.sign(
                "POST",
                self.url.as_str(),
                &mut headers,
                &body,
                SystemTime::now(),
            )?;
        }
        let mut request = self.client.post(self.url.clone()).body(body);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request
            .send()
            .await
            .map_err(|error| BpError::http_request("fetch JWT", self.url.as_str(), error))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(BpError::http_rejected(
                "fetch JWT",
                self.url.as_str(),
                status.as_u16(),
                body,
            ));
        }
        let response: JwtResponse = response
            .json()
            .await
            .map_err(|error| BpError::http_request("fetch JWT", self.url.as_str(), error))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(BpError::external)?
            .as_secs() as i64;
        let lifetime = response.expires_at_unix() - now;
        if response.token().trim().is_empty() || lifetime <= 0 {
            return Err(BpError::auth_invalid_jwt(
                "auth service returned an unusable JWT",
            ));
        }
        let refresh_after = if lifetime as u64 > REFRESH_MARGIN_SECS {
            lifetime as u64 - REFRESH_MARGIN_SECS
        } else {
            (lifetime as u64).max(1) / 2
        };
        Ok(CachedJwt {
            token: response.token().to_string(),
            refresh_at: SystemTime::now() + Duration::from_secs(refresh_after),
        })
    }
}
