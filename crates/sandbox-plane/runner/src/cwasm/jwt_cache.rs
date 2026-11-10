use platform_errors::{BpError, BpResult};
use platform_messages::jwt::{JwtRequest, JwtResponse};
use platform_types::url_scheme::UrlScheme;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

const REFRESH_MARGIN_SECS: u64 = 30;

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
    cached: Mutex<Option<CachedJwt>>,
}

impl JwtCache {
    pub fn new(url: &str, owner: impl Into<String>) -> BpResult<Self> {
        let url = reqwest::Url::parse(url)
            .map_err(|error| BpError::config_invalid_source("auth_service_url", error))?;
        if !UrlScheme::from_url(&url).is_some_and(UrlScheme::is_http) {
            return Err(BpError::config_invalid(
                "auth_service_url",
                "must use the http or https scheme",
            ));
        }
        let owner = owner.into();
        if owner.trim().is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        let client = reqwest::Client::builder()
            .build()
            .map_err(BpError::external)?;
        Ok(Self {
            url,
            owner,
            client,
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
        let response = self
            .client
            .post(self.url.clone())
            .json(&JwtRequest::new(self.owner.clone()))
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
