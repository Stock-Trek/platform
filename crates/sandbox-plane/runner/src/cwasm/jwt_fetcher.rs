use platform_errors::{BuildError, BuildResult};
use platform_messages::{JwtRequest, JwtResponse};
use platform_sigv4::{SigV4Config, SigV4Signer};
use platform_types::UrlScheme;
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;

const REFRESH_MARGIN_SECS: u64 = 30;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

pub struct FetchedJwt {
    pub token: String,
    pub refresh_at: Instant,
    pub expires_at: Instant,
}

#[derive(Clone)]
pub struct JwtFetcher {
    url: reqwest::Url,
    owner: String,
    client: reqwest::Client,
    signer: Option<SigV4Signer>,
}

impl JwtFetcher {
    pub fn with_sigv4(
        url: &str,
        owner: impl Into<String>,
        sigv4: &SigV4Config,
    ) -> BuildResult<Self> {
        let owner = owner.into().trim().to_string();
        if owner.is_empty() {
            return Err(BuildError::auth_invalid_field("owner", "must not be empty"));
        }
        let signer = sigv4.signer_for_owner(&owner)?;
        Self::with_signer(url, owner, Some(signer))
    }

    fn with_signer(url: &str, owner: String, signer: Option<SigV4Signer>) -> BuildResult<Self> {
        let owner = owner.trim().to_string();
        if owner.is_empty() {
            return Err(BuildError::auth_invalid_field("owner", "must not be empty"));
        }
        let url = reqwest::Url::parse(url)
            .map_err(|error| BuildError::config_invalid_source("auth_service_url", error))?;
        if !UrlScheme::from_url(&url).is_some_and(UrlScheme::is_http) {
            return Err(BuildError::config_invalid(
                "auth_service_url",
                "must use the http or https scheme",
            ));
        }
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(BuildError::external)?;
        Ok(Self {
            url,
            owner,
            client,
            signer,
        })
    }

    pub async fn fetch(&self) -> BuildResult<FetchedJwt> {
        let body = serde_json::to_vec(&JwtRequest::new(self.owner.clone()))
            .map_err(BuildError::external)?;
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
            .map_err(|error| BuildError::http_request("fetch JWT", self.url.as_str(), error))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(BuildError::http_rejected(
                "fetch JWT",
                self.url.as_str(),
                status.as_u16(),
                body,
            ));
        }
        let response: JwtResponse = response
            .json()
            .await
            .map_err(|error| BuildError::http_request("fetch JWT", self.url.as_str(), error))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(BuildError::external)?
            .as_secs() as i64;
        let lifetime = response.expires_at_unix() - now;
        if response.token().trim().is_empty() || lifetime <= 0 {
            return Err(BuildError::auth_invalid_jwt(
                "auth service returned an unusable JWT",
            ));
        }
        let refresh_after = if lifetime as u64 > REFRESH_MARGIN_SECS {
            lifetime as u64 - REFRESH_MARGIN_SECS
        } else {
            (lifetime as u64).max(1) / 2
        };
        let fetched_at = Instant::now();
        Ok(FetchedJwt {
            token: response.token().to_string(),
            refresh_at: fetched_at + Duration::from_secs(refresh_after),
            expires_at: fetched_at + Duration::from_secs(lifetime as u64),
        })
    }
}
