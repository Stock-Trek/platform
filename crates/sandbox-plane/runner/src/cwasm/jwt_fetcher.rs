use platform_errors::{BpError, BpResult};
use platform_messages::jwt::{JwtRequest, JwtResponse};
use platform_sigv4::{SigV4Config, SigV4Signer};
use platform_types::url_scheme::UrlScheme;
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const REFRESH_MARGIN_SECS: u64 = 30;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) struct FetchedJwt {
    pub token: String,
    pub refresh_at: SystemTime,
    pub expires_at: SystemTime,
}

#[derive(Clone)]
pub(super) struct JwtFetcher {
    url: reqwest::Url,
    owner: String,
    client: reqwest::Client,
    signer: Option<SigV4Signer>,
}

impl JwtFetcher {
    pub fn with_sigv4(url: &str, owner: impl Into<String>, sigv4: &SigV4Config) -> BpResult<Self> {
        let owner = owner.into().trim().to_string();
        if owner.is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        let signer = sigv4.signer_for_owner(&owner)?;
        Self::with_signer(url, owner, Some(signer))
    }

    fn with_signer(url: &str, owner: String, signer: Option<SigV4Signer>) -> BpResult<Self> {
        let owner = owner.trim().to_string();
        if owner.is_empty() {
            return Err(BpError::auth_invalid_field("owner", "must not be empty"));
        }
        let url = reqwest::Url::parse(url)
            .map_err(|error| BpError::config_invalid_source("auth_service_url", error))?;
        if !UrlScheme::from_url(&url).is_some_and(UrlScheme::is_http) {
            return Err(BpError::config_invalid(
                "auth_service_url",
                "must use the http or https scheme",
            ));
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
        })
    }

    pub async fn fetch(&self) -> BpResult<FetchedJwt> {
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
        Ok(FetchedJwt {
            token: response.token().to_string(),
            refresh_at: SystemTime::now() + Duration::from_secs(refresh_after),
            expires_at: UNIX_EPOCH + Duration::from_secs(response.expires_at_unix() as u64),
        })
    }
}
