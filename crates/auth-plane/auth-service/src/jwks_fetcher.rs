use crate::JwtVerifier;
use jsonwebtoken::jwk::JwkSet;
use platform_errors::{BuildError, BuildResult};
use std::{io, time::Duration};

const JWKS_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub enum Source {
    Remote(JwksFetcher),
    Static,
}

pub struct JwksFetcher {
    client: reqwest::Client,
    url: String,
}

impl Source {
    pub async fn fetch(&self) -> BuildResult<JwtVerifier> {
        match self {
            Self::Remote(fetcher) => fetcher.fetch().await,
            Self::Static => Err(BuildError::auth_invalid_jwt("JWKS source is not remote")),
        }
    }
}

impl JwksFetcher {
    pub fn new(url: impl Into<String>) -> BuildResult<Self> {
        let url = url.into();
        let parsed = reqwest::Url::parse(&url)
            .map_err(|error| BuildError::auth_invalid_field("jwks_url", error.to_string()))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(BuildError::auth_invalid_field(
                "jwks_url",
                "must use http or https",
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(JWKS_REQUEST_TIMEOUT)
            .build()
            .map_err(BuildError::external)?;
        Ok(Self { client, url })
    }

    pub async fn fetch(&self) -> BuildResult<JwtVerifier> {
        let response = self
            .client
            .get(&self.url)
            .send()
            .await
            .map_err(BuildError::external)?;
        if !response.status().is_success() {
            return Err(BuildError::external(io::Error::other(format!(
                "JWKS request failed with status {}",
                response.status()
            ))));
        }
        let jwks = response
            .json::<JwkSet>()
            .await
            .map_err(BuildError::external)?;
        if jwks.keys.is_empty() {
            return Err(BuildError::auth_invalid_jwt("JWKS contains no keys"));
        }
        Ok(JwtVerifier::new(jwks))
    }
}
