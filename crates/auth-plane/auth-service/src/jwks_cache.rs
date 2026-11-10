use crate::{JwtVerifier, OrderJwtClaims};
use chrono::{DateTime, Utc};
use jsonwebtoken::{Header, decode_header, jwk::JwkSet};
use platform_errors::{BpError, BpResult};
use std::{
    io,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

const DEFAULT_JWKS_TTL: Duration = Duration::from_secs(300);
const JWKS_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

enum Source {
    Remote {
        client: reqwest::Client,
        url: String,
    },
    Static,
}

#[derive(Default)]
struct CacheState {
    verifier: Option<JwtVerifier>,
    fetched_at: Option<Instant>,
}

pub struct JwksCache {
    source: Source,
    ttl: Duration,
    state: Mutex<CacheState>,
}

impl JwksCache {
    pub fn new(url: impl Into<String>) -> BpResult<Self> {
        Self::with_ttl(url, DEFAULT_JWKS_TTL)
    }

    pub fn with_ttl(url: impl Into<String>, ttl: Duration) -> BpResult<Self> {
        if ttl.is_zero() {
            return Err(BpError::auth_invalid_field(
                "jwks_ttl",
                "must be greater than zero",
            ));
        }
        let url = url.into();
        let parsed = reqwest::Url::parse(&url)
            .map_err(|error| BpError::auth_invalid_field("jwks_url", error.to_string()))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(BpError::auth_invalid_field(
                "jwks_url",
                "must use http or https",
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(JWKS_REQUEST_TIMEOUT)
            .build()
            .map_err(BpError::external)?;
        Ok(Self {
            source: Source::Remote { client, url },
            ttl,
            state: Mutex::new(CacheState::default()),
        })
    }

    pub fn from_jwks(jwks: JwkSet) -> Self {
        Self {
            source: Source::Static,
            ttl: DEFAULT_JWKS_TTL,
            state: Mutex::new(CacheState {
                verifier: Some(JwtVerifier::new(jwks)),
                fetched_at: None,
            }),
        }
    }

    pub async fn verify(&self, jwt: &str, now: DateTime<Utc>) -> BpResult<OrderJwtClaims> {
        let header =
            decode_header(jwt).map_err(|error| BpError::auth_invalid_jwt(error.to_string()))?;
        match &self.source {
            Source::Static => {
                let state = self.state.lock().await;
                state
                    .verifier
                    .as_ref()
                    .ok_or_else(|| BpError::auth_invalid_jwt("JWKS cache is empty"))?
                    .verify_with_header(&header, jwt, now)
            }
            Source::Remote { .. } => self.verify_remote(&header, jwt, now).await,
        }
    }

    async fn verify_remote(
        &self,
        header: &Header,
        jwt: &str,
        now: DateTime<Utc>,
    ) -> BpResult<OrderJwtClaims> {
        let mut state = self.state.lock().await;
        let is_fresh = state
            .fetched_at
            .is_some_and(|fetched_at| fetched_at.elapsed() < self.ttl);
        if is_fresh
            && let Some(verifier) = &state.verifier
            && verifier.has_key_for_header(header)
        {
            return verifier.verify_with_header(header, jwt, now);
        }
        match self.fetch().await {
            Ok(verifier) => {
                let verified = verifier.verify_with_header(header, jwt, now);
                *state = CacheState {
                    verifier: Some(verifier),
                    fetched_at: Some(Instant::now()),
                };
                verified
            }
            Err(error) => {
                if let Some(verifier) = &state.verifier
                    && verifier.has_key_for_header(header)
                {
                    return verifier.verify_with_header(header, jwt, now);
                }
                Err(error)
            }
        }
    }

    async fn fetch(&self) -> BpResult<JwtVerifier> {
        let Source::Remote { client, url } = &self.source else {
            return Err(BpError::auth_invalid_jwt("JWKS source is not remote"));
        };
        let response = client.get(url).send().await.map_err(BpError::external)?;
        if !response.status().is_success() {
            return Err(BpError::external(io::Error::other(format!(
                "JWKS request failed with status {}",
                response.status()
            ))));
        }
        let jwks = response.json::<JwkSet>().await.map_err(BpError::external)?;
        if jwks.keys.is_empty() {
            return Err(BpError::auth_invalid_jwt("JWKS contains no keys"));
        }
        Ok(JwtVerifier::new(jwks))
    }
}
