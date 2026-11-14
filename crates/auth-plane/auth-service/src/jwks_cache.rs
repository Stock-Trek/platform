use crate::{
    JwtVerifier, OrderJwtClaims,
    jwks_fetcher::{JwksFetcher, Source},
};
use chrono::{DateTime, Utc};
use jsonwebtoken::{Header, decode_header, jwk::JwkSet};
use parking_lot::Mutex;
use platform_errors::{PlatformError, PlatformResult};
use platform_time::RetryPolicy;
use std::{io, time::Duration};
use tokio::{sync::watch, time::Instant};

const DEFAULT_JWKS_TTL: Duration = Duration::from_secs(300);
const FAILURE_BACKOFF: Duration = Duration::from_secs(1);
const FAILURE_RETRY: RetryPolicy = RetryPolicy::constant(1, FAILURE_BACKOFF).without_jitter();

#[derive(Default)]
struct CacheState {
    verifier: Option<JwtVerifier>,
    fetched_at: Option<Instant>,
    refreshing: bool,
    retry_after: Option<Instant>,
    generation: u64,
}

enum Action {
    Verify(JwtVerifier),
    Refresh,
    Wait,
    Unavailable,
}

pub struct JwksCache {
    source: Source,
    ttl: Duration,
    state: Mutex<CacheState>,
    refresh: watch::Sender<u64>,
}

impl JwksCache {
    pub fn new(url: impl Into<String>) -> PlatformResult<Self> {
        Self::with_ttl(url, DEFAULT_JWKS_TTL)
    }

    pub fn with_ttl(url: impl Into<String>, ttl: Duration) -> PlatformResult<Self> {
        if ttl.is_zero() {
            return Err(PlatformError::auth_invalid_field(
                "jwks_ttl",
                "must be greater than zero",
            ));
        }
        let fetcher = JwksFetcher::new(url)?;
        let (refresh, _) = watch::channel(0);
        Ok(Self {
            source: Source::Remote(fetcher),
            ttl,
            state: Mutex::new(CacheState::default()),
            refresh,
        })
    }

    pub fn from_jwks(jwks: JwkSet) -> Self {
        let (refresh, _) = watch::channel(0);
        Self {
            source: Source::Static,
            ttl: DEFAULT_JWKS_TTL,
            state: Mutex::new(CacheState {
                verifier: Some(JwtVerifier::new(jwks)),
                ..CacheState::default()
            }),
            refresh,
        }
    }

    pub async fn verify(&self, jwt: &str, now: DateTime<Utc>) -> PlatformResult<OrderJwtClaims> {
        let header = decode_header(jwt)
            .map_err(|error| PlatformError::auth_invalid_jwt(error.to_string()))?;
        match &self.source {
            Source::Static => {
                let verifier = self.state.lock().verifier.clone();
                verifier
                    .ok_or_else(|| PlatformError::auth_invalid_jwt("JWKS cache is empty"))?
                    .verify_with_header(&header, jwt, now)
            }
            Source::Remote(_) => self.verify_remote(&header, jwt, now).await,
        }
    }

    async fn verify_remote(
        &self,
        header: &Header,
        jwt: &str,
        now: DateTime<Utc>,
    ) -> PlatformResult<OrderJwtClaims> {
        loop {
            let mut refreshed = self.refresh.subscribe();
            match self.action(header) {
                Action::Verify(verifier) => {
                    return verifier.verify_with_header(header, jwt, now);
                }
                Action::Unavailable => {
                    return Err(PlatformError::external(io::Error::other(
                        "JWKS refresh is backing off after an earlier failure",
                    )));
                }
                Action::Wait => {
                    let _ = refreshed.changed().await;
                }
                Action::Refresh => {
                    let fetched = self.source.fetch().await;
                    match fetched {
                        Ok(verifier) => {
                            self.install(verifier.clone());
                            return verifier.verify_with_header(header, jwt, now);
                        }
                        Err(error) => {
                            self.record_failure();
                            if let Some(verifier) = self.stale_verifier(header) {
                                return verifier.verify_with_header(header, jwt, now);
                            }
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    fn action(&self, header: &Header) -> Action {
        let mut state = self.state.lock();
        let now = Instant::now();
        if state.refreshing {
            return match &state.verifier {
                Some(verifier) if verifier.has_key_for_header(header) => {
                    Action::Verify(verifier.clone())
                }
                _ => Action::Wait,
            };
        }
        let is_fresh = state
            .fetched_at
            .is_some_and(|fetched_at| fetched_at.elapsed() < self.ttl);
        if let Some(verifier) = &state.verifier {
            if is_fresh && verifier.has_key_for_header(header) {
                return Action::Verify(verifier.clone());
            }
            if state
                .retry_after
                .is_some_and(|retry_after| now < retry_after)
            {
                return if verifier.has_key_for_header(header) {
                    Action::Verify(verifier.clone())
                } else {
                    Action::Unavailable
                };
            }
        } else if state
            .retry_after
            .is_some_and(|retry_after| now < retry_after)
        {
            return Action::Unavailable;
        }
        state.refreshing = true;
        Action::Refresh
    }

    fn stale_verifier(&self, header: &Header) -> Option<JwtVerifier> {
        self.state
            .lock()
            .verifier
            .clone()
            .filter(|verifier| verifier.has_key_for_header(header))
    }

    fn install(&self, verifier: JwtVerifier) {
        let generation = {
            let mut state = self.state.lock();
            state.verifier = Some(verifier);
            state.fetched_at = Some(Instant::now());
            state.refreshing = false;
            state.retry_after = None;
            state.generation = state.generation.wrapping_add(1);
            state.generation
        };
        self.refresh.send_replace(generation);
    }

    fn record_failure(&self) {
        let generation = {
            let mut state = self.state.lock();
            state.refreshing = false;
            state.retry_after = Some(Instant::now() + FAILURE_RETRY.delay(0));
            state.generation = state.generation.wrapping_add(1);
            state.generation
        };
        self.refresh.send_replace(generation);
    }
}
