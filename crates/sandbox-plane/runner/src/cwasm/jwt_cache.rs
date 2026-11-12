use super::jwt_fetcher::{FetchedJwt, JwtFetcher};
use parking_lot::Mutex;
use platform_errors::{BuildError, BuildResult};
use platform_sigv4::SigV4Config;
use std::{
    io,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::watch;

const FAILURE_BACKOFF: Duration = Duration::from_secs(1);

struct CachedJwt {
    token: String,
    refresh_at: SystemTime,
    expires_at: SystemTime,
}

impl CachedJwt {
    fn is_fresh(&self, now: SystemTime) -> bool {
        now < self.refresh_at
    }

    fn is_usable(&self, now: SystemTime) -> bool {
        now < self.expires_at
    }
}

impl From<FetchedJwt> for CachedJwt {
    fn from(fetched: FetchedJwt) -> Self {
        Self {
            token: fetched.token,
            refresh_at: fetched.refresh_at,
            expires_at: fetched.expires_at,
        }
    }
}

#[derive(Default)]
struct CacheState {
    cached: Option<CachedJwt>,
    refreshing: bool,
    retry_after: Option<SystemTime>,
    generation: u64,
}

enum Action {
    Return(String),
    Refresh,
    Wait,
    Unavailable,
}

pub struct JwtCache {
    fetcher: Arc<JwtFetcher>,
    state: Mutex<CacheState>,
    refresh: watch::Sender<u64>,
}

impl JwtCache {
    pub fn with_sigv4(
        url: &str,
        owner: impl Into<String>,
        sigv4: &SigV4Config,
    ) -> BuildResult<Self> {
        Ok(Self::from_fetcher(JwtFetcher::with_sigv4(
            url, owner, sigv4,
        )?))
    }

    fn from_fetcher(fetcher: JwtFetcher) -> Self {
        let (refresh, _) = watch::channel(0);
        Self {
            fetcher: Arc::new(fetcher),
            state: Mutex::new(CacheState::default()),
            refresh,
        }
    }

    pub async fn token(&self) -> BuildResult<String> {
        loop {
            let mut refreshed = self.refresh.subscribe();
            match self.action() {
                Action::Return(token) => return Ok(token),
                Action::Unavailable => {
                    return Err(BuildError::external(io::Error::other(
                        "JWT refresh is backing off after an earlier failure",
                    )));
                }
                Action::Wait => {
                    let _ = refreshed.changed().await;
                }
                Action::Refresh => {
                    let fetched = self.fetcher.fetch().await;
                    match fetched {
                        Ok(fetched) => {
                            let token = fetched.token.clone();
                            self.install(fetched);
                            return Ok(token);
                        }
                        Err(error) => {
                            self.record_failure();
                            if let Some(token) = self.usable_token() {
                                return Ok(token);
                            }
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    fn action(&self) -> Action {
        let mut state = self.state.lock();
        let now = SystemTime::now();
        if state.refreshing {
            return match &state.cached {
                Some(cached) if cached.is_usable(now) => Action::Return(cached.token.clone()),
                _ => Action::Wait,
            };
        }
        if let Some(cached) = &state.cached {
            if cached.is_fresh(now) {
                return Action::Return(cached.token.clone());
            }
            if state
                .retry_after
                .is_some_and(|retry_after| now < retry_after)
            {
                return if cached.is_usable(now) {
                    Action::Return(cached.token.clone())
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

    fn usable_token(&self) -> Option<String> {
        let now = SystemTime::now();
        self.state
            .lock()
            .cached
            .as_ref()
            .filter(|cached| cached.is_usable(now))
            .map(|cached| cached.token.clone())
    }

    fn install(&self, fetched: FetchedJwt) {
        let generation = {
            let mut state = self.state.lock();
            state.cached = Some(fetched.into());
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
            state.retry_after = Some(SystemTime::now() + FAILURE_BACKOFF);
            state.generation = state.generation.wrapping_add(1);
            state.generation
        };
        self.refresh.send_replace(generation);
    }
}
