use crate::api::ApiRequest;
use crate::api::Problem;
use crate::auth::SessionStore;
use crate::auth::{AuthContext, Scope};
use crate::auth::{LoginRequest, RefreshRequest, SessionTokens, StepUpRequest, StepUpTokens};
use crate::limits::{LimitService, Throttle};
use chrono::{DateTime, Duration, Utc};
use std::net::IpAddr;
use std::sync::Arc;

const LOGIN_THROTTLE_LIMIT: u64 = 10;
const LOGIN_THROTTLE_WINDOW: Duration = Duration::minutes(1);

pub struct SessionService {
    store: Arc<SessionStore>,
    limits: Arc<LimitService>,
    failed_logins: Throttle,
}

impl SessionService {
    pub fn new(store: Arc<SessionStore>, limits: Arc<LimitService>) -> Self {
        Self {
            store,
            limits,
            failed_logins: Throttle::new(LOGIN_THROTTLE_WINDOW),
        }
    }

    pub fn login(
        &self,
        request: &LoginRequest,
        source: Option<IpAddr>,
        now: DateTime<Utc>,
    ) -> Result<SessionTokens, Problem> {
        for key in Self::throttle_keys(source, Some(&request.login_id)) {
            if let Some(retry_after) = self.failed_logins.blocked(&key, LOGIN_THROTTLE_LIMIT, now) {
                return Err(Problem::rate_limited(retry_after));
            }
        }
        match self
            .store
            .verify_login(&request.login_id, &request.login_secret)
        {
            Some(owner) => Ok(self.store.issue_login(&owner, now)),
            None => {
                for key in Self::throttle_keys(source, Some(&request.login_id)) {
                    self.failed_logins.hit(&key, now);
                }
                Err(Problem::unauthorized("invalid or unknown credentials"))
            }
        }
    }

    pub fn refresh(&self, request: &RefreshRequest) -> Result<SessionTokens, Problem> {
        self.store
            .rotate_refresh(&request.refresh_token, Utc::now())
            .ok_or_else(|| {
                Problem::unauthorized("the refresh token is unknown, rotated, or expired")
            })
    }

    pub fn step_up(
        &self,
        auth: &AuthContext,
        request: &StepUpRequest,
        source: Option<IpAddr>,
        now: DateTime<Utc>,
    ) -> Result<StepUpTokens, Problem> {
        for key in Self::throttle_keys(source, None) {
            if let Some(retry_after) = self.failed_logins.blocked(&key, LOGIN_THROTTLE_LIMIT, now) {
                return Err(Problem::rate_limited(retry_after));
            }
        }
        match self
            .store
            .verify_login_by_owner(auth.owner(), &request.login_secret)
        {
            Some(owner) => Ok(self.store.issue_step_up(&owner, now)),
            None => {
                for key in Self::throttle_keys(source, None) {
                    self.failed_logins.hit(&key, now);
                }
                Err(Problem::unauthorized("invalid or unknown credentials"))
            }
        }
    }

    pub fn revoke(&self, auth: &AuthContext) {
        self.store.revoke(auth.token());
    }

    pub fn authorize(&self, token: &str, now: DateTime<Utc>) -> Result<AuthContext, Problem> {
        let (owner, scopes) = self
            .store
            .access(token, now)
            .ok_or_else(|| Problem::unauthorized("the token is missing, expired, or revoked"))?;
        Ok(AuthContext::new(owner, token, scopes))
    }

    pub fn authorize_request(
        &self,
        request: &ApiRequest,
        scope: Scope,
    ) -> Result<AuthContext, Problem> {
        let auth = self.authorize(request.bearer()?, Utc::now())?;
        self.limits.check_request_rate(auth.owner(), Utc::now())?;
        auth.require(scope)?;
        Ok(auth)
    }

    fn throttle_keys(source: Option<IpAddr>, login_id: Option<&str>) -> Vec<String> {
        let mut keys = Vec::new();
        if let Some(source) = source {
            keys.push(format!("source:{source}"));
        }
        if let Some(login_id) = login_id {
            keys.push(format!("login:{login_id}"));
        }
        keys
    }
}
