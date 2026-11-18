use crate::api::ApiRequest;
use crate::api::Problem;
use crate::auth::SessionStore;
use crate::auth::{AuthContext, Scope};
use crate::auth::{LoginRequest, RefreshRequest, SessionTokens, StepUpRequest, StepUpTokens};
use chrono::Utc;
use std::sync::Arc;

pub struct SessionService {
    store: Arc<SessionStore>,
}

impl SessionService {
    pub fn new(store: Arc<SessionStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &Arc<SessionStore> {
        &self.store
    }

    pub fn login(&self, request: &LoginRequest) -> Result<SessionTokens, Problem> {
        // TODO: throttle failed attempts per source address and per loginId.
        let owner = self
            .store
            .verify_login(&request.login_id, &request.login_secret)
            .ok_or_else(|| Problem::unauthorized("invalid or unknown credentials"))?;
        Ok(self.store.issue_login(&owner, Utc::now()))
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
    ) -> Result<StepUpTokens, Problem> {
        let owner = self
            .store
            .verify_login_by_owner(auth.owner(), &request.login_secret)
            .ok_or_else(|| Problem::unauthorized("invalid or unknown credentials"))?;
        Ok(self.store.issue_step_up(&owner, Utc::now()))
    }

    pub fn revoke(&self, auth: &AuthContext) {
        self.store.revoke(auth.token());
    }

    pub fn authorize(&self, token: &str) -> Result<AuthContext, Problem> {
        let (owner, scopes) = self
            .store
            .access(token, Utc::now())
            .ok_or_else(|| Problem::unauthorized("the token is missing, expired, or revoked"))?;
        Ok(AuthContext::new(owner, token, scopes))
    }

    pub fn authorize_request(
        &self,
        request: &ApiRequest,
        scope: Scope,
    ) -> Result<AuthContext, Problem> {
        let auth = self.authorize(request.bearer()?)?;
        auth.require(scope)?;
        Ok(auth)
    }
}
