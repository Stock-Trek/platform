use crate::api::ApiRequest;
use crate::api::Problem;
use crate::api::Respond;
use crate::auth::Scope;
use crate::auth::SessionService;
use crate::auth::{LoginRequest, RefreshRequest, StepUpRequest};
use chrono::Utc;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Response, StatusCode};
use std::sync::Arc;

pub(crate) struct SessionApi {
    sessions: Arc<SessionService>,
}

impl SessionApi {
    pub(crate) fn new(sessions: Arc<SessionService>) -> Self {
        Self { sessions }
    }

    pub(crate) fn route(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        match request.segments().as_slice() {
            ["session"] => match *request.method() {
                Method::POST => self.login(request),
                Method::DELETE => self.revoke(request),
                _ => Err(Problem::method_not_allowed(&[Method::POST, Method::DELETE])),
            },
            ["session", "refresh"] => match *request.method() {
                Method::POST => self.refresh(request),
                _ => Err(Problem::method_not_allowed(&[Method::POST])),
            },
            ["session", "step-up"] => match *request.method() {
                Method::POST => self.step_up(request),
                _ => Err(Problem::method_not_allowed(&[Method::POST])),
            },
            _ => Err(Problem::not_found("no such endpoint")),
        }
    }

    fn login(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        request.require_json_content_type()?;
        let login: LoginRequest = request.json()?;
        let tokens =
            self.sessions
                .login(&login, request.peer().map(|peer| peer.ip()), Utc::now())?;
        Ok(Respond::json(StatusCode::CREATED, &tokens))
    }

    fn refresh(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        request.require_json_content_type()?;
        let refresh: RefreshRequest = request.json()?;
        let tokens = self.sessions.refresh(&refresh)?;
        Ok(Respond::json(StatusCode::CREATED, &tokens))
    }

    fn step_up(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        request.require_json_content_type()?;
        let step_up: StepUpRequest = request.json()?;
        let tokens = self.sessions.step_up(
            &auth,
            &step_up,
            request.peer().map(|peer| peer.ip()),
            Utc::now(),
        )?;
        Ok(Respond::json(StatusCode::CREATED, &tokens))
    }

    fn revoke(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        self.sessions.revoke(&auth);
        Ok(Respond::empty(StatusCode::NO_CONTENT))
    }
}
