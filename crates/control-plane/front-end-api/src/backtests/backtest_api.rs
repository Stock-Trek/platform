use crate::api::ApiRequest;
use crate::api::PageQuery;
use crate::api::Problem;
use crate::api::Respond;
use crate::auth::Scope;
use crate::auth::SessionService;
use crate::backtests::BacktestService;
use crate::backtests::{BacktestState, BacktestStatus, CreateBacktestRequest};
use chrono::Utc;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Response, StatusCode};
use std::sync::Arc;

pub struct BacktestApi {
    backtests: Arc<BacktestService>,
    sessions: Arc<SessionService>,
}

impl BacktestApi {
    pub fn new(backtests: Arc<BacktestService>, sessions: Arc<SessionService>) -> Self {
        Self {
            backtests,
            sessions,
        }
    }

    pub fn route(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        match request.segments().as_slice() {
            ["backtests"] => match *request.method() {
                Method::POST => self.create(request),
                Method::GET => self.list(request),
                _ => Err(Problem::method_not_allowed(&[Method::POST, Method::GET])),
            },
            ["backtests", backtest_id] => match *request.method() {
                Method::GET => self.status(request, backtest_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            ["backtests", backtest_id, "cancel"] => match *request.method() {
                Method::POST => self.cancel(request, backtest_id),
                _ => Err(Problem::method_not_allowed(&[Method::POST])),
            },
            ["backtests", backtest_id, "results"] => match *request.method() {
                Method::GET => self.results(request, backtest_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            ["backtests", backtest_id, "results", "export"] => match *request.method() {
                Method::GET => self.export(request, backtest_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            _ => Err(Problem::not_found("no such endpoint")),
        }
    }

    fn create(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        request.require_json_content_type()?;
        let create: CreateBacktestRequest = request.json()?;
        let created = self.backtests.create(&auth, &create, Utc::now())?;
        Ok(Respond::json(StatusCode::ACCEPTED, &created))
    }

    fn list(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let state = Self::state_filter(request)?;
        let page = self
            .backtests
            .list(&auth, state, &PageQuery::parse(request));
        Ok(Respond::json(StatusCode::OK, &page))
    }

    fn status(
        &self,
        request: &ApiRequest,
        backtest_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let status = self.backtests.status(&auth, backtest_id)?;
        Ok(Respond::json(StatusCode::OK, &status))
    }

    fn cancel(
        &self,
        request: &ApiRequest,
        backtest_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        match self.backtests.cancel(&auth, backtest_id, Utc::now())? {
            Some(status) => Ok(Respond::json(StatusCode::ACCEPTED, &status)),
            None => {
                let status: BacktestStatus = self.backtests.status(&auth, backtest_id)?;
                Ok(Respond::json(StatusCode::OK, &status))
            }
        }
    }

    fn results(
        &self,
        request: &ApiRequest,
        backtest_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let page = self
            .backtests
            .results(&auth, backtest_id, &PageQuery::parse(request))?;
        Ok(Respond::json(StatusCode::OK, &page))
    }

    fn export(
        &self,
        request: &ApiRequest,
        backtest_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let link = self.backtests.export(&auth, backtest_id, Utc::now())?;
        Ok(Respond::json(StatusCode::OK, &link))
    }

    fn state_filter(request: &ApiRequest) -> Result<Option<BacktestState>, Problem> {
        let Some(value) = request.query("state") else {
            return Ok(None);
        };
        BacktestState::parse(value)
            .map(Some)
            .ok_or_else(|| Problem::validation_failed("unknown backtest state"))
    }
}
