use crate::api::ApiRequest;
use crate::api::PageQuery;
use crate::api::Problem;
use crate::api::Respond;
use crate::auth::Scope;
use crate::auth::SessionService;
use crate::bots::{CancelResponse, RunFilter, RunState};
use crate::bots::{RunService, run_service::CancelOutcome};
use chrono::Utc;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Response, StatusCode};
use std::sync::Arc;

pub struct RunApi {
    runs: Arc<RunService>,
    sessions: Arc<SessionService>,
}

impl RunApi {
    pub fn new(runs: Arc<RunService>, sessions: Arc<SessionService>) -> Self {
        Self { runs, sessions }
    }

    pub fn route(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        match request.segments().as_slice() {
            ["runs"] => match *request.method() {
                Method::GET => self.list(request),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            ["runs", run_id] => match *request.method() {
                Method::GET => self.get(request, run_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            ["runs", run_id, "cancel"] => match *request.method() {
                Method::POST => self.cancel(request, run_id),
                _ => Err(Problem::method_not_allowed(&[Method::POST])),
            },
            ["runs", run_id, "log"] => match *request.method() {
                Method::GET => self.log(request, run_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            _ => Err(Problem::not_found("no such endpoint")),
        }
    }

    fn list(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let filter = Self::filter(request)?;
        let page = self.runs.list(&auth, &filter, &PageQuery::parse(request)?);
        Ok(Respond::json(StatusCode::OK, &page))
    }

    fn get(&self, request: &ApiRequest, run_id: &str) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let run = self.runs.get(&auth, run_id)?;
        Ok(Respond::json(StatusCode::OK, &run))
    }

    fn cancel(&self, request: &ApiRequest, run_id: &str) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        match self.runs.cancel(&auth, run_id, Utc::now())? {
            CancelOutcome::Canceled(run) => Ok(Respond::json(StatusCode::OK, &run)),
            CancelOutcome::Requested => Ok(Respond::json(
                StatusCode::ACCEPTED,
                &CancelResponse {
                    cancel_requested: true,
                },
            )),
            CancelOutcome::Unchanged(run) => Ok(Respond::json(StatusCode::OK, &run)),
        }
    }

    fn log(&self, request: &ApiRequest, run_id: &str) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let log = self.runs.log(&auth, run_id)?;
        Ok(Respond::json(StatusCode::OK, &log))
    }

    pub fn apply_retention(&self, now: chrono::DateTime<chrono::Utc>) -> usize {
        self.runs.apply_retention(now)
    }

    fn filter(request: &ApiRequest) -> Result<RunFilter, Problem> {
        Ok(RunFilter {
            bot_id: request.query("botId").map(str::to_string),
            state: Self::state(request)?,
            from: Self::timestamp(request, "from")?,
            to: Self::timestamp(request, "to")?,
        })
    }

    fn state(request: &ApiRequest) -> Result<Option<RunState>, Problem> {
        let Some(value) = request.query("state") else {
            return Ok(None);
        };
        RunState::parse(value)
            .map(Some)
            .ok_or_else(|| Problem::validation_failed("unknown run state"))
    }

    fn timestamp(
        request: &ApiRequest,
        name: &str,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, Problem> {
        let Some(value) = request.query(name) else {
            return Ok(None);
        };
        chrono::DateTime::parse_from_rfc3339(value)
            .map(|parsed| parsed.with_timezone(&chrono::Utc))
            .map(Some)
            .map_err(|_| {
                Problem::validation_failed(format!("{name} must be an RFC 3339 timestamp"))
            })
    }
}
