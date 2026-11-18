use crate::algorithms::BuildStore;
use crate::algorithms::{BuildState, BuildSummary, TriggerBuildRequest};
use crate::api::{ApiRequest, IdempotencyStore, Page, PageQuery, Problem, Respond};
use crate::auth::{AuthContext, Scope, SessionService};
use crate::backtests::BacktestStore;
use crate::bots::BotStore;
use chrono::Utc;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Response, StatusCode};
use std::sync::Arc;

const MIN_COMMIT_LENGTH: usize = 7;
const MAX_COMMIT_LENGTH: usize = 64;

pub struct BuildApi {
    builds: Arc<BuildStore>,
    sessions: Arc<SessionService>,
    bots: Arc<BotStore>,
    backtests: Arc<BacktestStore>,
    idempotency: Arc<IdempotencyStore>,
}

impl BuildApi {
    pub fn new(
        builds: Arc<BuildStore>,
        sessions: Arc<SessionService>,
        bots: Arc<BotStore>,
        backtests: Arc<BacktestStore>,
        idempotency: Arc<IdempotencyStore>,
    ) -> Self {
        Self {
            builds,
            sessions,
            bots,
            backtests,
            idempotency,
        }
    }

    pub fn route(
        &self,
        request: &ApiRequest,
        algorithm_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        match request.segments().as_slice() {
            ["algorithms", _, "builds"] => match *request.method() {
                Method::GET => self.builds(request, algorithm_id),
                Method::POST => self.trigger_build(request, algorithm_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET, Method::POST])),
            },
            ["algorithms", _, "builds", build_id] => match *request.method() {
                Method::GET => self.build(request, algorithm_id, build_id),
                Method::DELETE => self.delete_build(request, algorithm_id, build_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET, Method::DELETE])),
            },
            ["algorithms", _, "builds", build_id, "strategies"] => match *request.method() {
                Method::GET => self.strategies(request, algorithm_id, build_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            ["algorithms", _, "builds", build_id, "log"] => match *request.method() {
                Method::GET => self.log(request, algorithm_id, build_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            _ => Err(Problem::not_found("no such endpoint")),
        }
    }

    fn builds(
        &self,
        request: &ApiRequest,
        algorithm_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let owner = auth.owner();
        self.algorithm(owner, algorithm_id)?;
        let git_ref = request.query("ref").map(str::to_string);
        let state = Self::state_filter(request)?;
        let listed = self
            .builds
            .builds(owner, algorithm_id, git_ref.as_deref(), state);
        let page = Page::of(listed, &PageQuery::parse(request));
        Ok(Respond::json(StatusCode::OK, &page))
    }

    fn trigger_build(
        &self,
        request: &ApiRequest,
        algorithm_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        request.require_json_content_type()?;
        self.idempotency
            .apply(request, auth.owner(), "trigger-build", || {
                let trigger: TriggerBuildRequest = request.json()?;
                let owner = auth.owner();
                self.algorithm(owner, algorithm_id)?;
                let git_ref = trigger
                    .git_ref
                    .unwrap_or_else(|| self.default_ref(owner, algorithm_id));
                Self::validate_ref(&git_ref)?;
                let commit = trigger.commit;
                if let Some(commit) = &commit {
                    Self::validate_commit(commit)?;
                }
                let commit = commit.unwrap_or_else(|| format!("{git_ref}-HEAD"));
                let build =
                    self.builds
                        .create_build(owner, algorithm_id, &git_ref, &commit, Utc::now());
                Ok(Respond::json(StatusCode::ACCEPTED, &build))
            })
    }

    fn build(
        &self,
        request: &ApiRequest,
        algorithm_id: &str,
        build_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let build = self
            .builds
            .build(auth.owner(), algorithm_id, build_id)
            .ok_or_else(|| Problem::not_found("build does not exist"))?;
        Ok(Respond::json(StatusCode::OK, &build))
    }

    fn delete_build(
        &self,
        request: &ApiRequest,
        algorithm_id: &str,
        build_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        let owner = auth.owner();
        self.algorithm(owner, algorithm_id)?;
        let build = self
            .builds
            .build(owner, algorithm_id, build_id)
            .ok_or_else(|| Problem::not_found("build does not exist"))?;
        if build.state.is_incomplete() {
            return Err(Problem::state_conflict(
                "a queued or building build cannot be deleted",
            ));
        }
        if !self
            .bots
            .pinning_build(owner, algorithm_id, build_id)
            .is_empty()
        {
            return Err(Problem::state_conflict("a bot still pins this build"));
        }
        self.builds.delete_build(owner, algorithm_id, build_id);
        self.backtests.delete_for_build(build_id);
        Ok(Respond::empty(StatusCode::NO_CONTENT))
    }

    fn strategies(
        &self,
        request: &ApiRequest,
        algorithm_id: &str,
        build_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let owner = auth.owner();
        self.require_build(&auth, algorithm_id, build_id)?;
        let strategies = self
            .builds
            .strategies(owner, algorithm_id, build_id)
            .unwrap_or_default();
        let page = Page::of(strategies, &PageQuery::parse(request));
        Ok(Respond::json(StatusCode::OK, &page))
    }

    fn log(
        &self,
        request: &ApiRequest,
        algorithm_id: &str,
        build_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let owner = auth.owner();
        self.require_build(&auth, algorithm_id, build_id)?;
        let log = self
            .builds
            .build_log(owner, algorithm_id, build_id, Utc::now())
            .ok_or_else(|| Problem::not_found("no build log exists yet"))?;
        Ok(Respond::json(StatusCode::OK, &log))
    }

    fn require_build(
        &self,
        auth: &AuthContext,
        algorithm_id: &str,
        build_id: &str,
    ) -> Result<BuildSummary, Problem> {
        self.builds
            .build(auth.owner(), algorithm_id, build_id)
            .ok_or_else(|| Problem::not_found("build does not exist"))
    }

    fn algorithm(&self, owner: &str, algorithm_id: &str) -> Result<(), Problem> {
        self.builds
            .algorithm(owner, algorithm_id)
            .ok_or_else(|| Problem::not_found("algorithm does not exist"))
            .map(|_| ())
    }

    fn default_ref(&self, owner: &str, algorithm_id: &str) -> String {
        self.builds
            .algorithm(owner, algorithm_id)
            .map(|algorithm| algorithm.default_ref)
            .unwrap_or_default()
    }

    fn validate_ref(git_ref: &str) -> Result<(), Problem> {
        if git_ref.starts_with("heads/") || git_ref.starts_with("tags/") {
            Ok(())
        } else {
            Err(Problem::validation_failed(
                "ref must use the heads/ or tags/ form",
            ))
        }
    }

    fn validate_commit(commit: &str) -> Result<(), Problem> {
        let valid = (MIN_COMMIT_LENGTH..=MAX_COMMIT_LENGTH).contains(&commit.len())
            && commit
                .chars()
                .all(|character| character.is_ascii_hexdigit());
        if valid {
            Ok(())
        } else {
            Err(Problem::validation_failed(
                "commit must be an abbreviation of at least 7 hexadecimal characters",
            ))
        }
    }

    fn state_filter(request: &ApiRequest) -> Result<Option<BuildState>, Problem> {
        let Some(value) = request.query("state") else {
            return Ok(None);
        };
        BuildState::parse(value)
            .map(Some)
            .ok_or_else(|| Problem::validation_failed("unknown build state"))
    }
}
