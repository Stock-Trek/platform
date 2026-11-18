use crate::accounts::AccountService;
use crate::algorithms::BuildStore;
use crate::algorithms::{AlgorithmSummary, BuildState, TriggerBuildRequest};
use crate::api::ApiRequest;
use crate::api::PageQuery;
use crate::api::Problem;
use crate::api::Respond;
use crate::auth::Scope;
use crate::auth::SessionService;
use chrono::Utc;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Response, StatusCode};
use std::sync::Arc;

pub struct AlgorithmApi {
    builds: Arc<BuildStore>,
    accounts: Arc<AccountService>,
    sessions: Arc<SessionService>,
}

impl AlgorithmApi {
    pub fn new(
        builds: Arc<BuildStore>,
        accounts: Arc<AccountService>,
        sessions: Arc<SessionService>,
    ) -> Self {
        Self {
            builds,
            accounts,
            sessions,
        }
    }

    pub fn route(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        match request.segments().as_slice() {
            ["algorithms"] => match *request.method() {
                Method::GET => self.list(request),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            ["algorithms", algorithm_id] => match *request.method() {
                Method::GET => self.get(request, algorithm_id),
                Method::DELETE => self.archive(request, algorithm_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET, Method::DELETE])),
            },
            ["algorithms", algorithm_id, "builds"] => match *request.method() {
                Method::GET => self.builds(request, algorithm_id),
                Method::POST => self.trigger_build(request, algorithm_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET, Method::POST])),
            },
            ["algorithms", algorithm_id, "builds", build_id] => match *request.method() {
                Method::GET => self.build(request, algorithm_id, build_id),
                Method::DELETE => self.delete_build(request, algorithm_id, build_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET, Method::DELETE])),
            },
            ["algorithms", algorithm_id, "builds", build_id, "strategies"] => {
                match *request.method() {
                    Method::GET => self.strategies(request, algorithm_id, build_id),
                    _ => Err(Problem::method_not_allowed(&[Method::GET])),
                }
            }
            ["algorithms", algorithm_id, "builds", build_id, "log"] => match *request.method() {
                Method::GET => self.log(request, algorithm_id, build_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            _ => Err(Problem::not_found("no such endpoint")),
        }
    }

    fn list(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let algorithms = self.builds.list_algorithms(auth.owner());
        Ok(Respond::json(StatusCode::OK, &algorithms))
    }

    fn get(
        &self,
        request: &ApiRequest,
        algorithm_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let algorithm = self.algorithm(&auth, algorithm_id)?;
        Ok(Respond::json_with_etag(
            StatusCode::OK,
            &algorithm,
            algorithm.version,
        ))
    }

    fn archive(
        &self,
        request: &ApiRequest,
        algorithm_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        let algorithm = self
            .builds
            .archive_algorithm(auth.owner(), algorithm_id)
            .ok_or_else(|| Problem::not_found("algorithm does not exist"))?;
        Ok(Respond::json(StatusCode::OK, &algorithm))
    }

    fn builds(
        &self,
        request: &ApiRequest,
        algorithm_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let owner = auth.owner();
        self.algorithm(&auth, algorithm_id)?;
        let git_ref = request.query("ref").map(str::to_string);
        let state = Self::state_filter(request)?;
        let listed = self
            .builds
            .builds(owner, algorithm_id, git_ref.as_deref(), state);
        let page = crate::api::Page::of(listed, &PageQuery::parse(request));
        Ok(Respond::json(StatusCode::OK, &page))
    }

    fn trigger_build(
        &self,
        request: &ApiRequest,
        algorithm_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        request.require_json_content_type()?;
        let trigger: TriggerBuildRequest = request.json()?;
        let owner = auth.owner();
        let algorithm = self.algorithm(&auth, algorithm_id)?;
        let git_ref = trigger
            .git_ref
            .unwrap_or_else(|| algorithm.default_ref.clone());
        // TODO: the build plane resolves `ref` to a commit and starts the build.
        let commit = trigger.commit.unwrap_or_else(|| format!("{git_ref}-HEAD"));
        let build = self
            .builds
            .create_build(owner, algorithm_id, &git_ref, &commit, Utc::now());
        Ok(Respond::json(StatusCode::ACCEPTED, &build))
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
        self.algorithm(&auth, algorithm_id)?;
        if self
            .builds
            .delete_build(auth.owner(), algorithm_id, build_id)
        {
            Ok(Respond::empty(StatusCode::NO_CONTENT))
        } else {
            Err(Problem::not_found("build does not exist"))
        }
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
        Ok(Respond::json(StatusCode::OK, &strategies))
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
        auth: &crate::auth::AuthContext,
        algorithm_id: &str,
        build_id: &str,
    ) -> Result<crate::algorithms::BuildSummary, Problem> {
        self.builds
            .build(auth.owner(), algorithm_id, build_id)
            .ok_or_else(|| Problem::not_found("build does not exist"))
    }

    fn algorithm(
        &self,
        auth: &crate::auth::AuthContext,
        algorithm_id: &str,
    ) -> Result<AlgorithmSummary, Problem> {
        self.builds
            .algorithm(auth.owner(), algorithm_id)
            .ok_or_else(|| Problem::not_found("algorithm does not exist"))
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
