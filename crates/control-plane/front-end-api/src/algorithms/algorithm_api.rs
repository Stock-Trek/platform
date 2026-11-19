use crate::algorithms::AlgorithmSummary;
use crate::algorithms::BuildStore;
use crate::algorithms::build_api::BuildApi;
use crate::api::{ApiRequest, Problem, Respond};
use crate::auth::{AuthContext, Scope, SessionService};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Response, StatusCode};
use std::sync::Arc;

pub struct AlgorithmApi {
    builds: Arc<BuildStore>,
    sessions: Arc<SessionService>,
    builds_api: BuildApi,
}

impl AlgorithmApi {
    pub fn new(
        builds: Arc<BuildStore>,
        sessions: Arc<SessionService>,
        bots: Arc<crate::bots::BotStore>,
        backtests: Arc<crate::backtests::BacktestStore>,
        idempotency: Arc<crate::api::IdempotencyStore>,
    ) -> Self {
        Self {
            builds: Arc::clone(&builds),
            sessions: Arc::clone(&sessions),
            builds_api: BuildApi::new(builds, sessions, bots, backtests, idempotency),
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
            ["algorithms", algorithm_id, rest @ ..] if !rest.is_empty() => {
                self.builds_api.route(request, algorithm_id)
            }
            _ => Err(Problem::not_found("no such endpoint")),
        }
    }

    fn list(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let algorithms = self.builds.list_algorithms(auth.owner());
        let page = crate::api::Page::of(algorithms, &crate::api::PageQuery::parse(request)?);
        Ok(Respond::json(StatusCode::OK, &page))
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
        Ok(Respond::json_with_etag(
            StatusCode::OK,
            &algorithm,
            algorithm.version,
        ))
    }

    fn algorithm(
        &self,
        auth: &AuthContext,
        algorithm_id: &str,
    ) -> Result<AlgorithmSummary, Problem> {
        self.builds
            .algorithm(auth.owner(), algorithm_id)
            .ok_or_else(|| Problem::not_found("algorithm does not exist"))
    }
}
