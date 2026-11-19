use crate::accounts::{AccountApi, AccountService, AccountStore};
use crate::algorithms::{AlgorithmApi, BuildStore};
use crate::api::HealthRoute;
use crate::api::cors::Cors;
use crate::api::{ApiRequest, IdempotencyStore, Problem, RequestHandler};
use crate::auth::{SessionApi, SessionService, SessionStore};
use crate::backtests::{BacktestApi, BacktestService, BacktestStore};
use crate::bots::{BotApi, BotService, BotStore, RunApi, RunService, RunStore};
use crate::limits::{LimitService, LimitSources, LimitsApi};
use crate::streaming::{ChannelRegistry, StreamApi, StreamService};
use async_trait::async_trait;
use http_body_util::Full;
use hyper::{Method, Request, Response, body::Bytes, header::HeaderValue};
use std::net::SocketAddr;
use std::sync::Arc;

const REQUEST_ID_HEADER: &str = "x-request-id";

pub struct FrontEndApi {
    accounts: Arc<AccountApi>,
    algorithms: Arc<AlgorithmApi>,
    auth: Arc<SessionApi>,
    backtests: Arc<BacktestApi>,
    bots: Arc<BotApi>,
    runs: Arc<RunApi>,
    limits: Arc<LimitsApi>,
    streams: Arc<StreamApi>,
    cors: Cors,
}

pub struct FrontEndApiDeps {
    pub session_store: Arc<SessionStore>,
    pub allowed_origins: Vec<String>,
}

impl FrontEndApi {
    pub fn new(deps: FrontEndApiDeps) -> Self {
        let accounts_store = Arc::new(AccountStore::new());
        let builds = Arc::new(BuildStore::new());
        let bots_store = Arc::new(BotStore::new());
        let runs_store = Arc::new(RunStore::new());
        let backtests_store = Arc::new(BacktestStore::new());
        let limits = Arc::new(LimitService::new(LimitSources {
            accounts: Arc::clone(&accounts_store),
            builds: Arc::clone(&builds),
            bots: Arc::clone(&bots_store),
            backtests: Arc::clone(&backtests_store),
        }));
        let bots = Arc::new(BotService::new(
            Arc::clone(&bots_store),
            Arc::clone(&runs_store),
            Arc::clone(&accounts_store),
            Arc::clone(&builds),
            Arc::clone(&limits),
        ));
        let accounts = Arc::new(AccountService::new(
            Arc::clone(&accounts_store),
            Arc::clone(&bots),
        ));
        let runs = Arc::new(RunService::new(Arc::clone(&runs_store)));
        let backtests = Arc::new(BacktestService::new(
            Arc::clone(&builds),
            Arc::clone(&backtests_store),
            Arc::clone(&limits),
        ));
        let streams = Arc::new(StreamService::new(
            Arc::new(ChannelRegistry::new()),
            Arc::clone(&accounts),
            Arc::clone(&builds),
            Arc::clone(&bots_store),
            Arc::clone(&backtests_store),
        ));
        let sessions = Arc::new(SessionService::new(deps.session_store, Arc::clone(&limits)));
        let idempotency = Arc::new(IdempotencyStore::new());
        Self {
            accounts: Arc::new(AccountApi::new(
                Arc::clone(&accounts),
                Arc::clone(&sessions),
                Arc::clone(&idempotency),
            )),
            algorithms: Arc::new(AlgorithmApi::new(
                Arc::clone(&builds),
                Arc::clone(&sessions),
                Arc::clone(&bots_store),
                Arc::clone(&backtests_store),
                Arc::clone(&idempotency),
            )),
            auth: Arc::new(SessionApi::new(Arc::clone(&sessions))),
            backtests: Arc::new(BacktestApi::new(
                Arc::clone(&backtests),
                Arc::clone(&sessions),
                Arc::clone(&idempotency),
            )),
            bots: Arc::new(BotApi::new(
                Arc::clone(&bots),
                Arc::clone(&sessions),
                Arc::clone(&idempotency),
            )),
            runs: Arc::new(RunApi::new(runs, Arc::clone(&sessions))),
            limits: Arc::new(LimitsApi::new(
                Arc::clone(&limits),
                Arc::clone(&sessions),
                Arc::clone(&streams),
            )),
            streams: Arc::new(StreamApi::new(streams, limits, Arc::clone(&sessions))),
            cors: Cors::new(deps.allowed_origins),
        }
    }

    fn route(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        match request.segments().first().copied() {
            Some("session") => self.auth.route(request),
            Some("accounts") | Some("portfolios") => self.accounts.route(request),
            Some("algorithms") => self.algorithms.route(request),
            Some("backtests") => self.backtests.route(request),
            Some("bots") | Some("kill") => self.bots.route(request),
            Some("runs") => self.runs.route(request),
            Some("limits") => self.limits.route(request),
            Some("stream") => self.streams.route(request),
            _ => Err(Problem::not_found("no such endpoint")),
        }
    }
}

#[async_trait]
impl RequestHandler for FrontEndApi {
    async fn handle(
        &self,
        request: Request<hyper::body::Incoming>,
        peer: SocketAddr,
    ) -> Response<Full<Bytes>> {
        if HealthRoute::matches(request.uri().path()) {
            return HealthRoute::dispatch(request.method());
        }
        let header_request_id = Self::header_request_id(request.headers());
        let parsed = match ApiRequest::parse(request, Some(peer)).await {
            Ok(parsed) => parsed,
            Err(problem) => {
                return problem.response(header_request_id.as_deref().unwrap_or_default());
            }
        };
        let request_id =
            header_request_id.unwrap_or_else(|| format!("req_{}", uuid::Uuid::new_v4().simple()));
        let outcome = match *parsed.method() {
            Method::OPTIONS => Ok(self.cors.preflight(&parsed)),
            _ => self.route(&parsed),
        };
        let mut response = match outcome {
            Ok(response) => response,
            Err(problem) => problem.response(&request_id),
        };
        response = self.cors.apply(&parsed, response);
        if let Ok(value) = HeaderValue::from_str(&request_id) {
            response.headers_mut().insert(REQUEST_ID_HEADER, value);
        }
        response
    }
}

impl FrontEndApi {
    pub(super) fn header_request_id(headers: &hyper::HeaderMap) -> Option<String> {
        headers
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    }
}
