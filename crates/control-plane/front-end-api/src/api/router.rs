use crate::accounts::{AccountApi, AccountService, AccountStore};
use crate::algorithms::{AlgorithmApi, BuildStore};
use crate::api::{ApiRequest, IdempotencyStore, Problem, RequestHandler};
use crate::auth::{SessionApi, SessionService};
use crate::backtests::{BacktestApi, BacktestService, BacktestStore};
use crate::bots::{BotApi, BotService, BotStore, RunApi, RunService, RunStore};
use crate::limits::{LimitService, LimitSources, LimitsApi};
use crate::streaming::{ChannelRegistry, StreamApi, StreamService};
use async_trait::async_trait;
use http_body_util::Full;
use hyper::{Request, Response, body::Bytes};
use std::sync::Arc;

pub struct FrontEndApi {
    accounts: Arc<AccountApi>,
    algorithms: Arc<AlgorithmApi>,
    auth: Arc<SessionApi>,
    backtests: Arc<BacktestApi>,
    bots: Arc<BotApi>,
    runs: Arc<RunApi>,
    limits: Arc<LimitsApi>,
    streams: Arc<StreamApi>,
}

impl FrontEndApi {
    pub fn new(sessions: Arc<SessionService>) -> Self {
        let accounts_store = Arc::new(AccountStore::new());
        let builds = Arc::new(BuildStore::new());
        let bots_store = Arc::new(BotStore::new());
        let runs_store = Arc::new(RunStore::new());
        let backtests_store = Arc::new(BacktestStore::new());
        let streams = Arc::new(StreamService::new(Arc::new(ChannelRegistry::new())));
        let limits = Arc::new(LimitService::new(LimitSources {
            accounts: Arc::clone(&accounts_store),
            builds: Arc::clone(&builds),
            bots: Arc::clone(&bots_store),
            backtests: Arc::clone(&backtests_store),
            streams: Arc::clone(&streams),
        }));
        let idempotency = Arc::new(IdempotencyStore::new());
        let bots = Arc::new(BotService::new(
            Arc::clone(&bots_store),
            Arc::clone(&runs_store),
            Arc::clone(&accounts_store),
            Arc::clone(&builds),
            Arc::clone(&limits),
        ));
        let accounts = Arc::new(AccountService::new(
            Arc::clone(&accounts_store),
            Arc::clone(&builds),
            Arc::clone(&bots),
        ));
        let runs = Arc::new(RunService::new(Arc::clone(&runs_store)));
        let backtests = Arc::new(BacktestService::new(
            Arc::clone(&builds),
            Arc::clone(&backtests_store),
            Arc::clone(&limits),
        ));
        Self {
            accounts: Arc::new(AccountApi::new(
                Arc::clone(&accounts),
                Arc::clone(&sessions),
                Arc::clone(&idempotency),
            )),
            algorithms: Arc::new(AlgorithmApi::new(
                Arc::clone(&builds),
                Arc::clone(&accounts),
                Arc::clone(&sessions),
            )),
            auth: Arc::new(SessionApi::new(Arc::clone(&sessions))),
            backtests: Arc::new(BacktestApi::new(
                Arc::clone(&backtests),
                Arc::clone(&sessions),
            )),
            bots: Arc::new(BotApi::new(
                Arc::clone(&bots),
                Arc::clone(&sessions),
                Arc::clone(&idempotency),
            )),
            runs: Arc::new(RunApi::new(runs, Arc::clone(&sessions))),
            limits: Arc::new(LimitsApi::new(Arc::clone(&limits), Arc::clone(&sessions))),
            streams: Arc::new(StreamApi::new(streams, limits, Arc::clone(&sessions))),
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
    async fn handle(&self, request: Request<hyper::body::Incoming>) -> Response<Full<Bytes>> {
        let parsed = match ApiRequest::parse(request).await {
            Ok(parsed) => parsed,
            Err(problem) => return problem.response(""),
        };
        let request_id = Self::request_id(&parsed);
        match self.route(&parsed) {
            Ok(mut response) => {
                if let Ok(value) = hyper::header::HeaderValue::from_str(&request_id) {
                    response.headers_mut().insert("x-request-id", value);
                }
                response
            }
            Err(problem) => problem.response(&request_id),
        }
    }
}

impl FrontEndApi {
    fn request_id(request: &ApiRequest) -> String {
        request
            .request_id()
            .unwrap_or_else(|| format!("req_{}", uuid::Uuid::new_v4().simple()))
    }
}
