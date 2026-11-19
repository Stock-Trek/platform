use super::{
    BacktestProgress, BacktestResultsPage, BacktestService, BacktestState, BacktestStatus,
    BacktestStore,
};
use crate::accounts::AccountStore;
use crate::algorithms::BuildStore;
use crate::api::{ApiRequest, PageQuery};
use crate::auth::{AuthContext, Scope};
use crate::bots::BotStore;
use crate::limits::{LimitService, LimitSources};
use crate::streaming::channel_registry::SubscribeOutcome;
use crate::streaming::{ChannelRegistry, stream_types::Frame};
use chrono::Utc;
use hyper::{HeaderMap, Method, Uri, body::Bytes};
use std::sync::Arc;

const OWNER: &str = "user_7gk2";
const BACKTEST_ID: &str = "backtest_1";
const MIB: usize = 1024 * 1024;

struct Fixture {
    store: Arc<BacktestStore>,
    registry: Arc<ChannelRegistry>,
    service: BacktestService,
    auth: AuthContext,
}

impl Fixture {
    fn new() -> Self {
        let registry = Arc::new(ChannelRegistry::new());
        let store = Arc::new(BacktestStore::new(Arc::clone(&registry)));
        let builds = Arc::new(BuildStore::new(Arc::clone(&registry)));
        let limits = Arc::new(LimitService::new(LimitSources {
            accounts: Arc::new(AccountStore::new()),
            builds: builds.clone(),
            bots: Arc::new(BotStore::new()),
            backtests: store.clone(),
        }));
        let fixture = Self {
            service: BacktestService::new(builds, store.clone(), limits),
            store,
            registry,
            auth: AuthContext::new(OWNER, "token", vec![Scope::Read]),
        };
        fixture.insert(BacktestState::Running);
        fixture
    }

    fn status(&self, state: BacktestState) -> BacktestStatus {
        BacktestStatus {
            backtest_id: BACKTEST_ID.to_string(),
            algorithm_id: "algorithm_1".to_string(),
            build_id: "build_1".to_string(),
            strategy_name: "momentum_v2".to_string(),
            state,
            error: None,
            seed: 7,
            progress: BacktestProgress {
                completed: 0,
                total: 2,
            },
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            stream_sequence: 0,
        }
    }

    fn insert(&self, state: BacktestState) {
        self.store.insert(OWNER, self.status(state));
    }

    fn finish(&self, state: BacktestState) {
        self.store.update(OWNER, BACKTEST_ID, |mut status| {
            status.state = state;
            status.completed_at = Some(Utc::now());
            status
        });
    }

    fn append(&self, simulation: u64, value: serde_json::Value) {
        self.store
            .append_result(OWNER, BACKTEST_ID, simulation, value);
    }

    fn page_query(&self, limit: usize) -> PageQuery {
        let request = self.request(Method::GET, &format!("/v1/backtests?limit={limit}"));
        PageQuery::parse(&request).expect("page query")
    }

    fn request(&self, method: Method, path: &str) -> ApiRequest {
        let uri: Uri = path.parse().expect("uri");
        ApiRequest::from_parts(
            method,
            uri,
            HeaderMap::new(),
            Bytes::new(),
            None,
            hyper::http::Extensions::new(),
        )
    }

    fn results(&self, limit: usize) -> BacktestResultsPage {
        self.service
            .results(&self.auth, BACKTEST_ID, &self.page_query(limit))
            .expect("results")
    }

    fn replay(&self, channel: &str) -> Vec<Frame> {
        match self.registry.subscribe(channel, Some(0)) {
            SubscribeOutcome::Joined { replay, .. } => replay,
            _ => panic!("backtest channels replay fully"),
        }
    }
}

#[test]
fn simulation_results_take_registry_sequences() {
    let fixture = Fixture::new();
    let channel = format!("backtests.{BACKTEST_ID}");
    for simulation in [1_u64, 2] {
        fixture.append(simulation, serde_json::json!({ "pnl": simulation }));
    }
    let head = fixture.registry.sequence(&channel);
    assert_eq!(head, 2);
    let (results, _, _) = fixture
        .store
        .results(OWNER, BACKTEST_ID, 0, 10)
        .expect("results");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].sequence, 1);
    assert_eq!(results[1].sequence, 2);
    let status = fixture.store.status(OWNER, BACKTEST_ID).expect("status");
    assert_eq!(status.stream_sequence, head);
    assert_eq!(status.progress.completed, 2);
}

#[test]
fn an_oversized_result_fails_the_backtest_with_result_too_large() {
    let fixture = Fixture::new();
    let channel = format!("backtests.{BACKTEST_ID}");
    fixture.append(1, serde_json::json!({ "pnl": 1 }));
    fixture.append(2, serde_json::json!({ "padding": "x".repeat(2 * MIB) }));
    let stored = fixture.store.status(OWNER, BACKTEST_ID).expect("status");
    assert_eq!(stored.state, BacktestState::Failed);
    let error = stored.error.expect("error");
    assert_eq!(error.code, "result_too_large");
    let (results, _, _) = fixture
        .store
        .results(OWNER, BACKTEST_ID, 0, 10)
        .expect("results");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].simulation, 1);
    let replay = fixture.replay(&channel);
    let last = replay.last().expect("final frame");
    assert_eq!(last.sequence, stored.stream_sequence);
    assert_eq!(last.complete, Some(true));
    assert_eq!(last.state.as_deref(), Some("failed"));
}

#[test]
fn reaching_a_terminal_state_publishes_a_final_frame() {
    let fixture = Fixture::new();
    let channel = format!("backtests.{BACKTEST_ID}");
    fixture.finish(BacktestState::Canceled);
    let stored = fixture.store.status(OWNER, BACKTEST_ID).expect("status");
    assert_eq!(stored.stream_sequence, 1);
    let replay = fixture.replay(&channel);
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].state.as_deref(), Some("canceled"));
    assert_eq!(replay[0].complete, Some(true));
}

#[test]
fn a_completed_backtest_reports_complete_on_every_results_page() {
    let fixture = Fixture::new();
    for simulation in [1_u64, 2] {
        fixture.append(simulation, serde_json::json!({}));
    }
    fixture.finish(BacktestState::Completed);
    let first = fixture.results(1);
    assert_eq!(first.state, BacktestState::Completed);
    assert!(first.complete, "complete tracks the backtest state");
    assert_eq!(first.page.data.len(), 1);
    assert!(first.page.next_cursor.is_some());
}

#[test]
fn failed_and_canceled_backtests_never_report_complete() {
    let fixture = Fixture::new();
    fixture.append(1, serde_json::json!({}));
    fixture.finish(BacktestState::Failed);
    let failed = fixture.results(10);
    assert_eq!(failed.state, BacktestState::Failed);
    assert!(!failed.complete);
    assert!(failed.page.next_cursor.is_none());
    fixture.finish(BacktestState::Canceled);
    assert!(!fixture.results(10).complete);
}
