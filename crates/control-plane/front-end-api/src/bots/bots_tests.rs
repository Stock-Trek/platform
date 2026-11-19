use super::BotService;
use crate::accounts::{AccountStore, Credentials};
use crate::algorithms::{BuildStore, Strategy};
use crate::api::{ApiRequest, PageQuery};
use crate::auth::{AuthContext, Scope};
use crate::backtests::BacktestStore;
use crate::bots::bot_types::Schedule;
use crate::bots::{
    Bot, BotState, BotStore, CreateBotRequest, PatchBotRequest, Run, RunFilter, RunState, RunStore,
    RunTrigger,
};
use crate::limits::{LimitService, LimitSources};
use crate::streaming::ChannelRegistry;
use chrono::{DateTime, Duration, Utc};
use hyper::{HeaderMap, Method, Uri, body::Bytes};
use platform_exchange_types::encrypt::EncryptionAlgorithm;
use std::sync::Arc;
use stock_trek_types::cex::cex_id::CexId;

struct Fixture {
    service: BotService,
    accounts: Arc<AccountStore>,
    auth: AuthContext,
    algorithm_id: String,
    build_id: String,
    account_id: String,
}

impl Fixture {
    const OWNER: &'static str = "user_7gk2";
    const STRATEGY: &'static str = "momentum_v2";

    fn new() -> Self {
        let registry = Arc::new(ChannelRegistry::new());
        let accounts = Arc::new(AccountStore::new());
        let builds = Arc::new(BuildStore::new(Arc::clone(&registry)));
        let bots = Arc::new(BotStore::new());
        let runs = Arc::new(RunStore::new(Arc::clone(&registry)));
        let limits = Arc::new(LimitService::new(LimitSources {
            accounts: accounts.clone(),
            builds: builds.clone(),
            bots: bots.clone(),
            backtests: Arc::new(BacktestStore::new(Arc::clone(&registry))),
        }));
        let algorithm =
            builds.register_algorithm(Self::OWNER, "momentum", "acme/momentum", "heads/main");
        let build = builds
            .create_build(
                Self::OWNER,
                &algorithm.algorithm_id,
                "heads/main",
                None,
                Utc::now(),
            )
            .expect("head build");
        builds.mark_ready(
            Self::OWNER,
            &algorithm.algorithm_id,
            &build.build_id,
            vec![Strategy {
                name: Self::STRATEGY.to_string(),
                description: "momentum strategy".to_string(),
            }],
            Utc::now(),
        );
        let account = accounts
            .link(
                Self::OWNER,
                CexId::Binance,
                "main",
                Credentials::new(
                    "0123456789abcdef",
                    "fedcba9876543210",
                    EncryptionAlgorithm::HmacSha256,
                ),
                Utc::now(),
            )
            .expect("link");
        Self {
            service: BotService::new(bots, runs, accounts.clone(), builds, limits),
            accounts,
            auth: AuthContext::new(
                Self::OWNER,
                "token",
                vec![Scope::Read, Scope::Manage, Scope::Admin],
            ),
            algorithm_id: algorithm.algorithm_id,
            build_id: build.build_id,
            account_id: account.account_id,
        }
    }

    fn create(&self) -> Bot {
        let request = CreateBotRequest {
            name: "momentum".to_string(),
            algorithm_id: self.algorithm_id.clone(),
            build_id: self.build_id.clone(),
            strategy_name: Self::STRATEGY.to_string(),
            account_ids: vec![self.account_id.clone()],
            schedule: Schedule::Interval {
                every_seconds: 60,
                misfire_grace_seconds: None,
            },
        };
        self.service
            .create(&self.auth, &request, Utc::now())
            .expect("create bot")
    }

    fn list_query(&self) -> PageQuery {
        PageQuery::parse(&request(Method::GET, "/v1/bots", &[])).expect("page query")
    }
}

fn request(method: Method, path: &str, headers: &[(&str, &str)]) -> ApiRequest {
    let uri: Uri = path.parse().expect("uri");
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        map.insert(
            hyper::header::HeaderName::from_bytes(name.as_bytes()).expect("name"),
            hyper::header::HeaderValue::from_str(value).expect("value"),
        );
    }
    ApiRequest::from_parts(
        method,
        uri,
        map,
        Bytes::new(),
        None,
        hyper::http::Extensions::new(),
    )
}

#[test]
fn archived_bots_are_hidden_from_the_default_list() {
    let fixture = Fixture::new();
    let first = fixture.create();
    let second = fixture.create();
    fixture
        .service
        .archive(&fixture.auth, &second.bot_id, Utc::now())
        .expect("archive");
    let listed = fixture
        .service
        .list(&fixture.auth, None, &fixture.list_query())
        .data;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].bot_id, first.bot_id);
    let archived = fixture
        .service
        .list(
            &fixture.auth,
            Some(BotState::Archived),
            &fixture.list_query(),
        )
        .data;
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].bot_id, second.bot_id);
}

#[test]
fn an_archived_bot_is_read_only() {
    let fixture = Fixture::new();
    let bot = fixture.create();
    let archived = fixture
        .service
        .archive(&fixture.auth, &bot.bot_id, Utc::now())
        .expect("archive");
    assert_eq!(archived.state, BotState::Archived);
    let enable = fixture.service.enable(
        &fixture.auth,
        &bot.bot_id,
        Some(archived.version),
        Utc::now(),
    );
    let disable = fixture.service.disable(
        &fixture.auth,
        &bot.bot_id,
        Some(archived.version),
        Utc::now(),
    );
    let patch = fixture.service.patch(
        &fixture.auth,
        &bot.bot_id,
        Some(archived.version),
        &PatchBotRequest {
            name: Some("renamed".to_string()),
            build_id: None,
            strategy_name: None,
            account_ids: None,
            schedule: None,
        },
        Utc::now(),
    );
    for problem in [enable, disable, patch] {
        let problem = problem.expect_err("an archived bot is read-only");
        assert_eq!(problem.status(), hyper::StatusCode::CONFLICT);
    }
}

#[test]
fn enabling_a_bot_needs_its_accounts_linked() {
    let fixture = Fixture::new();
    let bot = fixture.create();
    fixture.accounts.unlink(Fixture::OWNER, &fixture.account_id);
    let problem = fixture
        .service
        .enable(&fixture.auth, &bot.bot_id, Some(bot.version), Utc::now())
        .expect_err("enable must fail");
    assert_eq!(problem.status(), hyper::StatusCode::CONFLICT);
}

#[test]
fn a_created_bot_starts_disabled_and_can_be_enabled() {
    let fixture = Fixture::new();
    let bot = fixture.create();
    assert_eq!(bot.state, BotState::Disabled);
    let enabled = fixture
        .service
        .enable(&fixture.auth, &bot.bot_id, Some(bot.version), Utc::now())
        .expect("enable");
    assert_eq!(enabled.state, BotState::Enabled);
    assert_eq!(enabled.version, bot.version + 1);
}

#[test]
fn merge_patches_reject_explicit_nulls() {
    for field in ["name", "buildId", "strategyName", "accountIds", "schedule"] {
        let patch = PatchBotRequest::merge_patch(&serde_json::json!({ field: null }))
            .expect_err("explicit nulls must be rejected");
        assert_eq!(patch.status(), hyper::StatusCode::BAD_REQUEST);
    }
    let patch = PatchBotRequest::merge_patch(&serde_json::json!({
        "name": "renamed",
        "unknown": 1
    }))
    .expect_err("unknown fields must be rejected");
    assert_eq!(patch.status(), hyper::StatusCode::BAD_REQUEST);
    let patch = PatchBotRequest::merge_patch(&serde_json::json!({ "name": "renamed" }))
        .expect("valid patch");
    assert_eq!(patch.name.as_deref(), Some("renamed"));
    assert!(patch.build_id.is_none());
    assert!(patch.schedule.is_none());
}

fn retained_run(run_id: &str, state: RunState, completed_at: Option<DateTime<Utc>>) -> Run {
    Run {
        run_id: run_id.to_string(),
        bot_id: "bot_ret".to_string(),
        algorithm_id: "algorithm_ret".to_string(),
        build_id: None,
        strategy_name: None,
        trigger: RunTrigger::Schedule,
        scheduled_at: Utc::now(),
        state,
        completed_at,
        log_available: false,
        cancel_requested: false,
        error: None,
        skip_reason: None,
    }
}

#[test]
fn terminal_runs_leave_the_store_after_ninety_days() {
    let runs = RunStore::new(Arc::new(ChannelRegistry::new()));
    let mut fresh = retained_run("run_fresh", RunState::Completed, None);
    fresh.completed_at = Some(Utc::now() - Duration::days(89));
    let mut stale = retained_run("run_stale", RunState::Canceled, None);
    stale.completed_at = Some(Utc::now() - Duration::days(91));
    runs.insert(Fixture::OWNER, fresh);
    runs.insert(Fixture::OWNER, stale);
    assert!(runs.get(Fixture::OWNER, "run_fresh").is_some());
    assert!(runs.get(Fixture::OWNER, "run_stale").is_none());
    assert_eq!(runs.list(Fixture::OWNER, &RunFilter::default()).len(), 1);
}

#[test]
fn skipped_runs_leave_the_store_after_seven_days() {
    let runs = RunStore::new(Arc::new(ChannelRegistry::new()));
    let mut fresh = retained_run("run_fresh", RunState::Skipped, None);
    fresh.completed_at = Some(Utc::now() - Duration::days(6));
    let mut stale = retained_run("run_stale", RunState::Skipped, None);
    stale.completed_at = Some(Utc::now() - Duration::days(8));
    runs.insert(Fixture::OWNER, fresh);
    runs.insert(Fixture::OWNER, stale);
    assert!(runs.get(Fixture::OWNER, "run_fresh").is_some());
    assert!(runs.get(Fixture::OWNER, "run_stale").is_none());
}

#[test]
fn active_runs_are_retained_indefinitely() {
    let runs = RunStore::new(Arc::new(ChannelRegistry::new()));
    runs.insert(
        Fixture::OWNER,
        retained_run("run_pending", RunState::Pending, None),
    );
    assert!(runs.get(Fixture::OWNER, "run_pending").is_some());
}
