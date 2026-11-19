use super::ChannelRegistry;
use super::StreamService;
use super::stream_service::Subscription;
use super::stream_types::Frame;
use super::ticket_store::TicketStore;
use super::ws::Connection;
use crate::accounts::{AccountService, AccountStore};
use crate::algorithms::BuildStore;
use crate::auth::SessionStore;
use crate::backtests::{BacktestProgress, BacktestState, BacktestStatus, BacktestStore};
use crate::bots::bot_types::{Bot, BotState, Schedule};
use crate::bots::{BotService, BotStore, RunStore};
use crate::limits::{LimitService, LimitSources};
use chrono::{Duration, Utc};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_websockets::Message;

struct Fixture {
    store: Arc<SessionStore>,
    tickets: TicketStore,
}

impl Fixture {
    const OWNER: &'static str = "user_7gk2";
    const LOGIN_ID: &'static str = "login_9f2b";
    const LOGIN_SECRET: &'static str = "correct-horse";

    fn new() -> Self {
        let store = Arc::new(SessionStore::new());
        store.register_login(Self::LOGIN_ID, Self::OWNER, Self::LOGIN_SECRET);
        Self {
            store,
            tickets: TicketStore::new(),
        }
    }

    fn login(&self) -> (String, String) {
        let now = Utc::now();
        let tokens = self.store.issue_login(Self::OWNER, now);
        let session_id = self
            .store
            .session_id(&tokens.token)
            .expect("session id from token");
        (tokens.token, session_id)
    }

    fn ticket(&self, session_id: &str) -> String {
        self.tickets.issue(Self::OWNER, session_id, Utc::now()).0
    }
}

struct ChannelFixture {
    service: StreamService,
    registry: Arc<ChannelRegistry>,
    runs: Arc<RunStore>,
    account_id: String,
    algorithm_id: String,
    bot_id: String,
    backtest_id: String,
}

impl ChannelFixture {
    const OWNER: &'static str = "user_7gk2";

    fn new() -> Self {
        let registry = Arc::new(ChannelRegistry::new());
        let accounts = Arc::new(AccountStore::new());
        let builds = Arc::new(BuildStore::new(Arc::clone(&registry)));
        let bots = Arc::new(BotStore::new());
        let backtests = Arc::new(BacktestStore::new(Arc::clone(&registry)));
        let runs_store = Arc::new(RunStore::new(Arc::clone(&registry)));
        let limits = Arc::new(LimitService::new(LimitSources {
            accounts: accounts.clone(),
            builds: builds.clone(),
            bots: bots.clone(),
            backtests: backtests.clone(),
        }));
        let bot_service = Arc::new(BotService::new(
            bots.clone(),
            Arc::clone(&runs_store),
            accounts.clone(),
            builds.clone(),
            limits,
        ));
        let account = accounts
            .link(
                Self::OWNER,
                stock_trek_types::cex::cex_id::CexId::Binance,
                "main",
                "0123456789abcdef",
                "fedcba9876543210",
                Utc::now(),
            )
            .expect("link");
        let algorithm =
            builds.register_algorithm(Self::OWNER, "momentum", "acme/momentum", "heads/main");
        backtests.insert(
            Self::OWNER,
            BacktestStatus {
                backtest_id: "bt_1".to_string(),
                algorithm_id: algorithm.algorithm_id.clone(),
                build_id: "build_1".to_string(),
                strategy_name: "momentum_v2".to_string(),
                state: BacktestState::Completed,
                error: None,
                seed: 7,
                progress: BacktestProgress {
                    completed: 2,
                    total: 2,
                },
                created_at: Utc::now(),
                started_at: Some(Utc::now()),
                completed_at: Some(Utc::now()),
                stream_sequence: 0,
            },
        );
        bots.insert(
            Self::OWNER,
            Bot {
                bot_id: "bot_1".to_string(),
                name: "momentum".to_string(),
                algorithm_id: algorithm.algorithm_id.clone(),
                build_id: "build_1".to_string(),
                strategy_name: "momentum_v2".to_string(),
                account_ids: vec![account.account_id.clone()],
                schedule: Schedule::Interval {
                    every_seconds: 60,
                    misfire_grace_seconds: None,
                },
                state: BotState::Disabled,
                active_run_id: None,
                version: 1,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                stream_sequence: 0,
            },
        );
        let service = StreamService::new(
            Arc::clone(&registry),
            Arc::new(AccountService::new(accounts.clone(), bot_service)),
            builds,
            Arc::clone(&bots),
            backtests,
        );
        Self {
            service,
            registry,
            runs: Arc::clone(&runs_store),
            account_id: account.account_id,
            algorithm_id: algorithm.algorithm_id,
            bot_id: "bot_1".to_string(),
            backtest_id: "bt_1".to_string(),
        }
    }

    fn connect_and_subscribe(
        service: &StreamService,
        channel: &str,
        last_sequence: Option<u64>,
    ) -> Subscription {
        service.connect("conn_1", Self::OWNER);
        match service.subscribe("conn_1", channel, last_sequence) {
            joined @ Subscription::Joined { .. } => joined,
            Subscription::Gap(_) | Subscription::UnknownChannel => panic!("subscription must join"),
        }
    }
}

#[test]
fn stream_channels_follow_the_documented_resource_ids() {
    let fixture = ChannelFixture::new();
    let owner = ChannelFixture::OWNER;
    assert!(
        fixture
            .service
            .owns_channel(owner, &format!("portfolio.{}", fixture.account_id))
    );
    assert!(
        fixture
            .service
            .owns_channel(owner, &format!("builds.{}", fixture.algorithm_id))
    );
    assert!(
        fixture
            .service
            .owns_channel(owner, &format!("runs.{}", fixture.bot_id))
    );
    assert!(
        !fixture
            .service
            .owns_channel(owner, &format!("runs.{}", fixture.algorithm_id))
    );
    assert!(
        !fixture
            .service
            .owns_channel(owner, &format!("builds.{}", fixture.bot_id))
    );
    assert!(
        !fixture
            .service
            .owns_channel("user_other", &format!("portfolio.{}", fixture.account_id))
    );
    assert!(!fixture.service.owns_channel(owner, "portfolio.unknown"));
    assert!(!fixture.service.owns_channel(owner, "unknown.channel"));
}

#[test]
fn tickets_bind_the_owner_and_the_session() {
    let fixture = Fixture::new();
    let (_token, session_id) = fixture.login();
    let ticket = fixture.ticket(&session_id);
    let redeemed = fixture
        .tickets
        .redeem(&ticket, Utc::now())
        .expect("fresh ticket");
    assert_eq!(redeemed.0, Fixture::OWNER);
    assert_eq!(redeemed.1, session_id);
}

#[test]
fn tickets_redeem_exactly_once() {
    let fixture = Fixture::new();
    let (_token, session_id) = fixture.login();
    let ticket = fixture.ticket(&session_id);
    assert!(fixture.tickets.redeem(&ticket, Utc::now()).is_some());
    assert!(fixture.tickets.redeem(&ticket, Utc::now()).is_none());
}

#[test]
fn expired_tickets_are_rejected() {
    let fixture = Fixture::new();
    let (_token, session_id) = fixture.login();
    let (ticket, expires_at) = fixture
        .tickets
        .issue(Fixture::OWNER, &session_id, Utc::now());
    assert!(
        fixture
            .tickets
            .redeem(&ticket, expires_at + Duration::seconds(1))
            .is_none()
    );
}

#[test]
fn revoking_the_session_marks_it_revoked() {
    let fixture = Fixture::new();
    let (token, session_id) = fixture.login();
    let ticket = fixture.ticket(&session_id);
    assert!(fixture.tickets.redeem(&ticket, Utc::now()).is_some());
    assert!(!fixture.store.revoked(&session_id));
    fixture.store.revoke(&token);
    assert!(fixture.store.revoked(&session_id));
}

#[test]
fn unknown_tokens_have_no_session() {
    let fixture = Fixture::new();
    assert!(fixture.store.session_id("not-a-token").is_none());
    assert!(fixture.store.revoked("sess_unknown"));
}

#[test]
fn a_channel_accepts_a_subscription_before_any_frame_is_published() {
    let fixture = ChannelFixture::new();
    let channel = format!("runs.{}", fixture.bot_id);
    let Subscription::Joined {
        replay, mut live, ..
    } = ChannelFixture::connect_and_subscribe(&fixture.service, &channel, None)
    else {
        unreachable!()
    };
    assert!(replay.is_empty());
    fixture.service.registry().publish(
        &channel,
        Frame::data(&channel, 0, serde_json::json!({ "runId": "run_1" })),
    );
    let frame = live.blocking_recv().expect("live frame");
    assert_eq!(frame.sequence, 1);
    assert_eq!(frame.channel, channel);
}

#[test]
fn resubscribing_replaces_the_previous_subscription() {
    let fixture = ChannelFixture::new();
    let channel = format!("runs.{}", fixture.bot_id);
    fixture.service.connect("conn_1", ChannelFixture::OWNER);
    for expected in [1, 2] {
        match fixture.service.subscribe("conn_1", &channel, None) {
            Subscription::Joined { epoch, .. } => assert_eq!(epoch, expected),
            Subscription::Gap(_) | Subscription::UnknownChannel => panic!("subscription must join"),
        }
    }
    assert_eq!(
        fixture.service.subscription_epoch("conn_1", &channel),
        Some(2)
    );
    fixture.service.unsubscribe("conn_1", &channel);
    assert_eq!(fixture.service.subscription_epoch("conn_1", &channel), None);
}

#[test]
fn backtest_channels_replay_the_stored_frames_from_the_beginning() {
    let fixture = ChannelFixture::new();
    let channel = format!("backtests.{}", fixture.backtest_id);
    fixture.service.registry().publish(
        &channel,
        Frame::backtest_result(&channel, 0, 1, serde_json::json!({ "pnl": 1 })),
    );
    fixture
        .service
        .registry()
        .publish(&channel, Frame::backtest_final(&channel, 0, "completed"));
    for last_sequence in [None, Some(0)] {
        let Subscription::Joined { replay, .. } =
            ChannelFixture::connect_and_subscribe(&fixture.service, &channel, last_sequence)
        else {
            unreachable!()
        };
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].sequence, 1);
        assert_eq!(replay[1].sequence, 2);
    }
}

#[tokio::test]
async fn unsubscribing_stops_frame_forwarding() {
    let fixture = ChannelFixture::new();
    let service = Arc::new(fixture.service);
    let channel = format!("runs.{}", fixture.bot_id);
    let Subscription::Joined {
        replay,
        live,
        epoch,
    } = ChannelFixture::connect_and_subscribe(&service, &channel, None)
    else {
        unreachable!()
    };
    let replay = replay
        .iter()
        .map(|frame| serde_json::to_string(frame).expect("frame json"))
        .collect();
    let (outbound, mut outbound_rx) = mpsc::channel::<Message>(16);
    let forwarder = tokio::spawn(Connection::forward(
        Arc::clone(&service),
        "conn_1".to_string(),
        channel.clone(),
        epoch,
        replay,
        live,
        outbound,
    ));
    service.unsubscribe("conn_1", &channel);
    service.registry().publish(
        &channel,
        Frame::data(&channel, 0, serde_json::json!({ "runId": "run_1" })),
    );
    forwarder.await.expect("forwarder stops");
    assert!(outbound_rx.recv().await.is_none());
}

#[test]
fn client_actions_parse_the_documented_camel_case_wire_format() {
    let action: super::ws_channels::ClientAction = serde_json::from_str(
        r#"{"action": "subscribe", "channel": "runs.bot_1", "lastSequence": 41}"#,
    )
    .expect("documented subscribe action");
    assert_eq!(action.action, "subscribe");
    assert_eq!(action.channel, "runs.bot_1");
    assert_eq!(action.last_sequence, Some(41));
    assert!(
        serde_json::from_str::<super::ws_channels::ClientAction>(
            r#"{"action": "unsubscribe", "channel": "runs.bot_1", "unknown": true}"#
        )
        .is_err()
    );
}

#[test]
fn schedule_frames_carry_camel_case_fields() {
    let schedule = serde_json::to_value(Schedule::Interval {
        every_seconds: 60,
        misfire_grace_seconds: Some(60),
    })
    .expect("schedule json");
    assert_eq!(schedule["kind"], "interval");
    assert_eq!(schedule["everySeconds"], 60);
    assert_eq!(schedule["misfireGraceSeconds"], 60);
}

#[test]
fn run_state_changes_publish_through_the_shared_registry() {
    let fixture = ChannelFixture::new();
    let channel = format!("runs.{}", fixture.bot_id);
    assert_eq!(fixture.runs.stream_sequence(&fixture.bot_id), 0);
    let run = crate::bots::Run {
        run_id: "run_1".to_string(),
        bot_id: fixture.bot_id.clone(),
        algorithm_id: fixture.algorithm_id.clone(),
        build_id: Some("build_1".to_string()),
        strategy_name: Some("momentum_v2".to_string()),
        trigger: crate::bots::RunTrigger::AdHoc,
        scheduled_at: Utc::now(),
        state: crate::bots::RunState::Pending,
        completed_at: None,
        log_available: false,
        cancel_requested: false,
        error: None,
        skip_reason: None,
    };
    fixture.runs.insert(ChannelFixture::OWNER, run);
    let head = fixture.registry.sequence(&channel);
    assert_eq!(fixture.runs.stream_sequence(&fixture.bot_id), head);
    assert_eq!(head, 1);
    fixture.service.connect("conn_1", ChannelFixture::OWNER);
    let Subscription::Joined { replay, .. } =
        fixture.service.subscribe("conn_1", &channel, Some(head))
    else {
        panic!("subscribing at the REST head sequence must join without a gap")
    };
    assert!(replay.is_empty());
    let updated = fixture
        .runs
        .update(ChannelFixture::OWNER, "run_1", |mut run| {
            run.state = crate::bots::RunState::Running;
            run
        })
        .expect("run exists");
    let Subscription::Joined { replay, .. } =
        fixture.service.subscribe("conn_1", &channel, Some(head))
    else {
        panic!("resubscription must join")
    };
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].sequence, head + 1);
    let data = replay[0].data.as_ref().expect("run frame");
    assert_eq!(data["runId"], updated.run_id);
    assert_eq!(data["state"], "running");
}
