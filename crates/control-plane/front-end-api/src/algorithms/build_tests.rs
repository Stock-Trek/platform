use super::BuildStore;
use super::algorithm_api::AlgorithmApi;
use super::build_api::BuildApi;
use crate::accounts::AccountStore;
use crate::algorithms::Strategy;
use crate::api::{ApiRequest, IdempotencyStore};
use crate::auth::{SessionService, SessionStore};
use crate::backtests::{BacktestProgress, BacktestState, BacktestStatus, BacktestStore};
use crate::bots::BotStore;
use crate::limits::{LimitService, LimitSources};
use crate::streaming::ChannelRegistry;
use chrono::Utc;
use http_body_util::Full;
use hyper::{HeaderMap, Method, Uri, body::Bytes};
use std::sync::Arc;

struct Fixture {
    api: BuildApi,
    builds: Arc<BuildStore>,
    backtests: Arc<BacktestStore>,
    registry: Arc<ChannelRegistry>,
    bearer: String,
    algorithm_id: String,
    sessions: Arc<SessionService>,
    bots: Arc<BotStore>,
    idempotency: Arc<IdempotencyStore>,
    initial_build_id: String,
}

impl Fixture {
    const LOGIN_ID: &'static str = "login_9f2b";
    const LOGIN_SECRET: &'static str = "correct-horse";
    const OWNER: &'static str = "user_7gk2";

    fn new() -> Self {
        let registry = Arc::new(ChannelRegistry::new());
        let builds = Arc::new(BuildStore::new(Arc::clone(&registry)));
        let backtests = Arc::new(BacktestStore::new(Arc::clone(&registry)));
        let algorithm = builds.register_algorithm(Self::OWNER, "momentum", "acme/momentum", "main");
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
                name: "momentum_v2".to_string(),
                description: "momentum strategy".to_string(),
            }],
            Utc::now(),
        );
        let store = Arc::new(SessionStore::new());
        store.register_login(Self::LOGIN_ID, Self::OWNER, Self::LOGIN_SECRET);
        let limits = Arc::new(LimitService::new(LimitSources {
            accounts: Arc::new(AccountStore::new()),
            builds: builds.clone(),
            bots: Arc::new(BotStore::new()),
            backtests: Arc::new(BacktestStore::new(Arc::clone(&registry))),
        }));
        let sessions = Arc::new(SessionService::new(store.clone(), limits));
        let tokens = store.issue_login(Self::OWNER, Utc::now());
        let bots = Arc::new(BotStore::new());
        let idempotency = Arc::new(IdempotencyStore::new());
        Self {
            api: BuildApi::new(
                builds.clone(),
                sessions.clone(),
                bots.clone(),
                backtests.clone(),
                idempotency.clone(),
            ),
            builds,
            backtests,
            registry,
            bearer: format!("Bearer {}", tokens.token),
            algorithm_id: algorithm.algorithm_id,
            sessions,
            bots,
            idempotency,
            initial_build_id: build.build_id,
        }
    }

    fn algorithm_api(&self) -> AlgorithmApi {
        AlgorithmApi::new(
            self.builds.clone(),
            self.sessions.clone(),
            self.bots.clone(),
            self.backtests.clone(),
            self.idempotency.clone(),
        )
    }

    fn request(&self, method: Method, path: &str, body: &str) -> ApiRequest {
        let uri: Uri = path.parse().expect("uri");
        let mut map = HeaderMap::new();
        map.insert(
            hyper::header::AUTHORIZATION,
            hyper::header::HeaderValue::from_str(&self.bearer).expect("bearer"),
        );
        if !body.is_empty() {
            map.insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("application/json"),
            );
        }
        ApiRequest::from_parts(
            method,
            uri,
            map,
            Bytes::from(body.to_string()),
            None,
            hyper::http::Extensions::new(),
        )
    }

    fn plain_request(method: Method, path: &str) -> ApiRequest {
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

    fn trigger(&self) -> String {
        format!("/v1/algorithms/{}/builds", self.algorithm_id)
    }

    fn trigger_ready_build(&self, git_ref: &str) -> String {
        let response = self
            .api
            .route(
                &self.request(
                    Method::POST,
                    &self.trigger(),
                    &format!(r#"{{"ref": "{git_ref}"}}"#),
                ),
                &self.algorithm_id,
            )
            .expect("trigger");
        let build_id = Fixture::json(&response)["buildId"]
            .as_str()
            .expect("build id")
            .to_string();
        self.builds
            .mark_ready(
                Fixture::OWNER,
                &self.algorithm_id,
                &build_id,
                Vec::new(),
                Utc::now(),
            )
            .expect("ready");
        build_id
    }

    fn latest_ready(&self) -> Option<String> {
        self.builds
            .algorithm(Fixture::OWNER, &self.algorithm_id)
            .expect("algorithm")
            .latest_ready_build_id
    }
    fn json(response: &hyper::Response<Full<Bytes>>) -> serde_json::Value {
        serde_json::from_slice(
            response
                .body()
                .clone()
                .into_inner()
                .unwrap_or_default()
                .as_ref(),
        )
        .expect("body")
    }
}

#[test]
fn triggering_a_build_returns_the_queued_build() {
    let fixture = Fixture::new();
    let response = fixture
        .api
        .route(
            &fixture.request(
                Method::POST,
                &fixture.trigger(),
                r#"{"ref": "heads/develop"}"#,
            ),
            &fixture.algorithm_id,
        )
        .expect("trigger");
    assert_eq!(response.status(), hyper::StatusCode::ACCEPTED);
    let build = Fixture::json(&response);
    assert_eq!(build["state"], "queued");
    let commit = build["commit"].as_str().expect("commit");
    assert_eq!(commit.len(), 40);
    assert!(
        commit
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    );
}

#[test]
fn triggering_a_build_resolves_a_commit_abbreviation() {
    let fixture = Fixture::new();
    let ready = fixture
        .builds
        .builds(
            Fixture::OWNER,
            &fixture.algorithm_id,
            Some("heads/main"),
            Some(crate::algorithms::BuildState::Ready),
        )
        .remove(0);
    let abbreviation = &ready.commit[..7];
    let body = format!(r#"{{"ref": "heads/main", "commit": "{abbreviation}"}}"#);
    let response = fixture
        .api
        .route(
            &fixture.request(Method::POST, &fixture.trigger(), &body),
            &fixture.algorithm_id,
        )
        .expect("trigger");
    assert_eq!(response.status(), hyper::StatusCode::ACCEPTED);
    let build = Fixture::json(&response);
    assert_eq!(build["commit"], ready.commit.as_str());
    assert_eq!(build["buildId"], ready.build_id.as_str());
}

#[test]
fn triggering_a_build_rejects_an_unknown_commit() {
    let fixture = Fixture::new();
    let problem = fixture
        .api
        .route(
            &fixture.request(
                Method::POST,
                &fixture.trigger(),
                r#"{"ref": "heads/main", "commit": "abcdef1234567"}"#,
            ),
            &fixture.algorithm_id,
        )
        .expect_err("unknown commits are rejected");
    assert_eq!(problem.status(), hyper::StatusCode::BAD_REQUEST);
}

#[test]
fn triggering_a_build_reuses_an_identical_ready_build() {
    let fixture = Fixture::new();
    let response = fixture
        .api
        .route(
            &fixture.request(Method::POST, &fixture.trigger(), "{}"),
            &fixture.algorithm_id,
        )
        .expect("trigger");
    let build = Fixture::json(&response);
    assert_eq!(build["state"], "ready");
    let ready = fixture.builds.builds(
        Fixture::OWNER,
        &fixture.algorithm_id,
        Some("heads/main"),
        Some(crate::algorithms::BuildState::Ready),
    );
    assert_eq!(ready.len(), 1);
    assert_eq!(build["buildId"], ready[0].build_id);
}

#[test]
fn triggering_a_build_defaults_to_the_algorithm_ref() {
    let fixture = Fixture::new();
    let response = fixture
        .api
        .route(
            &fixture.request(Method::POST, &fixture.trigger(), "{}"),
            &fixture.algorithm_id,
        )
        .expect("trigger");
    let build = Fixture::json(&response);
    assert_eq!(build["ref"], "heads/main");
}

#[test]
fn triggering_a_build_on_an_archived_algorithm_conflicts() {
    let fixture = Fixture::new();
    fixture
        .builds
        .archive_algorithm(Fixture::OWNER, &fixture.algorithm_id)
        .expect("archive");
    let problem = fixture
        .api
        .route(
            &fixture.request(Method::POST, &fixture.trigger(), r#"{"ref": "heads/main"}"#),
            &fixture.algorithm_id,
        )
        .expect_err("archived algorithms refuse new builds");
    assert_eq!(problem.status(), hyper::StatusCode::CONFLICT);
}

#[test]
fn builds_require_authentication() {
    let fixture = Fixture::new();
    let request = Fixture::plain_request(Method::GET, &fixture.trigger());
    let problem = fixture
        .api
        .route(&request, &fixture.algorithm_id)
        .expect_err("list builds needs a token");
    assert_eq!(problem.status(), hyper::StatusCode::UNAUTHORIZED);
    assert_eq!(
        fixture
            .builds
            .builds(
                Fixture::OWNER,
                &fixture.algorithm_id,
                None,
                Some(crate::algorithms::BuildState::Ready)
            )
            .len(),
        1
    );
}

#[test]
fn build_frames_carry_the_state_change_without_a_stream_sequence() {
    let registry = Arc::new(ChannelRegistry::new());
    let builds = Arc::new(BuildStore::new(Arc::clone(&registry)));
    let algorithm = builds.register_algorithm("user_7gk2", "momentum", "acme/momentum", "main");
    let build = builds
        .create_build(
            "user_7gk2",
            &algorithm.algorithm_id,
            "heads/main",
            None,
            Utc::now(),
        )
        .expect("build");
    let frames = registry.frames(&format!("builds.{}", algorithm.algorithm_id));
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].sequence, build.stream_sequence);
    let data = frames[0].data.as_ref().expect("data frame");
    assert!(data.get("streamSequence").is_none());
    assert_eq!(data["state"], "queued");
}

#[test]
fn deleting_a_build_closes_its_backtest_channels() {
    let fixture = Fixture::new();
    let response = fixture
        .api
        .route(
            &fixture.request(Method::POST, &fixture.trigger(), r#"{}"#),
            &fixture.algorithm_id,
        )
        .expect("trigger");
    assert_eq!(response.status(), hyper::StatusCode::ACCEPTED);
    let build_id = Fixture::json(&response)["buildId"]
        .as_str()
        .expect("build id")
        .to_string();
    fixture
        .builds
        .mark_ready(
            Fixture::OWNER,
            &fixture.algorithm_id,
            &build_id,
            Vec::new(),
            Utc::now(),
        )
        .expect("ready");
    let backtest = BacktestStatus {
        backtest_id: "backtest_1".to_string(),
        algorithm_id: fixture.algorithm_id.clone(),
        build_id: build_id.clone(),
        strategy_name: "momentum_v2".to_string(),
        state: BacktestState::Running,
        error: None,
        seed: 7,
        progress: BacktestProgress {
            completed: 0,
            total: 1,
        },
        created_at: Utc::now(),
        started_at: None,
        completed_at: None,
        stream_sequence: 0,
    };
    fixture.backtests.insert(Fixture::OWNER, backtest);
    fixture
        .backtests
        .update(Fixture::OWNER, "backtest_1", |mut status| {
            status.state = BacktestState::Completed;
            status.completed_at = Some(Utc::now());
            status
        });
    let channel = "backtests.backtest_1";
    assert_eq!(fixture.registry.frames(channel).len(), 1);
    let response = fixture
        .api
        .route(
            &fixture.request(
                Method::DELETE,
                &format!("/v1/algorithms/{}/builds/{build_id}", fixture.algorithm_id),
                "",
            ),
            &fixture.algorithm_id,
        )
        .expect("delete");
    assert_eq!(response.status(), hyper::StatusCode::NO_CONTENT);
    assert!(fixture.registry.frames(channel).is_empty());
    assert_eq!(fixture.registry.sequence(channel), 0);
}

#[test]
fn deleting_the_latest_ready_build_repoints_latest_ready_build_id() {
    let fixture = Fixture::new();
    let first = fixture.trigger_ready_build("heads/one");
    let second = fixture.trigger_ready_build("heads/two");
    assert_eq!(fixture.latest_ready().as_deref(), Some(second.as_str()));
    assert!(
        fixture
            .builds
            .delete_build(Fixture::OWNER, &fixture.algorithm_id, &second)
    );
    assert_eq!(fixture.latest_ready().as_deref(), Some(first.as_str()));
    assert!(
        fixture
            .builds
            .delete_build(Fixture::OWNER, &fixture.algorithm_id, &first)
    );
    assert_eq!(
        fixture.latest_ready().as_deref(),
        Some(fixture.initial_build_id.as_str())
    );
}

#[test]
fn archiving_an_algorithm_returns_its_version_as_an_etag() {
    let fixture = Fixture::new();
    let response = fixture
        .algorithm_api()
        .route(&fixture.request(
            Method::DELETE,
            &format!("/v1/algorithms/{}", fixture.algorithm_id),
            "",
        ))
        .expect("archive");
    assert_eq!(response.status(), hyper::StatusCode::OK);
    let etag = response
        .headers()
        .get("etag")
        .expect("the archive carries an etag")
        .to_str()
        .expect("ascii etag")
        .to_string();
    let version = Fixture::json(&response)["version"]
        .as_i64()
        .expect("version");
    assert_eq!(etag, format!("\"{version}\""));
}
