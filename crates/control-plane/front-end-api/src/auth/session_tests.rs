use super::{SessionApi, SessionService, SessionStore};
use crate::api::ApiRequest;
use crate::auth::Scope;
use chrono::{Duration, Utc};
use http_body_util::Full;
use hyper::{HeaderMap, Method, Uri, body::Bytes};
use std::sync::Arc;

struct Fixture {
    api: SessionApi,
    sessions: Arc<SessionService>,
    store: Arc<SessionStore>,
}

impl Fixture {
    const LOGIN_ID: &'static str = "login_9f2b";
    const LOGIN_SECRET: &'static str = "correct-horse";
    const OWNER: &'static str = "user_7gk2";

    fn new() -> Self {
        let store = Arc::new(SessionStore::new());
        store.register_login(Self::LOGIN_ID, Self::OWNER, Self::LOGIN_SECRET);
        let sessions = Arc::new(SessionService::new(store.clone()));
        Self {
            api: SessionApi::new(sessions.clone()),
            sessions,
            store,
        }
    }

    fn request(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> ApiRequest {
        let uri: Uri = path.parse().expect("uri");
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.insert(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).expect("name"),
                hyper::header::HeaderValue::from_str(value).expect("value"),
            );
        }
        ApiRequest::from_parts(method, uri, map, Bytes::from(body.to_string()))
    }

    fn login_tokens(&self) -> serde_json::Value {
        let response = self
            .api
            .route(&self.request(Method::POST, "/v1/session", &[], &self.login_body()))
            .expect("login");
        let status = response.status();
        let body = Self::json(&response);
        assert_eq!(status, hyper::StatusCode::CREATED);
        body
    }

    fn login_body(&self) -> String {
        format!(
            r#"{{ "loginId": "{}", "loginSecret": "{}" }}"#,
            Self::LOGIN_ID,
            Self::LOGIN_SECRET
        )
    }

    fn bearer(&self, token: &str) -> String {
        format!("Bearer {token}")
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
fn login_issues_read_and_manage_scopes() {
    let fixture = Fixture::new();
    let tokens = fixture.login_tokens();
    assert_eq!(tokens["owner"], Fixture::OWNER);
    assert_eq!(tokens["scope"][0], "read");
    assert_eq!(tokens["scope"][1], "manage");
    assert!(tokens["refreshToken"].is_string());
}

#[test]
fn login_rejects_unknown_credentials() {
    let fixture = Fixture::new();
    let response = fixture.api.route(&fixture.request(
        Method::POST,
        "/v1/session",
        &[],
        r#"{"loginId": "login_9f2b", "loginSecret": "wrong"}"#,
    ));
    let problem = response.expect_err("login must fail");
    assert_eq!(problem.status(), hyper::StatusCode::UNAUTHORIZED);
}

#[test]
fn refresh_rotates_and_replays_within_grace() {
    let fixture = Fixture::new();
    let tokens = fixture.login_tokens();
    let refresh_token = tokens["refreshToken"]
        .as_str()
        .expect("refresh")
        .to_string();
    let first = fixture
        .api
        .route(&fixture.request(
            Method::POST,
            "/v1/session/refresh",
            &[],
            &format!(r#"{{ "refreshToken": "{refresh_token}" }}"#),
        ))
        .expect("rotate");
    assert_eq!(first.status(), hyper::StatusCode::CREATED);
    let rotated = Fixture::json(&first);
    let replay = fixture
        .api
        .route(&fixture.request(
            Method::POST,
            "/v1/session/refresh",
            &[],
            &format!(r#"{{ "refreshToken": "{refresh_token}" }}"#),
        ))
        .expect("replay");
    let replayed = Fixture::json(&replay);
    assert_eq!(rotated["token"], replayed["token"]);
    assert_eq!(rotated["refreshToken"], replayed["refreshToken"]);
}

#[test]
fn reuse_of_a_rotated_token_revokes_the_session() {
    let fixture = Fixture::new();
    let now = Utc::now();
    let tokens = fixture.store.issue_login(Fixture::OWNER, now);
    let refresh_token = tokens.refresh_token.expect("refresh");
    let rotated = fixture
        .store
        .rotate_refresh(&refresh_token, now + Duration::seconds(1))
        .expect("rotate");
    let replayed = fixture
        .store
        .rotate_refresh(&refresh_token, now + Duration::seconds(11))
        .expect("replay within grace");
    assert_eq!(rotated.token, replayed.token);
    assert!(
        fixture
            .store
            .rotate_refresh(&refresh_token, now + Duration::seconds(12))
            .is_none()
    );
    assert!(
        fixture
            .store
            .rotate_refresh(
                rotated.refresh_token.as_ref().expect("new refresh"),
                now + Duration::seconds(13)
            )
            .is_none()
    );
    assert!(
        fixture
            .store
            .access(&rotated.token, now + Duration::seconds(13))
            .is_none()
    );
}

#[test]
fn expired_refresh_tokens_revoke_the_family() {
    let fixture = Fixture::new();
    let now = Utc::now();
    let tokens = fixture.store.issue_login(Fixture::OWNER, now);
    let refresh_token = tokens.refresh_token.expect("refresh");
    assert!(
        fixture
            .store
            .rotate_refresh(&refresh_token, now + Duration::hours(13))
            .is_none()
    );
    assert!(fixture.store.access(&tokens.token, now).is_none());
}

#[test]
fn step_up_grants_admin_scope() {
    let fixture = Fixture::new();
    let tokens = fixture.login_tokens();
    let access = tokens["token"].as_str().expect("access").to_string();
    let bearer = fixture.bearer(&access);
    let response = fixture
        .api
        .route(&fixture.request(
            Method::POST,
            "/v1/session/step-up",
            &[("Authorization", bearer.as_str())],
            &format!(r#"{{ "loginSecret": "{}" }}"#, Fixture::LOGIN_SECRET),
        ))
        .expect("step-up");
    let step_up = Fixture::json(&response);
    assert_eq!(response.status(), hyper::StatusCode::CREATED);
    assert!(step_up["token"].is_string());
}

#[test]
fn step_up_rejects_a_wrong_secret() {
    let fixture = Fixture::new();
    let tokens = fixture.login_tokens();
    let bearer = fixture.bearer(tokens["token"].as_str().expect("access"));
    let problem = fixture
        .api
        .route(&fixture.request(
            Method::POST,
            "/v1/session/step-up",
            &[("Authorization", bearer.as_str())],
            r#"{"loginSecret": "nope"}"#,
        ))
        .expect_err("step-up must fail");
    assert_eq!(problem.status(), hyper::StatusCode::UNAUTHORIZED);
}

#[test]
fn revocation_stops_the_access_token_immediately() {
    let fixture = Fixture::new();
    let tokens = fixture.login_tokens();
    let access = tokens["token"].as_str().expect("access").to_string();
    let bearer = fixture.bearer(&access);
    let response = fixture
        .api
        .route(&fixture.request(
            Method::DELETE,
            "/v1/session",
            &[("Authorization", bearer.as_str())],
            "",
        ))
        .expect("revoke");
    assert_eq!(response.status(), hyper::StatusCode::NO_CONTENT);
    let problem = fixture
        .sessions
        .authorize_request(
            &fixture.request(
                Method::GET,
                "/v1/limits",
                &[("Authorization", bearer.as_str())],
                "",
            ),
            Scope::Read,
        )
        .expect_err("revoked token must fail");
    assert_eq!(problem.status(), hyper::StatusCode::UNAUTHORIZED);
}

#[test]
fn unknown_paths_answer_not_found() {
    let fixture = Fixture::new();
    let problem = fixture
        .api
        .route(&fixture.request(Method::GET, "/v1/session/unknown", &[], ""))
        .expect_err("must be not found");
    assert_eq!(problem.status(), hyper::StatusCode::NOT_FOUND);
}

#[test]
fn scopes_are_parsed_and_required() {
    assert_eq!(Scope::parse("read"), Some(Scope::Read));
    assert_eq!(Scope::parse("manage"), Some(Scope::Manage));
    assert_eq!(Scope::parse("admin"), Some(Scope::Admin));
    assert_eq!(Scope::parse("other"), None);
}
