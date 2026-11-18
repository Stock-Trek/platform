use super::ticket_store::TicketStore;
use crate::auth::SessionStore;
use chrono::{Duration, Utc};
use std::sync::Arc;

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
