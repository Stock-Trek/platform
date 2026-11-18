use crate::api::Id;
use crate::auth::Scope;
use crate::auth::jwt::JwtSigner;
use crate::auth::refresh_store::{RefreshStore, Rotation};
use crate::auth::{SessionTokens, StepUpTokens};
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;

pub const ACCESS_TTL: Duration = Duration::minutes(15);
pub const REFRESH_TTL: Duration = Duration::hours(12);
pub const STEP_UP_TTL: Duration = Duration::minutes(5);
pub const LOGIN_SCOPES: [Scope; 2] = [Scope::Read, Scope::Manage];
pub const STEP_UP_SCOPES: [Scope; 3] = [Scope::Read, Scope::Manage, Scope::Admin];

struct Login {
    owner: String,
    secret: String,
}

struct AccessSession {
    owner: String,
    scopes: Vec<Scope>,
    expires_at: DateTime<Utc>,
    family: Option<String>,
    revoked: bool,
}

#[derive(Default)]
struct State {
    logins: HashMap<String, Login>,
    access: HashMap<String, AccessSession>,
}

pub struct SessionStore {
    signer: JwtSigner,
    state: Mutex<State>,
    refresh: RefreshStore,
}

impl SessionStore {
    pub fn new() -> Self {
        Self {
            signer: JwtSigner::generate(),
            state: Mutex::new(State::default()),
            refresh: RefreshStore::new(),
        }
    }

    #[allow(dead_code)]
    pub fn register_login(&self, login_id: &str, owner: &str, secret: &str) {
        self.state.lock().logins.insert(
            login_id.to_string(),
            Login {
                owner: owner.to_string(),
                secret: secret.to_string(),
            },
        );
    }

    pub fn verify_login(&self, login_id: &str, secret: &str) -> Option<String> {
        let state = self.state.lock();
        let login = state.logins.get(login_id)?;
        (login.secret == secret).then(|| login.owner.clone())
    }

    pub fn verify_login_by_owner(&self, owner: &str, secret: &str) -> Option<String> {
        let state = self.state.lock();
        let login = state.logins.values().find(|login| login.owner == owner)?;
        (login.secret == secret).then(|| login.owner.clone())
    }

    pub fn issue_login(&self, owner: &str, now: DateTime<Utc>) -> SessionTokens {
        let family = Id::mint("family");
        let (tokens, session_id) = self.mint_tokens(
            owner,
            &LOGIN_SCOPES,
            now,
            ACCESS_TTL,
            Some(now + REFRESH_TTL),
        );
        self.refresh.open_family(&family, now + REFRESH_TTL);
        {
            let mut state = self.state.lock();
            Self::admit(&mut state, &family, &session_id, &tokens);
        }
        if let Some(refresh_token) = tokens.refresh_token.clone() {
            self.refresh.enroll(refresh_token, owner, &family);
        }
        tokens
    }

    pub fn access(&self, token: &str, now: DateTime<Utc>) -> Option<(String, Vec<Scope>)> {
        let claims = self.signer.verify_access(token)?;
        let scopes = JwtSigner::scopes(&claims.scope)?;
        let state = self.state.lock();
        let session = state.access.get(&claims.sid)?;
        if session.revoked
            || session.expires_at <= now
            || claims.exp <= now.timestamp()
            || session.owner != claims.sub
            || session.scopes != scopes
        {
            return None;
        }
        Some((session.owner.clone(), session.scopes.clone()))
    }

    pub fn session_id(&self, token: &str) -> Option<String> {
        self.signer.verify_session_id(token)
    }

    pub fn revoked(&self, session_id: &str) -> bool {
        let state = self.state.lock();
        state
            .access
            .get(session_id)
            .is_none_or(|session| session.revoked)
    }

    pub fn rotate_refresh(&self, token: &str, now: DateTime<Utc>) -> Option<SessionTokens> {
        match self.refresh.rotate(token, now, |owner, family, family_expiry| {
            let (replacement, session_id) =
                self.mint_tokens(owner, &LOGIN_SCOPES, now, ACCESS_TTL, Some(family_expiry));
            let mut state = self.state.lock();
            Self::admit(&mut state, family, &session_id, &replacement);
            replacement
        }) {
            Rotation::Replayed(tokens) | Rotation::Rotated(tokens) => Some(tokens),
            Rotation::FamilyRevoked { family } => {
                self.revoke_family_access(&family);
                None
            }
            Rotation::None => None,
        }
    }

    pub fn issue_step_up(&self, owner: &str, now: DateTime<Utc>) -> StepUpTokens {
        let (tokens, session_id) = self.mint_tokens(owner, &STEP_UP_SCOPES, now, STEP_UP_TTL, None);
        let mut state = self.state.lock();
        Self::admit(&mut state, "", &session_id, &tokens);
        StepUpTokens {
            token: tokens.token,
            expires_at: tokens.expires_at,
        }
    }

    pub fn revoke(&self, token: &str) {
        let Some(session_id) = self.signer.verify_session_id(token) else {
            return;
        };
        let family = {
            let mut state = self.state.lock();
            let Some(session) = state.access.get_mut(&session_id) else {
                return;
            };
            session.revoked = true;
            session.family.clone()
        };
        if let Some(family) = family {
            self.refresh.revoke_family(&family);
            self.revoke_family_access(&family);
        }
    }

    fn revoke_family_access(&self, family: &str) {
        let mut state = self.state.lock();
        for session in state.access.values_mut() {
            if session.family.as_deref() == Some(family) {
                session.revoked = true;
            }
        }
    }

    fn mint_tokens(
        &self,
        owner: &str,
        scopes: &[Scope],
        now: DateTime<Utc>,
        access_ttl: Duration,
        refresh_expires_at: Option<DateTime<Utc>>,
    ) -> (SessionTokens, String) {
        let session_id = Id::mint("sess");
        let expires_at = now + access_ttl;
        let token = self
            .signer
            .mint_access_token(owner, scopes, &session_id, now, expires_at);
        (
            SessionTokens {
                token,
                expires_at,
                refresh_token: refresh_expires_at.map(|_| Id::mint("rt")),
                refresh_expires_at,
                owner: owner.to_string(),
                scope: scopes.to_vec(),
            },
            session_id,
        )
    }

    fn admit(state: &mut State, family: &str, session_id: &str, tokens: &SessionTokens) {
        state.access.insert(
            session_id.to_string(),
            AccessSession {
                owner: tokens.owner.clone(),
                scopes: tokens.scope.clone(),
                expires_at: tokens.expires_at,
                family: (!family.is_empty()).then(|| family.to_string()),
                revoked: false,
            },
        );
    }
}
