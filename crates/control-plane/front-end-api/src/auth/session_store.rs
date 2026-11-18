use crate::api::Id;
use crate::auth::Scope;
use crate::auth::{SessionTokens, StepUpTokens};
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;

pub const ACCESS_TTL: Duration = Duration::minutes(15);
pub const REFRESH_TTL: Duration = Duration::hours(12);
pub const REFRESH_REPLAY_GRACE: Duration = Duration::seconds(10);
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

enum RefreshState {
    Active,
    Rotated {
        replay: SessionTokens,
        grace_until: DateTime<Utc>,
    },
    Revoked,
}

struct RefreshEntry {
    owner: String,
    family: String,
    state: RefreshState,
}

enum Decision {
    Unknown,
    Expired,
    Active { owner: String, family: String },
    Replayed(SessionTokens),
    Stolen,
    Revoked,
}

#[derive(Default)]
struct State {
    logins: HashMap<String, Login>,
    access: HashMap<String, AccessSession>,
    refresh: HashMap<String, RefreshEntry>,
    families: HashMap<String, DateTime<Utc>>,
}

pub struct SessionStore {
    state: Mutex<State>,
}

impl SessionStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }

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
        // TODO: delegate credential verification and owner resolution to the auth plane.
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
        let tokens = Self::tokens(owner, &LOGIN_SCOPES, now, Some(now + REFRESH_TTL));
        let mut state = self.state.lock();
        state.families.insert(family.clone(), now + REFRESH_TTL);
        Self::admit(&mut state, &family, &tokens);
        if let Some(refresh_token) = tokens.refresh_token.clone() {
            state.refresh.insert(
                refresh_token,
                RefreshEntry {
                    owner: owner.to_string(),
                    family,
                    state: RefreshState::Active,
                },
            );
        }
        tokens
    }

    pub fn access(&self, token: &str, now: DateTime<Utc>) -> Option<(String, Vec<Scope>)> {
        // TODO: verify the EdDSA (Ed25519) JWT against the auth plane JWKS and check revocation there.
        let state = self.state.lock();
        let session = state.access.get(token)?;
        if session.revoked || session.expires_at <= now {
            return None;
        }
        Some((session.owner.clone(), session.scopes.clone()))
    }

    pub fn rotate_refresh(&self, token: &str, now: DateTime<Utc>) -> Option<SessionTokens> {
        let mut state = self.state.lock();
        match Self::decision(&state, token, now) {
            Decision::Unknown | Decision::Revoked => None,
            Decision::Expired | Decision::Stolen => {
                Self::revoke_family(&mut state, token);
                None
            }
            Decision::Replayed(tokens) => Some(tokens),
            Decision::Active { owner, family } => {
                let family_expiry = *state.families.get(&family)?;
                let replacement = Self::tokens(&owner, &LOGIN_SCOPES, now, Some(family_expiry));
                Self::admit(&mut state, &family, &replacement);
                if let Some(refresh_token) = replacement.refresh_token.clone() {
                    state.refresh.insert(
                        refresh_token,
                        RefreshEntry {
                            owner: owner.clone(),
                            family: family.clone(),
                            state: RefreshState::Active,
                        },
                    );
                }
                if let Some(entry) = state.refresh.get_mut(token) {
                    entry.state = RefreshState::Rotated {
                        replay: replacement.clone(),
                        grace_until: now + REFRESH_REPLAY_GRACE,
                    };
                }
                Some(replacement)
            }
        }
    }

    pub fn issue_step_up(&self, owner: &str, now: DateTime<Utc>) -> StepUpTokens {
        let scopes = STEP_UP_SCOPES.to_vec();
        let tokens = Self::tokens(owner, &scopes, now, None);
        let mut state = self.state.lock();
        Self::admit(&mut state, "", &tokens);
        StepUpTokens {
            token: tokens.token,
            expires_at: tokens.expires_at,
        }
    }

    pub fn revoke(&self, token: &str) {
        let mut state = self.state.lock();
        let Some(session) = state.access.get(token) else {
            return;
        };
        let family = session.family.clone();
        if let Some(session) = state.access.get_mut(token) {
            session.revoked = true;
        }
        let Some(family) = family else {
            return;
        };
        for entry in state.refresh.values_mut() {
            if entry.family == family {
                entry.state = RefreshState::Revoked;
            }
        }
    }

    fn admit(state: &mut State, family: &str, tokens: &SessionTokens) {
        state.access.insert(
            tokens.token.clone(),
            AccessSession {
                owner: tokens.owner.clone(),
                scopes: tokens.scope.clone(),
                expires_at: tokens.expires_at,
                family: (!family.is_empty()).then(|| family.to_string()),
                revoked: false,
            },
        );
    }

    fn decision(state: &State, token: &str, now: DateTime<Utc>) -> Decision {
        let Some(entry) = state.refresh.get(token) else {
            return Decision::Unknown;
        };
        let Some(family_expiry) = state.families.get(&entry.family) else {
            return Decision::Unknown;
        };
        if *family_expiry <= now {
            return Decision::Expired;
        }
        match &entry.state {
            RefreshState::Active => Decision::Active {
                owner: entry.owner.clone(),
                family: entry.family.clone(),
            },
            RefreshState::Rotated {
                replay,
                grace_until,
            } => {
                if *grace_until >= now {
                    Decision::Replayed(replay.clone())
                } else {
                    Decision::Stolen
                }
            }
            RefreshState::Revoked => Decision::Revoked,
        }
    }

    fn revoke_family(state: &mut State, token: &str) {
        let Some(entry) = state.refresh.get(token) else {
            return;
        };
        let family = entry.family.clone();
        state
            .families
            .insert(family.clone(), DateTime::<Utc>::UNIX_EPOCH);
        for session in state.access.values_mut() {
            if session.family.as_deref() == Some(family.as_str()) {
                session.revoked = true;
            }
        }
        for entry in state.refresh.values_mut() {
            if entry.family == family {
                entry.state = RefreshState::Revoked;
            }
        }
    }

    fn tokens(
        owner: &str,
        scopes: &[Scope],
        now: DateTime<Utc>,
        refresh_expires_at: Option<DateTime<Utc>>,
    ) -> SessionTokens {
        // TODO: mint EdDSA (Ed25519) access tokens through the auth plane.
        SessionTokens {
            token: Id::mint("at"),
            expires_at: now + ACCESS_TTL,
            refresh_token: refresh_expires_at.map(|_| Id::mint("rt")),
            refresh_expires_at,
            owner: owner.to_string(),
            scope: scopes.to_vec(),
        }
    }
}
