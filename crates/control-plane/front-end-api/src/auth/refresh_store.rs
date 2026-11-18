use crate::auth::SessionTokens;
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;

pub const REFRESH_REPLAY_GRACE: Duration = Duration::seconds(10);

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

pub enum Rotation {
    None,
    Active { owner: String, family: String },
    Replayed(SessionTokens),
    FamilyRevoked { family: String },
}

#[derive(Default)]
struct State {
    entries: HashMap<String, RefreshEntry>,
    families: HashMap<String, DateTime<Utc>>,
}

pub struct RefreshStore {
    state: Mutex<State>,
}

impl RefreshStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }

    pub fn open_family(&self, family: &str, expires_at: DateTime<Utc>) {
        self.state
            .lock()
            .families
            .insert(family.to_string(), expires_at);
    }

    pub fn expiry(&self, family: &str) -> Option<DateTime<Utc>> {
        self.state.lock().families.get(family).copied()
    }

    pub fn enroll(&self, token: String, owner: &str, family: &str) {
        self.state.lock().entries.insert(
            token,
            RefreshEntry {
                owner: owner.to_string(),
                family: family.to_string(),
                state: RefreshState::Active,
            },
        );
    }

    pub fn rotate(&self, token: &str, now: DateTime<Utc>) -> Rotation {
        let mut state = self.state.lock();
        let Some(entry) = state.entries.get(token) else {
            return Rotation::None;
        };
        let Some(family_expiry) = state.families.get(&entry.family).copied() else {
            return Rotation::None;
        };
        if family_expiry <= now {
            let family = entry.family.clone();
            Self::revoke(&mut state, &family);
            return Rotation::FamilyRevoked { family };
        }
        match &entry.state {
            RefreshState::Active => Rotation::Active {
                owner: entry.owner.clone(),
                family: entry.family.clone(),
            },
            RefreshState::Rotated {
                replay,
                grace_until,
            } => {
                if *grace_until >= now {
                    Rotation::Replayed(replay.clone())
                } else {
                    let family = entry.family.clone();
                    Self::revoke(&mut state, &family);
                    Rotation::FamilyRevoked { family }
                }
            }
            RefreshState::Revoked => Rotation::None,
        }
    }

    pub fn mark_rotated(&self, token: &str, replacement: &SessionTokens, now: DateTime<Utc>) {
        let mut state = self.state.lock();
        if let Some(entry) = state.entries.get_mut(token) {
            entry.state = RefreshState::Rotated {
                replay: replacement.clone(),
                grace_until: now + REFRESH_REPLAY_GRACE,
            };
        }
    }

    pub fn revoke_family(&self, family: &str) {
        let mut state = self.state.lock();
        Self::revoke(&mut state, family);
    }

    fn revoke(state: &mut State, family: &str) {
        state
            .families
            .insert(family.to_string(), DateTime::<Utc>::UNIX_EPOCH);
        for entry in state.entries.values_mut() {
            if entry.family == family {
                entry.state = RefreshState::Revoked;
            }
        }
    }
}
