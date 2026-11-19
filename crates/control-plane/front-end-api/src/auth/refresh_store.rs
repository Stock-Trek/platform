use crate::auth::SessionTokens;
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub const REFRESH_REPLAY_GRACE: Duration = Duration::seconds(10);

fn digest(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
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

pub enum Rotation {
    None,
    Rotated(SessionTokens),
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

    pub fn enroll(&self, token: String, owner: &str, family: &str) {
        self.state.lock().entries.insert(
            digest(&token),
            RefreshEntry {
                owner: owner.to_string(),
                family: family.to_string(),
                state: RefreshState::Active,
            },
        );
    }

    pub fn rotate(
        &self,
        token: &str,
        now: DateTime<Utc>,
        mint: impl FnOnce(&str, &str, DateTime<Utc>) -> SessionTokens,
    ) -> Rotation {
        let mut state = self.state.lock();
        let Some(entry) = state.entries.get(&digest(token)) else {
            return Rotation::None;
        };
        let family = entry.family.clone();
        let owner = entry.owner.clone();
        let Some(family_expiry) = state.families.get(&family).copied() else {
            return Rotation::None;
        };
        if family_expiry <= now {
            Self::revoke(&mut state, &family);
            return Rotation::FamilyRevoked { family };
        }
        if let RefreshState::Rotated {
            replay,
            grace_until,
        } = &entry.state
        {
            if *grace_until >= now {
                return Rotation::Replayed(replay.clone());
            }
            Self::revoke(&mut state, &family);
            return Rotation::FamilyRevoked { family };
        }
        if matches!(&entry.state, RefreshState::Revoked) {
            return Rotation::None;
        }
        let replacement = mint(&owner, &family, family_expiry);
        let presented = digest(token);
        if let Some(refresh_token) = replacement.refresh_token.clone() {
            state.entries.insert(
                digest(&refresh_token),
                RefreshEntry {
                    owner,
                    family: family.clone(),
                    state: RefreshState::Active,
                },
            );
        }
        if let Some(entry) = state.entries.get_mut(&presented) {
            entry.state = RefreshState::Rotated {
                replay: replacement.clone(),
                grace_until: now + REFRESH_REPLAY_GRACE,
            };
        }
        Rotation::Rotated(replacement)
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
