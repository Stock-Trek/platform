use crate::replay_key::ReplayKey;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayReservation {
    key: ReplayKey,
    id: u64,
}

impl ReplayReservation {
    pub fn new(key: ReplayKey, id: u64) -> Self {
        Self { key, id }
    }

    pub fn key(&self) -> &ReplayKey {
        &self.key
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}
