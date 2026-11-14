#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReplayKey {
    token_id: String,
    nonce: String,
}

impl ReplayKey {
    pub fn new(token_id: impl Into<String>, nonce: impl Into<String>) -> Self {
        Self {
            token_id: token_id.into(),
            nonce: nonce.into(),
        }
    }

    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    pub fn nonce(&self) -> &str {
        &self.nonce
    }
}
