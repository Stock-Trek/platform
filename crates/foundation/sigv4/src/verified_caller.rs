#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCaller {
    access_key_id: String,
    owner: String,
}

impl VerifiedCaller {
    pub fn new(access_key_id: String, owner: String) -> Self {
        Self {
            access_key_id,
            owner,
        }
    }

    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }
}
