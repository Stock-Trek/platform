use crate::AccountKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipRecord {
    account: AccountKey,
    owner: String,
    version: u64,
}

impl OwnershipRecord {
    pub fn new(account: AccountKey, owner: impl Into<String>, version: u64) -> Self {
        Self {
            account,
            owner: owner.into(),
            version,
        }
    }

    pub fn account(&self) -> &AccountKey {
        &self.account
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn version(&self) -> u64 {
        self.version
    }
}
