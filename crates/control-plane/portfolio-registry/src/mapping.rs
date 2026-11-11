use chrono::{DateTime, Utc};
use platform_portfolio_ownership::AccountKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountMapping {
    account: AccountKey,
    owner: String,
    active: bool,
    version: u64,
    updated_at: DateTime<Utc>,
}

impl AccountMapping {
    pub fn new(
        account: AccountKey,
        owner: impl Into<String>,
        active: bool,
        version: u64,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            account,
            owner: owner.into(),
            active,
            version,
            updated_at,
        }
    }

    pub fn account(&self) -> &AccountKey {
        &self.account
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}
