use crate::AccountMapping;
use chrono::{DateTime, Utc};
use platform_portfolio_ownership::{AccountKey, OwnershipEvent};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipRecord {
    pub account: AccountKey,
    pub owner: String,
    pub active: bool,
    pub version: u64,
    pub updated_at: DateTime<Utc>,
    pub audit: Vec<OwnershipEvent>,
    pub pending_events: Vec<OwnershipEvent>,
}

impl OwnershipRecord {
    pub fn mapping(&self) -> AccountMapping {
        AccountMapping::new(
            self.account.clone(),
            self.owner.clone(),
            self.active,
            self.version,
            self.updated_at,
        )
    }
}
