use crate::AccountMapping;
use chrono::{DateTime, Utc};
use platform_portfolio_ownership::{AccountKey, OwnershipEvent};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OwnershipRecord {
    pub(crate) account: AccountKey,
    pub(crate) owner: String,
    pub(crate) active: bool,
    pub(crate) version: u64,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) audit: Vec<OwnershipEvent>,
    pub(crate) pending_events: Vec<OwnershipEvent>,
}

impl OwnershipRecord {
    pub(crate) fn mapping(&self) -> AccountMapping {
        AccountMapping::new(
            self.account.clone(),
            self.owner.clone(),
            self.active,
            self.version,
            self.updated_at,
        )
    }
}
