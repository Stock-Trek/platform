use crate::AccountKey;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const OWNERSHIP_STREAM_PREFIX: &str = "portfolio.ownership.changed.v1";
pub const OWNERSHIP_EVENT_SUBJECT: &str = "portfolio.ownership.changed.v1.events";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnershipAction {
    Add,
    Remove,
}

impl OwnershipAction {
    pub fn is_add(self) -> bool {
        matches!(self, Self::Add)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipEvent {
    audit_id: String,
    owner: String,
    account: AccountKey,
    action: OwnershipAction,
    version: u64,
    occurred_at: DateTime<Utc>,
}

impl OwnershipEvent {
    pub fn new(
        audit_id: impl Into<String>,
        owner: impl Into<String>,
        account: AccountKey,
        action: OwnershipAction,
        version: u64,
        occurred_at: DateTime<Utc>,
    ) -> Self {
        Self {
            audit_id: audit_id.into(),
            owner: owner.into(),
            account,
            action,
            version,
            occurred_at,
        }
    }

    pub fn audit_id(&self) -> &str {
        &self.audit_id
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn account(&self) -> &AccountKey {
        &self.account
    }

    pub fn action(&self) -> OwnershipAction {
        self.action
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
}
