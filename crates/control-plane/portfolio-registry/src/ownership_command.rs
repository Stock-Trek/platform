use chrono::{DateTime, Utc};
use platform_portfolio_ownership::{AccountKey, OwnershipAction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipCommand {
    pub audit_id: String,
    pub owner: String,
    pub account: AccountKey,
    pub action: OwnershipAction,
    pub occurred_at: DateTime<Utc>,
}

impl OwnershipCommand {
    pub fn new(
        owner: impl Into<String>,
        account: AccountKey,
        action: OwnershipAction,
        occurred_at: DateTime<Utc>,
    ) -> Self {
        Self {
            audit_id: uuid::Uuid::new_v4().to_string(),
            owner: owner.into(),
            account,
            action,
            occurred_at,
        }
    }
}
