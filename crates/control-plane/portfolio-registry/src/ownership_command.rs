use chrono::{DateTime, Utc};
use platform_portfolio_ownership::{AccountKey, OwnershipAction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwnershipCommand {
    pub(crate) audit_id: String,
    pub(crate) owner: String,
    pub(crate) account: AccountKey,
    pub(crate) action: OwnershipAction,
    pub(crate) occurred_at: DateTime<Utc>,
}

impl OwnershipCommand {
    pub(crate) fn new(
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
