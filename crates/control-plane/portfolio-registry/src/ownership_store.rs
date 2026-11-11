use crate::{AccountMapping, OwnershipCommand, OwnershipRecord};
use async_trait::async_trait;
use platform_errors::BpResult;
use platform_portfolio_ownership::{AccountKey, OwnershipEvent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OwnershipCommit {
    Applied(OwnershipEvent),
    Unchanged(OwnershipRecord),
}

#[async_trait]
pub(crate) trait OwnershipStore: Send + Sync {
    async fn commit(&self, command: OwnershipCommand) -> BpResult<OwnershipCommit>;

    async fn get(&self, account: &AccountKey) -> BpResult<Option<OwnershipRecord>>;

    async fn owner_accounts(&self, owner: &str) -> BpResult<Vec<AccountMapping>>;

    async fn pending_events(&self, limit: usize) -> BpResult<Vec<OwnershipEvent>>;

    async fn acknowledge(&self, event: &OwnershipEvent) -> BpResult<()>;
}
