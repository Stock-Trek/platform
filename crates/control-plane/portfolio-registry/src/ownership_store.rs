use crate::{AccountMapping, OwnershipCommand, OwnershipRecord};
use async_trait::async_trait;
use platform_errors::BuildResult;
use platform_portfolio_ownership::{AccountKey, OwnershipEvent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipCommit {
    Applied(OwnershipEvent),
    Unchanged(OwnershipRecord),
}

#[async_trait]
pub trait OwnershipStore: Send + Sync {
    async fn commit(&self, command: OwnershipCommand) -> BuildResult<OwnershipCommit>;

    async fn get(&self, account: &AccountKey) -> BuildResult<Option<OwnershipRecord>>;

    async fn owner_accounts(&self, owner: &str) -> BuildResult<Vec<AccountMapping>>;

    async fn pending_events(&self, limit: usize) -> BuildResult<Vec<OwnershipEvent>>;

    async fn acknowledge(&self, event: &OwnershipEvent) -> BuildResult<()>;
}
