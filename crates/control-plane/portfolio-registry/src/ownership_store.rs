use crate::{AccountMapping, OwnershipCommand, OwnershipRecord};
use async_trait::async_trait;
use platform_errors::PlatformResult;
use platform_portfolio_ownership::{AccountKey, OwnershipEvent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipCommit {
    Applied(OwnershipEvent),
    Unchanged(OwnershipRecord),
}

#[async_trait]
pub trait OwnershipStore: Send + Sync {
    async fn commit(&self, command: OwnershipCommand) -> PlatformResult<OwnershipCommit>;

    async fn get(&self, account: &AccountKey) -> PlatformResult<Option<OwnershipRecord>>;

    async fn owner_accounts(&self, owner: &str) -> PlatformResult<Vec<AccountMapping>>;

    async fn pending_events(&self, limit: usize) -> PlatformResult<Vec<OwnershipEvent>>;

    async fn acknowledge(&self, event: &OwnershipEvent) -> PlatformResult<()>;
}
