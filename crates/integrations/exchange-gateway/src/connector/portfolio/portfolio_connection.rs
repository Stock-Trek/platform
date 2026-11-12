use async_trait::async_trait;
use platform_errors::BpResult;
use std::collections::BTreeSet;
use stock_trek_types::cex::account_id::AccountId;

#[async_trait]
pub trait PortfolioConnection: Send + Sync {
    async fn update_accounts(&self, accounts: BTreeSet<AccountId>) -> BpResult<()>;

    async fn disconnect(&self) -> BpResult<()>;
}
