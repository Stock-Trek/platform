use super::PortfolioFeed;
use async_trait::async_trait;
use platform_errors::BpResult;
use std::collections::BTreeSet;
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[derive(Debug, Default)]
pub struct NoopPortfolioFeed;

impl NoopPortfolioFeed {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl PortfolioFeed for NoopPortfolioFeed {
    async fn set_accounts(&self, _cex_id: CexId, _accounts: BTreeSet<AccountId>) -> BpResult<()> {
        Ok(())
    }
}
