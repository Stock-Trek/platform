use super::ConnectorPortfolioUpdate;
use async_trait::async_trait;
use platform_errors::BpResult;
use std::{collections::BTreeSet, sync::Arc};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortfolioUpdateApply {
    Applied,
    IgnoredUntracked,
    IgnoredStale,
}

#[async_trait]
pub trait PortfolioUpdateSink: Send + Sync {
    async fn apply_update(
        &self,
        update: ConnectorPortfolioUpdate,
    ) -> BpResult<PortfolioUpdateApply>;
}

#[async_trait]
pub trait PortfolioConnection: Send + Sync {
    async fn update_accounts(&self, accounts: BTreeSet<AccountId>) -> BpResult<()>;

    async fn disconnect(&self) -> BpResult<()>;
}

#[async_trait]
pub trait PortfolioConnector: Send + Sync {
    async fn connect(
        &self,
        cex_id: CexId,
        sink: Arc<dyn PortfolioUpdateSink>,
    ) -> BpResult<Arc<dyn PortfolioConnection>>;
}
