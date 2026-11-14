use crate::connector::portfolio::PortfolioUpdateSink;
use async_trait::async_trait;
use platform_errors::PlatformResult;
use std::{collections::BTreeSet, sync::Arc};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[async_trait]
pub trait PortfolioFeed: Send + Sync {
    async fn set_accounts(
        &self,
        cex_id: CexId,
        accounts: BTreeSet<AccountId>,
    ) -> PlatformResult<()>;

    fn set_update_sink(&self, _sink: Arc<dyn PortfolioUpdateSink>) {}
}
