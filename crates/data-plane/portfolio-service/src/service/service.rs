use crate::{
    connector::PortfolioFeed,
    freshness::FreshnessPolicy,
    ownership_registry::OwnershipRegistry,
    state::PortfolioState,
};
use parking_lot::RwLock;
use platform_errors::BpResult;
use std::{collections::BTreeSet, sync::Arc};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

pub struct PortfolioService {
    pub(super) state: RwLock<PortfolioState>,
    pub(super) ownership: RwLock<OwnershipRegistry>,
    pub(super) feed: Arc<dyn PortfolioFeed>,
    pub(super) freshness: FreshnessPolicy,
}

impl PortfolioService {
    pub fn new(feed: Arc<dyn PortfolioFeed>) -> Self {
        Self::with_freshness_policy(feed, FreshnessPolicy::default_degraded())
    }

    pub fn with_freshness_policy(feed: Arc<dyn PortfolioFeed>, freshness: FreshnessPolicy) -> Self {
        Self {
            state: RwLock::new(PortfolioState::new()),
            ownership: RwLock::new(OwnershipRegistry::new()),
            feed,
            freshness,
        }
    }

    #[cfg(test)]
    pub fn in_memory() -> Self {
        Self::new(Arc::new(crate::connector::NoopPortfolioFeed::new()))
    }

    pub(super) async fn sync(
        &self,
        cex_id: CexId,
        current: BTreeSet<AccountId>,
        next: BTreeSet<AccountId>,
    ) -> BpResult<()> {
        if current == next {
            return Ok(());
        }
        self.feed.set_accounts(cex_id, next).await
    }
}
