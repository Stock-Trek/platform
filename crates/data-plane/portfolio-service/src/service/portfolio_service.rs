use crate::{
    freshness::FreshnessPolicy, ownership_registry::OwnershipRegistry, state::PortfolioState,
};
use parking_lot::RwLock;
use platform_errors::BuildResult;
use platform_exchange_gateway::PortfolioFeed;
use std::{collections::BTreeSet, sync::Arc};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

pub struct PortfolioService {
    pub state: RwLock<PortfolioState>,
    pub ownership: RwLock<OwnershipRegistry>,
    pub feed: Arc<dyn PortfolioFeed>,
    pub freshness: FreshnessPolicy,
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

    pub async fn sync(
        &self,
        cex_id: CexId,
        current: BTreeSet<AccountId>,
        next: BTreeSet<AccountId>,
    ) -> BuildResult<()> {
        if current == next {
            return Ok(());
        }
        self.feed.set_accounts(cex_id, next).await
    }
}
