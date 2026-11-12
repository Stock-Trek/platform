use crate::connector::PortfolioFeed;
use crate::{
    freshness::{FreshnessPolicy, PortfolioFreshness},
    ownership_registry::{OwnershipApply, OwnershipRegistry},
    portfolio::{AccountPortfolio, AccountPortfolios, PortfolioUpdate},
    state::{PortfolioState, PortfolioUpdateMetadata, PortfolioUpdateOutcome},
};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use platform_errors::BpResult;
use platform_metrics::PortfolioMetrics;
use platform_portfolio_ownership::{AccountKey, OwnershipEvent};
use std::{collections::BTreeSet, sync::Arc};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

pub struct PortfolioService {
    state: RwLock<PortfolioState>,
    ownership: RwLock<OwnershipRegistry>,
    feed: Arc<dyn PortfolioFeed>,
    freshness: FreshnessPolicy,
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

    #[cfg(test)]
    pub async fn track_account(&self, account: AccountKey) -> BpResult<bool> {
        if self.state.read().is_global(&account) {
            return Ok(false);
        }
        let cex_id = account.cex_id();
        let (current, next) = {
            let state = self.state.read();
            (
                state.accounts_for_cex(cex_id),
                state.accounts_for_cex_with(cex_id, &account),
            )
        };
        self.sync(cex_id, current, next).await?;
        self.state.write().track(&account);
        Ok(true)
    }

    #[cfg(test)]
    pub async fn untrack_account(&self, account: AccountKey) -> BpResult<bool> {
        if !self.state.read().is_tracked(&account) {
            return Ok(false);
        }
        let cex_id = account.cex_id();
        let (current, next) = {
            let state = self.state.read();
            let mut next = state.accounts_for_cex(cex_id);
            next.remove(account.account_id());
            (state.accounts_for_cex(cex_id), next)
        };
        self.sync(cex_id, current, next).await?;
        self.state.write().untrack_everywhere(&account);
        Ok(true)
    }

    #[cfg(test)]
    pub async fn track_user_account(
        &self,
        owner: impl Into<String>,
        account: AccountKey,
    ) -> BpResult<bool> {
        let owner = owner.into();
        if self.state.read().is_tracked_for_owner(&owner, &account) {
            return Ok(false);
        }
        let cex_id = account.cex_id();
        let (current, next) = {
            let state = self.state.read();
            (
                state.accounts_for_cex(cex_id),
                state.accounts_for_cex_with(cex_id, &account),
            )
        };
        self.sync(cex_id, current, next).await?;
        self.state.write().track_for_owner(&owner, &account);
        Ok(true)
    }

    #[cfg(test)]
    pub async fn untrack_user_account(&self, owner: &str, account: AccountKey) -> BpResult<bool> {
        if !self.state.read().is_tracked_for_owner(owner, &account) {
            return Ok(false);
        }
        let cex_id = account.cex_id();
        let (current, next) = {
            let state = self.state.read();
            (
                state.accounts_for_cex(cex_id),
                state.accounts_for_cex_without_owner(owner, cex_id, &account),
            )
        };
        self.sync(cex_id, current, next).await?;
        self.state.write().untrack_for_owner(owner, &account);
        Ok(true)
    }

    #[cfg(test)]
    pub async fn add_account(&self, account: AccountKey) -> BpResult<bool> {
        self.track_account(account).await
    }

    #[cfg(test)]
    pub async fn remove_account(&self, account: AccountKey) -> BpResult<bool> {
        self.untrack_account(account).await
    }

    #[cfg(test)]
    pub async fn add_user_account(
        &self,
        owner: impl Into<String>,
        account: AccountKey,
    ) -> BpResult<bool> {
        self.track_user_account(owner, account).await
    }

    #[cfg(test)]
    pub async fn remove_user_account(&self, owner: &str, account: AccountKey) -> BpResult<bool> {
        self.untrack_user_account(owner, account).await
    }

    pub async fn apply_ownership_event(&self, event: &OwnershipEvent) -> BpResult<OwnershipApply> {
        if !self.ownership.read().accepts(event) {
            return Ok(self.ownership.write().apply(event));
        }
        let account = event.account();
        let cex_id = account.cex_id();
        let (current, next) = {
            let state = self.state.read();
            let current = state.accounts_for_cex(cex_id);
            let next = if event.action().is_add() {
                state.accounts_for_cex_with(cex_id, account)
            } else {
                state.accounts_for_cex_without_owner(event.owner(), cex_id, account)
            };
            (current, next)
        };
        let pre_tracked =
            event.action().is_add() && self.state.write().track_for_owner(event.owner(), account);
        if let Err(error) = self.sync(cex_id, current, next).await {
            if pre_tracked {
                self.state.write().untrack_for_owner(event.owner(), account);
            }
            return Err(error);
        }
        let outcome = self.ownership.write().apply(event);
        if let OwnershipApply::Applied { previous_owner, .. } = &outcome {
            let mut state = self.state.write();
            if let Some(previous) = previous_owner
                && previous != event.owner()
            {
                state.untrack_for_owner(previous, account);
            }
            if event.action().is_add() {
                state.track_for_owner(event.owner(), account);
            } else {
                state.untrack_for_owner(event.owner(), account);
            }
        } else if pre_tracked {
            self.state.write().untrack_for_owner(event.owner(), account);
        }
        Ok(outcome)
    }

    pub fn ownership_version(&self, account: &AccountKey) -> Option<u64> {
        self.ownership.read().version(account)
    }

    pub fn is_tracked_for_owner(&self, owner: &str, account: &AccountKey) -> bool {
        self.state.read().is_tracked_for_owner(owner, account)
    }

    pub fn account_portfolio(&self, account: &AccountKey) -> Option<AccountPortfolio> {
        self.state.read().portfolio(account).cloned()
    }

    pub fn user_account_portfolio(
        &self,
        owner: &str,
        account: &AccountKey,
    ) -> Option<AccountPortfolio> {
        let state = self.state.read();
        state
            .is_tracked_for_owner(owner, account)
            .then(|| state.portfolio(account).cloned())
            .flatten()
    }

    pub fn portfolios(&self) -> AccountPortfolios {
        self.state.read().snapshot()
    }

    pub fn user_portfolios(&self, owner: &str) -> AccountPortfolios {
        self.state.read().snapshot_for_owner(owner)
    }

    pub fn tracked_accounts(&self) -> Vec<AccountKey> {
        self.state.read().tracked_accounts()
    }

    pub fn tracked_user_accounts(&self, owner: &str) -> Vec<AccountKey> {
        self.state.read().tracked_accounts_for_owner(owner)
    }

    pub fn apply_update(&self, update: PortfolioUpdate) -> bool {
        self.apply_portfolio_update(update) == PortfolioUpdateOutcome::Applied
    }

    pub fn apply_portfolio_update(&self, update: PortfolioUpdate) -> PortfolioUpdateOutcome {
        self.state.write().apply_update_with_outcome(update)
    }

    pub fn update_metadata(&self, account: &AccountKey) -> Option<PortfolioUpdateMetadata> {
        self.state.read().update(account).cloned()
    }

    pub fn snapshot_age(
        &self,
        account: &AccountKey,
        now: DateTime<Utc>,
    ) -> Option<chrono::Duration> {
        self.state.read().snapshot_age(account, now)
    }

    pub fn freshness(&self, account: &AccountKey, now: DateTime<Utc>) -> PortfolioFreshness {
        self.freshness.evaluate(self.snapshot_age(account, now))
    }

    pub fn authorize_order(
        &self,
        owner: &str,
        account: &AccountKey,
        now: DateTime<Utc>,
    ) -> BpResult<PortfolioFreshness> {
        if !self.is_tracked_for_owner(owner, account) {
            PortfolioMetrics::record_order_rejection("not_owned");
            return Err(platform_errors::BpError::auth_invalid_field(
                "account",
                "is not owned by the authenticated owner",
            ));
        }
        let age = self.snapshot_age(account, now);
        match self.freshness.authorize_order(age) {
            Ok(freshness) => Ok(freshness),
            Err(error) => {
                PortfolioMetrics::record_order_rejection("stale");
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub fn apply_portfolio(&self, account: AccountKey, portfolio: AccountPortfolio) -> bool {
        self.state.write().set_portfolio(account, portfolio)
    }

    pub fn account_count(&self) -> usize {
        self.state.read().tracked_accounts().len()
    }

    async fn sync(
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
