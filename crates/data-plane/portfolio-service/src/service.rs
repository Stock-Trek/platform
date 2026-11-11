use crate::{
    feed::PortfolioFeed,
    ownership_registry::{OwnershipApply, OwnershipRegistry},
    portfolio::{AccountPortfolio, AccountPortfolios, PortfolioUpdate},
    state::PortfolioState,
};
use parking_lot::RwLock;
use platform_errors::BpResult;
use platform_portfolio_ownership::{AccountKey, OwnershipEvent};
use std::{collections::BTreeSet, sync::Arc};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[cfg(test)]
use crate::feed::NoopPortfolioFeed;

pub struct PortfolioService {
    state: RwLock<PortfolioState>,
    ownership: RwLock<OwnershipRegistry>,
    feed: Arc<dyn PortfolioFeed>,
}

impl PortfolioService {
    pub fn new(feed: Arc<dyn PortfolioFeed>) -> Self {
        Self {
            state: RwLock::new(PortfolioState::new()),
            ownership: RwLock::new(OwnershipRegistry::new()),
            feed,
        }
    }

    #[cfg(test)]
    pub fn in_memory() -> Self {
        Self::new(Arc::new(NoopPortfolioFeed::new()))
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
        self.sync(cex_id, current, next).await?;
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
        self.state.write().apply_update(update)
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
