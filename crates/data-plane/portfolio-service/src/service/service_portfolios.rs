use super::PortfolioService;
use crate::portfolio::{AccountPortfolios, CexAccountPortfolio};
use platform_portfolio_ownership::AccountKey;

impl PortfolioService {
    pub fn account_portfolio(&self, account: &AccountKey) -> Option<CexAccountPortfolio> {
        self.state.read().portfolio(account).cloned()
    }

    pub fn user_account_portfolio(
        &self,
        owner: &str,
        account: &AccountKey,
    ) -> Option<CexAccountPortfolio> {
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

    pub fn account_count(&self) -> usize {
        self.state.read().tracked_accounts().len()
    }
}
