use super::PortfolioState;
use crate::portfolio::AccountPortfolios;
use platform_portfolio_ownership::AccountKey;

impl PortfolioState {
    pub fn snapshot(&self) -> AccountPortfolios {
        let mut snapshot = AccountPortfolios::new();
        for account in self.tracked_accounts() {
            self.insert_snapshot(&mut snapshot, &account);
        }
        snapshot
    }

    pub fn snapshot_for_owner(&self, owner: &str) -> AccountPortfolios {
        let mut snapshot = AccountPortfolios::new();
        for account in self.tracked_accounts_for_owner(owner) {
            self.insert_snapshot(&mut snapshot, &account);
        }
        snapshot
    }

    fn insert_snapshot(&self, snapshot: &mut AccountPortfolios, account: &AccountKey) {
        if let Some(portfolio) = self.portfolio(account) {
            snapshot
                .entry(account.cex_id())
                .or_default()
                .insert(account.account_id().clone(), portfolio.clone());
        }
    }
}
