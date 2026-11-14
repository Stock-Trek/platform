use crate::state::PortfolioState;
use platform_portfolio_ownership::AccountKey;

impl PortfolioState {
    pub fn accepts(&self, account: &AccountKey, version: u64) -> bool {
        self.updates.get(account).is_none_or(|metadata| {
            version > metadata.version() || (version == 0 && metadata.version() == 0)
        })
    }

    pub fn prune(&mut self, account: &AccountKey) {
        if self.is_tracked(account) {
            return;
        }
        self.updates.remove(account);
        let remove_cex = self
            .portfolios
            .get_mut(&account.cex_id())
            .is_some_and(|accounts| {
                accounts.remove(account.account_id());
                accounts.is_empty()
            });
        if remove_cex {
            self.portfolios.remove(&account.cex_id());
        }
    }
}
