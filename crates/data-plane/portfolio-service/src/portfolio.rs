use crate::account::AccountKey;
use std::collections::BTreeMap;
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

pub use stock_trek::PendingOrder;

pub type AccountPortfolio = stock_trek::CexAccountPortfolio;
pub type AccountPortfolios = BTreeMap<CexId, BTreeMap<AccountId, AccountPortfolio>>;

#[derive(Debug, Clone)]
pub struct PortfolioUpdate {
    account: AccountKey,
    portfolio: AccountPortfolio,
}

impl PortfolioUpdate {
    pub fn new(account: AccountKey, portfolio: AccountPortfolio) -> Self {
        Self { account, portfolio }
    }

    pub fn account(&self) -> &AccountKey {
        &self.account
    }

    pub fn portfolio(&self) -> &AccountPortfolio {
        &self.portfolio
    }

    pub fn into_parts(self) -> (AccountKey, AccountPortfolio) {
        (self.account, self.portfolio)
    }
}
