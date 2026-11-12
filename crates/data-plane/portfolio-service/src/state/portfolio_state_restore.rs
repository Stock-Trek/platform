use super::{PortfolioState, PortfolioUpdateMetadata};
use crate::portfolio::CexAccountPortfolio;
use platform_portfolio_ownership::AccountKey;

impl PortfolioState {
    pub(crate) fn restore_portfolio(
        &mut self,
        account: AccountKey,
        portfolio: CexAccountPortfolio,
        metadata: PortfolioUpdateMetadata,
    ) {
        self.portfolios
            .entry(account.cex_id())
            .or_default()
            .insert(account.account_id().clone(), portfolio);
        self.updates.insert(account, metadata);
    }
}
