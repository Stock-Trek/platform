#[cfg(test)]
use crate::portfolio::CexAccountPortfolio;
use crate::service::PortfolioService;
use crate::{
    freshness::PortfolioFreshness,
    portfolio::PortfolioUpdate,
    state::{PortfolioUpdateMetadata, PortfolioUpdateOutcome},
};
use chrono::{DateTime, Utc};
use platform_errors::BuildResult;
use platform_metrics::PortfolioMetrics;
use platform_portfolio_ownership::AccountKey;

impl PortfolioService {
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
    ) -> BuildResult<PortfolioFreshness> {
        if !self.is_tracked_for_owner(owner, account) {
            PortfolioMetrics::record_order_rejection("not_owned");
            return Err(platform_errors::BuildError::auth_invalid_field(
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
    pub fn apply_portfolio(&self, account: AccountKey, portfolio: CexAccountPortfolio) -> bool {
        self.state.write().set_portfolio(account, portfolio)
    }
}
