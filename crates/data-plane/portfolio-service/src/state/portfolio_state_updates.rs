use super::{PortfolioState, PortfolioUpdateMetadata, PortfolioUpdateOutcome};
use crate::portfolio::{CexAccountPortfolio, PortfolioUpdate};
use chrono::{DateTime, Utc};
use platform_portfolio_ownership::AccountKey;

impl PortfolioState {
    pub fn apply_update(&mut self, update: PortfolioUpdate) -> bool {
        self.apply_update_with_outcome(update) == PortfolioUpdateOutcome::Applied
    }

    pub fn apply_update_with_outcome(&mut self, update: PortfolioUpdate) -> PortfolioUpdateOutcome {
        let account = update.account().clone();
        let tracked = match update.owner() {
            Some(owner) => self.is_tracked_for_owner(owner, &account),
            None => self.is_tracked(&account),
        };
        if !tracked {
            return PortfolioUpdateOutcome::IgnoredUntracked;
        }
        if !self.accepts(&account, update.version()) {
            return PortfolioUpdateOutcome::IgnoredStale;
        }
        let metadata = PortfolioUpdateMetadata::new(
            update.version(),
            update.occurred_at(),
            update.exchange_time(),
            Utc::now(),
        );
        let (_, portfolio) = update.into_parts();
        self.portfolios
            .entry(account.cex_id())
            .or_default()
            .insert(account.account_id().clone(), portfolio);
        self.updates.insert(account, metadata);
        PortfolioUpdateOutcome::Applied
    }

    pub fn set_portfolio(&mut self, account: AccountKey, portfolio: CexAccountPortfolio) -> bool {
        self.apply_update(PortfolioUpdate::new(account, portfolio))
    }

    pub fn portfolio(&self, account: &AccountKey) -> Option<&CexAccountPortfolio> {
        self.portfolios
            .get(&account.cex_id())
            .and_then(|accounts| accounts.get(account.account_id()))
    }

    pub fn update(&self, account: &AccountKey) -> Option<&PortfolioUpdateMetadata> {
        self.updates.get(account)
    }

    pub fn snapshot_age(
        &self,
        account: &AccountKey,
        now: DateTime<Utc>,
    ) -> Option<chrono::Duration> {
        self.update(account)
            .map(|metadata| now.signed_duration_since(metadata.occurred_at()))
    }
}
