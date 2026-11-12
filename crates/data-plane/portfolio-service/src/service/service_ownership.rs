use super::PortfolioService;
use crate::ownership_registry::OwnershipApply;
use platform_errors::BpResult;
use platform_portfolio_ownership::{AccountKey, OwnershipEvent};

impl PortfolioService {
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
}
