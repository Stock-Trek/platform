use super::PortfolioService;
use platform_errors::BuildResult;
use platform_portfolio_ownership::AccountKey;

impl PortfolioService {
    pub async fn track_account(&self, account: AccountKey) -> BuildResult<bool> {
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

    pub async fn untrack_account(&self, account: AccountKey) -> BuildResult<bool> {
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

    pub async fn track_user_account(
        &self,
        owner: impl Into<String>,
        account: AccountKey,
    ) -> BuildResult<bool> {
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

    pub async fn untrack_user_account(&self, owner: &str, account: AccountKey) -> BuildResult<bool> {
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

    pub async fn add_account(&self, account: AccountKey) -> BuildResult<bool> {
        self.track_account(account).await
    }

    pub async fn remove_account(&self, account: AccountKey) -> BuildResult<bool> {
        self.untrack_account(account).await
    }

    pub async fn add_user_account(
        &self,
        owner: impl Into<String>,
        account: AccountKey,
    ) -> BuildResult<bool> {
        self.track_user_account(owner, account).await
    }

    pub async fn remove_user_account(&self, owner: &str, account: AccountKey) -> BuildResult<bool> {
        self.untrack_user_account(owner, account).await
    }
}
