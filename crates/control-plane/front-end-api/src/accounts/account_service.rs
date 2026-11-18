use crate::accounts::account_types::{
    AccountSummary, CredentialsRequest, ForceUnlinkResponse, LinkAccountRequest,
};
use crate::accounts::{AccountStore, LinkConflict, PortfolioSnapshot};
use crate::api::Problem;
use crate::auth::AuthContext;
use crate::bots::{BotService, BotState};
use chrono::{DateTime, Utc};
use std::sync::Arc;

pub struct AccountService {
    accounts: Arc<AccountStore>,
    bots: Arc<BotService>,
}

impl AccountService {
    pub fn new(accounts: Arc<AccountStore>, bots: Arc<BotService>) -> Self {
        Self { accounts, bots }
    }

    pub fn link(
        &self,
        auth: &AuthContext,
        request: &LinkAccountRequest,
        now: DateTime<Utc>,
    ) -> Result<AccountSummary, Problem> {
        let summary = self
            .accounts
            .link(
                auth.owner(),
                &request.cex_id,
                &request.label,
                &request.api_key,
                &request.api_secret,
                now,
            )
            .map_err(|error| match error {
                LinkConflict::LabelTaken => {
                    Problem::already_exists("an account already uses this label for the exchange")
                }
            })?;
        self.validated(auth.owner(), summary, now)
    }

    fn validated(
        &self,
        owner: &str,
        summary: AccountSummary,
        now: DateTime<Utc>,
    ) -> Result<AccountSummary, Problem> {
        self.accounts
            .validate(owner, &summary.account_id, now)
            .ok_or_else(|| Problem::not_found("account does not exist"))
    }

    pub fn list(&self, owner: &str) -> Vec<AccountSummary> {
        self.accounts.list(owner)
    }

    pub fn get(&self, owner: &str, account_id: &str) -> Result<AccountSummary, Problem> {
        self.accounts
            .get(owner, account_id)
            .ok_or_else(|| Problem::not_found("account does not exist"))
    }

    pub fn replace_credentials(
        &self,
        auth: &AuthContext,
        account_id: &str,
        request: &CredentialsRequest,
        now: DateTime<Utc>,
    ) -> Result<AccountSummary, Problem> {
        let summary = self
            .accounts
            .replace_credentials(
                auth.owner(),
                account_id,
                &request.api_key,
                &request.api_secret,
                now,
            )
            .ok_or_else(|| Problem::not_found("account does not exist"))?;
        self.validated(auth.owner(), summary, now)
    }

    pub fn unlink(
        &self,
        auth: &AuthContext,
        account_id: &str,
        force: bool,
        now: DateTime<Utc>,
    ) -> Result<Option<ForceUnlinkResponse>, Problem> {
        let owner = auth.owner();
        self.get(owner, account_id)?;
        let referencing = self
            .bots
            .referencing_accounts(owner, &[account_id.to_string()]);
        let enabled = referencing
            .iter()
            .filter(|bot| bot.state == BotState::Enabled)
            .count();
        if enabled > 0 && !force {
            return Err(Problem::state_conflict(
                "enabled bots reference the account; unlinking would break their authorization",
            ));
        }
        let response = force.then(|| {
            let killed = self.bots.disable_bots(owner, referencing, now);
            ForceUnlinkResponse {
                disabled_bot_ids: killed.disabled_bot_ids,
                canceled_run_ids: killed.canceled_run_ids,
            }
        });
        self.accounts.unlink(owner, account_id);
        Ok(response)
    }

    pub fn snapshot(&self, owner: &str, account_id: &str) -> Result<PortfolioSnapshot, Problem> {
        let account = self.get(owner, account_id)?;
        Ok(self
            .accounts
            .snapshot(owner, account_id)
            .unwrap_or_else(|| PortfolioSnapshot::pending(&account.account_id, &account.cex_id)))
    }

    pub fn snapshots(&self, owner: &str) -> Vec<PortfolioSnapshot> {
        self.accounts.snapshots(owner)
    }
}
