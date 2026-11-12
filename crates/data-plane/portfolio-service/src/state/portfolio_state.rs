use super::PortfolioUpdateMetadata;
use crate::portfolio::AccountPortfolios;
use platform_portfolio_ownership::AccountKey;
use std::collections::{BTreeMap, BTreeSet};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[derive(Debug, Default)]
pub struct PortfolioState {
    pub(super) portfolios: AccountPortfolios,
    pub(super) updates: BTreeMap<AccountKey, PortfolioUpdateMetadata>,
    global_accounts: BTreeSet<AccountKey>,
    owner_accounts: BTreeMap<String, BTreeSet<AccountKey>>,
}

impl PortfolioState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn track(&mut self, account: &AccountKey) -> bool {
        self.global_accounts.insert(account.clone())
    }

    pub fn untrack(&mut self, account: &AccountKey) -> bool {
        let removed = self.global_accounts.remove(account);
        self.prune(account);
        removed
    }

    pub fn untrack_everywhere(&mut self, account: &AccountKey) -> bool {
        let mut removed = self.global_accounts.remove(account);
        for accounts in self.owner_accounts.values_mut() {
            removed |= accounts.remove(account);
        }
        self.owner_accounts
            .retain(|_, accounts| !accounts.is_empty());
        self.prune(account);
        removed
    }

    pub fn track_for_owner(&mut self, owner: &str, account: &AccountKey) -> bool {
        self.owner_accounts
            .entry(owner.to_string())
            .or_default()
            .insert(account.clone())
    }

    pub fn untrack_for_owner(&mut self, owner: &str, account: &AccountKey) -> bool {
        let removed = self
            .owner_accounts
            .get_mut(owner)
            .is_some_and(|accounts| accounts.remove(account));
        if self
            .owner_accounts
            .get(owner)
            .is_some_and(BTreeSet::is_empty)
        {
            self.owner_accounts.remove(owner);
        }
        self.prune(account);
        removed
    }

    pub fn is_tracked(&self, account: &AccountKey) -> bool {
        self.is_global(account)
            || self
                .owner_accounts
                .values()
                .any(|accounts| accounts.contains(account))
    }

    pub fn is_global(&self, account: &AccountKey) -> bool {
        self.global_accounts.contains(account)
    }

    pub fn is_tracked_for_owner(&self, owner: &str, account: &AccountKey) -> bool {
        self.owner_accounts
            .get(owner)
            .is_some_and(|accounts| accounts.contains(account))
    }

    pub fn tracked_accounts(&self) -> Vec<AccountKey> {
        let mut accounts = self.global_accounts.clone();
        for owned in self.owner_accounts.values() {
            accounts.extend(owned.iter().cloned());
        }
        accounts.into_iter().collect()
    }

    pub fn tracked_accounts_for_owner(&self, owner: &str) -> Vec<AccountKey> {
        self.owner_accounts
            .get(owner)
            .map(|accounts| accounts.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn accounts_for_cex(&self, cex_id: CexId) -> BTreeSet<AccountId> {
        self.tracked_accounts()
            .into_iter()
            .filter(|account| account.cex_id() == cex_id)
            .map(|account| account.account_id().clone())
            .collect()
    }

    pub fn accounts_for_cex_with(
        &self,
        cex_id: CexId,
        account: &AccountKey,
    ) -> BTreeSet<AccountId> {
        let mut accounts = self.accounts_for_cex(cex_id);
        if account.cex_id() == cex_id {
            accounts.insert(account.account_id().clone());
        }
        accounts
    }

    pub fn accounts_for_cex_without_owner(
        &self,
        owner: &str,
        cex_id: CexId,
        account: &AccountKey,
    ) -> BTreeSet<AccountId> {
        let mut accounts = self.accounts_for_cex(cex_id);
        let remains_for_owner = self
            .owner_accounts
            .iter()
            .any(|(other, accounts)| other != owner && accounts.contains(account));
        if account.cex_id() == cex_id && !self.is_global(account) && !remains_for_owner {
            accounts.remove(account.account_id());
        }
        accounts
    }
}
