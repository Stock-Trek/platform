use crate::portfolio::{AccountPortfolio, AccountPortfolios, PortfolioUpdate};
use chrono::{DateTime, Utc};
use platform_portfolio_ownership::AccountKey;
use std::collections::{BTreeMap, BTreeSet};
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortfolioUpdateOutcome {
    Applied,
    IgnoredUntracked,
    IgnoredStale,
}

#[derive(Debug, Clone)]
pub struct PortfolioUpdateMetadata {
    version: u64,
    occurred_at: DateTime<Utc>,
    exchange_time: Option<DateTime<Utc>>,
    applied_at: DateTime<Utc>,
}

impl PortfolioUpdateMetadata {
    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    pub fn exchange_time(&self) -> Option<DateTime<Utc>> {
        self.exchange_time
    }

    pub fn applied_at(&self) -> DateTime<Utc> {
        self.applied_at
    }
}

#[derive(Debug, Default)]
pub struct PortfolioState {
    portfolios: AccountPortfolios,
    updates: BTreeMap<AccountKey, PortfolioUpdateMetadata>,
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
        let metadata = PortfolioUpdateMetadata {
            version: update.version(),
            occurred_at: update.occurred_at(),
            exchange_time: update.exchange_time(),
            applied_at: Utc::now(),
        };
        let (_, portfolio) = update.into_parts();
        self.portfolios
            .entry(account.cex_id())
            .or_default()
            .insert(account.account_id().clone(), portfolio);
        self.updates.insert(account, metadata);
        PortfolioUpdateOutcome::Applied
    }

    pub fn set_portfolio(&mut self, account: AccountKey, portfolio: AccountPortfolio) -> bool {
        self.apply_update(PortfolioUpdate::new(account, portfolio))
    }

    pub fn portfolio(&self, account: &AccountKey) -> Option<&AccountPortfolio> {
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

    fn accepts(&self, account: &AccountKey, version: u64) -> bool {
        self.updates.get(account).is_none_or(|metadata| {
            version > metadata.version() || (version == 0 && metadata.version() == 0)
        })
    }

    fn prune(&mut self, account: &AccountKey) {
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
