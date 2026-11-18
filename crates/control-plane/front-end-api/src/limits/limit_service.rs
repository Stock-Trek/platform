use crate::accounts::AccountStore;
use crate::algorithms::BuildStore;
use crate::api::{Problem, QuotaDetail};
use crate::backtests::BacktestStore;
use crate::bots::BotState;
use crate::bots::BotStore;
use crate::streaming::StreamService;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;

pub const QUOTA_LINKED_ACCOUNTS: &str = "linkedAccounts";
pub const QUOTA_ALGORITHMS: &str = "algorithms";
pub const QUOTA_BOTS: &str = "bots";
pub const QUOTA_SCHEDULED_RUNS_PER_DAY: &str = "scheduledRunsPerDay";
pub const QUOTA_CONCURRENT_BACKTESTS: &str = "concurrentBacktests";
pub const QUOTA_STREAM_CONNECTIONS: &str = "streamConnections";

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LimitEntry {
    pub limit: u64,
    pub used: u64,
}

pub struct LimitSources {
    pub accounts: Arc<AccountStore>,
    pub builds: Arc<BuildStore>,
    pub bots: Arc<BotStore>,
    pub backtests: Arc<BacktestStore>,
    pub streams: Arc<StreamService>,
}

pub struct LimitService {
    sources: LimitSources,
}

impl LimitService {
    pub fn new(sources: LimitSources) -> Self {
        Self { sources }
    }

    pub fn effective(&self, owner: &str) -> BTreeMap<&'static str, LimitEntry> {
        let mut entries = BTreeMap::new();
        for quota in [
            QUOTA_LINKED_ACCOUNTS,
            QUOTA_ALGORITHMS,
            QUOTA_BOTS,
            QUOTA_SCHEDULED_RUNS_PER_DAY,
            QUOTA_CONCURRENT_BACKTESTS,
            QUOTA_STREAM_CONNECTIONS,
        ] {
            entries.insert(quota, self.entry(owner, quota));
        }
        entries
    }

    pub fn check(&self, owner: &str, quota: &'static str, requested: u64) -> Result<(), Problem> {
        let entry = self.entry(owner, quota);
        if entry.used + requested <= entry.limit {
            Ok(())
        } else {
            Err(Problem::quota_exceeded(QuotaDetail {
                quota,
                limit: entry.limit,
                used: entry.used,
            }))
        }
    }

    pub fn check_rate(
        &self,
        owner: &str,
        quota: &'static str,
        requested: u64,
        retry_after: u64,
    ) -> Result<(), Problem> {
        let entry = self.entry(owner, quota);
        if entry.used + requested <= entry.limit {
            Ok(())
        } else {
            Err(Problem::rate_limited(retry_after))
        }
    }

    pub fn entry(&self, owner: &str, quota: &str) -> LimitEntry {
        LimitEntry {
            limit: self.limit(quota),
            used: self.used(owner, quota),
        }
    }

    fn limit(&self, quota: &str) -> u64 {
        // TODO: resolve limit values from the backend tier matrix using the owner's tier.
        match quota {
            QUOTA_LINKED_ACCOUNTS => 20,
            QUOTA_ALGORITHMS => 100,
            QUOTA_BOTS => 50,
            QUOTA_SCHEDULED_RUNS_PER_DAY => 20_000,
            QUOTA_CONCURRENT_BACKTESTS => 2,
            QUOTA_STREAM_CONNECTIONS => 5,
            _ => 0,
        }
    }

    fn used(&self, owner: &str, quota: &str) -> u64 {
        match quota {
            QUOTA_LINKED_ACCOUNTS => self.sources.accounts.count(owner) as u64,
            QUOTA_ALGORITHMS => self.sources.builds.list_algorithms(owner).len() as u64,
            QUOTA_BOTS => self.sources.bots.count(owner) as u64,
            QUOTA_SCHEDULED_RUNS_PER_DAY => {
                // TODO: expand cron schedules when computing the daily quota.
                self.sources
                    .bots
                    .all(owner)
                    .iter()
                    .filter(|bot| bot.state == BotState::Enabled)
                    .map(|bot| bot.schedule.runs_per_day())
                    .sum()
            }
            QUOTA_CONCURRENT_BACKTESTS => self.sources.backtests.count_active(owner) as u64,
            QUOTA_STREAM_CONNECTIONS => self.sources.streams.connection_count(owner),
            _ => 0,
        }
    }
}
